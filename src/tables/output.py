# src/tables/output.py
import pandas as pd
import duckdb
from hamilton.function_modifiers import tag, datasaver
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.output')
import pandera.pandas as pa
from pandera.dtypes import Float32
import json

from src.utils import (
    load_mapping_csv,
    save_to_rclif,
    convert_tz_to_utc,
    mimic_table_pathfinder,
)

VALID_OUTPUT_CATEGORIES = pd.read_csv(
    "data/mcide/clif_output_categories.csv"
)["output_category"].tolist()

VALID_OUTPUT_GROUPS = pd.read_csv(
    "data/mcide/clif_output_categories.csv"
)["output_group"].unique().tolist()

CLIF_OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "output_name": pa.Column(str, nullable=False),
        "output_category": pa.Column(
            str, checks=[pa.Check.isin(VALID_OUTPUT_CATEGORIES)], nullable=False
        ),
        "output_group": pa.Column(
            str, checks=[pa.Check.isin(VALID_OUTPUT_GROUPS)], nullable=False
        ),
        "output_volume": pa.Column(Float32, nullable=False),
    },
    strict=True,
)

OUTPUT_COLUMNS = list(CLIF_OUTPUT_SCHEMA.columns.keys())


def output_mapping() -> pd.DataFrame:
    logger.info("loading output mapping...")
    return load_mapping_csv("output")


def output_mapping_to_map(output_mapping: pd.DataFrame) -> pd.DataFrame:
    return output_mapping[output_mapping["decision"] == "TO MAP"]


@tag(property="final")
def clif_output(output_mapping_to_map: pd.DataFrame) -> pd.DataFrame:
    """
    Transform MIMIC outputevents to CLIF output schema.
    Currently scoped to urine output items.
    """
    logger.info("transforming MIMIC outputevents to CLIF output schema...")
    item_ids_str = ','.join(map(str, output_mapping_to_map["itemid"].tolist()))
    query = f"""
    FROM '{mimic_table_pathfinder("outputevents")}' oe
    INNER JOIN output_mapping_to_map m ON oe.itemid = m.itemid
    SELECT
        CAST(oe.hadm_id AS VARCHAR) AS hospitalization_id,
        CAST(oe.charttime AS TIMESTAMP) AS recorded_dttm,
        m.label AS output_name,
        m.output_category,
        m.output_group,
        CAST(oe.value AS FLOAT) AS output_volume
    WHERE oe.itemid IN ({item_ids_str})
        AND oe.hadm_id IS NOT NULL
        AND oe.value IS NOT NULL
    """
    df = duckdb.query(query).df()
    logger.info(f"fetched {len(df)} output events, converting timezone to UTC...")
    df["recorded_dttm"] = convert_tz_to_utc(pd.to_datetime(df["recorded_dttm"]))
    return df


@tag(property="test")
def schema_tested(clif_output: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logger.info("testing schema...")
    try:
        CLIF_OUTPUT_SCHEMA.validate(clif_output, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logger.error(json.dumps(exc.message, indent=2))
        logger.error("Schema errors and failure cases:")
        logger.error(exc.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(exc.data)
        return exc


@datasaver()
def save(clif_output: pd.DataFrame) -> dict:
    logger.info("saving to rclif...")
    save_to_rclif(clif_output, "output")

    metadata = {
        "table_name": "output"
    }

    logger.info("output saved to a parquet file, everything completed for the output table!")
    return metadata


def _main():
    logger.info("starting to build clif output table -- ")
    from hamilton import driver
    import src.tables.output as output
    dr = (
        driver.Builder()
        .with_modules(output)
        .build()
    )
    dr.execute(["save"])


def _test():
    logger.info("testing all...")
    from hamilton import driver
    import src.tables.output as output
    dr = (
        driver.Builder()
        .with_modules(output)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output_result = dr.execute(test_nodes)
    logger.debug(f"Test output: {output_result}")
    return output_result


if __name__ == "__main__":
    setup_logging()
    _main()
