# src/tables/input.py
import pandas as pd
import duckdb
from hamilton.function_modifiers import tag, datasaver
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.input')
import pandera.pandas as pa
from pandera.dtypes import Float32
import json

from src.utils import (
    load_mapping_csv,
    save_to_rclif,
    convert_tz_to_utc,
    mimic_table_pathfinder,
)

VALID_INPUT_CATEGORIES = pd.read_csv(
    "data/mcide/clif_input_category.csv"
)["input_category"].tolist()

VALID_INPUT_GROUPS = pd.read_csv(
    "data/mcide/clif_input_category.csv"
)["input_group"].unique().tolist()

CLIF_INPUT_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "input_name": pa.Column(str, nullable=False),
        "input_category": pa.Column(
            str, checks=[pa.Check.isin(VALID_INPUT_CATEGORIES)], nullable=False
        ),
        "input_group": pa.Column(
            str, checks=[pa.Check.isin(VALID_INPUT_GROUPS)], nullable=False
        ),
        "input_volume": pa.Column(Float32, nullable=False),
    },
    strict=True,
)

INPUT_COLUMNS = list(CLIF_INPUT_SCHEMA.columns.keys())


def input_mapping() -> pd.DataFrame:
    logger.info("loading input mapping...")
    return load_mapping_csv("input")


def input_mapping_to_map(input_mapping: pd.DataFrame) -> pd.DataFrame:
    return input_mapping[input_mapping["decision"] == "TO MAP"]


@tag(property="final")
def clif_input(input_mapping_to_map: pd.DataFrame) -> pd.DataFrame:
    """
    Transform MIMIC outputevents to CLIF input schema.
    NOTE: Item 227488 (GU Irrigant Volume In) lives in MIMIC outputevents
    but is conceptually an input (fluid into bladder for irrigation).
    """
    logger.info("transforming MIMIC outputevents to CLIF input schema...")
    item_ids_str = ','.join(map(str, input_mapping_to_map["itemid"].tolist()))
    query = f"""
    FROM '{mimic_table_pathfinder("outputevents")}' oe
    INNER JOIN input_mapping_to_map m ON oe.itemid = m.itemid
    SELECT
        CAST(oe.hadm_id AS VARCHAR) AS hospitalization_id,
        CAST(oe.charttime AS TIMESTAMP) AS recorded_dttm,
        m.label AS input_name,
        m.input_category,
        m.input_group,
        CAST(oe.value AS FLOAT) AS input_volume
    WHERE oe.itemid IN ({item_ids_str})
        AND oe.hadm_id IS NOT NULL
        AND oe.value IS NOT NULL
    """
    df = duckdb.query(query).df()
    logger.info(f"fetched {len(df)} input events, converting timezone to UTC...")
    df["recorded_dttm"] = convert_tz_to_utc(pd.to_datetime(df["recorded_dttm"]))
    return df


@tag(property="test")
def schema_tested(clif_input: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logger.info("testing schema...")
    try:
        CLIF_INPUT_SCHEMA.validate(clif_input, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logger.error(json.dumps(exc.message, indent=2))
        logger.error("Schema errors and failure cases:")
        logger.error(exc.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(exc.data)
        return exc


@datasaver()
def save(clif_input: pd.DataFrame) -> dict:
    logger.info("saving to rclif...")
    save_to_rclif(clif_input, "input")

    metadata = {
        "table_name": "input"
    }

    logger.info("output saved to a parquet file, everything completed for the input table!")
    return metadata


def _main():
    logger.info("starting to build clif input table -- ")
    from hamilton import driver
    import src.tables.input as input_table
    dr = (
        driver.Builder()
        .with_modules(input_table)
        .build()
    )
    dr.execute(["save"])


def _test():
    logger.info("testing all...")
    from hamilton import driver
    import src.tables.input as input_table
    dr = (
        driver.Builder()
        .with_modules(input_table)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    logger.debug(f"Test output: {output}")
    return output


if __name__ == "__main__":
    setup_logging()
    _main()
