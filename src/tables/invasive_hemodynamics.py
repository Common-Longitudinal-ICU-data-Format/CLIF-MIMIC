# src/tables/invasive_hemodynamics.py
import pandas as pd
import duckdb
from hamilton.function_modifiers import tag, datasaver
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.invasive_hemodynamics')
import pandera.pandas as pa
from pandera.dtypes import Float32
import json

from src.utils import (
    load_mapping_csv,
    fetch_mimic_events,
    get_relevant_item_ids,
    save_to_rclif,
    convert_tz_to_utc,
    EXCLUDED_LABELS_DEFAULT,
)

MEASUREMENT_CATEGORIES = [
    "cvp",
    "pa_systolic",
    "pa_diastolic",
    "pa_mean",
    "cco",
    "cardiac_output_thermodilution",
    "pcwp",
    "gef",
]

# Reference mapping for future use: infer device_category from measurement type.
# CVP is the only measurement obtainable from a central venous catheter alone;
# all other measurements require a pulmonary artery catheter.
# NOTE: not applied in the current version — device columns are left NULL.
# MEASUREMENT_TO_DEVICE_CATEGORY = {
#     "cvp": "central_venous_catheter",
#     "pa_systolic": "pulmonary_artery_catheter",
#     "pa_diastolic": "pulmonary_artery_catheter",
#     "pa_mean": "pulmonary_artery_catheter",
#     "cco": "pulmonary_artery_catheter",
#     "cardiac_output_thermodilution": "pulmonary_artery_catheter",
#     "pcwp": "pulmonary_artery_catheter",
#     "gef": "pulmonary_artery_catheter",
# }

CLIF_INVASIVE_HEMO_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "device_name": pa.Column(str, nullable=True),
        "device_category": pa.Column(str, nullable=True),
        "measurement_name": pa.Column(str, nullable=True),
        "measurement_category": pa.Column(
            str,
            checks=[pa.Check.isin(MEASUREMENT_CATEGORIES)],
            nullable=False,
        ),
        "hemodynamic_value": pa.Column(Float32, nullable=True),
    },
    strict=True,
)


def invasive_hemo_mapping() -> pd.DataFrame:
    logger.info("loading invasive_hemo mapping...")
    df = load_mapping_csv("invasive_hemo")
    return df[~df["measure_category"].isin(EXCLUDED_LABELS_DEFAULT)].reset_index(drop=True)


def itemid_to_measurement_name_mapper(invasive_hemo_mapping: pd.DataFrame) -> dict:
    return dict(zip(invasive_hemo_mapping["itemid"], invasive_hemo_mapping["label"]))


def itemid_to_measurement_category_mapper(invasive_hemo_mapping: pd.DataFrame) -> dict:
    return dict(zip(invasive_hemo_mapping["itemid"], invasive_hemo_mapping["measure_category"]))


def invasive_hemo_events(invasive_hemo_mapping: pd.DataFrame) -> pd.DataFrame:
    logger.info("fetching invasive hemodynamic events from chartevents...")
    item_ids = get_relevant_item_ids(
        mapping_df=invasive_hemo_mapping,
        decision_col="measure_category",
        excluded_labels=EXCLUDED_LABELS_DEFAULT,
    ).tolist()
    return fetch_mimic_events(item_ids)


@tag(property="final")
def clif_invasive_hemo(
    invasive_hemo_events: pd.DataFrame,
    itemid_to_measurement_name_mapper: dict,
    itemid_to_measurement_category_mapper: dict,
) -> pd.DataFrame:
    logger.info("building and cleaning clif_invasive_hemo table...")
    df = invasive_hemo_events.copy()
    df["measurement_name"] = df["itemid"].map(itemid_to_measurement_name_mapper)
    df["measurement_category"] = df["itemid"].map(itemid_to_measurement_category_mapper)

    result = duckdb.sql("""
        FROM df
        SELECT
            hospitalization_id: CAST(hadm_id AS VARCHAR)
            , recorded_dttm: CAST(time AS TIMESTAMP)
            , device_name: CAST(NULL AS VARCHAR)
            , device_category: CAST(NULL AS VARCHAR)
            , measurement_name
            , measurement_category
            , hemodynamic_value: CAST(valuenum AS FLOAT)
        WHERE hadm_id IS NOT NULL
            AND valuenum IS NOT NULL
        ORDER BY hospitalization_id, recorded_dttm
    """).df()

    logger.info("converting timestamps to UTC...")
    result["recorded_dttm"] = convert_tz_to_utc(pd.to_datetime(result["recorded_dttm"]))
    return result


@tag(property="test")
def schema_tested(clif_invasive_hemo: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logger.info("testing schema...")
    try:
        CLIF_INVASIVE_HEMO_SCHEMA.validate(clif_invasive_hemo, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logger.error(json.dumps(exc.message, indent=2))
        logger.error("Schema errors and failure cases:")
        logger.error(exc.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(exc.data)
        return exc


@datasaver()
def save(clif_invasive_hemo: pd.DataFrame) -> dict:
    logger.info("saving to rclif...")
    save_to_rclif(clif_invasive_hemo, "invasive_hemodynamics")

    metadata = {"table_name": "invasive_hemodynamics"}

    logger.info("output saved to a parquet file, everything completed for the invasive_hemodynamics table!")
    return metadata


def _main():
    logger.info("starting to build clif invasive_hemodynamics table -- ")
    from hamilton import driver
    import src.tables.invasive_hemodynamics as invasive_hemodynamics
    dr = (
        driver.Builder()
        .with_modules(invasive_hemodynamics)
        .build()
    )
    dr.execute(["save"])


def _test():
    logger.info("testing all...")
    from hamilton import driver
    import src.tables.invasive_hemodynamics as invasive_hemodynamics
    dr = (
        driver.Builder()
        .with_modules(invasive_hemodynamics)
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
