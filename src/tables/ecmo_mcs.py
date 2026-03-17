# src/tables/ecmo_mcs.py
import pandas as pd
import duckdb
from hamilton.function_modifiers import tag, datasaver
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.ecmo_mcs')
import pandera.pandas as pa
from pandera.dtypes import Float32
import json

from src.utils import (
    load_mapping_csv,
    save_to_rclif,
    convert_tz_to_utc,
    mimic_table_pathfinder,
)

CLIF_ECMO_MCS_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "_device_context": pa.Column(str, nullable=True),
        "device_name": pa.Column(str, nullable=True),
        "device_category": pa.Column(str, nullable=True),
        "mcs_group": pa.Column(str, nullable=True),
        "ecmo_configuration_category": pa.Column(str, nullable=True),
        "control_parameter_name": pa.Column(str, nullable=True),
        "control_parameter_category": pa.Column(str, nullable=True),
        "control_parameter_value": pa.Column(Float32, nullable=True),
        "flow": pa.Column(Float32, nullable=True),
        "sweep_set": pa.Column(Float32, nullable=True),
        "fdo2_set": pa.Column(Float32, nullable=True),
    },
    strict=True,
)


def ecmo_mapping() -> pd.DataFrame:
    logger.info("loading ecmo_mcs mapping...")
    return load_mapping_csv("ecmo_mcs")


def ecmo_mapping_filtered(ecmo_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    """Filter to items with a variable mapping. device_context is provided
    directly in the mapping CSV (normalizes MIMIC categories so device
    identification and measurement items share the same grouping key)."""
    return duckdb.sql("""
        FROM ecmo_mapping
        SELECT itemid, variable, label, category, device_context
        WHERE variable IS NOT NULL AND TRIM(variable) != ''
    """)


def device_lookup() -> duckdb.DuckDBPyRelation:
    """Lookup table mapping (itemid, charted_value) -> standardized CLIF categories.
    Only device-identification items (e.g., Circuit Configuration, Type of Catheter)
    appear here. The CSV stores 'NA' for non-ECMO devices; convert to SQL NULL."""
    return duckdb.sql("""
        FROM 'data/mappings/mimic-to-clif-mappings - ecmo_mcs_device.csv'
        SELECT itemid, label, value, device_category, mcs_group
            , ecmo_config: NULLIF(ecmo_configuration_category, 'NA')
    """)


def all_events(ecmo_mapping_filtered: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Fetch every chartevent row whose itemid is in the mapping.
    INNER JOIN filters to only mapped items; device_context comes along
    so downstream nodes can group by physical device."""
    _chartevents_path = mimic_table_pathfinder("chartevents")
    return duckdb.sql(f"""
        FROM '{_chartevents_path}' ce
        INNER JOIN ecmo_mapping_filtered m ON ce.itemid = m.itemid
        SELECT
            ce.hadm_id, ce.charttime, ce.itemid, ce.value, ce.valuenum
            , m.variable, m.label, m.device_context
        WHERE ce.hadm_id IS NOT NULL
    """)


def device_events(
    all_events: duckdb.DuckDBPyRelation,
    device_lookup: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Items where variable = 'device_name' identify WHICH device is in use
    (e.g., Circuit Configuration -> "VV", Type of Catheter -> "5.5").
    LEFT JOIN with device_lookup translates the charted text value into
    standardized device_category, mcs_group, and ecmo_configuration_category."""
    return duckdb.sql("""
        FROM all_events ae
        LEFT JOIN device_lookup dl ON ae.itemid = dl.itemid AND TRIM(ae.value) = TRIM(dl.value)
        SELECT
            ae.hadm_id, ae.charttime, ae.device_context
            , device_name: dl.label || ' = ' || ae.value
            , device_category: dl.device_category
            , mcs_group: dl.mcs_group
            , ecmo_configuration_category: dl.ecmo_config
        WHERE ae.variable = 'device_name'
    """)


def measurement_events(all_events: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Pivot all non-device items into wide format: one row per
    (hadm_id, charttime, device_context) with columns for each measurement.
    MAX() is safe because at most one item per variable fires per group."""
    return duckdb.sql(r"""
        FROM all_events ae
        SELECT
            ae.hadm_id, ae.charttime, ae.device_context
            -- Blood flow in L/min (numeric)
            , flow: MAX(CASE WHEN ae.variable = 'flow' THEN ae.valuenum END)
            -- Gas sweep rate: stored as text in MIMIC, extract numeric portion
            , sweep_set: MAX(CASE WHEN ae.variable = 'sweep_set'
                THEN TRY_CAST(REGEXP_EXTRACT(ae.value, '[\d\.]+') AS FLOAT) END)
            -- FiO2: MIMIC stores as % (0-100), divide by 100 -> fraction (0-1)
            , fdo2_set: MAX(CASE WHEN ae.variable = 'fdo2_set' THEN ae.valuenum / 100.0 END)
            -- Control parameters: the mapping CSV encodes category after the colon
            -- e.g., "control_parameter_category:rpm" -> category = "rpm"
            , control_parameter_name: MAX(CASE WHEN ae.variable LIKE 'control_parameter_category:%' THEN ae.label END)
            , control_parameter_category: MAX(CASE WHEN ae.variable LIKE 'control_parameter_category:%'
                THEN SPLIT_PART(ae.variable, ':', 2) END)
            -- RPM items are numeric; Impella power is text (P0-P9) -> extract digit
            , control_parameter_value: MAX(CASE
                WHEN ae.variable = 'control_parameter_category:rpm' THEN ae.valuenum
                WHEN ae.variable = 'control_parameter_category:impella_power'
                    THEN TRY_CAST(REGEXP_EXTRACT(ae.value, '\d+') AS FLOAT)
                END)
        WHERE ae.variable != 'device_name'
        GROUP BY 1, 2, 3
    """)


def clif_ecmo_mcs_raw(
    device_events: duckdb.DuckDBPyRelation,
    measurement_events: duckdb.DuckDBPyRelation,
) -> pd.DataFrame:
    """FULL OUTER JOIN: keeps measurement rows without a device event (common for
    Centrimag, legacy Hemodynamics items) and device events without measurements.
    Fallback CASE WHENs provide default device_category/mcs_group inferred from
    device_context when no device-identification event exists at that timestamp."""
    logger.info("joining device and measurement events...")
    return duckdb.sql("""
        FROM measurement_events m
        FULL OUTER JOIN device_events d
            ON m.hadm_id = d.hadm_id
            AND m.charttime = d.charttime
            AND m.device_context = d.device_context
        SELECT
            hospitalization_id: CAST(COALESCE(m.hadm_id, d.hadm_id) AS VARCHAR)
            , recorded_dttm: CAST(COALESCE(m.charttime, d.charttime) AS TIMESTAMP)
            , _device_context: COALESCE(m.device_context, d.device_context)
            , d.device_name
            -- Fallback: when no device event matched, infer device_category from context
            , device_category: COALESCE(d.device_category, CASE
                WHEN COALESCE(m.device_context, d.device_context) in ('ecmo_ecmo', 'ecmo_ch') THEN 'ecmo_other_unspec'
                WHEN COALESCE(m.device_context, d.device_context) = 'hm2' THEN 'heartmate_2'
                WHEN COALESCE(m.device_context, d.device_context) = 'rvad' THEN 'rvad_other_unspec'
                WHEN COALESCE(m.device_context, d.device_context) = 'centrimag_lv' THEN 'centrimag_lv'
                WHEN COALESCE(m.device_context, d.device_context) = 'centrimag_rv' THEN 'centrimag_rv'
                WHEN COALESCE(m.device_context, d.device_context) = 'heartware' THEN 'heartware'
                WHEN COALESCE(m.device_context, d.device_context) = 'impella_r' THEN 'impella_rp'
                END)
            -- Fallback: infer mcs_group from context (e.g., centrimag_lv -> temporary_lvad)
            , mcs_group: COALESCE(d.mcs_group, CASE
                WHEN COALESCE(m.device_context, d.device_context) in ('ecmo_ecmo', 'ecmo_ch') THEN 'ecmo'
                WHEN COALESCE(m.device_context, d.device_context) IN ('hm2', 'durable_vad', 'heartware') THEN 'durable_lvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'rvad' THEN 'temporary_rvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'centrimag_lv' THEN 'temporary_lvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'centrimag_rv' THEN 'temporary_rvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'impella_l' THEN 'impella_lvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'impella_r' THEN 'temporary_rvad'
                END)
            , d.ecmo_configuration_category
            , m.control_parameter_name
            , m.control_parameter_category
            , m.control_parameter_value
            , m.flow
            , m.sweep_set
            , m.fdo2_set
        ORDER BY hospitalization_id, recorded_dttm
    """).df()


@tag(property="final")
def clif_ecmo_mcs(clif_ecmo_mcs_raw: pd.DataFrame) -> pd.DataFrame:
    """Convert MIMIC US/Eastern timestamps to UTC."""
    logger.info("converting timestamps to UTC...")
    df = clif_ecmo_mcs_raw.copy()
    df["recorded_dttm"] = convert_tz_to_utc(pd.to_datetime(df["recorded_dttm"]))
    return df


@tag(property="test")
def schema_tested(clif_ecmo_mcs: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logger.info("testing schema...")
    try:
        CLIF_ECMO_MCS_SCHEMA.validate(clif_ecmo_mcs, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logger.error(json.dumps(exc.message, indent=2))
        logger.error("Schema errors and failure cases:")
        logger.error(exc.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(exc.data)
        return exc


@datasaver()
def save(clif_ecmo_mcs: pd.DataFrame) -> dict:
    logger.info("saving to rclif...")
    save_to_rclif(clif_ecmo_mcs, "ecmo_mcs")

    metadata = {
        "table_name": "ecmo_mcs"
    }

    logger.info("output saved to a parquet file, everything completed for the ecmo_mcs table!")
    return metadata


def _main():
    logger.info("starting to build clif ecmo_mcs table -- ")
    from hamilton import driver
    import src.tables.ecmo_mcs as ecmo_mcs
    dr = (
        driver.Builder()
        .with_modules(ecmo_mcs)
        .build()
    )
    dr.execute(["save"])


def _test():
    logger.info("testing all...")
    from hamilton import driver
    import src.tables.ecmo_mcs as ecmo_mcs
    dr = (
        driver.Builder()
        .with_modules(ecmo_mcs)
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
