# src/tables/mcs.py
import pandas as pd
import duckdb
from hamilton.function_modifiers import tag, datasaver
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.mcs')
import pandera.pandas as pa
from pandera.dtypes import Float32
import json

from src.utils import (
    load_mapping_csv,
    save_to_rclif,
    convert_tz_to_utc,
    mimic_table_pathfinder,
)

# CLIF 3.0.0 `mcs` table (renamed from `ecmo_mcs`): long format, one row per
# recorded setting/measurement, annotated with the support/device/config identity.
CLIF_MCS_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        # non-CLIF-standard QA aid (leading underscore): the physical-device
        # grouping key, preserved for auditing device linkage & the fallback.
        "_device_context": pa.Column(str, nullable=True),
        "support_name": pa.Column(str, nullable=True),
        "support_category": pa.Column(str, nullable=True),
        "device_name": pa.Column(str, nullable=True),
        "device_category": pa.Column(str, nullable=True),
        "config_name": pa.Column(str, nullable=True),
        "config_category": pa.Column(str, nullable=True),
        "setting_name": pa.Column(str, nullable=True),
        "setting_category": pa.Column(str, nullable=True),
        "setting_value": pa.Column(Float32, nullable=True),
    },
    strict=True,
)


def mcs_mapping() -> pd.DataFrame:
    logger.info("loading mcs mapping...")
    return load_mapping_csv("mcs")


def mcs_mapping_filtered(mcs_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    """Filter to items with a variable mapping. `variable` is either
    'device_name' (device-identification items) or 'setting_category:<x>'
    (measurement items). `device_context` is provided directly in the mapping
    CSV and normalizes MIMIC categories so device-identification and measurement
    items share the same physical-device grouping key."""
    return duckdb.sql("""
        FROM mcs_mapping
        SELECT itemid, variable, label, category, device_context
        WHERE variable IS NOT NULL AND TRIM(variable) != ''
    """)


def device_mapping() -> pd.DataFrame:
    logger.info("loading mcs_device mapping...")
    return load_mapping_csv("mcs_device")


def device_lookup(device_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    """Lookup table mapping (itemid, charted_value) -> standardized CLIF
    categories. Only device-identification items (Circuit Configuration,
    Device (VAD), Type of Catheter, Type (Tandem Heart)) appear here. Values
    are kept literal, including the 'UNSPECIFIED' catch-all defined by the
    mapping author (not a strict mCIDE value -- see the table docs)."""
    return duckdb.sql("""
        FROM device_mapping
        SELECT itemid, value, device_context
            -- 'UNSPECIFIED' is a mapping-file placeholder for "considered -> no value";
            -- map it to NULL in the output (a blank cell would instead mean "undecided").
            , support_category: NULLIF(support_category, 'UNSPECIFIED')
            , device_category: NULLIF(device_category, 'UNSPECIFIED')
            , config_category: NULLIF(config_category, 'UNSPECIFIED')
    """)


def device_context_mapping() -> pd.DataFrame:
    logger.info("loading mcs_device_context mapping...")
    return load_mapping_csv("mcs_device_context")


def device_context_lookup(device_context_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    """Per-device_context default identity for settings with no co-charted device
    event (settings-only devices like Centrimag / HeartWare / HM II, or timestamps
    where no device-ID item fired). Externalized from the old inline CASE blocks."""
    return duckdb.sql("""
        FROM device_context_mapping
        SELECT device_context
            -- 'UNSPECIFIED' placeholder -> NULL (see device_lookup)
            , support_category: NULLIF(support_category, 'UNSPECIFIED')
            , device_category: NULLIF(device_category, 'UNSPECIFIED')
            , config_category: NULLIF(config_category, 'UNSPECIFIED')
    """)


def all_events(mcs_mapping_filtered: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Fetch every chartevent row whose itemid is in the mapping.
    INNER JOIN filters to only mapped items; device_context comes along
    so downstream nodes can group by physical device."""
    _chartevents_path = mimic_table_pathfinder("chartevents")
    return duckdb.sql(f"""
        FROM '{_chartevents_path}' ce
        INNER JOIN mcs_mapping_filtered m ON ce.itemid = m.itemid
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
    (e.g., Circuit Configuration -> "VV", Type of Catheter -> "RP"). LEFT JOIN
    with device_lookup translates the charted value into standardized
    support/device/config categories AND a value-aware `device_context`: the
    charted value can re-route the event to the finest setting-side context it
    supports (e.g. Impella 'RP' -> impella_r so it annotates the right-sided
    "(R)" settings). Group by that effective context and aggregate conflict-safely:
    if every row in the group agrees on a field we keep it, otherwise we emit NULL
    (never an invented / arbitrary pick), so genuine biventricular support -- e.g.
    concurrent Tandem LA + RA -- surfaces as NULL on the ambiguous *category* fields
    while unambiguous ones (device=tandemheart) survive. The `*_name` columns instead
    keep the full raw source (item label + charted value, all concurrent devices).
    'UNSPECIFIED' category placeholders are mapped to NULL upstream in the lookups."""
    return duckdb.sql("""
        FROM all_events ae
        LEFT JOIN device_lookup dl ON ae.itemid = dl.itemid AND TRIM(ae.value) = TRIM(dl.value)
        SELECT
            ae.hadm_id, ae.charttime
            -- value-aware effective context; falls back to the nominal itemid
            -- context when the charted value isn't in the device lookup
            , device_context: COALESCE(dl.device_context, ae.device_context)
            -- *_name: keep the full raw source (item label + charted value); STRING_AGG
            -- retains ALL concurrent device strings so BiVAD stays visible in the name
            -- even where the category below is NULLed for ambiguity.
            , support_name: STRING_AGG(DISTINCT ae.label || ' = ' || ae.value, ' | ' ORDER BY ae.label || ' = ' || ae.value)
            , device_name: STRING_AGG(DISTINCT ae.label || ' = ' || ae.value, ' | ' ORDER BY ae.label || ' = ' || ae.value)
            , config_name: STRING_AGG(DISTINCT ae.label || ' = ' || ae.value, ' | ' ORDER BY ae.label || ' = ' || ae.value)
            -- categories: conflict-safe aggregation -> agree -> value, disagree -> NULL
            , support_category: CASE WHEN COUNT(DISTINCT dl.support_category) = 1 THEN MAX(dl.support_category) END
            , device_category: CASE WHEN COUNT(DISTINCT dl.device_category) = 1 THEN MAX(dl.device_category) END
            , config_category: CASE WHEN COUNT(DISTINCT dl.config_category) = 1 THEN MAX(dl.config_category) END
        WHERE ae.variable = 'device_name'
        GROUP BY 1, 2, 3
    """)


def setting_events(all_events: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Long format: one row per charted setting/measurement. setting_category is
    encoded after the colon in the mapping `variable` (e.g. 'setting_category:rpm'
    -> 'rpm'). setting_value is normalized to a FLOAT (float32) per category."""
    return duckdb.sql(r"""
        FROM all_events ae
        SELECT
            ae.hadm_id, ae.charttime, ae.device_context
            , setting_name: ae.label
            , setting_category: SPLIT_PART(ae.variable, ':', 2)
            , setting_value: CAST(CASE
                -- FiO2: MIMIC stores as % (0-100), divide by 100 -> fraction (0-1)
                WHEN setting_category = 'fdo2' THEN ae.valuenum / 100.0
                -- Sweep: charted as text, extract the numeric portion
                WHEN setting_category = 'sweep'
                    THEN TRY_CAST(REGEXP_EXTRACT(ae.value, '[\d\.]+') AS FLOAT)
                -- Impella performance 'PX' level: text -> extract the digit
                WHEN setting_category = 'performance'
                    THEN TRY_CAST(REGEXP_EXTRACT(ae.value, '\d+') AS FLOAT)
                -- flow, rpm, power, pulsatility_index, delta_pressure: numeric as-is
                ELSE ae.valuenum
                END AS FLOAT)
        WHERE ae.variable LIKE 'setting_category:%'
    """)


def clif_mcs_pre_utc(
    setting_events: duckdb.DuckDBPyRelation,
    device_events: duckdb.DuckDBPyRelation,
    device_context_lookup: duckdb.DuckDBPyRelation,
) -> pd.DataFrame:
    """LEFT JOIN setting_events -> device_events on the exact (hadm_id, charttime,
    device_context): a setting inherits the co-charted device identity (with the
    value-aware routing already applied in device_events). LEFT JOIN
    device_context_lookup supplies the per-context default for settings with no
    co-charted device event (settings-only devices, or gaps). support/device/config
    = COALESCE(event, default) -- after routing, the two agree or the event is a
    deliberate NULL, so no precedence flip is needed. Only real settings/measurements
    are emitted (no device-only rows), matching 'each row = a setting or measurement'."""
    logger.info("joining setting and device events...")
    return duckdb.sql("""
        FROM setting_events s
        LEFT JOIN device_events d
            ON s.hadm_id = d.hadm_id
            AND s.charttime = d.charttime
            AND s.device_context = d.device_context
        LEFT JOIN device_context_lookup ctx ON s.device_context = ctx.device_context
        SELECT
            hospitalization_id: CAST(s.hadm_id AS VARCHAR)
            , recorded_dttm: CAST(s.charttime AS TIMESTAMP)
            -- non-CLIF-standard QA aid: the physical-device grouping key
            , _device_context: s.device_context
            , d.support_name
            , support_category: COALESCE(d.support_category, ctx.support_category)
            , d.device_name
            , device_category: COALESCE(d.device_category, ctx.device_category)
            , d.config_name
            , config_category: COALESCE(d.config_category, ctx.config_category)
            , s.setting_name
            , s.setting_category
            , s.setting_value
        ORDER BY hospitalization_id, recorded_dttm
    """).df()


@tag(property="final")
def clif_mcs(clif_mcs_pre_utc: pd.DataFrame) -> pd.DataFrame:
    """Convert MIMIC US/Eastern timestamps to UTC."""
    logger.info("converting timestamps to UTC...")
    df = clif_mcs_pre_utc.copy()
    df["recorded_dttm"] = convert_tz_to_utc(pd.to_datetime(df["recorded_dttm"]))
    return df


@tag(property="test")
def schema_tested(clif_mcs: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logger.info("testing schema...")
    try:
        CLIF_MCS_SCHEMA.validate(clif_mcs, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logger.error(json.dumps(exc.message, indent=2))
        logger.error("Schema errors and failure cases:")
        logger.error(exc.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(exc.data)
        return exc


@datasaver()
def save(clif_mcs: pd.DataFrame) -> dict:
    logger.info("saving to rclif...")
    save_to_rclif(clif_mcs, "mcs")

    metadata = {
        "table_name": "mcs"
    }

    logger.info("output saved to a parquet file, everything completed for the mcs table!")
    return metadata


def _main():
    logger.info("starting to build clif mcs table -- ")
    from hamilton import driver
    import src.tables.mcs as mcs
    dr = (
        driver.Builder()
        .with_modules(mcs)
        .build()
    )
    dr.execute(["save"])


def _test():
    logger.info("testing all...")
    from hamilton import driver
    import src.tables.mcs as mcs
    dr = (
        driver.Builder()
        .with_modules(mcs)
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
