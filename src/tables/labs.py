# src/tables/labs.py
import numpy as np
import pandas as pd
import logging
import re
import duckdb
from hamilton.function_modifiers import tag, datasaver, cache
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.labs')
import pandera.pandas as pa
from typing import List
import json

from src.utils import (
    fetch_mimic_events,
    load_mapping_csv,
    save_to_rclif,
    convert_tz_to_utc,
)

from src.utils_qa import all_null_check

def _permitted_lab_categories() -> List[str]:
    clif_labs_mcide = pd.read_csv("data/mcide/clif_lab_categories.csv")
    return clif_labs_mcide["lab_category"].unique()

CLIF_LABS_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "lab_order_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "lab_collect_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "lab_result_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "lab_order_name": pa.Column(str, checks=[all_null_check], nullable=True),
        "lab_order_category": pa.Column(str, nullable=False),
        "lab_name": pa.Column(str, nullable=False),
        "lab_category": pa.Column(str, checks=[pa.Check.isin(_permitted_lab_categories())], nullable=False),
        "lab_value": pa.Column(str, nullable=True),
        "lab_value_numeric": pa.Column(float, nullable=True),
        "reference_unit": pa.Column(str, nullable=True),
        "lab_specimen_name": pa.Column(str, checks=[all_null_check], nullable=True),
        "lab_specimen_category": pa.Column(str, checks=[all_null_check], nullable=True),
        "lab_loinc_code": pa.Column(str, checks=[all_null_check], nullable=True),
    },
    strict=True,
)

LABS_COLUMNS: List[str] = list(CLIF_LABS_SCHEMA.columns.keys())


# =============================================================================
# Core Mapping Functions
# =============================================================================

def labs_mapping() -> pd.DataFrame:
    logger.info("loading labs mapping...")
    labs_mapping = load_mapping_csv("labs")
    # drop the row corresponding to procalcitonin which is not available in MIMIC
    labs_mapping.dropna(subset=["itemid"], inplace=True)
    labs_mapping["itemid"] = labs_mapping["itemid"].astype(int)
    return labs_mapping


def labs_items(labs_mapping: pd.DataFrame) -> pd.DataFrame:
    logger.info("filtering labs items...")
    return labs_mapping.loc[
        labs_mapping["decision"].isin(["TO MAP, CONVERT UOM", "TO MAP, AS IS", "UNSURE"]),
        ["lab_category", "itemid", "label", "count"]
    ].copy()


def lab_order_category_mapping() -> pd.DataFrame:
    """Load lab order category mapping for SQL JOIN."""
    return pd.read_csv("data/mcide/clif_labs_order_categories.csv")


# =============================================================================
# Extraction Functions (Cached)
# =============================================================================

@cache(behavior="default", format="parquet")
def le_labs_extracted(labs_items: pd.DataFrame) -> pd.DataFrame:
    """Extract labs from labevents table (5-digit itemids)."""
    logger.info("identifying lab items to be extracted from labevents table...")
    labs_items_le = labs_items[labs_items['itemid'].astype("string").str.len() == 5]
    logger.info("extracting from labevents table...")
    df_le = fetch_mimic_events(labs_items_le['itemid'], original=True, for_labs=True)
    return df_le


@cache(behavior="default", format="parquet")
def ce_labs_extracted(labs_items: pd.DataFrame) -> pd.DataFrame:
    """Extract labs from chartevents table (6-digit itemids)."""
    logger.info("identifying lab items to be extracted from chartevents table...")
    labs_items_ce = labs_items[labs_items['itemid'].astype("string").str.len() == 6]
    logger.info("extracting from chartevents table...")
    df_ce = fetch_mimic_events(labs_items_ce['itemid'], original=True, for_labs=False)
    return df_ce


# =============================================================================
# Comment Parsing (Python)
# =============================================================================

def _parse_labs_comment(comment: str) -> float:
    '''
    Use regular expression to parse the comment and extract the numeric value.
    '''
    match = re.search(r'\d+\.\d+|\d+', comment)
    parsed_number = float(match.group()) if match else np.nan
    comment_lower = comment.lower()
    if "ptt" in comment_lower and "unable" in comment_lower:
        return parsed_number
    # if any part of the comment contains "not done" or "unable to report" (case insensitive), return NA
    if "not done" in comment_lower or "unable" in comment_lower:
        return np.nan
    return parsed_number


def le_labs_comments_parsed(le_labs_extracted: pd.DataFrame) -> pd.DataFrame:
    """Parse lab comments to recover missing numeric values."""
    logger.info("parsing lab comments to recover otherwise missing lab values...")
    df = le_labs_extracted.copy()
    mask = df["valuenum"].isna()
    df.loc[mask, "valuenum"] = df.loc[mask, "comments"].map(
        lambda x: _parse_labs_comment(x) if pd.notna(x) else np.nan
    )
    df.loc[mask, "value"] = df.loc[mask, "comments"]
    return df


# =============================================================================
# DuckDB Processing Functions
# =============================================================================

def le_labs_processed(le_labs_comments_parsed: pd.DataFrame, labs_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    """Map item IDs to names/categories and convert units via mapping."""
    logger.info("processing labevents: mapping names/categories and converting units...")
    q = """
    FROM le_labs_comments_parsed e
    LEFT JOIN labs_mapping m ON e.itemid = m.itemid
        AND m.decision IN ('TO MAP, CONVERT UOM', 'TO MAP, AS IS', 'UNSURE')
    SELECT hadm_id AS hospitalization_id
        , charttime AS lab_collect_dttm
        , storetime AS lab_result_dttm
        , m.label AS lab_name
        , m.lab_category
        , lab_value_numeric: CASE
            WHEN m.decision = 'TO MAP, CONVERT UOM'
                THEN e.valuenum * m.conversion_multiplier
            ELSE e.valuenum END
        , lab_value: CASE
            WHEN m.decision = 'TO MAP, CONVERT UOM'
                THEN (e.valuenum * m.conversion_multiplier)::VARCHAR
            ELSE COALESCE(e.value, e.comments) END
        , m.target_uom AS reference_unit
    WHERE m.lab_category IS NOT NULL
    """
    return duckdb.sql(q)


def ce_labs_processed(ce_labs_extracted: pd.DataFrame, labs_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    """Map item IDs to names/categories for chartevents."""
    logger.info("processing chartevents: mapping names/categories...")
    q = """
    FROM ce_labs_extracted e
    LEFT JOIN labs_mapping m ON e.itemid = m.itemid
        AND m.decision IN ('TO MAP, CONVERT UOM', 'TO MAP, AS IS', 'UNSURE')
    SELECT hadm_id AS hospitalization_id
        , charttime AS lab_collect_dttm
        , storetime AS lab_result_dttm
        , m.label AS lab_name
        , m.lab_category
        , lab_value_numeric: CASE
            WHEN m.decision = 'TO MAP, CONVERT UOM'
                THEN e.valuenum * m.conversion_multiplier
            ELSE e.valuenum END
        , lab_value: CASE
            WHEN m.decision = 'TO MAP, CONVERT UOM'
                THEN (e.valuenum * m.conversion_multiplier)::VARCHAR
            ELSE COALESCE(e.value::VARCHAR, '') END
        , m.target_uom AS reference_unit
    WHERE m.lab_category IS NOT NULL
    """
    return duckdb.sql(q)


# =============================================================================
# Merge Pipeline
# =============================================================================

def merged(le_labs_processed: duckdb.DuckDBPyRelation,
           ce_labs_processed: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Merge labevents and chartevents."""
    logger.info("merging labevents and chartevents...")
    q = """
    SELECT * FROM le_labs_processed
    UNION ALL
    SELECT * FROM ce_labs_processed
    """
    return duckdb.sql(q)


def deduped(merged: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Remove duplicates and null result_dttm."""
    logger.info("removing duplicates and null result_dttm...")
    q = """
    FROM merged
    SELECT DISTINCT ON (hospitalization_id, lab_collect_dttm, lab_result_dttm,
                        lab_category, lab_value_numeric) *
    WHERE lab_result_dttm IS NOT NULL
    """
    return duckdb.sql(q)


@tag(property="final")
def cast(deduped: duckdb.DuckDBPyRelation,
         lab_order_category_mapping: pd.DataFrame) -> pd.DataFrame:
    """Cast dtypes and add lab_order_category via SQL JOIN."""
    logger.info("casting dtypes and mapping lab_order_category...")
    q = """
    FROM deduped d
    LEFT JOIN lab_order_category_mapping m ON d.lab_category = m.lab_category
    SELECT hospitalization_id: CAST(d.hospitalization_id AS VARCHAR)
        , lab_order_dttm: d.lab_collect_dttm
        , lab_collect_dttm: CAST(d.lab_collect_dttm AS TIMESTAMP)
        , lab_result_dttm: CAST(d.lab_result_dttm AS TIMESTAMP)
        , lab_order_name: NULL::VARCHAR
        , lab_order_category: CAST(m.lab_order_category AS VARCHAR)
        , lab_name: CAST(d.lab_name AS VARCHAR)
        , lab_category: CAST(d.lab_category AS VARCHAR)
        , lab_value: CAST(d.lab_value AS VARCHAR)
        , lab_value_numeric: CAST(d.lab_value_numeric AS DOUBLE)
        , reference_unit: CAST(d.reference_unit AS VARCHAR)
        , lab_specimen_name: NULL::VARCHAR
        , lab_specimen_category: NULL::VARCHAR
        , lab_loinc_code: NULL::VARCHAR
    ORDER BY hospitalization_id, lab_collect_dttm, lab_result_dttm, lab_category
    """
    df = duckdb.sql(q).df()
    logger.info("converting timestamps to UTC...")
    for col in ['lab_order_dttm', 'lab_collect_dttm', 'lab_result_dttm']:
        df[col] = convert_tz_to_utc(df[col])
    return df


# =============================================================================
# Schema Testing and Save
# =============================================================================

@tag(property="test")
def schema_tested(cast: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logger.info("testing schema...")
    try:
        CLIF_LABS_SCHEMA.validate(cast, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logger.error(json.dumps(exc.message, indent=2))
        logger.error("Schema errors and failure cases:")
        logger.error(exc.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(exc.data)
        return exc


@datasaver()
def save(cast: pd.DataFrame) -> dict:
    logger.info("saving to rclif...")
    save_to_rclif(cast, "labs")

    metadata = {
        "table_name": "labs"
    }

    return metadata


# =============================================================================
# Main Entry Points
# =============================================================================

def _main():
    from hamilton import driver
    import src.tables.labs as labs
    dr = (
        driver.Builder()
        .with_modules(labs)
        .build()
    )
    dr.execute(["save"])


def _test():
    logger.info("testing all...")
    from hamilton import driver
    import src.tables.labs as labs
    dr = (
        driver.Builder()
        .with_modules(labs)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    return output


if __name__ == "__main__":
    setup_logging()
    _main()
