# src/tables/labs.py
import numpy as np
import pandas as pd
import logging
import re
import importlib 
import duckdb
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
import pandera as pa
from typing import Dict, List
import json

from src.utils import (
    construct_mapper_dict,
    fetch_mimic_events,
    load_mapping_csv,
    get_relevant_item_ids,
    find_duplicates,
    rename_and_reorder_cols,
    save_to_rclif,
    convert_and_sort_datetime,
    setup_logging,
    convert_tz_to_utc,
    CLIF_DTTM_FORMAT,
)

def _permitted_lab_categories() -> List[str]:
    clif_labs_mcide = pd.read_csv("https://raw.githubusercontent.com/Common-Longitudinal-ICU-data-Format/CLIF/refs/heads/main/mCIDE/clif_lab_categories.csv")
    return clif_labs_mcide["lab_category"].unique()

all_null_check = pa.Check(
    lambda s: s.isna().all(), 
    element_wise=False, 
    error="Column must contain only null values"
    )

CLIF_LABS_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "lab_order_dttm": pa.Column(
            pd.DatetimeTZDtype(unit="ns", tz="UTC"), 
            checks=[all_null_check],
            nullable=True),
        "lab_collect_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "lab_result_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=True), # FIXME: this should be changed to nullable=False once we finish debugging the NA issue
        "lab_order_name": pa.Column(str, checks=[all_null_check], nullable=True),
        "lab_order_category": pa.Column(str, checks=[all_null_check], nullable=True),
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

COL_RENAME_MAPPER: Dict[str, str] = {
    "charttime": "lab_collect_dttm", 
    "storetime": "lab_result_dttm", 
    "value": "lab_value", 
    "valuenum": "lab_value_numeric", 
    "valueuom": "reference_unit"
}


def labs_mapping() -> pd.DataFrame:
    logging.info("starting data extraction")
    logging.info("loading mapping...")
    # Load and prepare mapping
    labs_mapping = load_mapping_csv("labs")
    # drop the row corresponding to procalcitonin which is not available in MIMIC
    labs_mapping.dropna(subset=["itemid"], inplace=True)
    labs_mapping["itemid"] = labs_mapping["itemid"].astype(int)
    return labs_mapping

def id_to_name_mapper(labs_mapping: pd.DataFrame) -> dict:
    logging.info("constructing item id to name mapper...")
    return construct_mapper_dict(
        labs_mapping, "itemid", "label", 
        excluded_labels=["NO MAPPING", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"]
    )

def id_to_category_mapper(labs_mapping: pd.DataFrame) -> dict:
    logging.info("constructing item id to category mapper...")
    return construct_mapper_dict(
        labs_mapping, "itemid", "lab_category", 
        excluded_labels=["NO MAPPING", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"]
    )

def labs_items(labs_mapping: pd.DataFrame) -> pd.DataFrame:
    logging.info("filtering labs items...")
    return labs_mapping.loc[
        labs_mapping["decision"].isin(["TO MAP, CONVERT UOM", "TO MAP, AS IS", "UNSURE"]),
        ["lab_category", "itemid", "label", "count"]
    ].copy()

@cache(behavior="default", format="parquet")
def extracted_le_labs(labs_items: pd.DataFrame) -> pd.DataFrame:
    logging.info("identifying lab items to be extracted from labevents table...")
    # labevents table has itemids with 5 digits
    labs_items_le = labs_items[labs_items['itemid'].astype("string").str.len() == 5]
    logging.info("extracting from labevents table...")
    df_le = fetch_mimic_events(labs_items_le['itemid'], original=True, for_labs=True)
    return df_le

@cache(behavior="default", format="parquet")
def extracted_ce_labs(labs_items: pd.DataFrame) -> pd.DataFrame:
    logging.info("identifying lab items to be extracted from chartevents table...")    
    # chartevents table has itemids with 6 digits
    labs_items_ce = labs_items[labs_items['itemid'].astype("string").str.len() == 6]
    logging.info("extracting from chartevents table...")
    df_ce = fetch_mimic_events(labs_items_ce['itemid'], original=True, for_labs=False)
    return df_ce

@cache(format="parquet")
def le_labs_translated(extracted_le_labs: pd.DataFrame, id_to_name_mapper: dict, id_to_category_mapper: dict) -> pd.DataFrame:
    df = extracted_le_labs
    logging.info("translating labevents item ids to lab names and categories...")
    df["lab_name"] = df["itemid"].map(id_to_name_mapper)
    df["lab_category"] = df["itemid"].map(id_to_category_mapper)
    return df

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

def le_labs_comments_parsed(le_labs_translated: pd.DataFrame) -> pd.DataFrame:
    logging.info("parsing lab comments to recover otherwise missing lab values...")
    df = le_labs_translated
    mask = df["valuenum"].isna()
    df.loc[mask, ["valuenum"]] = df.loc[mask, "comments"].map(
        lambda x: _parse_labs_comment(x) if pd.notna(x) else np.nan)
    df.loc[mask, ["value"]] = df.loc[mask, "comments"]
    return df

@cache(format="parquet")
def le_labs_renamed_reordered(le_labs_comments_parsed: pd.DataFrame) -> pd.DataFrame:
    logging.info("renaming and reordering labevents columns...")
    df = le_labs_comments_parsed
    return rename_and_reorder_cols(df, COL_RENAME_MAPPER, LABS_COLUMNS + ["itemid"])

@cache(format="parquet")
def le_labs_units_converted(le_labs_renamed_reordered: pd.DataFrame) -> pd.DataFrame:
    """Convert units of measurement for specific lab items."""
    df = le_labs_renamed_reordered
    logging.info("converting units of measurement...")
    # for ionized (free) calcium, to convert a result from mmol/L to mg/dL, multiply the mmol/L value by 4.  
    # https://www.abaxis.com/sites/default/files/resource-packages/Ionized%20Calcium%20CTI%20Sheete%20714179-00P.pdf
    mask_ca = df["itemid"].isin([50808, 51624])
    df.loc[mask_ca, "lab_value_numeric"] *= 4
    df.loc[mask_ca, "reference_unit"] = "mg/dL"
    df.loc[mask_ca, "lab_value"] = df.loc[mask_ca, "lab_value_numeric"].astype("string")
    
    # convert troponin_t and troponin_i from ng/mL to ng/L, which is to multiply by 1000
    mask_trop = df["itemid"].isin([51003, 52642])
    df.loc[mask_trop, "lab_value_numeric"] *= 1000
    df.loc[mask_trop, "reference_unit"] = "ng/L"
    df.loc[mask_trop, "lab_value"] = df.loc[mask_trop, "lab_value_numeric"].astype("string")
    
    return df

@cache(format="parquet")
def ce_labs_translated(extracted_ce_labs: pd.DataFrame, id_to_name_mapper: dict, id_to_category_mapper: dict) -> pd.DataFrame:
    logging.info("translating chartevents item ids to lab names and categories...")
    df = extracted_ce_labs
    df["lab_name"] = df["itemid"].map(id_to_name_mapper)
    df["lab_category"] = df["itemid"].map(id_to_category_mapper)
    return df

@cache(format="parquet")
def ce_labs_renamed_reordered(ce_labs_translated: pd.DataFrame) -> pd.DataFrame:
    logging.info("renaming and reordering chartevents columns...")
    df = ce_labs_translated
    return rename_and_reorder_cols(df, COL_RENAME_MAPPER, LABS_COLUMNS + ["itemid"])

@cache(format="parquet")
def merged(le_labs_units_converted: pd.DataFrame, ce_labs_renamed_reordered: pd.DataFrame) -> pd.DataFrame:
    logging.info("merging lab events...")
    merged = pd.concat([le_labs_units_converted, ce_labs_renamed_reordered])
    merged.drop(columns = "itemid", inplace = True)
    return merged

@cache(format="parquet")
def columns_recast(merged: pd.DataFrame) -> pd.DataFrame:
    """Clean and recast columns to appropriate data types."""
    logging.info("cleaning and recasting columns...")
    for col in merged.columns:
        if "dttm" in col:
            merged[col] = pd.to_datetime(merged[col], format=CLIF_DTTM_FORMAT)
            merged[col] = convert_tz_to_utc(merged[col])
        elif any(x in col for x in ["order", "specimen", "loinc"]):
            merged[col] = pd.NA
        elif col == "hospitalization_id":
            merged[col] = merged[col].astype(int).astype("string")
    return merged

@cache(format="parquet")
def duplicates_removed(columns_recast: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate lab results."""
    df = columns_recast
    logging.info("starting duplicates removal...")
    df.drop_duplicates(
        subset=["hospitalization_id", "lab_collect_dttm", "lab_result_dttm", 
                "lab_category", "lab_value_numeric"],
        inplace=True
    )
    return df

@tag(property="test")
def schema_tested(duplicates_removed: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logging.info("testing schema...")
    df = duplicates_removed
    try:
        CLIF_LABS_SCHEMA.validate(df, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@datasaver()
def save(duplicates_removed: pd.DataFrame) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(duplicates_removed, "labs")
    
    metadata = {
        "table_name": "labs"
    }
    
    return metadata

def _main():
    from hamilton import driver
    import src.tables.labs as labs
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(labs)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

if __name__ == "__main__":
    _main()
