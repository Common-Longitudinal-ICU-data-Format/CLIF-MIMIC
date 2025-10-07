# src/tables/hospitalization.py
import numpy as np
import pandas as pd
import duckdb
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
import pandera.pandas as pa
from pandera.dtypes import Float32
from typing import Dict, List
import json
import logging
from importlib import reload
import src.utils
# reload(src.utils)
from src.utils import construct_mapper_dict, load_mapping_csv, \
    rename_and_reorder_cols, save_to_rclif, setup_logging, mimic_table_pathfinder, convert_tz_to_utc
from src.utils_qa import all_null_check

setup_logging()
HOSP_COL_NAMES = [
    "patient_id", "hospitalization_id", "hospitalization_joined_id", "admission_dttm", "discharge_dttm",
    "age_at_admission", "admission_type_name", "admission_type_category",
    "discharge_name", "discharge_category", "zipcode_nine_digit", "zipcode_five_digit", 
    "census_block_code", "census_block_group_code", "census_tract", "state_code", "county_code"
]

HOSP_COL_RENAME_MAPPER = {
    "admittime": "admission_dttm",
    "dischtime": "discharge_dttm",
    "admission_type": "admission_type_name",
    "discharge_location": "discharge_name"
}

ADMISSION_TYPE_MAPPER = {
    "DIRECT EMER.": "ed",
    "OBSERVATION ADMIT": "ed",
    "URGENT": "ed",
    "EW EMER.": "ed",	
    "EU OBSERVATION": "ed",
    "DIRECT OBSERVATION": "direct",
    "ELECTIVE": "elective",
    "AMBULATORY OBSERVATION": "direct",
    "SURGICAL SAME DAY ADMISSION": "elective",
    None: "other"
}

ADMISSION_TYPE_CATEGORIES = [
    "ed", "direct", "elective", "other", "facility", "osh"
]

DISCHARGE_CATEGORIES = [
    "Home", 
    "Skilled Nursing Facility (SNF)", 
    "Expired", 
    "Acute Inpatient Rehab Facility", 
    "Hospice", 
    "Long Term Care Hospital (LTACH)", 
    "Acute Care Hospital", 
    "Group Home", 
    "Chemical Dependency", 
    "Against Medical Advice (AMA)", 
    "Assisted Living", 
    "Still Admitted", 
    "Missing", 
    "Other", 
    "Psychiatric Hospital", 
    "Shelter", 
    "Jail"
    ]

CLIF_HOSP_SCHEMA = pa.DataFrameSchema(
    {
        "patient_id": pa.Column(str, nullable=False),
        "hospitalization_id": pa.Column(str, nullable=False),
        "hospitalization_joined_id": pa.Column(str, nullable=True),
        "admission_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False), 
        "discharge_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "age_at_admission": pa.Column(int), 
        "admission_type_name": pa.Column(str, nullable=True), 
        "admission_type_category": pa.Column(str, checks=[pa.Check.isin(ADMISSION_TYPE_CATEGORIES)], nullable=False),
        "discharge_name": pa.Column(str, nullable=True), 
        "discharge_category": pa.Column(str, checks=[pa.Check.isin(DISCHARGE_CATEGORIES)], nullable=False), 
        "zipcode_nine_digit": pa.Column(str, checks=[all_null_check], nullable=True), 
        "zipcode_five_digit": pa.Column(str, checks=[all_null_check], nullable=True), 
        "census_block_code": pa.Column(str, checks=[all_null_check], nullable=True), 
        "census_block_group_code": pa.Column(str, checks=[all_null_check], nullable=True), 
        "census_tract": pa.Column(str, checks=[all_null_check], nullable=True), 
        "state_code": pa.Column(str, checks=[all_null_check], nullable=True), 
        "county_code": pa.Column(str, checks=[all_null_check], nullable=True),
    },
    strict=True,
)

def discharge_mapping() -> pd.DataFrame:
    return load_mapping_csv("discharge")

def discharge_mapper(discharge_mapping: pd.DataFrame) -> Dict:
    discharge_mapper = construct_mapper_dict(
        mapping_df=discharge_mapping, 
        key_col="discharge_location", 
        value_col="disposition_category"
    )
    # add mapping of all NA discharge_location to "missing"
    discharge_mapper[None] = "Missing" # OR: discharge_mapper[np.nan] = 'Missing'
    return discharge_mapper

def extracted_and_translated(discharge_mapper: Dict) -> pd.DataFrame:
    logging.info("extracting and mapping columns...")
    query = f"""
    SELECT 
        subject_id,
        hadm_id,
        admittime,
        dischtime,
        admission_type,
        discharge_location,
        anchor_age,
        anchor_year,
        anchor_age + date_diff('year', make_date(anchor_year, 1, 1), admittime) AS age_at_admission
    FROM '{mimic_table_pathfinder("admissions")}'
    LEFT JOIN '{mimic_table_pathfinder("patients")}'
    USING (subject_id)
    """
    df = duckdb.query(query).df()
    df["discharge_category"] = df["discharge_location"].map(discharge_mapper)  
    df["admission_type_category"] = df["admission_type"].map(ADMISSION_TYPE_MAPPER)
    return df

def renamed_and_reordered(extracted_and_translated: pd.DataFrame) -> pd.DataFrame:
    logging.info("renaming and reordering columns...")
    return rename_and_reorder_cols(extracted_and_translated, HOSP_COL_RENAME_MAPPER, HOSP_COL_NAMES)

@tag(property="final")
def recast(renamed_and_reordered: pd.DataFrame) -> pd.DataFrame:
    logging.info("recasting columns...")
    df = renamed_and_reordered
    for col in df.columns:
        if "dttm" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = convert_tz_to_utc(df[col])
        elif col == "age_at_admission":
            continue
        else:
            df[col] = df[col].astype("string")
    return df

@tag(property="test")
def schema_tested(recast: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    try:
        CLIF_HOSP_SCHEMA.validate(recast, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@datasaver()
def save(recast: pd.DataFrame) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(recast, "hospitalization")
    
    metadata = {
        "table_name": "hospitalization"
    }
    
    logging.info("output saved to a parquet file, everything completed for the hospitalization table!")
    return metadata

def _main():
    logging.info("starting to build clif hospitalization table -- ")
    from hamilton import driver
    import src.tables.hospitalization as hospitalization
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(hospitalization)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

def _test():
    logging.info("testing all...")
    from hamilton import driver
    import src.tables.hospitalization as hospitalization
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(hospitalization)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    print(output)
    return output
    
if __name__ == "__main__":
    _main()