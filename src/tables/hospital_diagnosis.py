# src/tables/hospital_diagnosis.py
import numpy as np
import pandas as pd
import logging
import re
import importlib 
import duckdb
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
import pandera as pa
from pandera.dtypes import Float32
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
    mimic_table_pathfinder,
)

setup_logging()

from src.utils_qa import all_null_check

DIAGNOSIS_CODE_FORMATS = ["ICD-10-CM", "ICD-9-CM"]

CLIF_HOSP_DX_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "diagnosis_code": pa.Column(str, nullable=False),
        "diagnosis_code_format": pa.Column(str, checks=[pa.Check.unique_values_eq(DIAGNOSIS_CODE_FORMATS)], nullable=False),
        "diagnosis_name": pa.Column(str, nullable=True),
        "diagnosis_type": pa.Column(str, checks=[all_null_check], nullable=True),
        "present_on_admission": pa.Column(str, checks=[all_null_check], nullable=True),
    },
    strict=True,
)

CRRT_COLUMNS: List[str] = list(CLIF_HOSP_DX_SCHEMA.columns.keys())

@tag(property="final")
def hospital_dx() -> pd.DataFrame:
    query = f"""
    SELECT 
        CAST(dx.hadm_id AS VARCHAR) as hospitalization_id,
        CAST(dx.icd_code AS VARCHAR) as diagnosis_code,
        CASE WHEN dx.icd_version = 10 THEN 'ICD-10-CM'
            WHEN dx.icd_version = 9 THEN 'ICD-9-CM'
            ELSE NULL
            END as diagnosis_code_format,
        CAST(d.long_title AS VARCHAR) as diagnosis_name,
        CAST(NULL AS VARCHAR) as diagnosis_type,
        CAST(NULL AS VARCHAR) as present_on_admission
    FROM '{mimic_table_pathfinder('diagnoses_icd')}' dx
    LEFT JOIN '{mimic_table_pathfinder('d_icd_diagnoses')}' d
        USING (icd_code, icd_version)
    """
    df = duckdb.sql(query).df()
    return df

@tag(property="test")
def schema_tested(hospital_dx: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    try:
        CLIF_HOSP_DX_SCHEMA.validate(hospital_dx, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@datasaver()
def save(hospital_dx: pd.DataFrame) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(hospital_dx, "hospital_diagnosis")
    
    metadata = {
        "table_name": "hospital_diagnosis"
    }
    
    logging.info("output saved to a parquet file, everything completed for the hospital diagnosis table!")
    return metadata

def _main():
    logging.info("starting to build clif hospital diagnosis table -- ")
    from hamilton import driver
    import src.tables.hospital_diagnosis as hospital_diagnosis
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(hospital_diagnosis)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

def _test():
    logging.info("testing all...")
    from hamilton import driver
    import src.tables.hospital_diagnosis as hospital_diagnosis
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(hospital_diagnosis)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    print(output)
    return output

if __name__ == "__main__":
    _main()