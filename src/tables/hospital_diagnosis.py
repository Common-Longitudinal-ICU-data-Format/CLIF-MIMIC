# src/tables/hospital_diagnosis.py.py
import numpy as np
import pandas as pd
import logging
import re
import importlib 
import duckdb
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
import pandera.pandas as pa
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
    mimic_table_pathfinder
)

setup_logging()

from src.utils_qa import all_null_check

DX_CODE_FORMATS = ['ICD10CM', 'ICD9CM']

SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "diagnosis_code": pa.Column(str, nullable=False),
        "diagnosis_code_format": pa.Column(str, checks=[pa.Check.isin(DX_CODE_FORMATS)], nullable=False),
        "diagnosis_primary": pa.Column(np.int32, checks=[pa.Check.isin([0, 1])],nullable=False),
        "poa_present": pa.Column(np.int32, checks=[all_null_check], nullable=True),
    },  
    strict=True,
)

COLUMN_NAMES: List[str] = list(SCHEMA.columns.keys())

@tag(property="final")
def extracted_and_mapped() -> pd.DataFrame:
    logging.info("extracting and mapping the dx codes from MIMIC's `diagnoses_icd` table...")
    q = f"""
    SELECT hospitalization_id: CAST(dx.hadm_id AS VARCHAR)
        , diagnosis_code: CAST(dx.icd_code AS VARCHAR)
        , diagnosis_code_format: CASE 
            WHEN dx.icd_version = 10 THEN 'ICD10CM'
            WHEN dx.icd_version = 9 THEN 'ICD9CM' END
        , diagnosis_primary: CASE 
            WHEN dx.seq_num = 1 THEN 1 
            WHEN dx.seq_num > 1 THEN 0 END
        , poa_present: CAST(NULL AS INT)
    FROM '{mimic_table_pathfinder('diagnoses_icd')}' dx
    """
    return duckdb.query(q).df()

@tag(property="test")
def schema_tested(extracted_and_mapped: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    try:
        SCHEMA.validate(extracted_and_mapped, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@datasaver()
def save(extracted_and_mapped: pd.DataFrame) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(extracted_and_mapped, "hospital_diagnosis")
    
    metadata = {
        "table_name": "hospital_diagnosis"
    }
    
    logging.info("output saved to a parquet file, everything completed for the code status table!")
    return metadata

def _main():
    logging.info("starting to build clif code status table -- ")
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