# src/tables/patient_procedures.py
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
    mimic_table_pathfinder
)

setup_logging()

from src.utils_qa import all_null_check

PROCEDURE_CODE_FORMATS = ['CPT', 'ICD10PCS', 'HCPCS', 'ICD9'] # ICD9 is technically not in the mcide

SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "billing_provider_id": pa.Column(str, checks=[all_null_check], nullable=True),
        "performing_provider_id": pa.Column(str, checks=[all_null_check], nullable=True),
        "procedure_code": pa.Column(str, nullable=False),
        "procedure_code_format": pa.Column(str, checks=[pa.Check.isin(PROCEDURE_CODE_FORMATS)], nullable=False),
        "procedure_billed_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
    },  
    strict=True,
)

COLUMN_NAMES: List[str] = list(SCHEMA.columns.keys())

def extracted_and_mapped_icd_codes() -> pd.DataFrame:
    logging.info("extracting and mapping the ICD codes from MIMIC's `procedures_icd` table...")
    q = f"""
    FROM '{mimic_table_pathfinder("procedures_icd")}' i
    SELECT hospitalization_id: CAST(i.hadm_id AS VARCHAR)
        , billing_provider_id: CAST(NULL AS VARCHAR)
        , performing_provider_id: CAST(NULL AS VARCHAR)
        , procedure_code: CAST(i.icd_code AS VARCHAR)
        , procedure_code_format: CASE
            WHEN icd_version in (10, '10') THEN 'ICD10PCS'
            WHEN icd_version in (9, '9') THEN 'ICD9'
            END
        , procedure_billed_dttm: CAST(i.chartdate AS TIMESTAMP)
    """
    return duckdb.query(q).df()

def extracted_and_mapped_cpt_hcpcs_codes() -> pd.DataFrame:
    logging.info("extracting and mapping the CPT/HCPCS codes from MIMIC's `hcpcsevents` table...")
    q = f"""
    FROM '{mimic_table_pathfinder("hcpcsevents")}' h
    SELECT hospitalization_id: CAST(h.hadm_id AS VARCHAR)
        , billing_provider_id: CAST(NULL AS VARCHAR)
        , performing_provider_id: CAST(NULL AS VARCHAR)
        , procedure_code: CAST(h.hcpcs_cd AS VARCHAR)
        , _procedure_code_format: CASE
            WHEN regexp_matches(procedure_code, '^[0-9]{{5}}$') THEN 'cpt_level_1'
            WHEN regexp_matches(procedure_code, '^[0-9]{{4}}[FT]$') THEN 'cpt_category_2_3'
            WHEN regexp_matches(procedure_code, '^[A-V][0-9]{{4}}$') THEN 'hcpcs_level_2'
            ELSE 'Unknown/Invalid' END
        , procedure_code_format: CASE
            WHEN _procedure_code_format in ('cpt_level_1', 'cpt_category_2_3') THEN 'CPT'
            WHEN _procedure_code_format = 'hcpcs_level_2' THEN 'HCPCS'
            END
        , procedure_billed_dttm: CAST(h.chartdate AS TIMESTAMP)
    """
    return duckdb.query(q).df()

@tag(property="final")
def concated(
    extracted_and_mapped_icd_codes: pd.DataFrame, 
    extracted_and_mapped_cpt_hcpcs_codes: pd.DataFrame
) -> pd.DataFrame:
    mimic_icd = extracted_and_mapped_icd_codes
    mimic_cpt_hcpcs = extracted_and_mapped_cpt_hcpcs_codes
    logging.info("Concating the ICD codes and the CPT/HCPCS codes...")
    q = f"""
    FROM mimic_icd
    SELECT *
    UNION ALL
    FROM mimic_cpt_hcpcs
    -- exclude columns that start with underscore
    SELECT COLUMNS('^[^_].*')
    """
    df = duckdb.query(q).df()
    df["procedure_billed_dttm"] = convert_tz_to_utc(df["procedure_billed_dttm"])
    return df

@tag(property="test")
def schema_tested(concated: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    try:
        SCHEMA.validate(concated, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@datasaver()
def save(concated: pd.DataFrame) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(concated, "patient_procedures")
    
    metadata = {
        "table_name": "patient_procedures"
    }
    
    logging.info("output saved to a parquet file, everything completed for the patient procedures table!")
    return metadata

def _main():
    logging.info("starting to build clif patient procedures table -- ")
    from hamilton import driver
    import src.tables.patient_procedures as patient_procedures
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(patient_procedures)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

def _test():
    logging.info("testing all...")
    from hamilton import driver
    import src.tables.patient_procedures as patient_procedures
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(patient_procedures)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    print(output)
    return output

if __name__ == "__main__":
    _main()