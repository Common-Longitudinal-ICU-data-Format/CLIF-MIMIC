# src/tables/code_status.py
import numpy as np
import pandas as pd
import logging
import re
import importlib 
import duckdb
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.code_status')
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
    convert_tz_to_utc,
    CLIF_DTTM_FORMAT,
    mimic_table_pathfinder
)

from src.utils_qa import all_null_check

CODE_STATUS_CATEGORIES = ['DNR', 'DNAR', 'UDNR', 'DNR/DNI', 'DNAR/DNI', 'AND', 'Full', 'Presume Full', 'Other']

SCHEMA = pa.DataFrameSchema(
    {
        "patient_id": pa.Column(str, nullable=False),
        "start_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "code_status_name": pa.Column(str, nullable=False),
        "code_status_category": pa.Column(str, checks=[pa.Check.isin(CODE_STATUS_CATEGORIES)], nullable=False),
    },  
    strict=True,
)

COLUMN_NAMES: List[str] = list(SCHEMA.columns.keys())

def extracted_events() -> pd.DataFrame:
    logger.info("extracting code status events...")
    return fetch_mimic_events(item_ids=[223758])

@tag(property="final")
def mapped_and_cast(extracted_events: pd.DataFrame) -> pd.DataFrame:
    logger.info("mapping and casting...")
    q = f"""
    FROM extracted_events e
    LEFT JOIN '{mimic_table_pathfinder('admissions')}' h 
        USING (hadm_id)
    SELECT patient_id: CAST(h.subject_id AS VARCHAR)
        , start_dttm: CAST(e.time AS TIMESTAMP)
        , code_status_name: CAST(e.value AS VARCHAR)
        , code_status_category: CASE
            WHEN code_status_name in ('Full code') THEN 'Full'
            WHEN code_status_name in ('DNR / DNI', 'DNI (do not intubate)') THEN 'DNR/DNI'
            WHEN code_status_name in ('DNR (do not resuscitate)') THEN 'DNR'
            WHEN code_status_name in ('Comfort measures only') THEN 'AND' END
    """
    df = duckdb.query(q).df()
    df["start_dttm"] = convert_tz_to_utc(df["start_dttm"])
    return df

@tag(property="test")
def schema_tested(mapped_and_cast: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    try:
        SCHEMA.validate(mapped_and_cast, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logger.error(json.dumps(exc.message, indent=2))
        logger.error("Schema errors and failure cases:")
        logger.error(exc.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(exc.data)
        return exc

@datasaver()
def save(mapped_and_cast: pd.DataFrame) -> dict:
    logger.info("saving to rclif...")
    save_to_rclif(mapped_and_cast, "code_status")
    
    metadata = {
        "table_name": "code_status"
    }
    
    logger.info("output saved to a parquet file, everything completed for the code status table!")
    return metadata

def _main():
    logger.info("starting to build clif code status table -- ")
    from hamilton import driver
    import src.tables.code_status as code_status
    dr = (
        driver.Builder()
        .with_modules(code_status)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

def _test():
    logger.info("testing all...")
    from hamilton import driver
    import src.tables.code_status as code_status
    dr = (
        driver.Builder()
        .with_modules(code_status)
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