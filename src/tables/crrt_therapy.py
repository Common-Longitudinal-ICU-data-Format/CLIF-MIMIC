# src/tables/crrt_therapy.py
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
)

setup_logging()

from src.utils_qa import all_null_check

CRRT_MODE_CATEGORIES = ["scuf", "cvvh", "cvvhd", "cvvhdf", "avvh"]

CLIF_CRRT_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "device_id": pa.Column(str, checks=[all_null_check], nullable=True),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "crrt_mode_name": pa.Column(str, nullable=True),
        "crrt_mode_category": pa.Column(str, checks=[pa.Check.isin(CRRT_MODE_CATEGORIES)], nullable=True),
        "dialysis_machine_name": pa.Column(str, checks=[all_null_check], nullable=True),
        "blood_flow_rate": pa.Column(Float32, nullable=True),
        "pre_filter_replacement_fluid_rate": pa.Column(Float32, nullable=True),
        "post_filter_replacement_fluid_rate": pa.Column(Float32, nullable=True),
        "dialysate_flow_rate": pa.Column(Float32, nullable=True),
        "ultrafiltration_out": pa.Column(Float32, nullable=True),
    },  
    strict=True,
)

CRRT_COLUMNS: List[str] = list(CLIF_CRRT_SCHEMA.columns.keys())

def dialysis_mapping() -> pd.DataFrame:
    logging.info("loading dialysis mapping...")
    return load_mapping_csv("dialysis")

def crrt_items(dialysis_mapping: pd.DataFrame) -> pd.DataFrame:
    return dialysis_mapping[dialysis_mapping["table"].astype(str).str.contains("crrt")]

def crrt_id_to_variable_mapper(crrt_items: pd.DataFrame) -> dict:
    return dict(zip(crrt_items["itemid"], crrt_items["clif_variable"]))

def extracted_crrt_events(crrt_items: pd.DataFrame, crrt_id_to_variable_mapper: dict) -> pd.DataFrame:
    logging.info("extracting crrt events...")
    crrt_item_ids = crrt_items["itemid"].tolist()
    df = fetch_mimic_events(crrt_item_ids)
    df["crrt_variable"] = df["itemid"].map(crrt_id_to_variable_mapper)
    return df
    
def crrt_events_pivoted_wider(extracted_crrt_events: pd.DataFrame) -> pd.DataFrame:
    logging.info("pivoting crrt events to wider...")
    return extracted_crrt_events.pivot(
        index=["hadm_id", "time"], columns=["crrt_variable"], values="value"
        ).reset_index().rename_axis(None, axis=1)

@tag(property="final")
def crrt_events_cast_and_cleaned(crrt_events_pivoted_wider: pd.DataFrame) -> pd.DataFrame:
    """
    - cast to correct dtypes
    - convert blood_flow_rate from mL/min to mL/hr
    - convert recorded_dttm to UTC
    """
    logging.info("casting and cleaning...")
    query = """
    SELECT
        CAST(hadm_id as VARCHAR) as hospitalization_id,
        CAST(NULL as VARCHAR) as device_id,
        CAST(time as TIMESTAMP) as recorded_dttm,
        CAST(crrt_mode_name as VARCHAR) as crrt_mode_name,
        CAST(lower(crrt_mode_name) as VARCHAR) as crrt_mode_category,
        CAST(NULL as VARCHAR) as dialysis_machine_name,
        CAST(blood_flow_rate as FLOAT) * 60 as blood_flow_rate, -- convert from mL/min to mL/hr
        CAST(pre_filter_replacement_fluid_rate as FLOAT) as pre_filter_replacement_fluid_rate,
        CAST(post_filter_replacement_fluid_rate as FLOAT) as post_filter_replacement_fluid_rate,
        CAST(dialysate_flow_rate as FLOAT) as dialysate_flow_rate,
        CAST(ultrafiltration_out as FLOAT) as ultrafiltration_out
    FROM crrt_events_pivoted_wider
    """
    df = duckdb.query(query).df()
    df["recorded_dttm"] = convert_tz_to_utc(df["recorded_dttm"])
    return df

@tag(property="test")
def schema_tested(crrt_events_cast_and_cleaned: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    try:
        CLIF_CRRT_SCHEMA.validate(crrt_events_cast_and_cleaned, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@datasaver()
def save(crrt_events_cast_and_cleaned: pd.DataFrame) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(crrt_events_cast_and_cleaned, "crrt_therapy")
    
    metadata = {
        "table_name": "crrt_therapy"
    }
    
    logging.info("output saved to a parquet file, everything completed for the crrt therapy table!")
    return metadata

def _main():
    logging.info("starting to build clif crrt therapy table -- ")
    from hamilton import driver
    import src.tables.crrt_therapy as crrt_therapy
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(crrt_therapy)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

def _test():
    logging.info("testing all...")
    from hamilton import driver
    import src.tables.crrt_therapy as crrt_therapy
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(crrt_therapy)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    print(output)
    return output

if __name__ == "__main__":
    _main()