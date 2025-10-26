# src/tables/medication_admin_intermittent.py
import numpy as np
import pandas as pd
import logging
import duckdb
from importlib import reload
from typing import Literal
import src.utils
# reload(src.utils)
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, search_mimic_items, convert_tz_to_utc, \
    mapping_path_finder
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
import pandera.pandas as pa
from pandera.dtypes import Float32
from typing import Dict, List
import json
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.medication_admin_intermittent')

def _main():
    logger.info("starting to build clif medication_admin_intermittent tables -- ")
    from hamilton import driver
    import src.tables.medication_admin as medication_admin
    dr = (
        driver.Builder()
        .with_modules(medication_admin)
        .build()
    )
    dr.execute(["save_intm"])
    
if __name__ == "__main__":
    setup_logging()
    _main()