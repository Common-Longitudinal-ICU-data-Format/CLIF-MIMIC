# src/tables/respiratory_support.py
import numpy as np
import pandas as pd
import logging
from functools import cache
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, EXCLUDED_LABELS_DEFAULT, convert_tz_to_utc
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.vitals')


VITAL_COL_NAMES = ["hospitalization_id", "recorded_dttm", "vital_name", "vital_category", "vital_value", "meas_site_name"]

VITAL_COL_RENAME_MAPPER = {
    "hadm_id": "hospitalization_id", 
    "time": "recorded_dttm",
    "value": "vital_value"
    }

@cache
def convert_f_to_c(temp_f) -> float:
    if isinstance(temp_f, str) or isinstance(temp_f, int):
        temp_f = float(temp_f) 
    
    if isinstance(temp_f, float):
        temp_c = (temp_f - 32) * 5 / 9
        return round(temp_c, 1) # so 39.3333 -> 39.3
    else:
        raise("wrong type")

def _main():
    logger.info("starting to build clif vitals table -- ")
    vitals_mapping = load_mapping_csv("vitals")
    vital_name_mapper = construct_mapper_dict(vitals_mapping, "itemid", "label = vital_name")
    vital_category_mapper = construct_mapper_dict(vitals_mapping, "itemid", "vital_category")

    logger.info("processing the standard cases (that do not need pivoting)")
    # find vital_items_ids
    vitals_items_ids = get_relevant_item_ids(
        mapping_df = vitals_mapping, decision_col = "vital_category", 
        excluded_labels = EXCLUDED_LABELS_DEFAULT + ["temp_c"]
        )
    vitals_events = fetch_mimic_events(vitals_items_ids)

    # use np.where to convert the unit for one item from lb to kg 
    # for the only weight item in undesired unit -- Admission Weight (lbs.)
    vitals_events["value"] = np.where(
        vitals_events["itemid"] == 226531,
        vitals_events["value"].astype(float).apply(lambda x: round(x/2.205, 1)),
        vitals_events["value"]
    )
    
    vitals_events["vital_name"] = vitals_events["itemid"].map(vital_name_mapper)
    vitals_events["vital_category"] = vitals_events["itemid"].map(vital_category_mapper)

    vitals_final = rename_and_reorder_cols(vitals_events, VITAL_COL_RENAME_MAPPER, VITAL_COL_NAMES)
    vitals_fd = vitals_final.drop_duplicates(
        subset = ["hospitalization_id", "recorded_dttm", "vital_category", "vital_value"])
    
    logger.info("processing the special cases for temp_c")
    temp_events = fetch_mimic_events([223761, 223762, 224642])
    
    # pivot directly
    temp_wider = temp_events.pivot(
        index = ["hadm_id", "time"], 
        columns = "itemid",
        values = "value"
        ).reset_index()
    # map temp_site to the clif categories of meas_site_name
    temp_wider["meas_site_name"] = temp_wider[224642]

    # convert temp from f to c with a coalesce logic
    # 223761 = temp in f, 223762 = temp in c
    temp_wider["vital_value"] = temp_wider[223762].fillna(
        temp_wider[223761].apply(convert_f_to_c)
        )
    temp_wider['vital_name'] = temp_wider.apply(
        lambda row: "Temperature Celsius" if pd.notna(row[223762]) else "Temperature Fahrenheit", 
        axis = "columns"
        )
    temp_wider["vital_category"] = "temp_c"
    temp_final = rename_and_reorder_cols(temp_wider, VITAL_COL_RENAME_MAPPER, VITAL_COL_NAMES)
    temp_final.dropna(subset=["vital_value"], inplace = True)

    logger.info("merging the standard and special cases")
    vitals_m = pd.concat([vitals_final, temp_final])
    
    vitals_m.drop_duplicates(
        subset = ["hospitalization_id",	"recorded_dttm", "vital_category", "vital_value"], inplace = True)

    logger.info("converting dtypes...")
    vitals_m["hospitalization_id"] = vitals_m["hospitalization_id"].astype("string")
    vitals_m["vital_value"] = vitals_m["vital_value"].astype(float)
    vitals_m["recorded_dttm"] = pd.to_datetime(vitals_m["recorded_dttm"])
    vitals_m["recorded_dttm"] = convert_tz_to_utc(vitals_m["recorded_dttm"])
    
    save_to_rclif(vitals_m, "vitals")
    logger.info("output saved to a parquet file, everything completed for the vitals table!")

if __name__ == "__main__":
    setup_logging()
    _main()