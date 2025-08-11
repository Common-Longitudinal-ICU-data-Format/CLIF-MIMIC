# src/tables/medication_admin_continuous.py
import numpy as np
import pandas as pd
import logging
from importlib import reload
import src.utils
reload(src.utils)
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging, search_mimic_items, convert_tz_to_utc

setup_logging()

MAC_COL_NAMES = [
    "hospitalization_id", "med_order_id", "admin_dttm", "med_name", "med_category", "med_group", 
    "med_route_name", "med_route_category", "med_dose", "med_dose_unit", "mar_action_name", "mar_action_category"
]

MAC_COL_RENAME_MAPPER = {
    "dose": "med_dose",
    "rateuom": "med_dose_unit",
    "amountuom": "med_dose_unit",
    "new_mar": "mar_action_name", 
    "linkorderid": "med_order_id",
    "recorded_dttm": "admin_dttm",
    "label": "med_name"
}

MAC_MCIDE_URL = "https://raw.githubusercontent.com/clif-consortium/CLIF/main/mCIDE/clif_medication_admin_continuous_med_categories.csv"

def _are_doses_close(doses):
    return (abs(doses.iloc[0] - doses.iloc[1]) / max(doses.iloc[0], doses.iloc[1])) <= 0.1

# drop the row with the shorter mar_action_name
def _drop_shorter_action_name(group):
    if len(group) == 2 and _are_doses_close(group['med_dose']):
        return group.loc[[group['mar_action_name'].str.len().idxmax()]]
    return group

def mac_mapping() -> pd.DataFrame:
    # TODO: the key word will become 'med_admin_cont' in the next update
    return load_mapping_csv("mac")

def mac_item_ids(mac_mapping) -> pd.Series:
    logging.info("parsing the mapping files to identify relevant items and fetch corresponding events...")
    mac_item_ids = get_relevant_item_ids(
        mapping_df = mac_mapping, 
        decision_col = "decision", 
        # i.e. we including "SPECIAL CASE" which means the med can be either continuous or intermittent
        excluded_labels = ["NO MAPPING", "UNSURE", "MAPPED ELSEWHERE", "NOT AVAILABLE", "TO MAP, ELSEWHERE"]
        ) 
    return mac_item_ids

def extracted_mac_events(mac_item_ids) -> pd.DataFrame:
    '''
    '''
    return fetch_mimic_events(mac_item_ids)

def seleceted_and_mapped(extracted_mac_events) -> pd.DataFrame:
    '''
    Simplify the columns of the extracted mac_events dataframe.
    '''
    pass

def intermittent_removed(extracted_mac_events) -> pd.DataFrame:
    '''
    Keep only continuous infusions; remove intermittent administration of the same medication (such as boluses)
    '''
    df = extracted_mac_events
    
    logging.info("filtering out intermittent events...")
    return df.query("ordercategoryname != '05-Med Bolus'") \
        .query("ordercategorydescription != 'Drug Push'") \

def _main():
    logging.info("starting to build clif medication_admin_continuous table -- ")
    
    # add mapping to med_group
    mac_mcide_mapping = pd.read_csv("data/mcide/clif_medication_admin_continuous_med_categories.csv")
    mac_category_to_group_mapper = dict(zip(
        mac_mcide_mapping['med_category'], mac_mcide_mapping['med_group']
    ))
    # mac_categories = mac_mcide_mapping['med_category'].unique()
        
  
    # s stands for simple    
    mac_events_s = mac_events[[
        'subject_id', 'hadm_id', 'starttime',
        'endtime', 'storetime', 'statusdescription', 'itemid', 'amount', 'amountuom', 'rate',
        'rateuom', # 'orderid', 
        'linkorderid', # 'ordercategoryname',
        'totalamount', 'totalamountuom', 'originalamount', 'originalrate', 'label'
        ]].reset_index(drop = True)
    
    mac_events_s = convert_and_sort_datetime(mac_events_s)
    
    # drop duplicates
    mac_events_s.drop_duplicates(subset = ["hadm_id", "itemid", "starttime", "rate"], inplace = True) # FIXME
    
    # pivot longer and merge the starttime and endtime into a single column
    mac_l = mac_events_s.melt(
        id_vars = [
            "hadm_id", "itemid", "index", "rate", "rateuom", # "amount", "amountuom", 
            "statusdescription", "linkorderid", "label"],
        value_vars = ["starttime", "endtime"],
        var_name = "time", value_name = "recorded_dttm"
    ).sort_values(["hadm_id", "itemid", "index", "time"], ascending = [True, True, True, False])

    mac_l["diff"] = mac_l.groupby(['hadm_id', 'itemid'])[['recorded_dttm']].transform("diff")
    mac_l['mar'] = np.where(mac_l['time'] == 'starttime', 'start', mac_l['statusdescription'])
    mac_l['dose'] = np.where(mac_l['time'] == 'starttime', mac_l['rate'], np.nan)
    # mac_l['dose'] = np.where(mac_l['time'] == 'starttime', mac_l['amount'], np.nan)
    mac_l['last_mar'] = mac_l['mar'].shift(1)

    mac_l['new_mar'] = np.where(
        mac_l['diff'] == pd.Timedelta(0),
        mac_l['last_mar'].apply(lambda x: f"continue after {x}"),
        mac_l['mar']
    )

    # removing duplicates by filter out rows with NA "dose"
    mac_l['time_dup'] = mac_l.duplicated(["hadm_id", "itemid", "recorded_dttm"], keep = False)
    mac_l['keep'] = (~mac_l["time_dup"]) | pd.notna(mac_l["dose"])
    mac_ld = mac_l[mac_l['keep']].copy()
    # mac_ld["med_name"] = mac_ld["itemid"].map(mac_id_to_name_mapper)
    mac_ld["med_category"] = mac_ld["itemid"].map(mac_mapper)
    mac_ld["med_group"] = mac_ld["med_category"].map(mac_category_to_group_mapper)
    mac_ldf = rename_and_reorder_cols(mac_ld, MAC_COL_RENAME_MAPPER, MAC_COL_NAMES)
    
    logging.info("deduplicating...")
    # mac_dups = find_duplicates(mac_ldf, ["hospitalization_id", "admin_dttm", "med_category", "mar_action_name"])
    mac_dups = find_duplicates(mac_ldf, ["hospitalization_id", "admin_dttm", "med_category"]).copy()
    meds_keycols = ["hospitalization_id", "admin_dttm", "med_category"]
    # 1. we first attempt to remove dups that have a NA dose value.
    mac_dups["dose_notna"] = mac_dups["med_dose"].apply(pd.notna)
    mac_dups.sort_values(meds_keycols+["dose_notna"], ascending = [True, True, True, False], inplace = True)
    mac_dups["mar_last"] = mac_dups.groupby(meds_keycols)["mar_action_name"].shift(-1)
    mac_dups["mar_new"] = np.where(
        mac_dups["dose_notna"],
        mac_dups["mar_last"] + ", " + mac_dups["mar_action_name"],
        mac_dups["mar_action_name"]
    )
    # didx = duplicates indices, indicating which rows to remove
    meds_didx_1 = mac_dups[~mac_dups["dose_notna"]].index
    # remaining dups to deal with:
    mac_dups_d = mac_dups[mac_dups["dose_notna"]]
    mac_dups_d = mac_dups_d[mac_dups_d.duplicated(subset = meds_keycols, keep = False)
    ]
    mac_dups_d.reset_index(inplace=True)
    # 2. we then move on to remove those dups that are very close in value -- so we are fine dropping either one.
    # group by meds_keycols and apply the function
    mac_dups_dd = mac_dups_d.groupby(meds_keycols).apply(_drop_shorter_action_name).reset_index(drop = True)
    meds_didx_2 = pd.Index(
        np.setdiff1d(mac_dups_d["index"], mac_dups_dd["index"])
    )
    # NOTE: this last bit of deduplication step is deferred to be handled collectively and systematically by pyCLIF
    # 3. this left us with all the "genuine" conflicts we cann't resolve -- so we better just drop them all, unfortunately.
    # final dups to drop
    # mac_dups_ddd = mac_dups_dd[mac_dups_dd.duplicated(subset = meds_keycols, keep = False)]
    # meds_didx_3 = pd.Index(mac_dups_ddd['index'])

    # EDA -- what if we drop all the NA doses? still some left
    # mask = mac_dups.dropna(subset = 'med_dose').duplicated(subset = ["hospitalization_id", "admin_dttm", "med_category"], keep = False)
    # mac_dups2 = mac_dups.dropna(subset = 'med_dose')[mask].sort_values(["hospitalization_id", "admin_dttm", "med_category"])
    
    # so finally, we drop the three sets of indices we identified above which represent genuine irreconcilable duplicates.
    # new temp approach
    # mac_ldfd = mac_ldf.drop(meds_didx_1, axis="index").drop_duplicates(
    #     subset = meds_keycols, keep = "first"
    # )
    mac_ldfd = mac_ldf.drop(meds_didx_1, axis="index") \
        .drop(meds_didx_2, axis="index") # \
        # .drop(meds_didx_3, axis="index")
    
    logging.info("casting dtypes...")
    mac_ldfd["hospitalization_id"] = mac_ldfd["hospitalization_id"].astype("string")
    mac_ldfd["admin_dttm"] = convert_tz_to_utc(mac_ldfd["admin_dttm"])
    mac_ldfd["med_order_id"] = mac_ldfd["med_order_id"].astype("string")
    mac_ldfdf = mac_ldfd.copy()
    mac_ldfdf['med_dose'] = np.where(
        (mac_ldfdf['mar_action_name'].isin(["Stopped", "FinishedRunning", "Paused"])) & (mac_ldfdf['med_dose'].isna()),
        0,
        mac_ldfdf['med_dose']
    )
    
    save_to_rclif(mac_ldfdf, "medication_admin_continuous")
    logging.info("output saved to a parquet file, everything completed for the medication_admin_continuous table!")
    
if __name__ == "__main__":
    _main()