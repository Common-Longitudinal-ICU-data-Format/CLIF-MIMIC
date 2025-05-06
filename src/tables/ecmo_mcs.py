# src/tables/ecmo_mcs.py
import numpy as np
import pandas as pd
import logging
from importlib import reload
import src.utils
reload(src.utils)
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging, item_id_to_label   

setup_logging()

def main():
    logging.info("starting to build clif ecmo_mcs table -- ")
    # load mapping 
    ecmo_mapping = load_mapping_csv("ecmo")
    ecmo_mapper = construct_mapper_dict(ecmo_mapping, "itemid", "variable")

    logging.info("parsing the mapping files to identify relevant items and fetch corresponding events...")

    ecmo_item_ids = get_relevant_item_ids(
        mapping_df = ecmo_mapping, decision_col = "variable" 
        ) 
    ecmo_events = fetch_mimic_events(ecmo_item_ids)

    ecmo_events["variable"] = ecmo_events["itemid"].map(ecmo_mapper)
    ecmo_events = ecmo_events.dropna(subset=['variable'])  

    # dedup - remove duplicates to prepare for pivoting 
    ecmo_duplicates: pd.DataFrame = find_duplicates(ecmo_events)
    
    logging.info(f"identified {len(ecmo_duplicates)} 'duplicated' events to be cleaned.")
    ecmo_events = ecmo_events.drop_duplicates()


    logging.info("pivoting and coalescing...")

    # hard coded this because for some reason all the Heartmate devices weren't listed in the "category" column
    ecmo_events.loc[ecmo_events['label'].str.contains('HM II', na=False), 'category'] = 'HM II'

    # hard coding mcs_category based on the labels for different measurements (flow, speed, etc.) - based on Curt's guidance. 
    # This could probably be converted to a second mcide--I think that is how the respiratory support table does it?
    ecmo_events['mcs_category'] = ecmo_events['label'].apply(
        lambda x: 'ECMO' if 'ECMO' in x else 
                'LVAD' if 'LVAD' in x else 
                'RVAD' if 'RVAD' in x else 
                'RVAD' if 'Flow Rate (Impella) (R)' in x else
                'LVAD' if 'Flow Rate (Impella)' in x else 
                'RVAD' if 'Performance Level (R)' in x else
                'LVAD' if 'Performance Level' in x else 
                'LVAD' if 'HM II' in x else 
                'LVAD' if 'Heartware' in x else 
                'LVAD' if 'Left Ventricular Assit Device Flow' in x else 
                'RVAD' if 'Right Ventricular Assist Device Flow' in x else pd.NA
    )
    # Fill mcs_category with 'LVAD' where value is '2.5 / CP' and mcs_category is NA - based on Curt's guidance
    ecmo_events.loc[
        (ecmo_events['value'] == '2.5 / CP') & (ecmo_events['mcs_category'].isna()),
        'mcs_category'
    ] = 'LVAD'

    # Fill mcs_category with 'RVAD' where value is 'RP' and mcs_category is NA - based on Curt's guidance
    ecmo_events.loc[
        (ecmo_events['value'] == 'RP') & (ecmo_events['mcs_category'].isna()),
        'mcs_category'
    ] = 'RVAD'

    ecmo_events['device_metric_name'] = ecmo_events['category'].apply(lambda x: 'Performance Level' if isinstance(x, str) and 'Impella' in x else 'RPM')
    ecmo_events = ecmo_events[["hadm_id", "time", 'category', 'mcs_category', 'device_metric_name', "itemid", "value"]]



    ecmo_wider_in_ids = ecmo_events.pivot(
        index = ["hadm_id", "time", 'category', 'mcs_category', 'device_metric_name'], 
        columns = ["itemid"],
        values = ["value"]
    ).reset_index()
    ecmo_wider_in_ids = convert_and_sort_datetime(ecmo_wider_in_ids)
    

    ecmo_wider_in_ids.columns = ['hospitalization_id', 'recorded_dttm', 'device_category', 'mcs_category', 'device_metric_name',
                             '220125', '220128', '228154', '228156', '228192', '228195', '228198', '228873', 
                             '228874', '229254', '229255', '229262', '229263', '229268', '229270','229277', '229278','229280', 
                             '229303', '229304', '229675', '229679', '229823', '229829', '229841', '229842', '229845',
                             '229846', '230086']
    
    # Coalescing the different labels for device rate, flow, sweep, fdo2, and device name based on the device.
    ecmo_wider_in_ids["device_rate"] = ecmo_wider_in_ids[["229262", "229263", "229829", "229845", "229277", "229303", "228874", "228156", "229675", "228195"]].bfill(axis=1).iloc[:, 0]
    ecmo_wider_in_ids["flow"] = ecmo_wider_in_ids[["229254", "229255", "229823", "229842", "229270", "229304", "228873", "220125", "220128", "228154", "228154", "228198"]].bfill(axis=1).iloc[:, 0]
    ecmo_wider_in_ids["sweep"] = ecmo_wider_in_ids[["229278", "229846", "228192"]].bfill(axis=1).iloc[:, 0]
    ecmo_wider_in_ids["fdo2"] = ecmo_wider_in_ids[["229280", "229841", "230086"]].bfill(axis=1).iloc[:, 0]
    ecmo_wider_in_ids["device_name"] = ecmo_wider_in_ids[["229268", "229679"]].bfill(axis=1).iloc[:, 0]

    logging.info("cleaning up column names and data types...")

    ecmo_wider_cleaned = ecmo_wider_in_ids.loc[:, ['hospitalization_id', 'recorded_dttm', 'device_name', 'device_category', 'mcs_category', 'device_metric_name', 'device_rate', 'flow', 'sweep', 'fdo2']]
    ecmo_wider_cleaned['flow'] = ecmo_wider_cleaned['flow'].astype(str).str.extract(r'([\d\.]+)')[0].astype(float)
    ecmo_wider_cleaned['sweep'] = ecmo_wider_cleaned['sweep'].astype(str).str.extract(r'([\d\.]+)')[0].astype(float)

    # Putting raw strings in the device_name, keeping device_category to defined mcide
    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['device_category'] == 'Hemodynamics', 'device_name'] = 'Hemodynamics'
    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['device_category'] == 'Hemodynamics', 'device_category'] = np.nan

    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['device_category'] == 'Durable VAD', 'device_name'] = 'Durable VAD'
    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['device_category'] == 'Durable VAD', 'device_category'] = np.nan

    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['device_category'] == 'HM II', 'device_name'] = 'HM II'
    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['device_category'] == 'HM II', 'device_category'] = 'HeartMate'

    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['mcs_category'].isna() & (ecmo_wider_cleaned['device_category'] == 'ECMO'), 'mcs_category'] = 'ECMO'
    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['device_category'].isna(), 'device_category'] = 'Other'

    ecmo_wider_cleaned = ecmo_wider_cleaned.drop_duplicates()
    ecmo_wider_cleaned.loc[ecmo_wider_cleaned['device_rate'].isna(), 'device_metric_name'] = pd.NA

    ## I think a possible test here could be crosstabs of device_name and device_category and device_category and mcs_category to make sure everything is mapped correctly

    # Define conditions for defining "side"
    conditions = [
        ecmo_wider_cleaned['mcs_category'] == 'LVAD',
        ecmo_wider_cleaned['mcs_category'] == 'RVAD',
        (ecmo_wider_cleaned['mcs_category'] == 'ECMO') & (ecmo_wider_cleaned['device_name'] == 'VV'),
        (ecmo_wider_cleaned['mcs_category'] == 'ECMO') & (ecmo_wider_cleaned['device_name'].isin(['VA', 'VAV']))
    ]

    # Define corresponding values
    choices = ['left', 'right', 'right', 'both']

    # Create 'side' column
    ecmo_wider_cleaned['side'] = np.select(conditions, choices, default=None)

    ## I think a possible test here could be crosstabs of mcs_category and side to make sure everything is mapped correctly


    # Convert specific columns to string
    ecmo_wider_cleaned[['hospitalization_id', 'device_name', 'device_category', 'device_metric_name']] = ecmo_wider_cleaned[['hospitalization_id', 'device_name', 'device_category', 'device_metric_name']].astype('string')

    # Convert specific columns to numeric
    ecmo_wider_cleaned[['flow', 'sweep', 'fdo2']] = ecmo_wider_cleaned[['flow', 'sweep', 'fdo2']].apply(pd.to_numeric, errors='coerce')


    # Value cutoffs -- should this be here or project specific?
    ecmo_final = ecmo_wider_cleaned
    ecmo_final.loc[~ecmo_final['sweep'].between(0, 15), 'sweep'] = pd.NA
    ecmo_final.loc[~ecmo_final['flow'].between(0, 10), 'flow'] = pd.NA
    ecmo_final.loc[~ecmo_final['fdo2'].between(0, 100), 'fdo2'] = pd.NA

    column_order = [
        'hospitalization_id', 'recorded_dttm', 'device_name', 'device_category',
        'mcs_category', 'side', 'device_metric_name', 'device_rate', 'flow', 'sweep', 'fdo2'
    ]

    # Reorder the DataFrame
    ecmo_final = ecmo_final[column_order]

    save_to_rclif(ecmo_final, "ecmo_mcs")
    logging.info("output saved to a parquet file, everything completed for the ecmo_mcs table!")

if __name__ == "__main__":
    main()





