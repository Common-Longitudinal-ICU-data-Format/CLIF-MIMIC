# src/tables/ecmo_mcs.py
import numpy as np
import pandas as pd
import logging
import duckdb
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
import pandera as pa
from pandera.dtypes import Float32
from typing import Dict, List
import json
from importlib import reload
import src.utils as utils
# reload(utils)
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging, item_id_to_label, convert_tz_to_utc

setup_logging()

CLIF_ECMO_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(pa.String, nullable=False),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "device_name": pa.Column(pa.String, nullable=True),
        "device_category": pa.Column(pa.String, nullable=True),
        "mcs_group": pa.Column(pa.String, nullable=True), # check whether it should be mcs_group instead
        "side": pa.Column(pa.String,  nullable=True, checks=[pa.Check.isin(["left", "right", "both", None])]),
        "device_metric_name": pa.Column(pa.String, nullable=True),
        "device_rate": pa.Column(pa.Float32, nullable=True),
        "flow": pa.Column(pa.Float32, nullable=True),
        "sweep": pa.Column(pa.Float32, nullable=True),
        "fdo2": pa.Column(pa.Float32, nullable=True),
    },
    strict=True,
)

def ecmo_mapping() -> pd.DataFrame:
    return load_mapping_csv("ecmo_mcs")

def ecmo_mapper(ecmo_mapping: pd.DataFrame) -> dict:
    ecmo_mapper = construct_mapper_dict(ecmo_mapping, "itemid", "variable")
    return ecmo_mapper

def ecmo_item_ids(ecmo_mapping: pd.DataFrame) -> pd.Series:
    # FIXME: this might be off or redundant -- need to check
    logging.info("parsing the mapping files to identify relevant items and fetch corresponding events...")
    return get_relevant_item_ids(
        mapping_df = ecmo_mapping, decision_col = "variable" 
        ) 

def extracted_ecmo_events(ecmo_item_ids: pd.Series, ecmo_mapper: dict) -> pd.DataFrame:
    df = fetch_mimic_events(ecmo_item_ids)
    df["variable"] = df["itemid"].map(ecmo_mapper)
    df.dropna(subset=['variable'], inplace=True)
    return df

def duplicates_removed(extracted_ecmo_events: pd.DataFrame) -> pd.DataFrame:
    '''remove duplicates to prepare for pivoting'''
    # FIXME: the logic here is clearly off as the find_duplicates is not being used -- need to check
    ecmo_duplicates: pd.DataFrame = find_duplicates(extracted_ecmo_events)
    logging.info(f"identified {len(ecmo_duplicates)} 'duplicated' events to be cleaned.")
    return extracted_ecmo_events.drop_duplicates()

def ecmo_events_cleaned(duplicates_removed: pd.DataFrame) -> pd.DataFrame:
    df = duplicates_removed
    # hard coded this because for some reason all the Heartmate devices weren't listed in the "category" column
    df.loc[df['label'].str.contains('HM II', na=False), 'category'] = 'HM II'

    # hard coding mcs_group based on the labels for different measurements (flow, speed, etc.) - based on Curt's guidance. 
    # This could probably be converted to a second mcide--I think that is how the respiratory support table does it?
    df['mcs_group'] = df['label'].apply(
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
    # Fill mcs_group with 'LVAD' where value is '2.5 / CP' and mcs_group is NA - based on Curt's guidance
    df.loc[
        (df['value'] == '2.5 / CP') & (df['mcs_group'].isna()),
        'mcs_group'
    ] = 'LVAD'

    # Fill mcs_group with 'RVAD' where value is 'RP' and mcs_group is NA - based on Curt's guidance
    df.loc[
        (df['value'] == 'RP') & (df['mcs_group'].isna()),
        'mcs_group'
    ] = 'RVAD'

    df['device_metric_name'] = df['category'].apply(lambda x: 'Performance Level' if isinstance(x, str) and 'Impella' in x else 'RPM')
    return df[["hadm_id", "time", 'category', 'mcs_group', 'device_metric_name', "itemid", "value"]]

def pivoted_wider(ecmo_events_cleaned: pd.DataFrame) -> pd.DataFrame:
    df = ecmo_events_cleaned.pivot(
        index = ["hadm_id", "time", 'category', 'mcs_group', 'device_metric_name'], 
        columns = ["itemid"],
        values = ["value"]
    ).reset_index()
    return convert_and_sort_datetime(df)

def coalesced(pivoted_wider: pd.DataFrame) -> pd.DataFrame:
    df = pivoted_wider
    df.columns = ['hospitalization_id', 'recorded_dttm', 'device_category', 'mcs_group', 'device_metric_name',
                             '220125', '220128', '228154', '228156', '228192', '228195', '228198', '228873', 
                             '228874', '229254', '229255', '229262', '229263', '229268', '229270','229277', '229278','229280', 
                             '229303', '229304', '229675', '229679', '229823', '229829', '229841', '229842', '229845',
                             '229846', '230086']
    
    # Coalescing the different labels for device rate, flow, sweep, fdo2, and device name based on the device.
    df["device_rate"] = df[["229262", "229263", "229829", "229845", "229277", "229303", "228874", "228156", "229675", "228195"]].bfill(axis=1).iloc[:, 0]
    df["flow"] = df[["229254", "229255", "229823", "229842", "229270", "229304", "228873", "220125", "220128", "228154", "228154", "228198"]].bfill(axis=1).iloc[:, 0]
    df["sweep"] = df[["229278", "229846", "228192"]].bfill(axis=1).iloc[:, 0]
    df["fdo2"] = df[["229280", "229841", "230086"]].bfill(axis=1).iloc[:, 0]
    df["device_name"] = df[["229268", "229679"]].bfill(axis=1).iloc[:, 0] 
    return df

def cleaned(coalesced: pd.DataFrame) -> pd.DataFrame:
    logging.info("cleaning up column names and data types...")
    df = coalesced.loc[:, ['hospitalization_id', 'recorded_dttm', 'device_name', 'device_category', 'mcs_group', 'device_metric_name', 'device_rate', 'flow', 'sweep', 'fdo2']]
    df['flow'] = df['flow'].astype(str).str.extract(r'([\d\.]+)')[0].astype(float)
    df['sweep'] = df['sweep'].astype(str).str.extract(r'([\d\.]+)')[0].astype(float)

    # Putting raw strings in the device_name, keeping device_category to defined mcide
    df.loc[df['device_category'] == 'Hemodynamics', 'device_name'] = 'Hemodynamics'
    df.loc[df['device_category'] == 'Hemodynamics', 'device_category'] = np.nan

    df.loc[df['device_category'] == 'Durable VAD', 'device_name'] = 'Durable VAD'
    df.loc[df['device_category'] == 'Durable VAD', 'device_category'] = np.nan

    df.loc[df['device_category'] == 'HM II', 'device_name'] = 'HM II'
    df.loc[df['device_category'] == 'HM II', 'device_category'] = 'HeartMate'

    df.loc[df['mcs_group'].isna() & (df['device_category'] == 'ECMO'), 'mcs_group'] = 'ECMO'
    df.loc[df['device_category'].isna(), 'device_category'] = 'Other'

    df = df.drop_duplicates()
    df.loc[df['device_rate'].isna(), 'device_metric_name'] = pd.NA

    ## NOTE: a possible test here could be crosstabs of device_name and device_category and device_category and mcs_group to make sure everything is mapped correctly
    return df

def side(cleaned: pd.DataFrame) -> pd.Series:
    df = cleaned
    # Define conditions for defining "side"
    conditions = [
        df['mcs_group'] == 'LVAD',
        df['mcs_group'] == 'RVAD',
        (df['mcs_group'] == 'ECMO') & (df['device_name'] == 'VV'),
        (df['mcs_group'] == 'ECMO') & (df['device_name'].isin(['VA', 'VAV']))
    ]

    # Define corresponding values
    choices = ['left', 'right', 'right', 'both']

    ## NOTE: a possible test here could be crosstabs of mcs_group and side to make sure everything is mapped correctly
    
    # Create 'side' column
    return np.select(conditions, choices, default=None)


def recast(cleaned: pd.DataFrame, side: pd.Series) -> pd.DataFrame:
    df = cleaned
    df['side'] = side
    # Convert specific columns to string
    df[['hospitalization_id', 'device_name', 'device_category', 'device_metric_name']] = df[['hospitalization_id', 'device_name', 'device_category', 'device_metric_name']].astype('string')

    # Convert specific columns to numeric
    df[['device_rate', 'flow', 'sweep', 'fdo2']] = df[['device_rate', 'flow', 'sweep', 'fdo2']].apply(pd.to_numeric, errors='coerce', downcast='float')
    
    df['recorded_dttm'] = convert_tz_to_utc(df['recorded_dttm'])
    return df

def outliers_removed(recast: pd.DataFrame) -> pd.DataFrame:
    # Value cutoffs -- NOTE: should this be here or project specific?
    df = recast
    df.loc[~df['sweep'].between(0, 15), 'sweep'] = pd.NA
    df.loc[~df['flow'].between(0, 10), 'flow'] = pd.NA
    df.loc[~df['fdo2'].between(0, 100), 'fdo2'] = pd.NA
    return df

@tag(property="final")
def reordered(outliers_removed: pd.DataFrame) -> pd.DataFrame:
    column_order = [
        'hospitalization_id', 'recorded_dttm', 'device_name', 'device_category',
        'mcs_group', 'side', 'device_metric_name', 'device_rate', 'flow', 'sweep', 'fdo2'
    ]
    # Reorder the DataFrame
    return outliers_removed[column_order]

@tag(property="test")
def schema_tested(reordered: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    try:
        CLIF_ECMO_SCHEMA.validate(reordered, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@datasaver()
def save(reordered: pd.DataFrame) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(reordered, "ecmo_mcs")
    
    metadata = {
        "table_name": "ecmo_mcs"
    }
    
    logging.info("output saved to a parquet file, everything completed for the ecmo_mcs table!")
    return metadata

def _main():
    logging.info("starting to build clif ecmo_mcs table -- ")
    from hamilton import driver
    import src.tables.ecmo_mcs as ecmo_mcs
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(ecmo_mcs)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

def _test():
    logging.info("testing all...")
    from hamilton import driver
    import src.tables.ecmo_mcs as ecmo_mcs
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(ecmo_mcs)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    print(output)
    return output

if __name__ == "__main__":
    _main()
