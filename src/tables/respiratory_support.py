# src/tables/respiratory_support.py
import numpy as np
import pandas as pd
import logging
import importlib 
import duckdb
from hamilton.function_modifiers import tag, datasaver, config, check_output
from src.logging_config import setup_logging, get_logger

logger = get_logger('tables.respiratory_support')
import pandera.pandas as pa
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
)

RESP_DEVICE_RANK = [
    "IMV",
    "NIPPV",
    "CPAP",
    "High Flow NC",
    "Face Mask",
    "Trach Collar",
    "Nasal Cannula",
    "Room Air", # NOTE: we currently do not have this "device" represented in MIMIC-CLIF
    "Other",
    ]

RESP_DEVICE_REPRESENTED = [
    "IMV",
    "NIPPV",
    "CPAP",
    "High Flow NC",
    "Face Mask",
    "Trach Collar",
    "Nasal Cannula",
    "Other",
    ]

MODE_CATEGORIES = [
    "Assist Control-Volume Control",
    "Pressure Control",
    "Pressure-Regulated Volume Control",
    "SIMV",
    "Pressure Support/CPAP",
    "Volume Support",
    "Other",
    "Blow by",
]

CLIF_RESP_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "device_name": pa.Column(str, nullable=True),
        "device_category": pa.Column(str, checks=[pa.Check.isin(RESP_DEVICE_REPRESENTED)], nullable=True),
        "vent_brand_name": pa.Column(str, nullable=True),
        "mode_name": pa.Column(str, nullable=True),
        "mode_category": pa.Column(str, checks=[pa.Check.unique_values_eq(MODE_CATEGORIES)], nullable=True),
        "tracheostomy": pa.Column(bool, nullable=False),
        "fio2_set": pa.Column(float, checks=[pa.Check.in_range(0.2, 1)], nullable=True),
        "lpm_set": pa.Column(float, nullable=True),
        "tidal_volume_set": pa.Column(float, nullable=True),
        "resp_rate_set": pa.Column(float, nullable=True),
        "pressure_control_set": pa.Column(float, nullable=True),
        "pressure_support_set": pa.Column(float, nullable=True),
        "flow_rate_set": pa.Column(float, nullable=True),
        "peak_inspiratory_pressure_set": pa.Column(float, nullable=True),
        "inspiratory_time_set": pa.Column(float, nullable=True),
        "peep_set": pa.Column(float, nullable=True),
        "tidal_volume_obs": pa.Column(float, nullable=True),
        "resp_rate_obs": pa.Column(float, nullable=True),
        "plateau_pressure_obs": pa.Column(float, nullable=True),
        "peak_inspiratory_pressure_obs": pa.Column(float, nullable=True),
        "peep_obs": pa.Column(float, nullable=True),
        "minute_vent_obs": pa.Column(float, nullable=True),
        "mean_airway_pressure_obs": pa.Column(float, nullable=True)
    },  
    strict=True,
)

RESP_COLUMNS = list(CLIF_RESP_SCHEMA.columns.keys())

def resp_mapping() -> pd.DataFrame:
    return load_mapping_csv("respiratory_support")

def resp_mapper(resp_mapping: pd.DataFrame) -> dict:
    return construct_mapper_dict(
        mapping_df=resp_mapping,
        key_col="itemid",
        value_col="variable",
        map_none_to_none=True,
        decision_col="variable"
    )

def resp_device_mapper() -> dict:
    resp_device_mapping = load_mapping_csv("device_category")
    return construct_mapper_dict(
        mapping_df=resp_device_mapping,
        key_col="device_name",
        value_col="device_category",
        map_none_to_none=True,
        excluded_item_ids=["223848"],
        decision_col="device_category"
    )

def resp_mode_mapper() -> dict:
    resp_mode_mapping = load_mapping_csv("mode_category")
    return construct_mapper_dict(
        mapping_df=resp_mode_mapping,
        key_col="mode_name",
        value_col="mode_category",
        map_none_to_none=False,
        decision_col="mode_category"
        )
    
def mimic_item_ids(resp_mapping: pd.DataFrame) -> pd.Series:
    return get_relevant_item_ids(
        mapping_df=resp_mapping,
        decision_col="variable"
    )

def extracted_mimic_events(mimic_item_ids: pd.Series) -> pd.DataFrame:
    logger.info(
        "parsing the mapping files to identify relevant items and fetch corresponding events..."
    )
    return fetch_mimic_events(mimic_item_ids)

def extracted_mimic_events_translated(extracted_mimic_events: pd.DataFrame, resp_mapper: dict) -> pd.DataFrame:
    extracted_mimic_events["variable"] = extracted_mimic_events["itemid"].map(resp_mapper)
    return extracted_mimic_events

def none_value_rows_removed(extracted_mimic_events_translated: pd.DataFrame = None) -> pd.DataFrame:
    """Remove rows where value is the string 'None'."""
    logger.info("removing rows where O2 Delivery Device(s) is the string 'None'...")
    mask = extracted_mimic_events_translated["value"] == "None"
    none_value_rows = extracted_mimic_events_translated[mask] # df to drop
    # TODO: turn this into a validation check
    if (
        none_value_rows["itemid"].nunique() == 1
        and none_value_rows["itemid"].iloc[0] == 226732
    ):
        # drop all rows where value is the string 'None'
        return extracted_mimic_events_translated[~mask]
    else:
        raise ValueError(
            "The rows with 'None' value have itemid other than 226732 (O2 Delivery Device(s))."
        )

def _clean_fio2_set_helper(value: float) -> float:
    """
    ref: https://github.com/MIT-LCP/mimic-code/blob/e39825259beaa9d6bc9b99160049a5d251852aae/mimic-iv/concepts/measurement/bg.sql#L130
    """
    value = float(value)
    if value >= 20 and value <= 100:
        return value / 100
    elif value > 1 and value < 20:
        return np.nan
    elif value > 0.2 and value <= 1:
        return value
    else:
        return np.nan

def fio2_set_cleaned(none_value_rows_removed: pd.DataFrame) -> pd.DataFrame:
    '''
    Apply outlier handling and drop the nulls thus generated.
    '''     
    logger.info("cleaning fio2_set...")
    mask = none_value_rows_removed['variable'] == 'fio2_set'
    none_value_rows_removed.loc[mask, 'value'] = none_value_rows_removed.loc[mask, 'value'].apply(_clean_fio2_set_helper)        
    return none_value_rows_removed.dropna(subset=['value'])

def duplicates_removed(
    fio2_set_cleaned: pd.DataFrame,
    resp_mapper: dict,
    resp_device_mapper: dict,
) -> pd.DataFrame:
    """Remove duplicates to support long-to-wide pivoting.
    two kinds of duplicates to handle: by devices and other
    """
    resp_duplicates: pd.DataFrame = find_duplicates(fio2_set_cleaned)

    logger.info(
        f"identified {len(resp_duplicates)} 'duplicated' events to be cleaned."
    )
    logger.info(
        "removing the first type of duplicates: lower-ranked 'duplicated' devices..."
    )
    # 1/ deal with dups over devices
    resp_duplicates_devices: pd.DataFrame = resp_duplicates.query(
        "itemid == 226732"
    ).copy()
    resp_duplicates_devices["device_category"] = resp_duplicates_devices[
        "value"
    ].apply(lambda x: resp_device_mapper[x.strip()] if pd.notna(x) else None)
    resp_duplicates_devices.dropna(subset="device_category", inplace=True)
    resp_duplicates_devices["rank"] = resp_duplicates_devices[
        "device_category"
    ].apply(lambda x: RESP_DEVICE_RANK.index(x.strip()))

    # deal with the device case - find indices to drop
    top_ranked_device_indices = resp_duplicates_devices.groupby(
        ["hadm_id", "time", "itemid"]
    )["rank"].idxmin()
    # non top-ranked categories to be dropped
    lower_ranked_device_indices = resp_duplicates_devices.index.difference(
        top_ranked_device_indices
    )
    # drop the designated indices
    resp_events_clean = fio2_set_cleaned.drop(lower_ranked_device_indices)
    # drop None
    resp_events_clean.dropna(subset="value", inplace=True)

    logger.info(
        "removing the second type of duplicates: duplicated device reads..."
    )
    # 2/ deal with duplicate vent reads:
    setting_duplicate_indices_to_drop = (
        find_duplicates(resp_events_clean).query("stay_id == 36123037").index
    )
    resp_events_clean.drop(setting_duplicate_indices_to_drop, inplace=True)
    resp_events_clean.drop_duplicates(
        subset=["hadm_id", "time", "itemid"], inplace=True
    )
    # NOTE: this approach drop one observation that has conflicting value

    resp_events_clean["variable"] = resp_events_clean["itemid"].map(
        resp_mapper
    )
    return resp_events_clean

def pivoted_wider_and_coalesced(
    duplicates_removed: pd.DataFrame,
    resp_mapper: dict,
    resp_device_mapper: dict,
    resp_mode_mapper: dict,
) -> pd.DataFrame:
    
    logger.info("pivoting to a wide format and coalescing duplicate columns...")
    # this is for EDA
    # resp_wider_in_lables = resp_events_clean.pivot(
    #     index = ["hadm_id", "time"],
    #     columns = ["variable", "label"],
    #     values = "value"
    # )

    # this is for actually cleaning based on item ids
    resp_wider_in_ids = (
        duplicates_removed.pivot(
            index=["hadm_id", "time"], columns=["itemid"], values="value"
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    resp_wider_in_ids = convert_and_sort_datetime(resp_wider_in_ids)
    # implement the coalease logic
    resp_wider_in_ids["tracheostomy"] = resp_wider_in_ids[225448].fillna(
        resp_wider_in_ids[226237]
    )
    resp_wider_in_ids["lpm_set"] = resp_wider_in_ids[223834].fillna(
        resp_wider_in_ids[227287]
    )
    resp_wider_in_ids["tidal_volume_obs"] = (
        resp_wider_in_ids[224685]
        .fillna(resp_wider_in_ids[224686])
        .fillna(resp_wider_in_ids[224421])
    )
    resp_wider_in_ids["resp_rate_set"] = resp_wider_in_ids[224688].fillna(
        resp_wider_in_ids[227581]
    )
    resp_wider_in_ids["resp_rate_obs"] = resp_wider_in_ids[224690].fillna(
        resp_wider_in_ids[224422]
    )
    resp_wider_in_ids["flow_rate_set"] = resp_wider_in_ids[224691].fillna(
        resp_wider_in_ids[227582]
    )
    resp_wider_in_ids["peep_set"] = resp_wider_in_ids[220339].fillna(
        resp_wider_in_ids[227579]
    )
    resp_wider_in_ids["mode_name"] = resp_wider_in_ids[223849].fillna(
        resp_wider_in_ids[229314].fillna(resp_wider_in_ids[227577])
    )  # FIXME
    # remove duplicate variable columns that were coaleased into one
    resp_wider_cleaned = resp_wider_in_ids.drop(
        columns=[
            225448,
            226237,
            223834,
            227287,
            224685,
            224686,
            224421,
            224688,
            227581,
            224690,
            224422,
            224691,
            227582,
            220339,
            227579,
            223849,
            229314,
            227577,
        ]
    )
    resp_wider_cleaned.rename(columns=resp_mapper, inplace=True)

    logger.info("mapping device and mode names to categories...")
    # map _name to _category
    resp_wider_cleaned["device_category"] = resp_wider_cleaned["device_name"].map(
        lambda x: resp_device_mapper[x.strip()] if pd.notna(x) else None
    )
    resp_wider_cleaned["mode_category"] = resp_wider_cleaned["mode_name"].map(
        resp_mode_mapper
    )
    return resp_wider_cleaned

def renamed_reordered_recasted(
    pivoted_wider_and_coalesced: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("renaming, reordering, and re-casting columns...")
    resp_final = rename_and_reorder_cols(
        pivoted_wider_and_coalesced,
        rename_mapper_dict={
            "hadm_id": "hospitalization_id",
            "time": "recorded_dttm",
        },
        new_col_order= RESP_COLUMNS,
    )
    # convert dtypes:
    resp_float_cols = [
        col for col in resp_final.columns if "_set" in col or "_obs" in col
    ]
    resp_final["hospitalization_id"] = resp_final["hospitalization_id"].astype(
        "string"
    )
    resp_final["recorded_dttm"] = convert_tz_to_utc(resp_final["recorded_dttm"])
    for col_name in resp_float_cols:
        resp_final[col_name] = resp_final[col_name].astype(float)
    return resp_final

@tag(property="final")
def tracheostomy_imputed(renamed_reordered_recasted: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        "imputing whether tracheostomy had been performed at the time of observation..."
    )
    renamed_reordered_recasted.rename(columns={"tracheostomy": "trach_performed"}, inplace=True)
    renamed_reordered_recasted["trach_implied"] = (
        renamed_reordered_recasted["device_name"].str.strip().isin(["Tracheostomy tube", "Trach mask"])
    ) | (renamed_reordered_recasted["trach_performed"] == 1)
    renamed_reordered_recasted["trach_bool"] = renamed_reordered_recasted.groupby("hospitalization_id")[
        "trach_implied"
    ].transform(lambda x: x.cumsum().astype(bool))
    return rename_and_reorder_cols(
        renamed_reordered_recasted, {"trach_bool": "tracheostomy"}, RESP_COLUMNS
    )

def _find_and_report_all_null_rows(df: pd.DataFrame):
    """Report the rows with null in device_name, mode_name, vent_brand_name, and all value fields."""
    df["all_value_na"] = df.loc[:, "tracheostomy": "mean_airway_pressure_obs"].isna().all(axis=1)
    mask = (
        (df["device_name"].isna())
        & (df["mode_name"].isna())
        & (df["vent_brand_name"].isna())
        & (df["all_value_na"] == True)
    )
    logger.info(f"{mask.sum()} ({mask.mean()*100:.2f}%) rows have null in device_name, vent_brand_name, mode_name, and all value fields.")
    return df[mask]

@tag(property="test")
def schema_tested(tracheostomy_imputed: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logger.info("testing schema...")
    df = tracheostomy_imputed
    try:
        CLIF_RESP_SCHEMA.validate(df, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logger.error(json.dumps(exc.message, indent=2))
        logger.error("Schema errors and failure cases:")
        logger.error(exc.failure_cases)
        logger.error("\nDataFrame object that failed validation:")
        logger.error(exc.data)
        return exc

@tag(property="test")
def no_nulls_tested(tracheostomy_imputed: pd.DataFrame) -> bool:
    df = tracheostomy_imputed.copy()
    all_null_rows = _find_and_report_all_null_rows(df)
    return len(all_null_rows) == 0

@datasaver()
def save(tracheostomy_imputed: pd.DataFrame) -> dict:
    save_to_rclif(tracheostomy_imputed, "respiratory_support")
    
    metadata = {
        "table_name": "respiratory_support"
    }
    
    return metadata

def _test():
    logger.info("testing all...")
    from hamilton import driver
    import src.tables.respiratory_support as respiratory_support
    dr = (
        driver.Builder()
        .with_modules(respiratory_support)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    logger.debug(f"Test output: {output}")
    return output

def _main():
    from hamilton import driver
    import src.tables.respiratory_support as respiratory_support
    dr = (
        driver.Builder()
        .with_modules(respiratory_support)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

if __name__ == "__main__":
    setup_logging()
    _main()