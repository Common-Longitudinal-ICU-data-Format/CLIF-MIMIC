# src/tables/medication_admin.py
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
    convert_and_sort_datetime, setup_logging, search_mimic_items, convert_tz_to_utc, \
    mapping_path_finder
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
import pandera.pandas as pa
from pandera.dtypes import Float32
from typing import Dict, List
import json

setup_logging()

CONT_MAR_ACTION_CATEGORIES = ['dose_change', 'start', 'stop', 'going', 'other']
INTM_MAR_ACTION_CATEGORIES = ['given', 'other']
CONT_MED_ROUTE_CATEGORIES = ['im', 'iv', 'inhaled']
INTM_MED_ROUTE_CATEGORIES = ['im', 'iv', 'enteral', 'buccal_sublingual', 'intrapleural']

CONT_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "med_order_id": pa.Column(str, nullable=False),
        "admin_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "med_name": pa.Column(str, nullable=False),
        "med_category": pa.Column(str, nullable=False),
        "mar_action_name": pa.Column(str, nullable=False),
        "mar_action_category": pa.Column(str, checks=[pa.Check.isin(CONT_MAR_ACTION_CATEGORIES)], nullable=False),
        "med_dose": pa.Column(Float32, nullable=False),
        "med_dose_unit": pa.Column(str, nullable=False),
        "med_route_name": pa.Column(str, nullable=False),
        "med_route_category": pa.Column(str, checks=[pa.Check.isin(CONT_MED_ROUTE_CATEGORIES)], nullable=False),
        "med_group": pa.Column(str, nullable=False),
    },  
    strict=True,
)

INTM_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "med_order_id": pa.Column(str, nullable=False),
        "admin_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "med_name": pa.Column(str, nullable=False),
        "med_category": pa.Column(str, nullable=False),
        "mar_action_name": pa.Column(str, nullable=False),
        "mar_action_category": pa.Column(str, checks=[pa.Check.isin(INTM_MAR_ACTION_CATEGORIES)], nullable=False),
        "med_dose": pa.Column(Float32, nullable=False),
        "med_dose_unit": pa.Column(str, nullable=False),
        "med_route_name": pa.Column(str, nullable=False),
        "med_route_category": pa.Column(str, checks=[pa.Check.isin(INTM_MED_ROUTE_CATEGORIES)], nullable=False),
        "med_group": pa.Column(str, nullable=False),
    },  
    strict=True,
)

# FIXME: likely remove this
# MAC_MCIDE_URL = "https://raw.githubusercontent.com/clif-consortium/CLIF/main/mCIDE/clif_medication_admin_continuous_med_categories.csv"

def med_category_mapping() -> pd.DataFrame:
    return load_mapping_csv("med_category")

def med_item_ids(med_category_mapping: pd.DataFrame) -> pd.Series:
    logging.info("identifying relevant items from the mapping file...")

    return get_relevant_item_ids(
        mapping_df = med_category_mapping, 
        decision_col = "decision", 
        excluded_labels = ["NO MAPPING", "UNSURE", "NOT AVAILABLE"]
        ) 

def med_events_extracted(med_item_ids: pd.Series) -> pd.DataFrame:
    logging.info("fetching corresponding events...")
    med_events = fetch_mimic_events(med_item_ids).pipe(convert_and_sort_datetime)
    logging.info("removing extra whitespaces in the `ordercomponenttypedescription` column that disrupts later joins that need exact matching...")
    med_events['ordercomponenttypedescription'] = med_events['ordercomponenttypedescription'].str.replace(r'\s+', ' ', regex=True).str.strip()
    return med_events

def med_route_mapping() -> pd.DataFrame:
    q = f"""
    SELECT CAST(COLUMNS('clif_|mimic_') AS VARCHAR)
    FROM '{mapping_path_finder("med_route_category")}' 
    """
    return duckdb.sql(q).df()

def med_route_mapping_by_id() -> pd.DataFrame:
    '''
    for the 'SPECIAL' cases which require itemid (med_id)-specific mapping
    '''
    q = f"""
    SELECT CAST(COLUMNS('clif_|mimic_') AS VARCHAR)
        , CAST(med_name AS VARCHAR)
        , CAST(med_id AS INT) AS med_id
    FROM '{mapping_path_finder("med_route_category_special")}' 
    """
    return duckdb.sql(q).df()

def med_route_mapped(med_events_extracted: pd.DataFrame, med_route_mapping: pd.DataFrame, med_route_mapping_by_id: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    logging.info("mapping med route...")
    q = """
    FROM med_events_extracted e
    LEFT JOIN med_route_mapping m
        ON e.ordercategoryname IS NOT DISTINCT FROM m.mimic_ordercategoryname
        AND e.secondaryordercategoryname IS NOT DISTINCT FROM m.mimic_secondaryordercategoryname
        AND e.ordercomponenttypedescription IS NOT DISTINCT FROM m.mimic_ordercomponenttypedescription
        AND e.ordercategorydescription IS NOT DISTINCT FROM m.mimic_ordercategorydescription
        AND e.category IS NOT DISTINCT FROM m.mimic_category
        AND m.clif_med_route_category NOT IN ('SPECIAL', 'UNINFORMATIVE')
    LEFT JOIN med_route_mapping_by_id m2 -- for the 'SPECIAL' cases which require itemid (med_id)-specific mapping
        ON e.ordercategoryname IS NOT DISTINCT FROM m2.mimic_ordercategoryname
        AND e.secondaryordercategoryname IS NULL -- NOT DISTINCT FROM m2.mimic_secondaryordercategoryname
        AND e.ordercomponenttypedescription IS NOT DISTINCT FROM m2.mimic_ordercomponenttypedescription
        AND e.ordercategorydescription IS NOT DISTINCT FROM m2.mimic_ordercategorydescription
        AND e.category IS NOT DISTINCT FROM m2.mimic_category
        AND e.itemid IS NOT DISTINCT FROM m2.med_id
    SELECT e.*
        , med_route_name: CONCAT_WS('; '
            , e.ordercategoryname
            , e.secondaryordercategoryname
            , e.ordercomponenttypedescription
            , e.ordercategorydescription
            , e.category
            )
        , med_route_category: COALESCE(m.clif_med_route_category, m2.clif_med_route_category)
    """
    med_route_mapped = duckdb.sql(q)
    logging.debug(f"len before: {len(med_events_extracted)}")
    logging.debug(f"len after: {len(med_route_mapped)}")
    assert len(med_route_mapped) == len(med_events_extracted), 'df length altered after mapping med route'
    return med_route_mapped

def mapped_and_augmented(med_route_mapped: duckdb.DuckDBPyRelation, med_category_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    find_intm_where_clause = """
    ordercategoryname = '05-Med Bolus'
        OR ordercategorydescription = 'Drug Push'
        OR ordercategorydescription = 'Bolus'
        OR statusdescription = 'Bolus'
    """
    logging.info("mapping med_category and adding helper columns to distinguish intermittent vs. continuous...")
    query = f"""
    SELECT subject_id, hadm_id
        , starttime, endtime --, storetime
        , linkorderid
        , statusdescription
        , med_category
        , rate, rateuom
        , amount, amountuom
        --, patientweight
        --, totalamount, totalamountuom, originalamount, originalrate
        --, ordercategoryname, ordercategorydescription
        , e.itemid
        , e.label
        , e.med_route_name
        , e.med_route_category
        , _item_class: m.decision 
        , _duration_in_mins: EXTRACT(EPOCH FROM (endtime - starttime)) / 60 
        , _last_1min: CASE WHEN (endtime - starttime) = INTERVAL '1 minute'
            THEN 1 ELSE 0 END 
        -- flags to identify intermittents
        , _intm_by_ordercategoryname: CASE WHEN ordercategoryname = '05-Med Bolus'
            THEN 1 ELSE 0 END 
        , _intm_by_ordercategorydescription: CASE WHEN ordercategorydescription IN ('Drug Push', 'Bolus')
            THEN 1 ELSE 0 END 
        , _intm_by_statusdescription: CASE WHEN statusdescription = 'Bolus'
            THEN 1 ELSE 0 END 
        , _to_table: CASE
            WHEN _item_class = 'INTERMITTENT'
                OR (_item_class = 'BOTH' AND ({find_intm_where_clause})) THEN 'intm'
            WHEN _item_class = 'CONTINUOUS'
                OR (_item_class = 'BOTH' AND NOT ({find_intm_where_clause})) THEN 'cont'
            END
    FROM med_route_mapped e
    LEFT JOIN med_category_mapping m
        ON e.itemid = m.itemid
        AND m.decision IN ('BOTH', 'CONTINUOUS', 'INTERMITTENT')
    WHERE _duration_in_mins > 0 -- remove the few cases where duration is zero or negative
    ORDER BY hadm_id, starttime, linkorderid, med_category, endtime
    """
    mapped_and_augmented = duckdb.sql(query)
    if len(mapped_and_augmented) != 8511695:
        logging.warning(f'df length after augmentation and mapping is different from expected in last run')
    # if mapped_and_augmented['_to_table'].isna().sum() != 0:
    #     logging.warning('there are still NAs in the column that determines the split to intermittent or continuous')
    return mapped_and_augmented

def cont_only(mapped_and_augmented: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    logging.info("splitting into a separate table for continuous med...")
    q = f"""
    SELECT *
    FROM mapped_and_augmented
    WHERE _to_table = 'cont'
    ORDER BY hadm_id, med_category, starttime, endtime, linkorderid
    -- ORDER BY hadm_id, starttime, linkorderid, med_category, endtime
    """
    cont_only = duckdb.sql(q)
    return cont_only

def intm_only(mapped_and_augmented: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    logging.info("splitting into a separate table for intermittent med...")
    q = f"""
    SELECT *
    FROM mapped_and_augmented
    WHERE _to_table = 'intm'
    ORDER BY hadm_id, med_category, starttime, endtime, linkorderid
    -- ORDER BY hadm_id, starttime, linkorderid, med_category, endtime
    """
    intm_only = duckdb.sql(q)#.df()
    return intm_only

def long_intm_to_cont_table(intm_only: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    logging.info("identifying some meds with longer than 1 min of duration in the intermittent table and moving them to the continuous table...")
    q = """
    SELECT *
    FROM intm_only
    WHERE _duration_in_mins > 1
        AND itemid in (222168, 225158, 220949, 221668, 225942)
    """
    long_intm_to_cont_table = duckdb.sql(q)#.df()
    return long_intm_to_cont_table

def intm_reassembled(intm_only: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    q = """
    SELECT *
    FROM intm_only
    WHERE NOT (
        _duration_in_mins > 1
        AND itemid in (222168, 225158, 220949, 221668, 225942)
    )
    """
    intm_reassembled = duckdb.sql(q)#.df()
    return intm_reassembled

def intm_flattened(intm_reassembled: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    logging.info("flattening timestamps in the intermittent table...")
    q = """
    FROM intm_reassembled
    SELECT hospitalization_id: hadm_id
        , med_order_id: linkorderid
        , med_name: label
        , med_category
        , admin_dttm: starttime
        , mar_action_name: statusdescription
        , mar_action_category: 'given'
        , med_dose: amount
        , med_dose_unit: LAST_VALUE(amountuom IGNORE NULLS) OVER (
            PARTITION BY hospitalization_id, med_order_id, med_category 
            ORDER BY admin_dttm
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        , med_route_name
        , med_route_category
    ORDER BY hospitalization_id, med_order_id, med_category, admin_dttm
    """
    intm_flattened = duckdb.sql(q)
    return intm_flattened

def intm_med_group_mapping() -> pd.DataFrame:
    q = """
    FROM 'data/mcide/clif_medication_admin_intermittent_med_categories.csv'
    SELECT med_category
        , med_group
        , n: COUNT(*) OVER (PARTITION BY med_category)
    ORDER BY n DESC, med_category
    """
    intm_med_group_mapping = duckdb.sql(q).df()
    return intm_med_group_mapping

@tag(property="final")
def intm_cast_w_med_group(intm_flattened: duckdb.DuckDBPyRelation, intm_med_group_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    q = """
    FROM intm_flattened
    LEFT JOIN intm_med_group_mapping USING (med_category)
    SELECT hospitalization_id: CAST(hospitalization_id AS VARCHAR)
        , med_order_id: CAST(med_order_id AS VARCHAR)
        , med_name: CAST(med_name AS VARCHAR)
        , med_category: CAST(med_category AS VARCHAR)
        , admin_dttm: CAST(admin_dttm AS TIMESTAMP)
        , mar_action_name: CAST(mar_action_name AS VARCHAR)
        , mar_action_category: CAST(mar_action_category AS VARCHAR)
        , med_dose: CAST(med_dose AS FLOAT)
        , med_dose_unit: CAST(med_dose_unit AS VARCHAR)
        , med_route_name: CAST(med_route_name AS VARCHAR)
        , med_route_category: CAST(med_route_category AS VARCHAR)
        , med_group: CAST(med_group AS VARCHAR)
    """
    intm_cast_w_med_group = duckdb.sql(q).df()
    intm_cast_w_med_group['admin_dttm'] = convert_tz_to_utc(intm_cast_w_med_group['admin_dttm'])
    # check length is not altered
    assert len(intm_flattened) == len(intm_cast_w_med_group), 'length altered after casting and mapping med_group'
    return intm_cast_w_med_group

@datasaver()
def save_intm(intm_cast_w_med_group: duckdb.DuckDBPyRelation) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(intm_cast_w_med_group, "medication_admin_intermittent")
    metadata = {
        "table_name": "medication_admin_intermittent"
    }
    
    logging.info("output saved to a parquet file, everything completed for the medication_admin_intermittent table!")
    return metadata

def cont_reassembled(cont_only: duckdb.DuckDBPyRelation, long_intm_to_cont_table: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    q = """
    SELECT * FROM cont_only
    UNION ALL
    SELECT * FROM long_intm_to_cont_table
    """
    cont_reassembled = duckdb.sql(q)#.df()
    return cont_reassembled

def cont_null_dose_rate_imputed(cont_reassembled: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    logging.info("imputing null dose rate in the continuous table as amount divided by duration...")
    q = """
    SELECT * REPLACE(
        COALESCE(rate, amount / _duration_in_mins) as rate
        , COALESCE(rateuom, amountuom || '/min') as rateuom
    )
    FROM cont_reassembled
    """
    cont_null_dose_rate_imputed = duckdb.sql(q)#.df()
    return cont_null_dose_rate_imputed

def cont_deduped_by_timestamps(cont_null_dose_rate_imputed: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    logging.info("Deduplicating entries with the same 'starttime' and 'endtime' in the continuous table...")
    q = """
    SELECT *
    FROM cont_null_dose_rate_imputed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY hadm_id, linkorderid, med_category, starttime, endtime
    ) = 1
    """
    cont_deduped_by_timestamps = duckdb.sql(q)
    logging.info(f"Removed {len(cont_null_dose_rate_imputed) - len(cont_deduped_by_timestamps)} rows")
    return cont_deduped_by_timestamps

def cont_flattened(cont_deduped_by_timestamps: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    logging.info("flattening timestamps in the continuous table (from the start-end double timestamps to a single `admin_dttm`)...")
    q = """
    -- pivot to longer and create the fill-in [Started] and [Restarted] MAR actions from 'starttime'
    WITH longer as (
        PIVOT_LONGER cont_deduped_by_timestamps
        ON starttime, endtime
        INTO NAME _timestamp_type
            VALUE admin_dttm
    )
    FROM longer
    SELECT hospitalization_id: hadm_id
        , med_order_id: linkorderid
        , med_name: label
        , med_category
        , admin_dttm
        , _rn: ROW_NUMBER() OVER (PARTITION BY hospitalization_id, med_order_id, med_category ORDER BY admin_dttm)
        , _mar_action_name: CASE 
            WHEN statusdescription = 'Bolus' AND _timestamp_type = 'starttime'
                THEN '[Started Bolus]'
            WHEN statusdescription = 'Bolus' AND _timestamp_type = 'endtime'
                THEN '[Finished Bolus]'
            WHEN _timestamp_type = 'starttime' 
                AND _rn = 1
                THEN '[Started]' 
            WHEN _timestamp_type = 'starttime' 
                AND _rn > 1
                THEN '[Restarted]'
            ELSE statusdescription END
        , med_dose: rate
        , med_dose_unit: rateuom
        , med_route_name
        , med_route_category
    ORDER BY hospitalization_id, med_order_id, med_category, admin_dttm, _mar_action_name
    """
    cont_flattened = duckdb.sql(q)
    return cont_flattened

def mar_action_dedup_mapping(cont_flattened: duckdb.DuckDBPyRelation) -> pd.DataFrame:
    return pd.read_csv(mapping_path_finder("mar_action_dedup"))

def cont_deduped(cont_flattened: duckdb.DuckDBPyRelation, mar_action_dedup_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    logging.info("removing duplicated timestamps that naturally occur as a result of flattening timestamps...")
    q = """
    WITH base as (
        FROM cont_flattened
        SELECT *
            , mar_action_names: STRING_AGG(_mar_action_name, ', ' ORDER BY _mar_action_name) 
                OVER (PARTITION BY hospitalization_id, med_order_id, med_category, admin_dttm)
    ), mapped as (
        FROM base b
        LEFT JOIN mar_action_dedup_mapping m USING (mar_action_names)
        SELECT * 
    )
    FROM mapped
    SELECT hospitalization_id
        , med_order_id
        , med_name
        , med_category
        , admin_dttm
        , mar_action_name: COALESCE(mar_action_name_to_display, _mar_action_name)
        , mar_action_category: CASE
            WHEN 'ChangeDose/Rate' in mar_action_name OR 'Bolus' in mar_action_name
                THEN 'dose_change'
            WHEN mar_action_name in ('[Started]', '[Restarted]') THEN 'start'
            WHEN mar_action_name in ('FinishedRunning', 'FinishedRunning, FinishedRunning', 'Stopped', 'Paused', 'Bolus') THEN 'stop'
            WHEN '[Restarted]' in mar_action_name THEN 'going'
            ELSE 'other' END
        , med_dose: CASE
            WHEN mar_action_category in ('stop') THEN 0.0
            ELSE med_dose END
        , med_dose_unit
        , med_route_name
        , med_route_category
    WHERE mar_action_name_w_correct_dose = _mar_action_name
        OR mar_action_name_w_correct_dose IS NULL -- when there are no duplicates
    ORDER BY hospitalization_id, med_order_id, med_category, admin_dttm
    """
    cont_deduped = duckdb.sql(q)#.df()
    logging.info(f"Removed {len(cont_flattened) - len(cont_deduped)} rows")
    return cont_deduped

def cont_med_group_mapping() -> pd.DataFrame:
    q = """
    FROM 'data/mcide/clif_medication_admin_continuous_med_categories.csv'
    SELECT med_category
        , med_group
        , n: COUNT(*) OVER (PARTITION BY med_category)
        -- down-rank mapping to med_group that contain 'inhaled' (case-insensitive)
        , rn: ROW_NUMBER() OVER (
            PARTITION BY med_category
            ORDER BY CASE WHEN med_group ILIKE '%inhale%' THEN 9 ELSE 1 END
        )
    QUALIFY rn = 1
    ORDER BY n DESC, med_category, rn
    """
    cont_med_group_mapping = duckdb.sql(q).df()
    return cont_med_group_mapping

@tag(property="final")
def cont_cast_w_med_group(cont_deduped: duckdb.DuckDBPyRelation, cont_med_group_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    q = """
    FROM cont_deduped
    LEFT JOIN cont_med_group_mapping USING (med_category)
    SELECT hospitalization_id: CAST(hospitalization_id AS VARCHAR)
        , med_order_id: CAST(med_order_id AS VARCHAR)
        , med_name: CAST(med_name AS VARCHAR)
        , med_category: CAST(med_category AS VARCHAR)
        , admin_dttm: CAST(admin_dttm AS TIMESTAMP)
        , mar_action_name: CAST(mar_action_name AS VARCHAR)
        , mar_action_category: CAST(mar_action_category AS VARCHAR)
        , med_dose: CAST(med_dose AS FLOAT)
        , med_dose_unit: CAST(med_dose_unit AS VARCHAR)
        , med_group: CAST(med_group AS VARCHAR)
        , med_route_name: CAST(med_route_name AS VARCHAR)
        , med_route_category: CAST(med_route_category AS VARCHAR)
    """
    cont_cast_w_med_group = duckdb.sql(q).df()
    # check length is not altered
    cont_cast_w_med_group['admin_dttm'] = convert_tz_to_utc(cont_cast_w_med_group['admin_dttm'])
    assert len(cont_deduped) == len(cont_cast_w_med_group), 'length altered after casting and mapping med_group'
    return cont_cast_w_med_group

@datasaver()
def save_cont(cont_cast_w_med_group: duckdb.DuckDBPyRelation) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(cont_cast_w_med_group, "medication_admin_continuous")
    
    metadata = {
        "table_name": "medication_admin_continuous"
    }
    
    logging.info("output saved to a parquet file, everything completed for the medication_admin_continuous table!")
    return metadata

@tag(property="test")
def cont_schema_tested(cont_cast_w_med_group: duckdb.DuckDBPyRelation) -> bool | pa.errors.SchemaErrors:
    try:
        CONT_SCHEMA.validate(cont_cast_w_med_group, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@tag(property="test")
def intm_schema_tested(intm_cast_w_med_group: duckdb.DuckDBPyRelation) -> bool | pa.errors.SchemaErrors:
    try:
        INTM_SCHEMA.validate(intm_cast_w_med_group, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

def _main():
    logging.info("starting to build clif medication_admin_continuous and medication_admin_intermittent tables -- ")
    from hamilton import driver
    import src.tables.medication_admin as medication_admin
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(medication_admin)
        .build()
    )
    dr.execute(["save_cont", "save_intm"])
    
def _test():
    logging.info("testing all...")
    from hamilton import driver
    import src.tables.medication_admin as medication_admin
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(medication_admin)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    print(output)
    return output
    
if __name__ == "__main__":
    _main()