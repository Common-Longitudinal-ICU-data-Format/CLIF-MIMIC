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

setup_logging()

SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "med_order_id": pa.Column(str, nullable=False),
        "admin_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "med_name": pa.Column(str, nullable=False),
        "med_category": pa.Column(str, nullable=False),
        "mar_action_name": pa.Column(str, nullable=False),
        "mar_action_category": pa.Column(str, nullable=False),
        "med_dose": pa.Column(Float32, nullable=False),
        "med_dose_unit": pa.Column(str, nullable=False),
        "med_route_name": pa.Column(str, nullable=False),
        "med_route_category": pa.Column(str, nullable=False),
    },  
    strict=True,
)


# FIXME: likely remove this
# MAC_MCIDE_URL = "https://raw.githubusercontent.com/clif-consortium/CLIF/main/mCIDE/clif_medication_admin_continuous_med_categories.csv"

def med_admin_mapping() -> pd.DataFrame:
    return load_mapping_csv("med_admin")

def med_item_ids(med_admin_mapping) -> pd.Series:
    logging.info("identifying relevant items from the mapping file...")

    return get_relevant_item_ids(
        mapping_df = med_admin_mapping, 
        decision_col = "decision", 
        excluded_labels = ["NO MAPPING", "UNSURE", "NOT AVAILABLE"]
        ) 

def med_events_extracted(med_item_ids) -> pd.DataFrame:
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

def med_route_mapped(med_events: pd.DataFrame, med_route_mapping: pd.DataFrame, med_route_mapping_by_id: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    logging.info("mapping med route...")
    q = """
    FROM med_events e
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
    logging.debug(f"len before: {len(med_events)}")
    logging.debug(f"len after: {len(med_route_mapped)}")
    assert len(med_route_mapped) == len(med_events), 'df length altered after mapping med route'
    return med_route_mapped

def mapped_and_augmented(med_route_mapped: duckdb.DuckDBPyRelation, med_admin_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
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
    LEFT JOIN med_admin_mapping m
        ON e.itemid = m.itemid
        AND m.decision IN ('BOTH', 'CONTINUOUS', 'INTERMITTENT')
    WHERE _duration_in_mins > 0 -- remove the few cases where duration is zero or negative
    ORDER BY hadm_id, starttime, linkorderid, med_category, endtime
    """
    mapped_and_augmented = duckdb.sql(query)
    if len(mapped_and_augmented) != 8511695:
        logging.warning(f'df length after augmentation and mapping is different from expected in last run')
    if mapped_and_augmented['_to_table'].isna().sum() != 0:
        logging.warning('there are still NAs in the column that determines the split to intermittent or continuous')
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

@tag(property="final")
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
    ORDER BY hospitalization_id, med_order_id, med_category, admin_dttm
    """
    intm_flattened = duckdb.sql(q)
    return intm_flattened

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

def cont_flattened(cont_reassembled: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
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
        , mar_action_name: CASE 
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
    ORDER BY hospitalization_id, med_order_id, med_category, admin_dttm, mar_action_name
    """
    cont_flattened = duckdb.sql(q)
    return cont_flattened

def mar_action_dedup_mapping(cont_flattened: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    return pd.read_csv(mapping_path_finder("mar_action_dedup"))

@tag(property="final")
def cont_deduped(cont_flattened: duckdb.DuckDBPyRelation, mar_action_dedup_mapping: pd.DataFrame) -> duckdb.DuckDBPyRelation:
    logging.info("removing 'duplicates' that naturally occur as a result of flattening timestamps...")
    q = """
    WITH base as (
        FROM cont_flattened
        SELECT *
            , mar_action_names: STRING_AGG(mar_action_name, ', ' ORDER BY mar_action_name) 
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
        , mar_action_name: COALESCE(mar_action_name_to_display, mar_action_name) 
        , mar_action_category: CASE
            WHEN 'ChangeDose/Rate' in mar_action_name OR 'Bolus' in mar_action_name
                THEN 'dose_change'
            WHEN mar_action_name in ('[Started]', '[Restarted]') THEN 'start'
            WHEN mar_action_name in ('FinishedRunning', 'FinishedRunning, FinishedRunning', 'Stopped', 'Paused', 'Bolus') THEN 'stop'
            WHEN '[Restarted]' in mar_action_name THEN 'going'
            ELSE 'other' END
        , med_dose
        , med_dose_unit
        , med_route_name
        , med_route_category
    WHERE mar_action_name_w_correct_dose = mar_action_name
        OR mar_action_name_w_correct_dose IS NULL -- when there are no duplicates
    ORDER BY hospitalization_id, med_order_id, med_category, admin_dttm
    """
    cont_deduped = duckdb.sql(q).df()
    logging.info(f"Removed {len(cont_flattened) - len(cont_deduped)} rows")
    return cont_deduped

def _prepare_for_timestamp_flattening(df: pd.DataFrame) -> pd.DataFrame:
    """
    [LIKELY DEPRECATED]
    Prepare the dataframe for timestamp flattening by creating lead and lag.
    """
    query = f"""
    SELECT subject_id, hadm_id
        , linkorderid
        , starttime, endtime 
        , LEAD(starttime) OVER (PARTITION BY hadm_id, linkorderid, med_category ORDER BY starttime) AS starttime_next
        , ROW_NUMBER() OVER (PARTITION BY hadm_id, linkorderid, med_category ORDER BY starttime) AS rn
        , starttime = MIN(starttime) OVER (PARTITION BY hadm_id, linkorderid, med_category) AS is_first_row
        , starttime = MAX(starttime) OVER (PARTITION BY hadm_id, linkorderid, med_category) AS is_last_row
        , statusdescription
        , LAG(statusdescription) OVER (PARTITION BY hadm_id, linkorderid, med_category ORDER BY starttime) AS statusdescription_prev
        , med_category
        , to_table
        , EXTRACT(EPOCH FROM (endtime - starttime)) / 60 AS duration_in_mins
        , amount / duration_in_mins AS rate_imputed
        , CONCAT(amountuom, '/min') AS rateuom_imputed
        , COALESCE(rate, rate_imputed) AS rate
        , COALESCE(rateuom, rateuom_imputed) AS rateuom
        , amount, amountuom
        , patientweight
        --, totalamount, totalamountuom, originalamount, originalrate
        , ordercategoryname, ordercategorydescription
        , itemid
        , label
        , item_class
    FROM df
    ORDER BY hadm_id, linkorderid, med_category, starttime, endtime
    """
    return duckdb.sql(query).df()

def _remove_crossovers(df: pd.DataFrame) -> pd.DataFrame:
    q = """
    SELECT *
        , LAG(endtime) OVER (PARTITION BY hadm_id, med_category, linkorderid ORDER BY starttime) AS endtime_prev
        , starttime < endtime_prev AS crossover
    FROM df
    QUALIFY crossover is False
    -- ORDER BY hadm_id, med_category, starttime, endtime, linkorderid
    ORDER BY hadm_id, starttime, endtime, linkorderid, med_category
    """
    crossovers = duckdb.sql(q).df()
    print(f"given there are only {len(crossovers)} cases, we might consider dropping them all")

def _flatten_timestamps_staging(df: pd.DataFrame, dose_name: Literal["rate", "amount"]) -> pd.DataFrame:
    query = f"""
    WITH l as (
        SELECT hadm_id, linkorderid, med_category, starttime, statusdescription, rate, rateuom, med_route_category, label
        FROM df -- WHERE linkorderid in (294375, 1771638)
    ), r as (
        SELECT hadm_id, linkorderid, med_category, endtime, statusdescription, rate, rateuom, med_route_category, label
        FROM df -- WHERE linkorderid in (294375, 1771638)
    )
    -- the base table after full join
    SELECT COALESCE(l.hadm_id, r.hadm_id) AS hospitalization_id
        , COALESCE(l.linkorderid, r.linkorderid) AS med_order_id
        , COALESCE(l.med_category, r.med_category) AS _med_category
        , COALESCE(l.starttime, r.endtime) AS admin_dttm
        , COALESCE(l.label, r.label) AS med_name
        , l.starttime, r.endtime
        , admin_dttm = MIN(admin_dttm) OVER (PARTITION BY hospitalization_id, med_order_id, _med_category) AS is_first_row
        , admin_dttm = MAX(admin_dttm) OVER (PARTITION BY hospitalization_id, med_order_id, _med_category) AS is_last_row
        , CASE WHEN is_first_row = 1 THEN COALESCE(r.statusdescription, '[Started]')
            ELSE COALESCE(r.statusdescription, '[Restarted]') END as mar_action_name
        , l.rate as med_dose
        , l.rateuom as med_dose_unit
    FROM l
    FULL JOIN r
    ON l.hadm_id = r.hadm_id
        AND l.linkorderid = r.linkorderid
        AND l.med_category = r.med_category
        AND l.starttime = r.endtime
    ORDER BY hospitalization_id, med_order_id, _med_category, admin_dttm
    """
    return duckdb.sql(query).df()

def _flatten_timestamps(df: pd.DataFrame, dose_name: Literal["rate", "amount"]) -> pd.DataFrame:
    """after staging. """
    
    query = f"""
    -- the additional Starting rows for 'Paused' and 'FinishedRunning' to be joined back
    WITH t1 as (
        SELECT hospitalization_id, med_order_id, med_name, med_category: _med_category
            , admin_dttm --, starttime, endtime
            , mar_action_name: '[Restarted]' 
            , med_dose
            , med_dose_unit
        FROM df
        WHERE mar_action_name in ('Paused', 'FinishedRunning') 
            AND med_dose IS NOT NULL
    ), t2 as (
        SELECT hospitalization_id, med_order_id, med_name, _med_category as med_category
            , admin_dttm --, starttime, endtime
            , mar_action_name
            , med_dose: CASE 
                WHEN mar_action_name in ('Paused', 'FinishedRunning') THEN 0
                ELSE med_dose
            END
            , med_dose_unit
        FROM df
        UNION 
        SELECT * FROM t1
        ORDER BY hospitalization_id, med_order_id, med_category, admin_dttm
    )
    SELECT hospitalization_id
        , med_order_id
        , med_name
        , med_category
        , admin_dttm
        , mar_action_name
        , mar_action_category: CASE
            WHEN mar_action_name in ('ChangeDose/Rate') THEN 'dose_change'
            WHEN mar_action_name in ('[Started]', '[Restarted]') THEN 'start'
            WHEN mar_action_name in ('FinishedRunning', 'Stopped', 'Paused', 'Bolus') THEN 'stop'
            ELSE 'other' END
        , med_dose: COALESCE(med_dose, 0)
        , med_dose_unit: LAST_VALUE(med_dose_unit IGNORE NULLS) OVER (
            PARTITION BY hospitalization_id, med_order_id, med_category 
            ORDER BY admin_dttm
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
    FROM t2
    """
    return duckdb.sql(query).df()

def _flatten_timestamps_v1(df: pd.DataFrame, dose_name: Literal["rate", "amount"]) -> pd.DataFrame:
    query = f"""
    SELECT hadm_id as hospitalization_id
        , linkorderid as med_order_id
        , label as med_name
        , med_category
        , starttime AS admin_dttm
        , CASE WHEN is_first_row = 1 THEN 'start' ELSE statusdescription_prev END AS mar_action_name
        , {dose_name} AS med_dose
        , {dose_name}uom as med_dose_unit
    FROM df
    UNION ALL
    SELECT hadm_id as hospitalization_id
        , linkorderid as med_order_id
        , label as med_name
        , med_category
        , endtime AS admin_dttm
        , statusdescription AS mar_action_name
        , 0 AS med_dose
        , {dose_name}uom as med_dose_unit
    FROM df
    WHERE is_last_row = 1 
    ORDER BY hadm_id, linkorderid, med_category, admin_dttm
    """
    return duckdb.sql(query).df()


def _main():
    logging.info("starting to build clif medication_admin_continuous table -- ")
    
    
if __name__ == "__main__":
    _main()