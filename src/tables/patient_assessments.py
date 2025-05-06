# src/tables/patient_assessments.py
import numpy as np
import pandas as pd
import logging
import duckdb
from importlib import reload
import src.utils
# reload(src.utils)
from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
    get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
    convert_and_sort_datetime, setup_logging, con, REPO_ROOT, mimic_table_pathfinder, \
    convert_tz_to_utc
from hamilton.function_modifiers import tag, datasaver, config, cache, dataloader
import pandera as pa
from pandera.dtypes import Float32
import json
setup_logging()

PA_COL_NAMES = [
    "hospitalization_id", "recorded_dttm", "assessment_name", "assessment_category",
    "assessment_group", "numerical_value", "categorical_value", "text_value"
    ]

PA_SCHEMA = pa.DataFrameSchema(
    {
        "hospitalization_id": pa.Column(str, nullable=False),
        "recorded_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=False),
        "assessment_name": pa.Column(str, nullable=False),
        "assessment_category": pa.Column(str, nullable=False),
        "assessment_group": pa.Column(str, nullable=False),
        "numerical_value": pa.Column(Float32, nullable=True),
        "categorical_value": pa.Column(str, nullable=True),
        "text_value": pa.Column(str, nullable=True),
    }
)

def hadmid_to_stayid() -> pd.DataFrame:
    query = f"""
    SELECT DISTINCT
        hadm_id, stay_id
    FROM '{mimic_table_pathfinder("icustays")}'
    """
    return con.execute(query).fetchdf()

def gcs_fetched(hadmid_to_stayid: pd.DataFrame) -> pd.DataFrame:
    logging.info("executing official MIMIC script to fetch GCS data...")
    gcs_sql_path = REPO_ROOT / 'src/tables/patient_assessments_gcs.sql'
    with open(str(gcs_sql_path), 'r') as file:
        gcs_sql_script = file.read()
    query = gcs_sql_script.format(chartevents = mimic_table_pathfinder("chartevents"))
    gcs = con.execute(query).fetchdf()
    
    logging.info("pivoting and cleaning GCS data...")
    gcs_c = pd.merge(
        gcs, hadmid_to_stayid, on = "stay_id", how = "left"
    )
    gcs_cl = pd.melt(
        gcs_c, id_vars = ["subject_id", "hadm_id", "charttime"], 
        value_vars = ["gcs", "gcs_motor", "gcs_verbal", "gcs_eyes"],
        var_name = "assessment_name", value_name = "numerical_value")
    gcs_cl["assessment_category"] = np.where(
        gcs_cl["assessment_name"].isin(["gcs", "gcs_eyes"]),
        gcs_cl["assessment_name"].map({"gcs": "gcs_total", "gcs_eyes": "gcs_eye"}),
        gcs_cl["assessment_name"]
    )
    gcs_cl.dropna(subset = ["hadm_id"], inplace = True)
    gcs_cl["hadm_id"] = gcs_cl["hadm_id"].astype(int).astype("string")
    gcs_clf = rename_and_reorder_cols(gcs_cl, {"charttime": "recorded_dttm"}, PA_COL_NAMES)
    return gcs_clf


def rass_fetched() -> pd.DataFrame:
    logging.info("fetching RASS data...")
    rass_events = fetch_mimic_events([228096])
    rass_events = convert_and_sort_datetime(rass_events)
    rass_events['numerical_value'] = rass_events['value'].str.slice(0,3).astype(float)
    rass_events["assessment_name"] = "Richmond-RAS Scale"
    rass_events["assessment_category"] = "RASS"
    rass_events_clean = rename_and_reorder_cols(
        df = rass_events, 
        rename_mapper_dict = {
            "time": "recorded_dttm", "value": "text_value"
        }, 
        new_col_order = PA_COL_NAMES
    )
    rass_events_clean.drop_duplicates(
        subset=["hospitalization_id", "recorded_dttm", "assessment_category", "numerical_value"], inplace = True)
    return rass_events_clean

def braden_fetched() -> pd.DataFrame:
    logging.info("fetching Braden data...")
    braden = fetch_mimic_events([224054, 224055, 224056, 224057, 224058, 224059])
    query = f"""
    PIVOT braden
    ON label
    USING FIRST(value)
    GROUP BY hadm_id, time
    """
    braden_w = con.execute(query).fetchdf()
    query = f"""
    SELECT
        hadm_id, time, 
        CASE
            WHEN "Braden Activity" = 'Bedfast' THEN 1
            WHEN "Braden Activity" = 'Chairfast' THEN 2
            WHEN "Braden Activity" = 'Walks Occasionally' THEN 3
            WHEN "Braden Activity" = 'Walks Frequently' THEN 4
            ELSE NULL
            END AS braden_activity,
        CASE
            WHEN "Braden Friction/Shear" = 'Problem' THEN 1
            WHEN "Braden Friction/Shear" = 'Potential Problem' THEN 2
            WHEN "Braden Friction/Shear" = 'No Apparent Problem' THEN 3
            ELSE NULL
            END AS braden_friction,
        CASE
            WHEN "Braden Mobility" = 'Completely Immobile' THEN 1
            WHEN "Braden Mobility" = 'Very Limited' THEN 2
            WHEN "Braden Mobility" = 'Slight Limitations' THEN 3
            WHEN "Braden Mobility" = 'No Limitations' THEN 4
            ELSE NULL
            END AS braden_mobility,
        CASE
            WHEN "Braden Moisture" = 'Consistently Moist' THEN 1
            WHEN "Braden Moisture" = 'Moist' THEN 2
            WHEN "Braden Moisture" = 'Occasionally Moist' THEN 3
            WHEN "Braden Moisture" = 'Rarely Moist' THEN 4
            ELSE NULL
            END AS braden_moisture,
        CASE
            WHEN "Braden Nutrition" = 'Very Poor' THEN 1
            WHEN "Braden Nutrition" = 'Probably Inadequate' THEN 2
            WHEN "Braden Nutrition" = 'Adequate' THEN 3
            WHEN "Braden Nutrition" = 'Excellent' THEN 4
            ELSE NULL
            END AS braden_nutrition,
        CASE
            WHEN "Braden Sensory Perception" = 'Completely Limited' THEN 1
            WHEN "Braden Sensory Perception" = 'Very Limited' THEN 2
            WHEN "Braden Sensory Perception" = 'Slight Impairment' THEN 3
            WHEN "Braden Sensory Perception" = 'No Impairment' THEN 4
            ELSE NULL
            END AS braden_sensory,
        (braden_activity + braden_friction + braden_mobility + braden_moisture + braden_nutrition + braden_sensory) AS braden_total
    FROM braden_w
    """
    braden_wc = con.execute(query).fetchdf()
    # unpivot (from wide to long) for the numerical values
    query = f"""
    UNPIVOT braden_wc
    ON COLUMNS('braden_')
    INTO
        NAME assessment_category
        VALUE numerical_value;
    """
    braden_wcl = con.execute(query).fetchdf()
    # unpivot (from wide to long) for the *categorical* values
    query = f"""
    UNPIVOT (
        SELECT
            hadm_id, time,
            "Braden Activity" AS braden_activity,
            "Braden Friction/Shear" AS braden_friction,
            "Braden Mobility" AS braden_mobility,
            "Braden Moisture" AS braden_moisture,
            "Braden Nutrition" AS braden_nutrition,
            "Braden Sensory Perception" AS braden_sensory
        FROM braden_w
    )
    ON COLUMNS('braden_')
    INTO
        NAME assessment_category
        VALUE categorical_value;
    """
    braden_wl = con.execute(query).fetchdf()
    # merging numerical_value and categorical_value
    braden_m = pd.merge(braden_wcl, braden_wl, on=["hadm_id", "time", "assessment_category"], how="outer")
    query = f"""
    SELECT
        hadm_id as hospitalization_id,
        time as recorded_dttm,
        CASE
            WHEN assessment_category = 'braden_total' THEN 'COMPUTED FROM SUB-SCORES; NOT ORIGINALLY AVAILABLE IN MIMIC-IV'
            WHEN assessment_category = 'braden_activity' THEN 'Braden Activity'
            WHEN assessment_category = 'braden_friction' THEN 'Braden Friction/Shear'
            WHEN assessment_category = 'braden_mobility' THEN 'Braden Mobility'
            WHEN assessment_category = 'braden_moisture' THEN 'Braden Moisture'
            WHEN assessment_category = 'braden_nutrition' THEN 'Braden Nutrition'
            WHEN assessment_category = 'braden_sensory' THEN 'Braden Sensory Perception'
            ELSE assessment_category
            END AS assessment_name,
        assessment_category,
        numerical_value,
        categorical_value
    FROM braden_m
    """
    braden_mf = con.execute(query).fetchdf()
    return braden_mf

def cam_extracted() -> pd.DataFrame:
    logging.info("fetching CAM data...")
    return fetch_mimic_events(
        [228300, 228337, 229326, 228301, 228336, 229325, 228302, 228334, 228303, 228335, 229324]
    )

def cam_wide(cam_extracted: pd.DataFrame) -> pd.DataFrame:
    query = f"""
    PIVOT (
        SELECT
            hadm_id, time,
            itemid,
            CASE
                WHEN itemid = 228300 THEN 'CAM-ICU MS Change'
                ELSE label
            END AS label,
            value
        FROM cam_extracted
    )
    ON label
    USING FIRST(value)
    GROUP BY hadm_id, time
    """
    return con.execute(query).fetchdf()

def cam_total_computed(cam_wide: pd.DataFrame) -> pd.DataFrame:
    '''
    delirious = Yes if 
    (mental = True) AND (inattention = True) AND (LOC OR thinking = True)
    which implies that delirious = No
    if mental = False OR inattention = False OR (LOC = False AND thinking = False)
    '''
    query = f"""
    SELECT
        hadm_id, time, COLUMNS('CAM-ICU'),
        coalesce("CAM-ICU Altered LOC", "CAM-ICU RASS LOC") AS loc,
        CASE
            WHEN ("CAM-ICU MS Change" LIKE '%Yes%') 
                AND ("CAM-ICU Inattention" LIKE '%Yes%') 
                AND (("CAM-ICU Disorganized thinking" LIKE '%Yes%') OR (loc LIKE '%Yes%'))
                THEN 'Positive'
            WHEN ("CAM-ICU MS Change" LIKE '%No%') 
                OR ("CAM-ICU Inattention" LIKE '%No%') 
                OR (("CAM-ICU Disorganized thinking" LIKE '%No%') AND (loc LIKE '%No%'))
                THEN 'Negative'
            ELSE NULL   
            END AS cam_total
    FROM cam_wide
    """
    return con.execute(query).fetchdf()
 
def cam_long(cam_total_computed: pd.DataFrame) -> pd.DataFrame:
    # unpivot (from wide to long) 
    query = f"""
    UNPIVOT (
        SELECT
            COLUMNS(* EXCLUDE ('loc'))
        FROM cam_total_computed
    )
    ON COLUMNS('CAM-ICU|cam')
    INTO
        NAME assessment_name
        VALUE categorical_value;
    """
    return con.execute(query).fetchdf()

def cam_fetched(cam_long: pd.DataFrame) -> pd.DataFrame:
    query = f"""
    SELECT
        hadm_id as hospitalization_id,
        time as recorded_dttm,
        CASE
            WHEN assessment_name = 'CAM-ICU MS Change' THEN 'cam_mental'
            WHEN assessment_name = 'cam_total' THEN assessment_name
            WHEN assessment_name = 'CAM-ICU Inattention' THEN 'cam_inattention'
            WHEN assessment_name = 'CAM-ICU Disorganized thinking' THEN 'cam_thinking'
            WHEN assessment_name in ('CAM-ICU RASS LOC', 'CAM-ICU Altered LOC') THEN 'cam_loc'
            ELSE NULL
            END AS assessment_category,
        CASE
            WHEN assessment_name = 'cam_total' THEN 'COMPUTED FROM SUB-SCORES; NOT ORIGINALLY AVAILABLE IN MIMIC-IV'
            ELSE assessment_name
            END AS assessment_name,
        categorical_value
    FROM cam_long
    """
    return con.execute(query).fetchdf()
    
def sbt_id_to_category_mapper() -> dict:
    return {
        224717: "sbt_delivery_pass_fail",
        224833: "sbt_fail_reason",
        224716: "sbt_fail_reason"
    }

def sbt_extracted() -> pd.DataFrame:
    logging.info("fetching SBT data...")
    return fetch_mimic_events([224717, 224833, 224716]) 

def sbt_translated(sbt_id_to_category_mapper: dict, sbt_extracted: pd.DataFrame) -> pd.DataFrame:
    return sbt_extracted.assign(
        assessment_category = lambda x: x["itemid"].map(sbt_id_to_category_mapper)
    )
   
def sbt_fetched(sbt_translated: pd.DataFrame) -> pd.DataFrame:
    query = f"""
    SELECT
        CAST(hadm_id as VARCHAR) as hospitalization_id,
        time as recorded_dttm,
        label as assessment_name,
        assessment_category,
        CAST(NULL as FLOAT) as numerical_value,
        CASE WHEN assessment_category = 'sbt_delivery_pass_fail' 
            THEN (CASE WHEN value = 'Yes' THEN 'Pass' 
                WHEN value = 'No' THEN 'Fail' 
                ELSE NULL END) 
            ELSE NULL
            END AS categorical_value,
        CASE WHEN assessment_category = 'sbt_fail_reason' 
            THEN value ELSE NULL
            END AS text_value
    FROM sbt_translated
    """
    return con.execute(query).fetchdf()

@tag(property="test")
def sbt_tested(sbt_fetched: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    assessment_category_schema = pa.SeriesSchema(
        str, 
        checks=[pa.Check.unique_values_eq(['sbt_delivery_pass_fail', 'sbt_fail_reason'])], 
        nullable=False
    )
    
    categorical_value_schema = pa.SeriesSchema(
        str, 
        checks=[pa.Check.unique_values_eq(['Pass', 'Fail'])], 
        nullable=True
    )
    
    logging.info("testing schema...")
    try:
        assessment_category_schema.validate(sbt_fetched["assessment_category"], lazy=True)
        categorical_value_schema.validate(sbt_fetched["categorical_value"], lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc
 
def pa_category_to_group_mapper() -> dict:
    pa_mcide_url = "https://raw.githubusercontent.com/clif-consortium/CLIF/main/mCIDE/clif_patient_assessment_categories.csv"
    pa_mcide_mapping = pd.read_csv(pa_mcide_url)
    pa_category_to_group_mapper = dict(
        zip(pa_mcide_mapping["assessment_category"], pa_mcide_mapping["assessment_group"]))
    return pa_category_to_group_mapper

@tag(property="final")
def merged_and_cleaned(
    pa_category_to_group_mapper: dict,
    gcs_fetched: pd.DataFrame, 
    rass_fetched: pd.DataFrame, 
    braden_fetched: pd.DataFrame, 
    cam_fetched: pd.DataFrame, 
    sbt_fetched: pd.DataFrame) -> pd.DataFrame:
    logging.info("merging all of the above...")
    df = pd.concat([gcs_fetched, rass_fetched, braden_fetched, cam_fetched, sbt_fetched])
    logging.info("converting column dtypes...")
    df["hospitalization_id"] = df["hospitalization_id"].astype("string")
    df["categorical_value"] = df["categorical_value"].astype("string")
    df["recorded_dttm"] = pd.to_datetime(df["recorded_dttm"])
    df["recorded_dttm"] = convert_tz_to_utc(df["recorded_dttm"])
    df["assessment_group"] = df["assessment_category"].map(pa_category_to_group_mapper)
    return df

@tag(property="test")
def schema_tested(merged_and_cleaned: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logging.info("testing schema...")
    try:
        PA_SCHEMA.validate(merged_and_cleaned, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc
    
@datasaver()
def save(merged_and_cleaned: pd.DataFrame) -> dict:
    logging.info("saving to rclif...")
    save_to_rclif(merged_and_cleaned, "patient_assessments")
    
    metadata = {
        "table_name": "patient_assessments"
    }
    
    logging.info("output saved to a parquet file, everything completed for the patient assessments table!")
    return metadata

def _main():
    logging.info("starting to build clif patient assessments table -- ")
    from hamilton import driver
    import src.tables.patient_assessments as patient_assessments
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(patient_assessments)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])

def _test():
    logging.info("testing all...")
    from hamilton import driver
    import src.tables.patient_assessments as patient_assessments
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(patient_assessments)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    print(output)
    return output

if __name__ == "__main__":
    _main()

