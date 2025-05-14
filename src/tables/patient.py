# src/tables/patient.py
import numpy as np
import pandas as pd
import duckdb
import logging
from importlib import reload
import src.utils
from hamilton.function_modifiers import tag, datasaver, config, check_output
import pandera as pa
import json
# reload(src.utils)
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
    mimic_table_pathfinder,
)

setup_logging()

PATIENT_COL_NAMES = [
    "patient_id", "race_name", "race_category", "ethnicity_name", "ethnicity_category",
    "sex_name", "sex_category", "birth_date", "death_dttm", "language_name", "language_category"
]

LANGUAGE_MAPPER = {
    'English': 'English', 
    'Spanish': 'Spanish', 
    'Russian': 'Russian', 
    'Chinese': 'Chinese', 
    'Kabuverdianu': 'Portuguese', 
    'Portuguese': 'Portuguese', 
    'Haitian': 'Haitian Creole', 
    'Other': 'Other and unspecified languages', 
    'Vietnamese': 'Vietnamese', 
    'Italian': 'Italian', 
    'Modern Greek (1453-)': 'Greek', 
    None: 'Unknown or NA',  # NOTE: test this -- tested
    'Arabic': 'Arabic', 
    'American Sign Language': 'Sign Language', 
    'Persian': 'Persian', 
    'Polish': 'Polish', 
    'Korean': 'Korean', 
    'Thai': 'Thai, Lao, or other Tai-Kadai languages', 
    'Khmer': 'Khmer', 
    'Amharic': 'Amharic, Somali, or other Afro-Asiatic languages', 
    'Hindi': 'Hindi', 
    'French': 'French', 
    'Somali': 'Amharic, Somali, or other Afro-Asiatic languages', 
    'Japanese': 'Japanese', 
    'Bengali': 'Bengali', 
    'Armenian': 'Armenian'
}

def _permissible_language_categories():
    # language_df = pd.read_excel("data/data_models/language_category.1.xlsx")
    language_mcide = pd.read_csv("data/mcide/clif_patient_language_categories.csv")
    return language_mcide["language_category"].unique()

PERMISSIBLE_RACE_CATEGORIES = [
    "Black or African American", "White", "American Indian or Alaska Native", 
    "Asian", "Native Hawaiian or Other Pacific Islander", "Unknown", "Other"
]

PERMISSIBLE_ETHNICITY_CATEGORIES = [
    "Hispanic", "Not Hispanic", "Unknown"
]


CLIF_PATIENT_SCHEMA = pa.DataFrameSchema(
    {
        "patient_id": pa.Column(str, nullable=False),
        "race_name": pa.Column(str, nullable=True),
        "race_category": pa.Column(str, nullable=False),
        "ethnicity_name": pa.Column(str, nullable=True),
        "ethnicity_category": pa.Column(str, nullable=False),
        "sex_name": pa.Column(str, nullable=True),
        "sex_category": pa.Column(str, nullable=True),
        "birth_date": pa.Column(pa.dtypes.Timestamp, nullable=True),
        "death_dttm": pa.Column(pd.DatetimeTZDtype(unit="us", tz="UTC"), nullable=True),
        "language_name": pa.Column(str, nullable=True),
        "language_category": pa.Column(str, nullable=False, checks = pa.Check.isin(_permissible_language_categories()))
    }
)

def _check_consistency_over_encounters(df, col: str = "race_category"):
    '''
    Check if a patient has inconsistent values (e.g. races) over different encounters.
    '''
    race_counts = df.groupby('patient_id')[col].nunique()
    multi_race_indices = race_counts[race_counts > 1].index
    multi_race_encounters = df[
        df['patient_id'].isin(multi_race_indices)
        ]
    return multi_race_encounters

def _report_nonunique_race_ethn_across_encounters(df):
    '''
    Report patients with non-unique race and ethnicity across encounters.
    # TODO: should be called like this
    
    # report patients with non-unique race and ethnicity across encounters
    n1, n2 = _report_nonunique_race_ethn_across_encounters(race_ethn_ranked)
    '''
    query = """
    SELECT 
        patient_id,
        FIRST(race_category) as race_category,
        FIRST(ethnicity_category) as ethnicity_category,
        COUNT(DISTINCT race_category) AS unique_race_count,
        COUNT(DISTINCT ethnicity_category) AS unique_ethn_count
    FROM df
    /* WHERE race_category NOT IN ('Other', 'Unknown') OR ethnicity_category NOT IN ('Other', 'Unknown') */
    GROUP BY patient_id
    HAVING unique_race_count > 1 OR unique_ethn_count > 1
    """
    df2 = duckdb.query(query).df()
    n1 = (df2['unique_race_count'] > 1).sum() 
    n2 = (df2['unique_ethn_count'] > 1).sum()
    n_total = df.patient_id.nunique()
    logging.info(f"number of patients with non-unique race: {n1} ({n1/n_total:.2%})")
    logging.info(f"number of patients with non-unique ethnicity: {n2} ({n2/n_total:.2%})")
    return n1, n2

def race_ethnicity_mapping() -> pd.DataFrame:
    return load_mapping_csv("race_ethnicity")

def race_mapper(race_ethnicity_mapping: pd.DataFrame) -> dict:
    race_mapper = construct_mapper_dict(race_ethnicity_mapping, "mimic_race", "race")
    race_mapper[None] = "Unknown"
    return race_mapper

def ethnicity_mapper(race_ethnicity_mapping: pd.DataFrame) -> dict:
    ethnicity_mapper = construct_mapper_dict(race_ethnicity_mapping, "mimic_race", "ethnicity")
    ethnicity_mapper[None] = "Unknown"
    return ethnicity_mapper

def sex_translated() -> pd.DataFrame:
    logging.info("fetching and processing the first component of the patient table: sex/gender data...")
    # fetch sex (intended in CLIF) / gender (available in MIMIC) from mimic_patients
    query = f"""
    SELECT 
        subject_id as patient_id,
        gender as sex_name,
        CASE WHEN gender = 'M' THEN 'Male'
             WHEN gender = 'F' THEN 'Female'
             ELSE NULL
        END AS sex_category
    FROM '{mimic_table_pathfinder("patients")}'
    """
    return duckdb.query(query).df()

def race_ethn_translated(race_mapper: dict, ethnicity_mapper: dict) -> pd.DataFrame:
    logging.info("fetching and processing the second component of the patient table: race and ethnicity data...")
    query = f"""
    SELECT 
        subject_id as patient_id, 
        hadm_id as hospitalization_id,
        race as race_name, 
        race as ethnicity_name,
        admittime as admittime
    FROM '{mimic_table_pathfinder("admissions")}'
    """
    df = duckdb.query(query).df()
    df["race_category"] = df["race_name"].map(race_mapper)
    df["ethnicity_category"] = df["ethnicity_name"].map(ethnicity_mapper)
    return df

def race_ethn_uninformative(race_ethn_translated: pd.DataFrame) -> pd.DataFrame:
    query = """
    SELECT 
        patient_id,
        hospitalization_id,
        race_name,
        race_category,
        ethnicity_name,
        ethnicity_category,
        admittime,
        /* mark patients with 'truly' uninformative race and ethnicity, defined by both race and ethnicity being "Other" or "Unknown". */
        CASE
            WHEN (race_category IN ('Other', 'Unknown')) AND (ethnicity_category IN ('Other', 'Unknown')) THEN 1
            ELSE 0
        END AS true_uninfo 
    FROM race_ethn_translated
    """
    return duckdb.query(query).df()

def race_ethn_ranked(race_ethn_uninformative: pd.DataFrame) -> pd.DataFrame:
    query = """
    SELECT 
        patient_id, 
        race_name,
        race_category,
        ethnicity_name,
        ethnicity_category,
        COUNT(*) AS count,
        MAX(admittime) AS most_recent,
        true_uninfo,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id 
            ORDER BY 
                count DESC, 
                true_uninfo,
                most_recent DESC
                ) 
            AS rn /* row number */
    FROM race_ethn_uninformative
    GROUP BY patient_id, race_name, race_category, ethnicity_name, ethnicity_category, true_uninfo
    """
    return duckdb.query(query).df()

def race_ethn_cleaned(race_ethn_ranked: pd.DataFrame) -> pd.DataFrame:
    query = """
    SELECT 
        patient_id,
        race_name,
        race_category,
        ethnicity_name,
        ethnicity_category
    FROM race_ethn_ranked
    WHERE rn = 1
    """
    return duckdb.query(query).df()

@tag(property="test")
def test_no_null_race_ethn_categories(race_ethn_cleaned: pd.DataFrame) -> bool:
    return race_ethn_cleaned.race_category.isna().sum() == 0 and race_ethn_cleaned.ethnicity_category.isna().sum() == 0

def death_extracted() -> pd.DataFrame:
    logging.info("fetching and processing the third component: death data...")
    query = f"""
    SELECT 
        subject_id as patient_id,
        deathtime as death_dttm
    FROM '{mimic_table_pathfinder("admissions")}'
    """
    df = duckdb.query(query).df()
    df.dropna(subset=["death_dttm"], inplace=True)
    df.drop_duplicates(subset=["patient_id"], inplace=True)
    # TODO: add out of hospital death
    return df

def language_translated() -> pd.DataFrame:
    logging.info("fetching and processing the fourth component: language data...")
    query = f"""
    SELECT 
        subject_id as patient_id,
        FIRST(language) as language_name
    FROM '{mimic_table_pathfinder("admissions")}'
    GROUP BY subject_id
    """
    df = duckdb.query(query).df()
    df["language_category"] = df["language_name"].map(LANGUAGE_MAPPER)
    return df

def merged(
    race_ethn_cleaned: pd.DataFrame, 
    sex_translated: pd.DataFrame, 
    death_extracted: pd.DataFrame, 
    language_translated: pd.DataFrame
    ) -> pd.DataFrame:
    logging.info("merging the four components...")
    query = """
    SELECT 
        CAST(patient_id AS string) as patient_id,
        race_name,
        COALESCE(race_category, 'Unknown') as race_category,
        ethnicity_name,
        COALESCE(ethnicity_category, 'Unknown') as ethnicity_category,
        sex_name,
        COALESCE(sex_category, 'Unknown') as sex_category,
        CAST(NULL AS date) as birth_date,
        CAST(death_dttm AS timestamp) as death_dttm,
        language_name,
        COALESCE(language_category, 'Unknown or NA') as language_category
    FROM race_ethn_cleaned as race
    FULL JOIN sex_translated as sex USING (patient_id)
    FULL JOIN death_extracted as death USING (patient_id)
    FULL JOIN language_translated as language USING (patient_id)
    """
    df = duckdb.query(query).df()
    df["death_dttm"] = convert_tz_to_utc(df["death_dttm"])
    return df

@tag(property="final")
def duplicates_removed(merged: pd.DataFrame) -> pd.DataFrame:
    return merged.drop_duplicates()

@tag(property="test")
def schema_tested(duplicates_removed: pd.DataFrame) -> bool | pa.errors.SchemaErrors:
    logging.info("testing schema...")
    df = duplicates_removed
    try:
        CLIF_PATIENT_SCHEMA.validate(df, lazy=True)
        return True
    except pa.errors.SchemaErrors as exc:
        logging.error(json.dumps(exc.message, indent=2))
        logging.error("Schema errors and failure cases:")
        logging.error(exc.failure_cases)
        logging.error("\nDataFrame object that failed validation:")
        logging.error(exc.data)
        return exc

@datasaver()
def save(duplicates_removed: pd.DataFrame) -> dict:
    save_to_rclif(duplicates_removed, "patient")
    logging.info("output saved to a parquet file, everything completed for the patient table!")
    
    metadata = {
        "table_name": "patient"
    }
    
    return metadata

def _main():
    logging.info("starting to build clif patient table -- ")
    from hamilton import driver
    import src.tables.patient as patient
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(patient)
        # .with_cache()
        .build()
    )
    dr.execute(["save"])
   
def _test():
    logging.info("testing all...")
    from hamilton import driver
    import src.tables.patient as patient
    setup_logging()
    dr = (
        driver.Builder()
        .with_modules(patient)
        .build()
    )
    all_nodes = dr.list_available_variables()
    test_nodes = [node.name for node in all_nodes if 'test' == node.tags.get('property')]
    output = dr.execute(test_nodes)
    return output

if __name__ == "__main__":
    _main()