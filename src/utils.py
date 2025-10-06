# src/utils.py
import logging
import os, sys
import pandas as pd
import json
from pathlib import Path
from functools import cache
import duckdb  # type: ignore

con = duckdb.connect()
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

def load_config():
    json_path = SCRIPT_DIR / "../config/config.json"
    with open(json_path, "r") as file:
        config = json.load(file)
    print(f"loaded configuration from {json_path}")
    return config

config = load_config()

# Cache to store loaded tables
CURRENT_WORKSPACE = config["current_workspace"]
MIMIC_CSV_DIR = config[CURRENT_WORKSPACE]["mimic_csv_dir"]
MIMIC_PARQUET_DIR = config[CURRENT_WORKSPACE]["mimic_parquet_dir"]
MIMIC_PARQUET_DIR = f"{MIMIC_CSV_DIR}/parquet" if MIMIC_PARQUET_DIR == "" else MIMIC_PARQUET_DIR
CLIF_OUTPUT_DIR_NAME = config["clif_output_dir_name"]
CLIF_VERSION = config["clif_version"]
EXCLUDED_LABELS_DEFAULT = [
    "NO MAPPING",
    "UNSURE",
    "MAPPED ELSEWHERE",
    "SPECIAL CASE",
    "ALREADY MAPPED",
]  # NOTE: retire "ALREDAY MAPPED" in the future
HOSP_TABLES = [
    "admissions",
    "d_hcpcs",
    "d_icd_diagnoses",
    "d_icd_procedures",
    "d_labitems",
    "diagnoses_icd",
    "drgcodes",
    "emar_detail",
    "emar",
    "hcpcsevents",
    "labevents",
    "microbiologyevents",
    "omr",
    "patients",
    "pharmacy",
    "poe_detail",
    "poe",
    "prescriptions",
    "procedures_icd",
    "provider",
    "services",
    "transfers",
]
ICU_TABLES = [
    "caregivers",
    "chartevents",
    "d_items",
    "datetimeevents",
    "icustays",
    "ingredientevents",
    "inputevents",
    "outputevents",
    "procedureevents",
]

MIMIC_TABLES_NEEDED_FOR_CLIF = [
    "admissions",
    "d_labitems",
    "labevents",
    "patients",
    "transfers",
    "chartevents",
    "d_items",
    "datetimeevents",
    "icustays",
    "ingredientevents",
    "inputevents",
    "outputevents",
    "procedureevents"
]

CLIF_DTTM_FORMAT = "YYYY-MM-DD HH:MM:SS+00:00"

def setup_logging(log_file: str = "logs/etl.log"):
    """
    Sets up logging for the ETL pipeline.

    Args:
        log_file (str): Path to the log file. 
    """
    # create a log file at the given path if it does not exist yet
    if not Path(log_file).parent.exists():
        Path(log_file).parent.mkdir(parents=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    logging.info(f"initialized logging at {log_file}")

# -----------
#     I/O
# -----------

def create_dir_if_not_exists(dir_path: str):
    if not Path(dir_path).exists():
        Path(dir_path).mkdir(parents=True)

def parquet_stored_in_submodules() -> bool:
    '''
    Check if the parquet files are stored in subdirectories: "hosp" and "icu" (True)
    or stored together under the same directory (False).
    '''
    return Path(MIMIC_PARQUET_DIR + "/hosp").exists() and Path(MIMIC_PARQUET_DIR + "/icu").exists()

def mimic_table_pathfinder(table: str, data_format: str = "parquet") -> str:
    '''
    Return the path to a MIMIC table given the table name and data format.
    '''
    if table in HOSP_TABLES:
        module = "hosp"
    elif table in ICU_TABLES:
        module = "icu"
    else:
        raise ValueError(f"This table name for MIMIC is not found: {table}. Please check for typo.")    
    
    # first check the dir structure of the parquet path -- whether the parquet files are (1) stored together 
    # under the same directory or (2) seperated into two subdirectories by modules as is the case for csv
    if data_format == "parquet":
        # check if the parquet path contains two subdirectories called "hosp" and "icu"
        if parquet_stored_in_submodules():
            return f"{MIMIC_PARQUET_DIR}/{module}/{table}.parquet"
        else:
            return f"{MIMIC_PARQUET_DIR}/{table}.parquet"
    elif data_format == "csv":
        # otherwise, assume the csv files are stored under the two subdirectories
        return f"{MIMIC_CSV_DIR}/{module}/{table}.csv.gz"
    else:
        raise ValueError(
            f"Unsupported file format: {data_format}; only 'parquet' and 'csv' are supported."
        )

def resave_mimic_table_from_csv_to_parquet(table: str, overwrite: bool = False):
    '''
    Resave one MIMIC table from csv to parquet.
    '''
    # first check if the table is already converted to parquet
    if Path(mimic_table_pathfinder(table, data_format="parquet")).exists():
        if not overwrite:
            raise FileExistsError(f"{table}.parquet already exists at {mimic_table_pathfinder(table, data_format='parquet')}. Set overwrite = True to overwrite it.")
        else:
            logging.info(f"overwriting {table}.parquet that already exists at {mimic_table_pathfinder(table, data_format='parquet')}.")
    
    # resave the table from csv to parquet using duckdb
    logging.info(f"resaving {table} from .csv.gz to .parquet using duckdb...")
    query = f"""
    COPY (
        SELECT * 
        FROM read_csv_auto('{str(mimic_table_pathfinder(table, data_format='csv'))}')
        )
    TO '{str(mimic_table_pathfinder(table, data_format='parquet'))}' (FORMAT 'PARQUET');
    """
    con.execute(query)
    logging.info(f"finished resaving {table} from .csv.gz to .parquet!")
    
def resave_select_mimic_tables_from_csv_to_parquet(tables: list[str], overwrite: bool = False):
    '''
    Resave a list of MIMIC tables from csv to parquet.
    
    - overwrite: if True, will overwrite existing parquet files under the same name; otherwise, 
    a FileExistsError will be raised, and we will skip to the next table.
    '''
    logging.info(f"converting the following {len(tables)} mimic tables from csv to parquet: {tables}")
    # first check which tables are already converted to parquet by checking the parquet dir
    counter = 0
    for table in tables:
        counter += 1
        logging.info(f"resaving table {counter} out of {len(tables)}:")
        try: 
            resave_mimic_table_from_csv_to_parquet(table, overwrite = overwrite)
        except FileExistsError as e:
            logging.info(e)
            continue
    logging.info(f"finished resaving all {len(tables)} tables from .csv.gz to .parquet!")

def resave_all_mimic_tables_from_csv_to_parquet(overwrite: bool = False):
    '''
    Resave all MIMIC tables from csv to parquet.
    '''
    logging.info(f"resaving all {len(HOSP_TABLES + ICU_TABLES)} tables from .csv.gz to .parquet.")
    resave_select_mimic_tables_from_csv_to_parquet(HOSP_TABLES + ICU_TABLES, overwrite = overwrite)

def clif_table_pathfinder(table_name: str) -> str:
    global CLIF_OUTPUT_DIR_NAME
    if not CLIF_OUTPUT_DIR_NAME:
        # if it is an empty str (not specified by user), use the default syntax
        CLIF_OUTPUT_DIR_NAME = f"rclif-{CLIF_VERSION}"
    clif_path = (
        SCRIPT_DIR / f"../output/{CLIF_OUTPUT_DIR_NAME}/clif_{table_name}.parquet"
    ) # e.g. '../output/rclif-2.0/clif_adt.parquet'
    return str(clif_path)

def clif_test_data_pathfinder(table_name: str) -> str:
    clif_path = (
        SCRIPT_DIR / f"../tests/fixtures/test_{table_name}.csv"
    ) # e.g. '../tests/fixtures/test_patient.csv'
    return str(clif_path)

def save_to_rclif(df: pd.DataFrame, table_name: str):
    output_path = clif_table_pathfinder(table_name)
    # check if the directory exists, if not, create it
    if not Path(output_path).parent.exists():
        Path(output_path).parent.mkdir(parents=True)
    logging.info(f"saving {table_name} rclif table as a parquet file at {output_path}.")
    return df.to_parquet(output_path, index=False)

def read_from_rclif(table_name):
    return pd.read_parquet(clif_table_pathfinder(table_name))

# ----------------------
#   ETL - mapping
# ----------------------

def mapping_path_finder(csv_name: str):
    return SCRIPT_DIR / f"../data/mappings/mimic-to-clif-mappings - {csv_name}.csv"

def load_mapping_csv(csv_name: str, dtype=None):
    return pd.read_csv(
        mapping_path_finder(csv_name),
        dtype=dtype,
    )

def construct_mapper_dict(
    mapping_df: pd.DataFrame,
    key_col: str,
    value_col: str,
    map_none_to_none=False,
    excluded_item_ids: list = None,
    decision_col: str = "decision",
    excluded_labels: list = ["NO MAPPING", "UNSURE", "MAPPED ELSEWHERE", "ALREADY MAPPED", "NOT AVAILABLE"],
):
    """
    covert to a dict for df col renaming later
    """
    if not excluded_item_ids:
        excluded_item_ids = []

    if "itemid" in mapping_df.columns:
        mapping_df = mapping_df.loc[~mapping_df["itemid"].isin(excluded_item_ids),]

    if decision_col in mapping_df.columns:
        mapping_df = mapping_df.loc[~mapping_df[decision_col].isin(excluded_labels),]
    
    mapper_dict = dict(zip(mapping_df[key_col], mapping_df[value_col]))

    # Replace "NO MAPPING" with NA
    for key, value in mapper_dict.items():
        if value == "NO MAPPING":
            mapper_dict[key] = None

    # to enable a None -> None mapping
    if map_none_to_none:
        mapper_dict[None] = None
        mapper_dict["None"] = None

    return mapper_dict


def get_relevant_item_ids(
    mapping_df: pd.DataFrame,
    decision_col: str,
    excluded_labels: list = EXCLUDED_LABELS_DEFAULT,
    excluded_item_ids: list = None,
) -> pd.Series:
    """
    Parse the mapping files to identify all the relevant item ids for a table.
    - decision_col: the col on which to apply the excluded_labels
    - excluded_item_ids: additional item ids to exclude
    """
    if not excluded_item_ids:
        excluded_item_ids = []

    return mapping_df.loc[
        (~mapping_df[decision_col].isin(excluded_labels))
        & (~mapping_df["itemid"].isin(excluded_item_ids)),
        "itemid",
    ].unique()


# -----------------------------
#   ETL - data manipulation
# -----------------------------

def convert_tz_to_utc(series: pd.Series) -> pd.Series:
    """
    Convert a series of timestamps to UTC.
    """
    MIMIC_TZ = "US/Eastern"
    series = series.dt.tz_localize(MIMIC_TZ, ambiguous="NaT", nonexistent="shift_forward")
    return series.dt.tz_convert("UTC")

def convert_and_sort_datetime(df: pd.DataFrame, additional_cols: list[str] = None):
    if not additional_cols:
        additional_cols = []
    # for procedure events
    if "starttime" in df.columns and "endtime" in df.columns:
        df["starttime"] = pd.to_datetime(df["starttime"], format=CLIF_DTTM_FORMAT)
        df["endtime"] = pd.to_datetime(df["endtime"], format=CLIF_DTTM_FORMAT)
        ordered_cols = [
            "hadm_id",
            "starttime",
            "endtime",
            "storetime",
        ] + additional_cols
        df = df.sort_values(ordered_cols).reset_index(drop=True).reset_index()
    # for chart events
    elif "charttime" in df.columns:
        df["charttime"] = pd.to_datetime(df["charttime"], format=CLIF_DTTM_FORMAT)
        ordered_cols = ["hadm_id", "charttime", "storetime"] + additional_cols
        df = df.sort_values(ordered_cols).reset_index(drop=True).reset_index()
    elif "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], format=CLIF_DTTM_FORMAT)
        ordered_cols = ["hadm_id", "time"] + additional_cols
        df = df.sort_values(ordered_cols)
    return df


def rename_and_reorder_cols(
    df: pd.DataFrame, rename_mapper_dict: dict, new_col_order: list
) -> pd.DataFrame:
    '''
    Rename and reorder columns of a dataframe.
    '''
    baseline_rename_mapper = {
        "subject_id": "patient_id",
        "hadm_id": "hospitalization_id",
    }

    return df.rename(columns = baseline_rename_mapper | rename_mapper_dict) \
        .reindex(columns = new_col_order)


def find_duplicates(df: pd.DataFrame, cols: list[str] = ["hadm_id", "time", "itemid"]):
    """
    Check whether there are duplicates -- more than one populated value -- for what is supposed to be
    unique combination of columns. That is, for the same measured variable (e.g. vital_category) at
    the same time during the same encounter, there should be only one corresponding value.

    Use this in pre-CLIFing EDA.
    """
    return df[df.duplicated(subset=cols, keep=False)]

def check_duplicates(df: pd.DataFrame, additional_cols: list = None):
    """
    Check whether there are duplicates -- more than one populated value -- for what is supposed to be
    unique combination of columns. That is, for the same measured variable (e.g. vital_category) at
    the same time during the same hospitalization, there should be only one corresponding value.

    Use this in post-CLIFing validation.
    
    - additional_cols: a list of columns to check for duplicates in addition to `hospitalization_id` and `recorded_dttm`.
    """
    if not additional_cols:
        additional_cols = []
    cols_to_check = ["hospitalization_id", "recorded_dttm"] + additional_cols
    return df[df.duplicated(subset=cols_to_check, keep=False)]

@cache
def item_id_to_feature_value(df: pd.DataFrame, item_id: int, col: str = "label"):
    """
    df = d_items

    Find the corresponding feature value of an item by id.
    i.e. find the label, or linksto, of item id 226732.
    """
    row = df.loc[df["itemid"] == item_id, :]
    label = row["label"].values[0]
    if col == "label":
        logging.info(f"the {col} for item {item_id} is {label}")
        return label
    else:
        feature_value = row[col].values[0]
        logging.info(f"the {col} for item {item_id} ({label}) is {feature_value}")
        return feature_value


@cache
def item_id_to_label(item_id: int) -> str:
    """
    Helper function that returns the "label" string of an item given its item_id.
    """
    return item_id_to_feature_value(item_id)


# -----------------------------
#   ETL - fetching mimic events
# -----------------------------

def item_id_to_events_df_old(item_id: int, original: bool = False) -> pd.DataFrame:
    """
    TODO: likely rewrite or even retire
    Return in a pandas df all the events associated with an item id.
    - original: whether to return the original df (True), or a simplified one
    with some columns (particulary timestamps) renamed to support integration
    between different events df.
    """
    # find whether it is chartevents, or procedure events, etc.
    linksto_table_name = item_id_to_feature_value(item_id, col="linksto")
    # turn string into a dj object
    linksto_df: pd.DataFrame = globals()[linksto_table_name]
    events_df = linksto_df.loc[linksto_df["itemid"] == item_id, :]
    # return the original columns
    if original:
        return events_df
    # else = if not original, then return the simplified version
    elif (
        linksto_table_name == "procedureevents"
    ):  # FIXME: trach is complex and need additional attention
        events_df_simplified = events_df.loc[
            :,
            [
                "subject_id",
                "hadm_id",
                "stay_id",
                "endtime",
                "itemid",
                "value",
                "valueuom",
            ],
        ].rename(columns={"endtime": "time"})
        return events_df_simplified
    elif linksto_table_name == "chartevents":
        events_df_simplified = events_df.loc[
            :,
            [
                "subject_id",
                "hadm_id",
                "stay_id",
                "charttime",
                "itemid",
                "value",
                "valueuom",
            ],
        ].rename(columns={"charttime": "time"})
        return events_df_simplified
    # FIXME: likely an issue if data struct of different events table are different

def fetch_mimic_events_by_eventtable(
    item_ids: list[int], table_name: str, original: bool = False
):
    """
    Fetch all the events associated with a list of item ids from a given event table.
    """
    logging.info(f"fetching events from {table_name} table for {len(item_ids)} items")
    if original:
        cols = "*"
    elif table_name == "chartevents":
        cols = "itemid, label, hadm_id, stay_id, charttime as time, value, valueuom"
    elif table_name == "procedureevents":
        cols = "itemid, label, hadm_id, stay_id, endtime as time, value, valueuom"
    else:
        cols = "*"
        logging.warning(f"{table_name} not yet supported, thus returning all columns")
    query = f"""
    SELECT {cols}
    FROM '{mimic_table_pathfinder(table_name)}' 
    LEFT JOIN '{mimic_table_pathfinder("d_items")}' USING (itemid)
    WHERE itemid IN ({','.join(map(str, item_ids))})
    """
    df = con.execute(query).fetchdf()
    logging.info(
        f"fetched {len(df)} events from {table_name} table for {len(item_ids)} items"
    )
    return df

def fetch_mimic_events(item_ids: list[int], original: bool = False, for_labs: bool = False) -> pd.DataFrame:
    """
    Takes a list of item IDs and returns a DataFrame containing all the events
    associated with those item IDs.
    """
    if for_labs:
        query = f"""
        SELECT *
        FROM '{mimic_table_pathfinder("labevents")}'
        WHERE itemid IN ({','.join(map(str, item_ids))})
            AND hadm_id IS NOT NULL
        """
        df = con.execute(query).fetchdf()
        return df
    else:
        logging.info(
            f"querying the d_items table to identify which event tables to be separately queried for {len(item_ids)} items"
        )
        query = f"""
        SELECT itemid, linksto
        FROM '{mimic_table_pathfinder("d_items")}'
        WHERE itemid IN ({','.join(map(str, item_ids))})
        """
        df = con.execute(query).fetchdf()
        eventtable_to_itemids_mapper = df.groupby("linksto")["itemid"].apply(list).to_dict()
        logging.info(
            f"identified {len(eventtable_to_itemids_mapper)} event tables to be separately queried: {list(eventtable_to_itemids_mapper.keys())}"
        )
        df_list = [
            fetch_mimic_events_by_eventtable(item_ids, table_name, original=original)
            for table_name, item_ids in eventtable_to_itemids_mapper.items()
        ]
        df_m = pd.concat(df_list)
        logging.info(
            f"concatenated {len(df_m)} events from {len(eventtable_to_itemids_mapper)} event table(s)"
        )
        return df_m


def item_finder_to_events(items: pd.DataFrame):
    # TODO: unsure, may be redundant
    items = items.dropna(subset=["count"])
    itemids = items["itemid"].tolist()
    # itemid_to_label_mapper = dict(zip(items["itemid"], items["label"]))
    events = fetch_mimic_events(itemids)
    events["label"] = events["itemid"].apply(item_id_to_label)
    return events


# -----------------------------
#   ETL - item search
# -----------------------------

def generate_item_stats_by_eventtable(item_ids: list[int], table_name: str):
    '''
    Calculate the frequency and value instances of a list of items in a given event table.
    '''
    table_path = mimic_table_pathfinder(table_name)
    d_items_path = mimic_table_pathfinder("d_items")
    item_ids_str = ','.join(map(str, item_ids))
    
    if table_name == "chartevents":
        query = f"""
        WITH items_select AS (
            SELECT *
            FROM '{d_items_path}'
            WHERE itemid IN ({item_ids_str})
        )
        SELECT
            i.itemid,
            i.label,
            i.abbreviation,
            i.linksto,
            i.category,
            i.unitname,
            i.param_type,
            COUNT(*) AS count,
            CASE
                WHEN i.param_type = 'Numeric' THEN 
                    CONCAT('Min: ', MIN(e.valuenum), ', Mean: ', ROUND(AVG(e.valuenum), 2), ', Max: ', MAX(e.valuenum))
                WHEN i.param_type IN ('Text', 'Checkbox') THEN
                    (SELECT STRING_AGG(
                        CONCAT(value, ': ', value_count), ', ' 
                        ORDER BY value_count DESC)
                    FROM (
                        SELECT value, COUNT(*) AS value_count
                        FROM '{table_path}' AS e
                        WHERE e.itemid = i.itemid AND value IS NOT NULL AND value <> ''
                        GROUP BY value
                    ) AS value_counts)
                ELSE i.param_type
            END AS value_instances
        FROM items_select AS i
            LEFT JOIN '{table_path}' AS e USING (itemid)
        GROUP BY i.itemid, i.label, i.abbreviation, i.linksto, i.category, i.unitname, i.param_type;
        """
    elif table_name == "procedureevents":
        query = f"""
        SELECT
            i.itemid,
            i.label,
            i.abbreviation,
            i.linksto,
            i.category,
            i.unitname,
            i.param_type,
            COUNT(*) AS count,
            CONCAT(
                'Min: ', MIN(e.value), ', Mean: ', ROUND(AVG(e.value), 2), ', Max: ', MAX(e.value)
            ) AS value_instances
        FROM '{d_items_path}' AS i
            LEFT JOIN '{table_path}' AS e USING (itemid)
        WHERE i.itemid IN ({item_ids_str})
        GROUP BY i.itemid, i.label, i.abbreviation, i.linksto, i.category, i.unitname, i.param_type;
        """
    elif table_name == "datetimeevents":
        query = f"""
        SELECT
            i.itemid,
            i.label,
            i.abbreviation,
            i.linksto,
            i.category,
            i.unitname,
            i.param_type,
            COUNT(*) AS count,
            CONCAT(
                'Earliest: ', MIN(e.value), ', Latest: ', MAX(e.value)
            ) AS value_instances
        FROM '{d_items_path}' AS i
            LEFT JOIN '{table_path}' AS e USING (itemid)
        WHERE i.itemid IN ({item_ids_str})
        GROUP BY i.itemid, i.label, i.abbreviation, i.linksto, i.category, i.unitname, i.param_type;
        """
    elif table_name == "inputevents":
        query = f"""
        WITH items_select AS (
            SELECT *
            FROM '{d_items_path}'
            WHERE itemid IN ({item_ids_str})
        )
        SELECT
            i.itemid,
            i.label,
            i.abbreviation,
            i.linksto,
            i.category,
            i.unitname,
            i.param_type,
            COUNT(*) AS count,
            CONCAT(
                'Rate: ', ROUND(MIN(e.rate), 2), ', ', ROUND(MEDIAN(e.rate), 2), ', ', ROUND(MAX(e.rate), 2), '; Amount: ', ROUND(MIN(e.amount), 2), ', ', ROUND(MEDIAN(e.amount), 2), ', ', ROUND(MAX(e.amount), 2)
            ) AS value_instances,
            (SELECT STRING_AGG(
                    CONCAT(amountuom, ': ', amountuom_count), ', ' 
                    ORDER BY amountuom_count DESC)
                FROM (
                    SELECT amountuom, COUNT(*) AS amountuom_count
                    FROM '{table_path}' AS e
                    WHERE e.itemid = i.itemid AND amountuom IS NOT NULL AND rateuom <> ''
                    GROUP BY amountuom
                ) AS amountuom_counts) as amountuom_instances,
            (SELECT STRING_AGG(
                    CONCAT(rateuom, ': ', rateuom_count), ', ' 
                    ORDER BY rateuom_count DESC)
                FROM (
                    SELECT rateuom, COUNT(*) AS rateuom_count
                    FROM '{table_path}' AS e
                    WHERE e.itemid = i.itemid AND rateuom IS NOT NULL AND rateuom <> ''
                    GROUP BY rateuom
                ) AS rateuom_counts) as rateuom_instances,
            (SELECT STRING_AGG(
                    CONCAT(ordercategoryname, ': ', ordercategoryname_count), ', ' 
                    ORDER BY ordercategoryname_count DESC)
                FROM (
                    SELECT ordercategoryname, COUNT(*) AS ordercategoryname_count
                    FROM '{table_path}' AS e
                    WHERE e.itemid = i.itemid AND ordercategoryname IS NOT NULL AND ordercategoryname <> ''
                    GROUP BY ordercategoryname
                ) AS ordercategoryname_counts) as ordercategoryname_instances,
            (SELECT STRING_AGG(
                    CONCAT(secondaryordercategoryname, ': ', secondaryordercategoryname_count), ', ' 
                    ORDER BY secondaryordercategoryname_count DESC)
                FROM (
                    SELECT secondaryordercategoryname, COUNT(*) AS secondaryordercategoryname_count
                    FROM '{table_path}' AS e
                    WHERE e.itemid = i.itemid AND secondaryordercategoryname IS NOT NULL AND secondaryordercategoryname <> ''
                    GROUP BY secondaryordercategoryname
                ) AS secondaryordercategoryname_counts) as secondaryordercategoryname_instances,
            (SELECT STRING_AGG(
                    CONCAT(ordercategorydescription, ': ', ordercategorydescription_count), ', ' 
                    ORDER BY ordercategorydescription_count DESC)
                FROM (
                    SELECT ordercategorydescription, COUNT(*) AS ordercategorydescription_count
                    FROM '{table_path}' AS e
                    WHERE e.itemid = i.itemid AND ordercategorydescription IS NOT NULL AND ordercategorydescription <> ''
                    GROUP BY ordercategorydescription
                ) AS ordercategorydescription_counts) as ordercategorydescription_instances
        FROM items_select AS i
            LEFT JOIN '{table_path}' AS e USING (itemid)
        GROUP BY i.itemid, i.label, i.abbreviation, i.linksto, i.category, i.unitname, i.param_type;
        """
    elif table_name == "ingredientevents":
        query = f"""
        SELECT
            i.itemid,
            i.label,
            i.abbreviation,
            i.linksto,
            i.category,
            i.unitname,
            i.param_type,
            COUNT(*) AS count,
            CONCAT(
                'Rate: ', ROUND(MIN(e.rate), 2), ', ', ROUND(MEDIAN(e.rate), 2), ', ', ROUND(MAX(e.rate), 2), '; Amount: ', ROUND(MIN(e.amount), 2), ', ', ROUND(MEDIAN(e.amount), 2), ', ', ROUND(MAX(e.amount), 2)
            ) AS value_instances
        FROM '{d_items_path}' AS i
            LEFT JOIN '{table_path}' AS e USING (itemid)
        WHERE i.itemid IN ({item_ids_str})
        GROUP BY i.itemid, i.label, i.abbreviation, i.linksto, i.category, i.unitname, i.param_type;
        """
    else:
        raise NotImplementedError(f"Event table '{table_name}' not yet supported.")
    df = con.execute(query).fetchdf()
    return df

def search_mimic_items(kw, col: str = "label", case_sensitive: bool = False, for_labs: bool = False, report_na = True):
    '''
    Search for items by keyword in the `d_items` table.
    '''
    logging.info("--------------------------------")
    logging.info(f"searching for items with keyword '{kw}' in column '{col}' with case sensitive = {case_sensitive}.")
    kw_condition = f"{col} {'LIKE' if case_sensitive else 'ILIKE'} '%{kw}%'"
    query = f"""
    SELECT itemid, linksto
    FROM '{mimic_table_pathfinder("d_items")}'
    WHERE {kw_condition}
    """
    df = con.execute(query).fetchdf()
    # check if there is any match
    if len(df) == 0:
        logging.warning(f"No match for '{kw}' in column '{col}' with case sensitive = {case_sensitive}.")
        return pd.DataFrame({"kw": [kw]})
    else:
        eventtable_to_itemids_mapper = df.groupby("linksto")["itemid"].apply(list).to_dict()
        logging.info(
            f"identified {len(eventtable_to_itemids_mapper)} event tables to be separately queried: {list(eventtable_to_itemids_mapper.keys())}"
        )
        df_list = [
            generate_item_stats_by_eventtable(item_ids, table_name)
            for table_name, item_ids in eventtable_to_itemids_mapper.items()
        ]
        df_m = pd.concat(df_list).sort_values(by="count", ascending=False)
        df_m["kw"] = kw
        # move the kw column to the front
        df_m = df_m[["kw"] + [col for col in df_m.columns if col != "kw"]]
            
        logging.info(
            f"Found and concatenated {len(df_m)} items from across {len(eventtable_to_itemids_mapper)} event table(s)"
        )
        return df_m

class ItemFinder:
    """
    TODO likely replacing this with duckdb sql queries
    """

    def __init__(
        self,
        items_df: pd.DataFrame,
        kw=None,
        col: str = "label",
        case_sensitive: bool = False,
        for_labs: bool = False,
        report_na=True,
    ) -> pd.DataFrame:
        """
        items_df = d_items

        Look up an item by keyword from the `d_items` table of the `icu` module.
        - case: whether the search is case sensitive
        - report_na: whether to print when there is no match; or simply return a
        """
        self.kw = kw
        self.df = items_df
        self.col = "abbreviation" if col == "abbr" else col
        self.for_labs = for_labs

        # df of items that match the key words -- a raw output
        self.items_select_df: pd.DataFrame = items_df[
            items_df[self.col].str.contains(kw, case=case_sensitive, na=False)
        ]

        # first check whether there is any return in the raw output
        if len(self.items_select_df) == 0:
            if report_na:
                raise Exception(
                    f"No matching result found in column {col} with case sensitive being {case_sensitive}"
                )
            else:
                logging.warning(
                    f"No matching result for {kw} in column {col} with case sensitive being {case_sensitive}"
                )
                self.candidate_table = pd.DataFrame()

        # ... only proceed when the return is not of zero length
        # and enhance the simple raw output with counts and value instances
        else:
            logging.info(
                f"{len(self.items_select_df)} matching item(s) found for {self.kw}."
            )
            # list of ids for items that match the key words
            self.items_select_ids = self.items_select_df["itemid"].values
            # a np array of non-duplicated events table names, e.g. ["chartevents", "procedureevents"]
            self.linksto_table_names = self.items_select_df["linksto"].unique()
            self.item_freq = self.generate_item_freq()
            # logging.info(f"type is {type(self.item_freq)}")
            self.candidate_table = self.make_candidate_table()

    def generate_item_freq(self):
        """
        Iterative over each events table, find the items freq therein, and combine into one df.
        # FIXME - should maybe make this map style without loop
        """
        freq_df_ls = []  # a list of df's
        for table_name in self.linksto_table_names:
            # fetch the object by name str, i.e. chartevents, procedureevents, etc.
            events_df: pd.DataFrame = globals()[table_name]
            # filter for all the selected events in that events table
            events_select_df = events_df.loc[
                events_df["itemid"].isin(self.items_select_ids), :
            ]
            # a df of item freq for one event type
            item_freq = events_select_df.value_counts("itemid")
            item_freq.name = "count"

            # check if the df is empty -- there shouldn't be an empty one FIXME
            # if not item_freq_df.empty:
            freq_df_ls.append(item_freq)

        return pd.concat(freq_df_ls)

    def make_candidate_table(self):
        """
        merge item freq and values instances to the raw output to generate the enhanced table of the
        candidate items.
        """
        cand_table = (
            self.items_select_df.loc[
                :,
                [
                    "itemid",
                    "label",
                    "abbreviation",
                    "linksto",
                    "category",
                    "unitname",
                    "param_type",
                ],
            ]
            # FIXME
            .join(self.item_freq, on="itemid", validate="1:1")
            .sort_values(by="count", ascending=False)
            .assign(
                value_instances=lambda df: df["itemid"].apply(
                    item_id_to_value_instances
                )
            )
        )
        if self.for_labs:
            return cand_table.reindex(
                columns=[
                    "itemid",
                    "label",
                    "abbreviation",
                    "linksto",
                    "category",
                    "count",
                    "value_instances",
                    "unitname",
                ]
            )
        else:
            return cand_table


@cache
def item_id_to_value_instances(item_id: int):
    """
    Wrapper
    """
    label = item_id_to_feature_value(item_id, "label")

    param_type = item_id_to_feature_value(item_id, "param_type")

    if param_type == "Numeric":
        val_instances = item_id_to_value_instances_numeric(item_id)
    elif param_type == "Text":
        val_instances = item_id_to_value_instances_categorical(item_id).to_dict()
    else:
        return param_type
    print(f"item label: {label}; value instances: {str(val_instances)}")
    return str(val_instances)


def item_id_to_value_instances_categorical(item_id: int, events: pd.DataFrame):
    """
    events: pd.DataFrame = chartevents

    Return all the unique categories
    """
    assoc_events = events.loc[events["itemid"] == item_id, :]
    categories: pd.Series = assoc_events.value_counts("value")
    return categories


def item_id_to_value_instances_numeric(item_id: int, events: pd.DataFrame):
    """
    events: pd.DataFrame = chartevents

    Find max, min, mean of a continuous, or numeric, item.
    """
    valuenum_col = events.loc[events["itemid"] == item_id, :]["valuenum"]
    val_max, val_min, val_mean = (
        valuenum_col.max(),
        valuenum_col.min(),
        round(valuenum_col.mean(), 2),
    )
    return f"Max: {val_max}, Min: {val_min}, Mean: {val_mean}"
