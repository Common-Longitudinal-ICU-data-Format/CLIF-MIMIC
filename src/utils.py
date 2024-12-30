# src/utils.py
import logging
import os
import pandas as pd
import json
from pathlib import Path
from functools import cache
import duckdb  # type: ignore

con = duckdb.connect()
SCRIPT_DIR = Path(__file__).resolve().parent

def load_config():
    json_path = SCRIPT_DIR / "../config/config.json"
    with open(json_path, "r") as file:
        config = json.load(file)
    print("loaded configuration from config.json")

    return config

config = load_config()

# Cache to store loaded tables
TABLE_CACHE = {}
ENVIRONMENT_CHOICE = config["environment_choice"]
MIMIC_CSV_DIR = config[ENVIRONMENT_CHOICE]["mimic_csv_dir"]
MIMIC_PARQUET_DIR = config[ENVIRONMENT_CHOICE]["mimic_parquet_dir"]
CLIF_OUTPUT_DIR_NAME = config["clif_output_dir_name"]
CLIF_VERSION = config["clif_version"]
EXCLUDED_LABELS_DEFAULT = [
    "NO MAPPING",
    "UNSURE",
    "MAPPED ELSEWHERE",
    "SPECIAL CASE",
    "ALREADY MAPPED",
]  # NOTE: retire "ALREDAY MAPPED" at some pt
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

def setup_logging(log_file: str = "logs/test.log"):
    """
    TODO: change the input arg to config
    Sets up logging for the ETL pipeline.

    Args:
        log_file (str): Path to the log file. Default is "logs/pipeline.log".
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    logging.info(f"logging initialized and saved at {log_file}")

# -----------
#     I/O
# -----------

def parquet_stored_in_submodules() -> bool:
    '''
    Check if the parquet files are stored in subdirectories: "hosp" and "icu" (True)
    or stored together under the same directory (False).
    '''
    return Path(MIMIC_PARQUET_DIR + "/hosp").exists() and Path(MIMIC_PARQUET_DIR + "/icu").exists()

def mimic_table_pathfinder(table: str, data_format: str = "parquet"):
    '''
    Return the path to a MIMIC table given the table name and data format.
    '''
    if table in HOSP_TABLES:
        module = "hosp"
    elif table in ICU_TABLES:
        module = "icu"
    else:
        raise ValueError(f"Table not found: {table}")    
    
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
    logging.info(f"resaving {table} from .csv.gz to .parquet using duckdb.")
    query = f"""
    COPY (SELECT * FROM read_csv_auto('{str(mimic_table_pathfinder(table, data_format='csv'))}'))
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
    # first check which tables are already converted to parquet by checking the parquet dir
    counter = 0
    for table in tables:
        counter += 1
        logging.info(f"resaving {table} from .csv.gz to .parquet (table {counter} out of {len(tables)}).")
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


def load_mimic_table(table, file_type: {"csv", "parquet", "pq"} = None, mimic_version="3.1"):
    """
    # TODO: likely to retire this function
    Loads a MIMIC-IV table from the specified module and caches it to avoid reloading.

    Args:
        module (str): The MIMIC module (e.g., "icu", "hosp").
        table (str): The table name (e.g., "patients", "admissions").
        file_type (str): The file type to load (e.g., "csv", "parquet").
        mimic_version (str): Version of MIMIC-IV to use (default is "3.1").

    Returns:
        pd.DataFrame: The loaded table as a Pandas DataFrame.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If an unsupported file type is provided.
    """
    global TABLE_CACHE
    cache_key = table

    # Check if table is already in cache
    if cache_key in TABLE_CACHE:
        logging.info(f"Using cached data for {table}.")
        return TABLE_CACHE[cache_key]

    # Define file paths
    parquet_path = (
        SCRIPT_DIR
        / f"../data/mimic-data/mimic-iv-{mimic_version}/{module}/{table}.parquet"
    )
    csv_path = (
        SCRIPT_DIR
        / f"../data/mimic-data/mimic-iv-{mimic_version}/{module}/{table}.csv.gz"
    )

    if file_type is not None:
        if file_type in ["pq", "parquet"]:
            if os.path.exists(parquet_path):
                logging.info(f"Loading {table} from {parquet_path}.")
                table_df = pd.read_parquet(parquet_path)
            else:
                raise FileNotFoundError(f"Parquet file not found at {parquet_path}")
        elif file_type == "csv":
            if os.path.exists(csv_path):
                logging.info(f"Loading {table} from {csv_path}.")
                table_df = pd.read_csv(csv_path, low_memory=False)
            else:
                raise FileNotFoundError(f"CSV file not found at {csv_path}")
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    else:
        if os.path.exists(parquet_path):
            logging.info(f"Reading already converted parquet file for {module}/{table}")
            table_df = pd.read_parquet(parquet_path, engine="auto")
        else:
            logging.info(
                f"No parquet file exists yet for {module}/{table}, so reading from csv"
            )
            table_df = pd.read_csv(csv_path, low_memory=False)
            table_df.to_parquet(parquet_path)

    # Cache the table
    TABLE_CACHE[cache_key] = table_df
    return table_df


def load_mimic_tables(tables, cache=True):
    # TODO: likely to retire
    for module, table in tables:
        logging.info(f"Loading {table} from module {module}")
        var_name = table
        if (cache) and (var_name in globals()):
            logging.info(
                f"Table {table} from module {module} is already loaded in memory"
            )
            continue
        try:
            globals()[var_name] = load_mimic_table(module, table)
            logging.info(f"Successfully loaded table {table} from module {module}")
        except Exception as e:
            logging.error(
                f"Error loading table: {table} from module: {module}. Error: {e}"
            )

def save_to_rclif(df: pd.DataFrame, table_name: str):
    global CLIF_OUTPUT_DIR_NAME
    if not CLIF_OUTPUT_DIR_NAME:
        # if it is an empty str (not specified by user), use the default syntax
        CLIF_OUTPUT_DIR_NAME = f"rclif-{CLIF_VERSION}"
    output_path = (
        SCRIPT_DIR / f"../output/{CLIF_OUTPUT_DIR_NAME}/clif_{table_name}.parquet"
    ) # e.g. '../data/rclif-2.0/clif_adt.parquet'
    # check if the directory exists, if not, create it
    if not os.path.exists(output_path.parent):
        os.makedirs(output_path.parent)
    logging.info(f"saving {table_name} rclif table as a parquet file at {output_path}.")
    return df.to_parquet(output_path, index=False)


def read_from_rclif(table_name, file_format="pq"):
    # FIXME: update this when validation / testing
    if file_format in ["pq", "parquet"]:
        return pd.read_parquet(SCRIPT_DIR / f"../rclif/clif_{table_name}.parquet")


# -------
#   ETL - mapping
# -------

def load_mapping_csv(csv_name: str, dtype=None):
    return pd.read_csv(
        SCRIPT_DIR / f"../data/mappings/mimic-to-clif-mappings - {csv_name}.csv",
        dtype=dtype,
    )

def construct_mapper_dict(
    mapping_df: pd.DataFrame,
    key_col: str,
    value_col: str,
    map_none_to_none=False,
    excluded_item_ids: list = None,
):
    """
    covert to a dict for df col renaming later
    """
    if not excluded_item_ids:
        excluded_item_ids = []

    if "itemid" in mapping_df.columns:
        mapping_df = mapping_df.loc[~mapping_df["itemid"].isin(excluded_item_ids),]

    mapper_dict = dict(zip(mapping_df[key_col], mapping_df[value_col]))

    # Replace "NO MAPPING" with NA
    for key, value in mapper_dict.items():
        if value == "NO MAPPING":
            mapper_dict[key] = None

    # to enable a None -> None mapping
    if map_none_to_none:
        mapper_dict[None] = None

    return mapper_dict


def get_relevant_item_ids(
    mapping_df: pd.DataFrame,
    decision_col: str,
    excluded_labels: list = EXCLUDED_LABELS_DEFAULT,
    excluded_item_ids: list = None,
):
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

def convert_and_sort_datetime(df: pd.DataFrame, additional_cols: list[str] = None):
    if not additional_cols:
        additional_cols = []
    # for procedure events
    if "starttime" in df.columns and "endtime" in df.columns:
        df["starttime"] = pd.to_datetime(df["starttime"])
        df["endtime"] = pd.to_datetime(df["endtime"])
        ordered_cols = [
            "hadm_id",
            "starttime",
            "endtime",
            "storetime",
        ] + additional_cols
        df = df.sort_values(ordered_cols).reset_index(drop=True).reset_index()
    # for chart events
    elif "charttime" in df.columns:
        df["charttime"] = pd.to_datetime(df["charttime"])
        ordered_cols = ["hadm_id", "charttime", "storetime"] + additional_cols
        df = df.sort_values(ordered_cols).reset_index(drop=True).reset_index()
    elif "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"])
        ordered_cols = ["hadm_id", "time"] + additional_cols
        df = df.sort_values(ordered_cols)
    return df


def rename_and_reorder_cols(
    df, rename_mapper_dict: dict, new_col_order: list
) -> pd.DataFrame:
    baseline_rename_mapper = {
        "subject_id": "patient_id",
        "hadm_id": "hospitalization_id",
    }

    return df.rename(columns=baseline_rename_mapper | rename_mapper_dict).reindex(
        columns=new_col_order
    )


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

    TODO: add further specification for the original argument
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


def fetch_mimic_events(item_ids: list[int], original: bool = False) -> pd.DataFrame:
    """
    Takes a list of item IDs and returns a DataFrame containing all the events
    associated with those item IDs.
    """
    # first query the d_items table to get the linksto table name for each item id
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
        f"concatenated {len(df_m)} events from {len(eventtable_to_itemids_mapper)} event tables"
    )
    return df_m


def item_finder_to_events(items: pd.DataFrame):
    items = items.dropna(subset=["count"])
    itemids = items["itemid"].tolist()
    # itemid_to_label_mapper = dict(zip(items["itemid"], items["label"]))
    events = item_ids_list_to_events_df(itemids)
    events["label"] = events["itemid"].apply(item_id_to_label)
    return events


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
