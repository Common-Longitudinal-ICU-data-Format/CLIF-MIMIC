import pandas as pd
import pytest
from importlib import reload
import src.utils
# reload(src.utils)
from src.utils import clif_table_pathfinder, clif_test_data_pathfinder

clif_labs_table = pd.read_parquet(clif_table_pathfinder("labs"))
clif_labs_mcide = pd.read_csv("https://raw.githubusercontent.com/Common-Longitudinal-ICU-data-Format/CLIF/refs/heads/main/mCIDE/clif_lab_categories.csv")
permitted_lab_categories = clif_labs_mcide["lab_category"].unique()

def test_no_wrong_category():
    ''' 
    Check that no wrong category is included (there should be no `height` in 
    `vitals` since it should be `height_cm`).
    '''
    assert set(clif_labs_table["lab_category"]).issubset(permitted_lab_categories), \
        f"There are some wrong categories included: {set(clif_labs_table['lab_category']) - set(permitted_lab_categories)}"

def test_no_null():
    '''
    Check that there are no null values in the lab_name column.
    '''
    no_null_cols = ["hospitalization_id", "lab_name", "lab_category", "lab_value", "reference_unit"]
    for col in no_null_cols:
        assert clif_labs_table[col].notna().all(), f"There are {clif_labs_table[col].isna().sum()} null values in the {col} column"


    