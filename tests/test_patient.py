import pandas as pd
import pytest
from importlib import reload
import src.utils
# reload(src.utils)
from src.utils import clif_table_pathfinder, clif_test_data_pathfinder
from src.tables.patient import PATIENT_COL_NAMES

clif_patient_test_df = pd.read_csv(clif_test_data_pathfinder("patient"))
patient_ids = clif_patient_test_df['patient_id'].unique().astype("string")

@pytest.fixture
def clif_patient_data():
    return pd.read_parquet(clif_table_pathfinder("patient"))

@pytest.fixture
def clif_patient_test_data():
    df = clif_patient_test_df
    df['patient_id'] = df['patient_id'].astype("string")
    df['birth_date'] = pd.to_datetime(df['birth_date'])
    df['death_dttm'] = pd.to_datetime(df['death_dttm'])
    df['language_category'] = df['language_category'].astype("string")
    return df

@pytest.mark.parametrize("patient_id", patient_ids)
def test_patient_race_dedup(patient_id, clif_patient_data, clif_patient_test_data):
    '''
    Test that the patient table has only one row per patient and that the mapped race and ethnicity are as expected.
    '''
    expected_row = clif_patient_test_data[clif_patient_test_data['patient_id'] == patient_id]
    actual_row = clif_patient_data[clif_patient_data['patient_id'] == patient_id]
    assert len(actual_row) == 1, f"Patient {patient_id} has {len(actual_row)} rows, but should have only one row."
    for var in ["patient_id", "race_name", "race_category", "ethnicity_name", "ethnicity_category"]:
        actual_val = actual_row.iloc[0][var] # if not actual_row.empty else None
        expected_val = expected_row.iloc[0][var] # if not expected_row.empty else None
        assert actual_val == expected_val, f"Patient {patient_id}'s {var} is {actual_val}, but should be {expected_val}."

# RESUME -- add checking for NaT = Nat TODO