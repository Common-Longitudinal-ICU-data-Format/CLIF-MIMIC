import pandas as pd
import pytest
from importlib import reload
import src.utils
# reload(src.utils)
from src.utils import clif_table_pathfinder, clif_test_data_pathfinder

def test_age_at_admission():
    '''
    FIXME 
    '''
    # take a random sample of 5 patients from the hospitalization table
    hosp_sample = pd.read_parquet(clif_table_pathfinder("hospitalization"))
    hosp_sample = hosp_sample.sample(5)
    # calculate the age at admission for each patient
    hosp_sample["age_at_admission"] = hosp_sample["anchor_age"] + hosp_sample["admittime"].dt.year - hosp_sample["anchor_year"]
    # check that the age at admission is within 1 year of the anchor age
    assert hosp_sample["age_at_admission"].between(hosp_sample["anchor_age"] - 1, hosp_sample["anchor_age"] + 1).all()

# TODO add test of discharge location mapping
