"""Tests for the medication_admin module.

This module contains tests for the timestamp flattening functions in the
src.tables.medication_admin module, which transform medication administration
events from start/end time pairs to individual timestamped events.
"""
import pytest
import pandas as pd
from pathlib import Path
from src.tables.medication_admin import (
    cont_flattened, cont_deduped
)
from src.utils import mapping_path_finder
import duckdb

# --- Helper Fixtures for CSV Loading ---
@pytest.fixture
def load_fixture_csv():
    """Load CSV fixture from tests/fixtures/med_admin/.

    Returns
    -------
    callable
        Function that loads CSV files from the fixture directory.
    """
    def _load(filename) -> pd.DataFrame:
        path = Path(__file__).parent / 'fixtures' / 'med_admin' / filename
        df = pd.read_csv(path)
        return df
    return _load


# --- Data Fixtures ---
@pytest.fixture
def med_admin_input_data(load_fixture_csv):
    """Load test data for medication admin timestamp flattening.

    Returns
    -------
    pd.DataFrame
        CSV data with columns:

        - subject_id: Patient identifier
        - hadm_id: Hospitalization identifier
        - starttime: Start of medication administration
        - endtime: End of medication administration
        - linkorderid: Order identifier
        - statusdescription: Status of the medication event
        - med_category: Medication category
        - rate: Dose rate value
        - rateuom: Dose rate unit
        - amount: Dose amount value
        - amountuom: Dose amount unit
        - patientweight: Patient weight in kg
    """
    df = load_fixture_csv('mimic-to-clif-test-fixtures - med_admin_in.csv')

    # Convert datetime columns
    df['starttime'] = pd.to_datetime(df['starttime'])
    df['endtime'] = pd.to_datetime(df['endtime'])

    # Calculate duration_in_mins (required by the functions)
    df['duration_in_mins'] = (df['endtime'] - df['starttime']).dt.total_seconds() / 60

    df['med_route_name'] = 'placeholder'

    return df


@pytest.fixture
def med_admin_expected_output(load_fixture_csv):
    """Load expected output for medication admin timestamp flattening.

    Returns
    -------
    pd.DataFrame
        Expected output CSV data with columns:

        - hospitalization_id: Hospitalization identifier
        - med_order_id: Order identifier
        - admin_dttm: Administration datetime (flattened from start/end)
        - med_category: Medication category
        - med_route_name: Route of administration
        - med_dose: Dose value
        - med_dose_unit: Dose unit
        - mar_action_name: Medication action (e.g., [start], Paused, etc.)
    """
    df = load_fixture_csv('mimic-to-clif-test-fixtures - med_admin_out.csv')

    # Convert datetime - handle both formats in the CSV
    df['admin_dttm'] = pd.to_datetime(df['admin_dttm'], format='mixed')

    # Ensure correct data types
    df['hospitalization_id'] = df['hospitalization_id'].astype(str)
    df['med_order_id'] = df['med_order_id'].astype(str)

    return df


# ===========================================
# Tests for timestamp flattening pipeline
# ===========================================
def test_flatten_timestamps_combined(med_admin_input_data, med_admin_expected_output):
    """Test the combined _flatten_timestamps_staging and _flatten_timestamps pipeline.

    This test validates the complete end-to-end timestamp flattening process
    that transforms medication administration events from start/end time pairs
    to individual timestamped events with proper mar_action_name assignments.

    Parameters
    ----------
    med_admin_input_data : pd.DataFrame
        Test fixture containing input medication events with start/end times.
    med_admin_expected_output : pd.DataFrame
        Test fixture containing expected flattened output.
    """
    input_df = med_admin_input_data
    expected_df: pd.DataFrame = med_admin_expected_output
    
    q = """
    SELECT * FROM input_df
    """
    input_pr = duckdb.sql(q)
    
    # Run the flattening function
    flattened = cont_flattened(input_pr)
    mar_action_dedup_mapping = pd.read_csv(mapping_path_finder("mar_action_dedup"))
    
    # Run the deduplication function
    result_df = cont_deduped(flattened, mar_action_dedup_mapping).df()

    # Convert ID columns to string to match CLIF schema
    result_df['hospitalization_id'] = result_df['hospitalization_id'].astype(str)
    result_df['med_order_id'] = result_df['med_order_id'].astype(str)

    # Sort both dataframes to ensure order-independent comparison
    sort_cols = ['hospitalization_id', 'med_order_id', 'med_category', 'admin_dttm', 'med_dose']
    result_df = result_df.sort_values(by=sort_cols).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=sort_cols).reset_index(drop=True)

    # Verify output columns exist
    assert 'hospitalization_id' in result_df.columns
    assert 'med_order_id' in result_df.columns
    assert 'admin_dttm' in result_df.columns
    assert 'med_category' in result_df.columns
    assert 'med_dose' in result_df.columns
    assert 'med_dose_unit' in result_df.columns
    assert 'mar_action_name' in result_df.columns
    assert 'mar_action_category' in result_df.columns

    # Verify number of rows matches expected
    assert len(result_df) == len(expected_df), \
        f"Expected {len(expected_df)} rows but got {len(result_df)}"

    # Filter to only the columns we're testing
    test_columns = ['hospitalization_id', 'med_order_id', 'admin_dttm',
                    'med_category', 'med_dose', 'med_dose_unit', 'mar_action_name', 'mar_action_category']

    result_subset = result_df[test_columns]
    expected_subset = expected_df[test_columns]

    # Verify hospitalization_id
    pd.testing.assert_series_equal(
        result_subset['hospitalization_id'].reset_index(drop=True),
        expected_subset['hospitalization_id'].reset_index(drop=True),
        check_names=False
    )

    # Verify med_order_id
    pd.testing.assert_series_equal(
        result_subset['med_order_id'].reset_index(drop=True),
        expected_subset['med_order_id'].reset_index(drop=True),
        check_names=False
    )

    # Verify admin_dttm
    pd.testing.assert_series_equal(
        result_subset['admin_dttm'].reset_index(drop=True),
        expected_subset['admin_dttm'].reset_index(drop=True),
        check_names=False
    )

    # Verify med_category
    pd.testing.assert_series_equal(
        result_subset['med_category'].reset_index(drop=True),
        expected_subset['med_category'].reset_index(drop=True),
        check_names=False
    )

    # Verify med_dose (with tolerance for floating point)
    pd.testing.assert_series_equal(
        result_subset['med_dose'].reset_index(drop=True),
        expected_subset['med_dose'].reset_index(drop=True),
        check_names=False,
        rtol=1e-6,
        atol=1e-8
    )

    # Verify med_dose_unit
    pd.testing.assert_series_equal(
        result_subset['med_dose_unit'].reset_index(drop=True),
        expected_subset['med_dose_unit'].reset_index(drop=True),
        check_names=False
    )

    # Verify mar_action_name
    pd.testing.assert_series_equal(
        result_subset['mar_action_name'].reset_index(drop=True),
        expected_subset['mar_action_name'].reset_index(drop=True),
        check_names=False
    )
    
    # Verify mar_action_category
    pd.testing.assert_series_equal(
        result_subset['mar_action_category'].reset_index(drop=True),
        expected_subset['mar_action_category'].reset_index(drop=True),
        check_names=False
    )
