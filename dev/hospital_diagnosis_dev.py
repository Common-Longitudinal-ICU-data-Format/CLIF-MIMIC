import marimo

__generated_with = "0.14.17"
app = marimo.App(width="columns")


@app.cell
def _():
    import os
    os.getcwd()
    return (os,)


@app.cell
def _(os):
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from src import utils
    return


@app.cell
def _():
    import marimo as mo

    import numpy as np
    import pandas as pd
    import logging
    # from importlib import reload
    # import src.utils
    #reload(src.utils)
    import duckdb
    #reload(duckdb)
    from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
        get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
        convert_and_sort_datetime, setup_logging, search_mimic_items, mimic_table_pathfinder, \
        resave_mimic_table_from_csv_to_parquet

    return mimic_table_pathfinder, mo, pd, save_to_rclif


@app.cell
def _():
    # resave_mimic_table_from_csv_to_parquet(table = 'hcpcsevents')
    return


@app.cell
def _(pd):
    pt_demo = pd.read_parquet('tests/clif_patient.parquet')
    hosp_demo = pd.read_parquet('tests/clif_hospitalization.parquet')
    return


@app.cell
def _(mimic_table_pathfinder, mo, null):
    clif_hospital_diagnosis = mo.sql(
        f"""
        SELECT hospitalization_id: CAST(dx.hadm_id AS VARCHAR)
            , diagnosis_code: CAST(dx.icd_code AS VARCHAR)
            , diagnosis_code_format: CASE 
                WHEN dx.icd_version = 10 THEN 'ICD10CM'
                WHEN dx.icd_version = 9 THEN 'ICD9CM' END
            , diagnosis_primary: CASE 
                WHEN dx.seq_num = 1 THEN 1 
                WHEN dx.seq_num > 1 THEN 0 END
            , poa_present: CAST(NULL AS INT)
        FROM '{mimic_table_pathfinder('diagnoses_icd')}' dx
        """
    )
    return


@app.cell
def _(mo):
    clif_demo_hospital_diagnosis = mo.sql(
        f"""
        FROM clif_hospital_diagnosis dx
        INNER JOIN hosp_demo d USING (hospitalization_id)
        SELECT dx.*
        """
    )
    return (clif_demo_hospital_diagnosis,)


@app.cell
def _(clif_demo_hospital_diagnosis, save_to_rclif):
    save_to_rclif(df=clif_demo_hospital_diagnosis, table_name='demo_hospital_diagnosis')
    return


if __name__ == "__main__":
    app.run()
