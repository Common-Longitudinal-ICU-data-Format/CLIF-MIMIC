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
        resave_mimic_table_from_csv_to_parquet, clif_table_pathfinder

    return clif_table_pathfinder, fetch_mimic_events, mo, pd, save_to_rclif


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
def _(fetch_mimic_events):
    cs_events = fetch_mimic_events(item_ids=[223758])
    return (cs_events,)


@app.cell
def _(cs_events):
    cs_events
    return


@app.cell(hide_code=True)
def _(clif_table_pathfinder, cs_events, mo, null):
    clif_code_status = mo.sql(
        f"""
        FROM cs_events e
        LEFT JOIN '{clif_table_pathfinder('hospitalization')}' h 
            ON e.hadm_id = h.hospitalization_id
        SELECT patient_id: CAST(h.patient_id AS VARCHAR)
            , start_dttm: CAST(e.time AS TIMESTAMP)
            , code_status_name: CAST(e.value AS VARCHAR)
            , code_status_category: CASE
                WHEN code_status_name in ('Full code') THEN 'Full'
                WHEN code_status_name in ('DNR / DNI', 'DNI (do not intubate)') THEN 'DNR/DNI'
                WHEN code_status_name in ('DNR (do not resuscitate)') THEN 'DNR'
                WHEN code_status_name in ('Comfort measures only') THEN 'AND' END

        """
    )
    return


@app.cell
def _(mo):
    clif_demo_code_status = mo.sql(
        f"""
        FROM clif_code_status p
        INNER JOIN pt_demo d USING (patient_id)
        SELECT p.*
        """
    )
    return (clif_demo_code_status,)


@app.cell
def _(clif_demo_code_status, save_to_rclif):
    save_to_rclif(df=clif_demo_code_status, table_name='demo_code_status')
    return


if __name__ == "__main__":
    app.run()
