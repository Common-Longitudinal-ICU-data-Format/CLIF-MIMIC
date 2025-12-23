import marimo

__generated_with = "0.16.4"
app = marimo.App(width="columns")


@app.cell
def _():
    import os
    os.getcwd()
    return (os,)


@app.cell
def _():
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
        resave_mimic_table_from_csv_to_parquet, read_from_rclif
    return mo, pd, read_from_rclif


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
def _(read_from_rclif):
    ecmo_mcs = read_from_rclif('ecmo_mcs')
    return (ecmo_mcs,)


@app.cell
def _(ecmo_mcs):
    ecmo_mcs.value_counts(['device_name', 'device_category', 'mcs_group'])
    return


@app.cell
def _(mo):
    clif_demo_patient_procedure = mo.sql(
        f"""
        FROM clif_patient_procedure p
        INNER JOIN hosp_demo d USING (hospitalization_id)
        SELECT p.*
        """
    )
    return


@app.cell
def _():
    # save_to_rclif(df=clif_demo_patient_procedure, table_name='demo_patient_procedure')
    return


if __name__ == "__main__":
    app.run()
