import marimo

__generated_with = "0.16.4"
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
    return mo, pd, search_mimic_items


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


@app.cell
def _(search_mimic_items):
    search_mimic_items('oxygen', for_labs=True)
    return


if __name__ == "__main__":
    app.run()
