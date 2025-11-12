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
    crrt = read_from_rclif("crrt_therapy")
    return (crrt,)


@app.cell
def _(crrt):
    crrt
    return


@app.cell
def _(mo):
    clif_demo_crrt = mo.sql(
        f"""
        FROM crrt p
        INNER JOIN hosp_demo d USING (hospitalization_id)
        SELECT p.*
        """
    )
    return


@app.cell
def _():
    # save_to_rclif(df=clif_demo_crrt, table_name='demo_crrt_therapy')
    return


@app.cell
def _():
    from src.tables import crrt_therapy

    crrt_therapy._test()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
