import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return


@app.cell
def _():
    import marimo as mo
    import numpy as np
    import pandas as pd
    import logging
    import duckdb
    from src.utils import (
        construct_mapper_dict, fetch_mimic_events, load_mapping_csv,
        get_relevant_item_ids, find_duplicates, rename_and_reorder_cols,
        save_to_rclif, convert_and_sort_datetime, setup_logging,
        search_mimic_items, mimic_table_pathfinder,
        resave_mimic_table_from_csv_to_parquet, read_from_rclif,
        convert_tz_to_utc
    )

    return (search_mimic_items,)


@app.cell
def _(search_mimic_items):
    search_mimic_items(kw='catheter', col='label')
    return


@app.cell
def _(search_mimic_items):
    search_mimic_items(kw='PAOP', col='abbreviation')
    return


@app.cell
def _(search_mimic_items):
    search_mimic_items(kw='PAOP', col='label')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
