import marimo

__generated_with = "0.18.2"
app = marimo.App(width="columns")

@app.cell
def _():
    import marimo as mo
    import numpy as np
    import pandas as pd
    import logging, duckdb, os
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
def _(search_mimic_items):
    search_mimic_items('oxygen', for_labs=True)
    return


if __name__ == "__main__":
    app.run()
