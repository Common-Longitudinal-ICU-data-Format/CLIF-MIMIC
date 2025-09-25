import marimo

__generated_with = "0.14.17"
app = marimo.App(width="columns")


@app.cell
def _():
    import os
    os.getcwd()
    return


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
        resave_mimic_table_from_csv_to_parquet

    return mimic_table_pathfinder, mo, resave_mimic_table_from_csv_to_parquet


@app.cell
def _(resave_mimic_table_from_csv_to_parquet):
    resave_mimic_table_from_csv_to_parquet(table = 'hcpcsevents')
    return


@app.cell
def _(mimic_table_pathfinder, mo, null):
    mimic_procedures_icd = mo.sql(
        f"""
        FROM '{mimic_table_pathfinder("procedures_icd")}'
        SELECT *
        """
    )
    return


@app.cell
def _(mimic_table_pathfinder, mo, null):
    mimic_drgcodes = mo.sql(
        f"""
        FROM '{mimic_table_pathfinder("drgcodes")}'
        SELECT *
        """
    )
    return


@app.cell
def _(mimic_table_pathfinder, mo, null):
    mimic_d_hcpcs = mo.sql(
        f"""
        FROM '{mimic_table_pathfinder("d_hcpcs")}'
        SELECT *
        """
    )
    return (mimic_d_hcpcs,)


@app.cell
def _(mimic_d_hcpcs):
    mimic_d_hcpcs
    return


@app.cell
def _(mimic_table_pathfinder, mo, null):
    mimic_hcpcsevents = mo.sql(
        f"""
        FROM '{mimic_table_pathfinder("hcpcsevents")}'
        SELECT *
        """
    )
    return


@app.cell
def _(mimic_table_pathfinder, mo, null):
    clif_patient_procedures = mo.sql(
        f"""
        FROM '{mimic_table_pathfinder("hcpcsevents")}' h
        SELECT hospitalization_id: CAST(h.hadm_id AS VARCHAR)
            , billing_provider_id: CAST(NULL AS VARCHAR)
            , performing_provider_id: CAST(NULL AS VARCHAR)
            , procedure_code: CAST(h.hcpcs_cd AS VARCHAR)
            , procedure_code_starts_with_letter: CASE WHEN LEFT(h.hcpcs_cd, 1) ~ '^[A-Za-z]' THEN 1 ELSE 0 END
            , _procedure_code_format: CASE
                WHEN regexp_matches(code, '^[0-9]{5}$') THEN 'CPT (Level I)'
                WHEN regexp_matches(code, '^[0-9]{4}[FT]$') THEN 'CPT (Category II/III)'
                WHEN regexp_matches(code, '^[A-V][0-9]{4}$') THEN 'HCPCS Level II'
                ELSE 'Unknown/Invalid' END
            , procedure_billed_dttm: CAST(h.chartdate AS TIMESTAMP)
        """
    )
    return


if __name__ == "__main__":
    app.run()
