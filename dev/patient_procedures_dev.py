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
    return mimic_table_pathfinder, mo, pd, read_from_rclif


@app.cell
def _():
    # resave_mimic_table_from_csv_to_parquet(table = 'hcpcsevents')
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
    mimic_hcpcsevents = mo.sql(
        f"""
        FROM '{mimic_table_pathfinder("hcpcsevents")}'
        SELECT *
        """
    )
    return


@app.cell
def _(mimic_table_pathfinder, mo, null):
    # build 
    mimic_cpt_hcpcs = mo.sql(
        f"""
        FROM '{mimic_table_pathfinder("hcpcsevents")}' h
        SELECT hospitalization_id: CAST(h.hadm_id AS VARCHAR)
            , billing_provider_id: CAST(NULL AS VARCHAR)
            , performing_provider_id: CAST(NULL AS VARCHAR)
            , procedure_code: CAST(h.hcpcs_cd AS VARCHAR)
            , _procedure_code_format: CASE
                WHEN regexp_matches(procedure_code, '^[0-9]{{5}}$') THEN 'cpt_level_1'
                WHEN regexp_matches(procedure_code, '^[0-9]{{4}}[FT]$') THEN 'cpt_category_2_3'
                WHEN regexp_matches(procedure_code, '^[A-V][0-9]{{4}}$') THEN 'hcpcs_level_2'
                ELSE 'Unknown/Invalid' END
            , procedure_code_format: CASE
                WHEN _procedure_code_format in ('cpt_level_1', 'cpt_category_2_3') THEN 'CPT'
                WHEN _procedure_code_format = 'hcpcs_level_2' THEN 'HCPCS'
                END
            , procedure_billed_dttm: CAST(h.chartdate AS TIMESTAMP)
        """
    )
    return (mimic_cpt_hcpcs,)


@app.cell
def _(mimic_table_pathfinder, mo, null):
    mimic_icd = mo.sql(
        f"""
        FROM '{mimic_table_pathfinder("procedures_icd")}' i
        SELECT hospitalization_id: CAST(i.hadm_id AS VARCHAR)
            , billing_provider_id: CAST(NULL AS VARCHAR)
            , performing_provider_id: CAST(NULL AS VARCHAR)
            , procedure_code: CAST(i.icd_code AS VARCHAR)
            , procedure_code_format: CASE
                WHEN icd_version in (10, '10') THEN 'ICD10PCS'
                WHEN icd_version in (9, '9') THEN 'ICD9'
                END
            , procedure_billed_dttm: CAST(i.chartdate AS TIMESTAMP)
        """
    )
    return (mimic_icd,)


@app.cell
def _(mimic_cpt_hcpcs, mimic_icd, mo):
    clif_patient_procedure = mo.sql(
        f"""
        FROM mimic_icd
        SELECT *
        UNION ALL
        FROM mimic_cpt_hcpcs
        -- exclude columns that start with underscore
        SELECT COLUMNS('^[^_].*')
        """
    )
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
def _():
    from src.tables import patient_procedures

    patient_procedures._main()
    return


@app.cell
def _(read_from_rclif):
    pp = read_from_rclif('patient_procedures')
    return (pp,)


@app.cell
def _(pp):
    pp
    return


if __name__ == "__main__":
    app.run()
