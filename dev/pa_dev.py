import marimo

__generated_with = "0.16.4"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell
def _():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    os.getcwd()
    return


@app.cell
def _():
    import marimo as mo
    import numpy as np
    import pandas as pd
    import logging
    import duckdb
    from src.utils import construct_mapper_dict, fetch_mimic_events, load_mapping_csv, \
        get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
        convert_and_sort_datetime, setup_logging, search_mimic_items, mimic_table_pathfinder, \
        resave_mimic_table_from_csv_to_parquet, read_from_rclif, convert_tz_to_utc, save_to_rclif
    return convert_tz_to_utc, fetch_mimic_events, mo, pd, save_to_rclif


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Raw GCS""")
    return


@app.cell
def _(fetch_mimic_events, pd):
    raw_gcs_mapper = {
        '223900': 'gcs_verbal', 
        '223901': 'gcs_motor', #GCS - Motor Response
        '220739': 'gcs_eyes' #GCS - Eye Opening
    }

    raw_gcs_mapper_df = pd.DataFrame(list(raw_gcs_mapper.items()), columns=['itemid', 'assessment_category'])
    gcs_itemids = list(raw_gcs_mapper.keys())

    raw_gcs_events = fetch_mimic_events(gcs_itemids)
    return (raw_gcs_events,)


@app.cell
def _(raw_gcs_events):
    raw_gcs_events#.value_counts(['itemid', 'label', 'value', 'valuenum'])
    return


@app.cell
def _(mo):
    gcs_mapped = mo.sql(
        f"""
        FROM raw_gcs_events e
        LEFT JOIN raw_gcs_mapper_df m USING (itemid)
        SELECT hospitalization_id: e.hadm_id::STRING
            , recorded_dttm: time::TIMESTAMP
            , assessment_name: label
            , assessment_category: m.assessment_category
            , assessment_group: 'Neurological'
            , numerical_value: CASE
                WHEN e.value = 'No Response-ETT' THEN 0
                ELSE e.valuenum::FLOAT
                END
            , categorical_value: value
            , text_value: NULL
        """
    )
    return (gcs_mapped,)


@app.cell
def _(mo):
    gcs_w = mo.sql(
        f"""
        WITH w AS (
            PIVOT (
                FROM gcs_mapped
                SELECT hospitalization_id, recorded_dttm, assessment_category, numerical_value
            )
            ON assessment_category
            USING MAX(numerical_value)
        )
        FROM w
        SELECT *
            , gcs_total: gcs_eyes + gcs_verbal + gcs_motor
        """
    )
    return (gcs_w,)


@app.cell
def _(gcs_w):
    gcs_w['gcs_total'].isna().mean() * 100
    return


@app.cell
def _(gcs_mapped, gcs_w, mo):
    gcs_final = mo.sql(
        f"""
        FROM gcs_w
        	SELECT hospitalization_id
                , recorded_dttm
                , assessment_name: 'COMPUTED FROM SUB-SCORES; NOT ORIGINALLY AVAILABLE IN MIMIC-IV'
                , assessment_category: 'gcs_total'
                , assessment_group: 'Neurological'  
                , numerical_value: gcs_total
                , categorical_value: NULL
                , text_value: NULL
            WHERE gcs_total IS NOT NULL
        UNION ALL
        FROM gcs_mapped SELECT *
        """
    )
    return (gcs_final,)


@app.cell
def _(convert_tz_to_utc, gcs_final):
    gcs_final['recorded_dttm'] = convert_tz_to_utc(gcs_final['recorded_dttm'])
    return


@app.cell
def _(gcs_final, save_to_rclif):
    save_to_rclif(gcs_final, 'patient_assessments_raw_gcs')
    return


if __name__ == "__main__":
    app.run()
