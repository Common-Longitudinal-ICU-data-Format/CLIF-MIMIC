import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import duckdb
    return


@app.cell
def _():
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from src.utils import clif_table_pathfinder
    return (clif_table_pathfinder,)


@app.cell
def _():
    tables_by_hosp_id = [
        'adt', 'crrt_therapy', 'hospital_diagnosis', 'hospitalization', 'labs',
        'medication_admin_continuous', 'medication_admin_intermittent',
        'patient_assessments', 'patient_procedures', 'position',
        'respiratory_support', 'vitals'
    ]
    tables_by_pt_id = ['code_status', 'patient']
    return tables_by_hosp_id, tables_by_pt_id


@app.cell
def _(clif_table_pathfinder, mo, null):
    demo_cohort_ids = mo.sql(
        f"""
        FROM "{clif_table_pathfinder('hospitalization')}" h
        RIGHT JOIN 'data/mimic-data/mimic-iv-clinical-database-demo-2.2/demo_subject_id.csv' d
        ON h.patient_id = d.subject_id
        SELECT patient_id, hospitalization_id
        """
    )
    return (demo_cohort_ids,)


@app.cell
def _():
    from pathlib import Path
    output_dir = Path('output/demo-data-2.1')
    output_dir.mkdir(parents=True, exist_ok=True)
    return (output_dir,)


@app.cell
def _(clif_table_pathfinder):
    p = clif_table_pathfinder('patient')
    p
    return


@app.cell
def _():
    import pandas as pd
    pd.read_parquet('/Users/wliao0504/code/clif/CLIF-MIMIC/output/demo-data-2.1/clif_code_status.parquet')
    return (pd,)


@app.cell
def _(mo):
    df = mo.sql(
        f"""
        FROM
            '/Users/wliao0504/code/clif/CLIF-MIMIC/output/demo-data-2.1/clif_patient.parquet'
        """
    )
    return


@app.cell
def _(
    clif_table_pathfinder,
    demo_cohort_ids,
    output_dir,
    pd,
    tables_by_hosp_id,
):
    for _table_name in tables_by_hosp_id:
        _table_path = clif_table_pathfinder(_table_name)
        # rewrite using pandas to perform a semi-join
        _df = pd.read_parquet(_table_path)
        _df = _df[_df['hospitalization_id'].isin(demo_cohort_ids['hospitalization_id'])]
        # _q = f"""
        # FROM '{_table_path}' t
        # INNER JOIN demo_cohort_ids c
        #     USING (hospitalization_id)
        # SELECT t.*
        # """
        # _df = duckdb.sql(_q).df()
        _df.to_parquet(output_dir / f'clif_{_table_name}.parquet')
    return


@app.cell
def _(clif_table_pathfinder, demo_cohort_ids, output_dir, pd, tables_by_pt_id):
    for _table_name in tables_by_pt_id:
        _table_path = clif_table_pathfinder(_table_name)
        # rewrite using pandas to perform a semi-join
        _df = pd.read_parquet(_table_path)
        _df = _df[_df['patient_id'].isin(demo_cohort_ids['patient_id'])]
        # _q = f"""
        # FROM '{_table_path}' t
        # INNER JOIN demo_cohort_ids c
        #     USING (patient_id)
        # SELECT t.*
        # """
        # _df = duckdb.sql(_q).df()
        _df.to_parquet(output_dir / f'clif_{_table_name}.parquet')
    return


if __name__ == "__main__":
    app.run()
