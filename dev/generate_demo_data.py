import marimo

__generated_with = "0.16.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import duckdb
    return (duckdb,)


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
def _(clif_table_pathfinder, mo):
    demo_cohort_ids = mo.sql(
        f"""
        FROM "{clif_table_pathfinder('hospitalization')}" h
        RIGHT JOIN 'data/mimic-data/mimic-iv-clinical-database-demo-2.2/demo_subject_id.csv' d
        ON h.patient_id = d.subject_id
        SELECT patient_id, hospitalization_id
        """
    )
    return


@app.cell
def _():
    from pathlib import Path
    output_dir = Path('output/demo-data-2.1')
    output_dir.mkdir(parents=True, exist_ok=True)
    return (output_dir,)


@app.cell
def _(clif_table_pathfinder, duckdb, output_dir, tables_by_hosp_id):
    for _table_name in tables_by_hosp_id:
        _table_path = clif_table_pathfinder(_table_name)
        _q = f"""
        FROM '{_table_path}' t
        INNER JOIN demo_cohort_ids c
            USING (hospitalization_id)
        SELECT t.*
        """
        _df = duckdb.sql(_q).df()
        _df.to_parquet(output_dir / f'clif_{_table_name}.parquet')
    return


@app.cell
def _(clif_table_pathfinder, duckdb, output_dir, tables_by_pt_id):
    for _table_name in tables_by_pt_id:
        _table_path = clif_table_pathfinder(_table_name)
        _q = f"""
        FROM '{_table_path}' t
        INNER JOIN demo_cohort_ids c
            USING (patient_id)
        SELECT t.*
        """
        _df = duckdb.sql(_q).df()
        _df.to_parquet(output_dir / f'clif_{_table_name}.parquet')
    return


if __name__ == "__main__":
    app.run()
