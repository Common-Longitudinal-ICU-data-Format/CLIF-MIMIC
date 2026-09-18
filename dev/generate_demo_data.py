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
    import pandas as pd
    return (pd,)


@app.cell
def _():
    from pathlib import Path
    # NOTE: all tables come from the CLIF-MIMIC 1.2.0 build, except mcs (renamed from ecmo_mcs),
    # which comes from the 3.0.0 build. Paths are relative -- run from the repo root.
    source_dirs = {
        'default': Path('output/clif-mimic-1.2.0'),
        'mcs': Path('output/clif-mimic-3.0.0'),
    }

    def source_path(table_name: str) -> Path:
        return source_dirs.get(table_name, source_dirs['default']) / f'clif_{table_name}.parquet'
    return Path, source_path


@app.cell
def _():
    # NOTE: the raw GCS tables must always be included in the demo data
    tables_by_hosp_id = [
        'adt', 'crrt_therapy', 'hospital_diagnosis', 'hospitalization', 'input',
        'invasive_hemodynamics', 'labs', 'mcs',
        'medication_admin_continuous', 'medication_admin_intermittent',
        'microbiology_culture', 'output',
        'patient_assessments', 'patient_assessments_raw_gcs', 'patient_assessments_raw_gcs_only',
        'patient_procedures', 'position',
        'respiratory_support', 'respiratory_support_bfill_processed', 'vitals'
    ]
    tables_by_pt_id = ['code_status', 'patient']
    return tables_by_hosp_id, tables_by_pt_id


@app.cell
def _(mo, source_path):
    demo_cohort_ids = mo.sql(
        f"""
        FROM "{source_path('hospitalization')}" h
        RIGHT JOIN 'data/mimic-data/mimic-iv-clinical-database-demo-2.2/demo_subject_id.csv' d
        ON h.patient_id = d.subject_id
        SELECT patient_id, hospitalization_id
        """
    )
    return (demo_cohort_ids,)


@app.cell
def _(Path):
    output_dir = Path('output/demo-data-2.1')
    output_dir.mkdir(parents=True, exist_ok=True)
    return (output_dir,)


@app.cell
def _(demo_cohort_ids, output_dir, pd, source_path, tables_by_hosp_id):
    for _table_name in tables_by_hosp_id:
        _table_path = source_path(_table_name)
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
        if _df.empty:
            # e.g. mcs: no demo hospitalization has MCS/ECMO data
            print(f'skipping {_table_name}: no rows for the demo cohort')
            continue
        _df.to_parquet(output_dir / f'clif_{_table_name}.parquet', index=False)
    return


@app.cell
def _(demo_cohort_ids, output_dir, pd, source_path, tables_by_pt_id):
    for _table_name in tables_by_pt_id:
        _table_path = source_path(_table_name)
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
        if _df.empty:
            print(f'skipping {_table_name}: no rows for the demo cohort')
            continue
        _df.to_parquet(output_dir / f'clif_{_table_name}.parquet', index=False)
    return


if __name__ == "__main__":
    app.run()
