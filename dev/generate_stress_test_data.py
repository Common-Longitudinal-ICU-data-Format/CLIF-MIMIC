import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import duckdb
    import string
    from pathlib import Path
    return Path, duckdb, string


@app.cell
def _(Path):
    n_copies = 4
    input_dir = Path('output/clif-mimic-1.2.0')
    output_dir = Path('output/clif-mimic-stress-test')
    return input_dir, n_copies, output_dir


@app.cell
def _():
    tables_hosp_id = [
        'adt', 'crrt_therapy', 'ecmo_mcs', 'hospital_diagnosis', 'input',
        'labs', 'medication_admin_continuous', 'medication_admin_intermittent',
        'output', 'patient_assessments', 'patient_assessments_raw_gcs',
        'patient_assessments_raw_gcs_only', 'patient_procedures', 'position',
        'respiratory_support', 'respiratory_support_bfill_processed', 'vitals',
    ]
    tables_pt_id = ['code_status', 'patient']
    tables_both = {
        'hospitalization': ['patient_id', 'hospitalization_id'],
        'microbiology_culture': [
            'patient_id', 'hospitalization_id', 'organism_id',
        ],
    }
    return tables_both, tables_hosp_id, tables_pt_id


@app.cell
def _():
    def build_copy_query(input_path, output_path, suffixes, id_cols):
        """Build a DuckDB COPY query using CROSS JOIN for single-scan duplication."""
        suffix_list = ', '.join(f"'{s}'" for s in suffixes)
        select_lines = [f"t.{col} || s.suffix AS {col}" for col in id_cols]
        select_lines.append(f"t.* EXCLUDE ({', '.join(id_cols)})")
        select_clause = '\n        , '.join(select_lines)
        return f"""COPY (
    FROM read_parquet('{input_path}') t
    CROSS JOIN (SELECT unnest([{suffix_list}]) AS suffix) s
    SELECT
        {select_clause}
) TO '{output_path}' (FORMAT PARQUET)"""
    return (build_copy_query,)


@app.cell
def _(
    build_copy_query,
    duckdb,
    input_dir,
    n_copies,
    output_dir,
    string,
    tables_both,
    tables_hosp_id,
    tables_pt_id,
):
    output_dir.mkdir(parents=True, exist_ok=True)
    _suffixes = list(string.ascii_uppercase[:n_copies])
    _mapping = (
        [(_t, ['hospitalization_id']) for _t in tables_hosp_id]
        + [(_t, ['patient_id']) for _t in tables_pt_id]
        + [(_t, _cols) for _t, _cols in tables_both.items()]
    )

    _results = []
    for _table_name, _id_cols in _mapping:
        _in_path = input_dir / f'clif_{_table_name}.parquet'
        if not _in_path.exists():
            continue
        _out_path = output_dir / f'clif_{_table_name}.parquet'
        _q = build_copy_query(
            str(_in_path), str(_out_path), _suffixes, _id_cols
        )
        duckdb.sql(_q)
        _count = duckdb.sql(
            f"SELECT count(*) FROM read_parquet('{_out_path}')"
        ).fetchone()[0]
        _results.append({
            'table_name': _table_name,
            'id_cols': ', '.join(_id_cols),
            'output_rows': _count,
        })

    results = _results
    return (results,)


@app.cell
def _(mo, results):
    import pandas as pd
    _df = pd.DataFrame(results)
    mo.ui.table(_df)
    return


if __name__ == "__main__":
    import argparse
    import duckdb
    import string
    from pathlib import Path

    parser = argparse.ArgumentParser(
        description='Generate CLIF stress test dataset'
    )
    parser.add_argument(
        '--n-copies', type=int, default=4,
        help='Number of copies to generate (default: 4)',
    )
    parser.add_argument(
        '--input-dir', type=str, default='output/clif-mimic-1.2.0',
        help='Input directory with CLIF parquet files',
    )
    parser.add_argument(
        '--output-dir', type=str, default='output/clif-mimic-stress-test',
        help='Output directory for stress test data',
    )
    args = parser.parse_args()

    suffixes = list(string.ascii_uppercase[:args.n_copies])
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tables_hosp = [
        'adt', 'crrt_therapy', 'ecmo_mcs', 'hospital_diagnosis', 'input',
        'labs', 'medication_admin_continuous', 'medication_admin_intermittent',
        'output', 'patient_assessments', 'patient_assessments_raw_gcs',
        'patient_assessments_raw_gcs_only', 'patient_procedures', 'position',
        'respiratory_support', 'respiratory_support_bfill_processed', 'vitals',
    ]
    mapping = (
        [(t, ['hospitalization_id']) for t in tables_hosp]
        + [(t, ['patient_id']) for t in ['code_status', 'patient']]
        + [
            ('hospitalization', ['patient_id', 'hospitalization_id']),
            (
                'microbiology_culture',
                ['patient_id', 'hospitalization_id', 'organism_id'],
            ),
        ]
    )

    print(f"\nGenerating stress test dataset:")
    print(f"  Input:   {input_dir}")
    print(f"  Output:  {output_dir}")
    print(f"  Copies:  {args.n_copies}\n")

    for table_name, id_cols in mapping:
        in_path = input_dir / f'clif_{table_name}.parquet'
        if not in_path.exists():
            print(f"  Skipping {table_name} (not found)")
            continue
        out_path = output_dir / f'clif_{table_name}.parquet'

        suffix_list = ', '.join(f"'{s}'" for s in suffixes)
        select_lines = [f"t.{col} || s.suffix AS {col}" for col in id_cols]
        select_lines.append(f"t.* EXCLUDE ({', '.join(id_cols)})")
        select_clause = '\n            , '.join(select_lines)

        q = f"""COPY (
    FROM read_parquet('{in_path}') t
    CROSS JOIN (SELECT unnest([{suffix_list}]) AS suffix) s
    SELECT
        {select_clause}
) TO '{out_path}' (FORMAT PARQUET)"""

        print(f"  Processing {table_name}...")
        duckdb.sql(q)
        count = duckdb.sql(
            f"SELECT count(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
        print(f"    -> {count:,} rows")

    print("\nDone!")
