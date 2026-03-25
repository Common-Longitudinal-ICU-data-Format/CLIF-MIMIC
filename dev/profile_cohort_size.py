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
    from pathlib import Path
    return Path, duckdb


@app.cell
def _(Path):
    data_dir = Path('output/clif-mimic-1.2.0')
    return (data_dir,)


@app.cell
def _(data_dir, duckdb):
    import pandas as pd

    _rows = []
    for _f in sorted(data_dir.glob('clif_*.parquet')):
        _table = _f.stem.removeprefix('clif_')
        _cols = {
            r[0]
            for r in duckdb.sql(
                f"FROM parquet_schema('{_f}') SELECT name"
            ).fetchall()
        }

        _exprs = ["count(*) AS total_rows"]
        if 'hospitalization_id' in _cols:
            _exprs.append(
                "count(DISTINCT hospitalization_id) AS unique_hosp_ids"
            )
        if 'patient_id' in _cols:
            _exprs.append(
                "count(DISTINCT patient_id) AS unique_patient_ids"
            )

        _row = duckdb.sql(
            f"FROM read_parquet('{_f}') SELECT {', '.join(_exprs)}"
        ).fetchone()

        _result = {'table_name': _table, 'total_rows': _row[0]}
        _idx = 1
        if 'hospitalization_id' in _cols:
            _result['unique_hosp_ids'] = _row[_idx]
            _idx += 1
        if 'patient_id' in _cols:
            _result['unique_patient_ids'] = _row[_idx]
        _rows.append(_result)

    profile_df = pd.DataFrame(_rows)
    for _col in ['total_rows', 'unique_hosp_ids', 'unique_patient_ids']:
        if _col in profile_df.columns:
            profile_df[_col] = profile_df[_col].astype('Int64')
    return (profile_df,)


@app.cell
def _(mo, profile_df):
    mo.ui.table(profile_df)
    return


if __name__ == "__main__":
    import sys
    import duckdb
    import pandas as pd
    from pathlib import Path

    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'output/clif-mimic-1.2.0'
    )
    if not data_dir.exists():
        print(f"Error: {data_dir} does not exist")
        sys.exit(1)

    rows = []
    for f in sorted(data_dir.glob('clif_*.parquet')):
        table = f.stem.removeprefix('clif_')
        cols = {
            r[0]
            for r in duckdb.sql(
                f"FROM parquet_schema('{f}') SELECT name"
            ).fetchall()
        }
        exprs = ["count(*) AS total_rows"]
        if 'hospitalization_id' in cols:
            exprs.append(
                "count(DISTINCT hospitalization_id) AS unique_hosp_ids"
            )
        if 'patient_id' in cols:
            exprs.append(
                "count(DISTINCT patient_id) AS unique_patient_ids"
            )
        row = duckdb.sql(
            f"FROM read_parquet('{f}') SELECT {', '.join(exprs)}"
        ).fetchone()
        result = {'table_name': table, 'total_rows': row[0]}
        idx = 1
        if 'hospitalization_id' in cols:
            result['unique_hosp_ids'] = row[idx]
            idx += 1
        if 'patient_id' in cols:
            result['unique_patient_ids'] = row[idx]
        rows.append(result)

    df = pd.DataFrame(rows)
    for col in ['total_rows', 'unique_hosp_ids', 'unique_patient_ids']:
        if col in df.columns:
            df[col] = df[col].astype('Int64')

    def _fmt(x):
        return f'{x:,}' if pd.notna(x) else ''

    fmt = {c: _fmt for c in df.columns if c != 'table_name'}
    print(f"\nCLIF Cohort Profile: {data_dir}\n")
    print(df.to_string(index=False, formatters=fmt))
    print()
