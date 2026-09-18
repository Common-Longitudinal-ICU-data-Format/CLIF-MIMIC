import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # Convert the CLIF 2.1 demo data to CLIF 3.0

    Reads `output/demo-data-2.1/` (built by `dev/generate_demo_data.py`) and writes
    `output/demo-data-3.0.0/`, aligned with the pyCLIF 3.0 schemas.

    - values: `clifpy.utils.crosswalk.crosswalk_table_2_1_to_3_0`
    - structure: table/column renames, casts to the 3.0 data types, missing columns
      added as typed NULLs, extra columns dropped, schema column order

    NOTE: CLIF-MIMIC's own venv pins a clifpy release without the crosswalk module, so
    run this with the local pyCLIF venv, from the CLIF-MIMIC repo root:

    ```bash
    ../pyCLIF/.venv/bin/python dev/convert_demo_data_to_3_0.py
    ```
    """)
    return


@app.cell
def _():
    import json
    import logging
    from pathlib import Path

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    import pandas as pd
    from clifpy.schemas import load_schema
    from clifpy.utils.crosswalk import crosswalk_table_2_1_to_3_0

    # NOTE: input/output/invasive_hemodynamics have no 2.1 schema, so the crosswalk logs a
    # "Schema file not found" warning for each of their columns -- expected, silenced here
    logging.getLogger('clifpy.schemas').setLevel(logging.ERROR)
    return (
        Path,
        crosswalk_table_2_1_to_3_0,
        json,
        load_schema,
        pa,
        pc,
        pd,
        pq,
    )


@app.cell
def _(Path):
    src_dir = Path('output/demo-data-2.1')
    out_dir = Path('output/demo-data-3.0.0')
    report_path = Path('output/logs/demo_data_3.0.0_crosswalk_report.json')
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    # NOTE: same table set as the 2.1 demo (incl. the raw GCS tables, which must always be shipped)
    tables = sorted(p.stem.removeprefix('clif_') for p in src_dir.glob('clif_*.parquet'))
    return out_dir, report_path, src_dir, tables


@app.cell
def _():
    # 2.1 table name -> 3.0 table name (output file follows clifpy's clif_{table_name}.parquet)
    table_rename = {'crrt_therapy': 'renal_replacement_therapy'}

    # MIMIC-only tables with no 3.0 schema of their own -> the schema they are converted against
    schema_parent = {
        'patient_assessments_raw_gcs': 'patient_assessments',
        'patient_assessments_raw_gcs_only': 'patient_assessments',
        'respiratory_support_bfill_processed': 'respiratory_support',
    }

    # processed (non-CLIF) tables: cast the shared columns but keep their own column set
    keep_own_columns = {'respiratory_support_bfill_processed'}
    return keep_own_columns, schema_parent, table_rename


@app.cell
def _():
    # column renames between the CLIF-MIMIC 1.2.0 build and the 3.0 schemas
    column_renames = {
        'crrt_therapy': {'crrt_mode_name': 'mode_name', 'crrt_mode_category': 'mode_category'},
        'invasive_hemodynamics': {'hemodynamic_value': 'measurement_value'},
        'output': {'output_volume': 'output_value'},
        'medication_admin_continuous': {'caregiver_id': 'administering_provider_id'},
        'medication_admin_intermittent': {'caregiver_id': 'administering_provider_id'},
    }

    # NOTE: the MIMIC output table is urine only, charted in mL
    constant_cols = {'output': {'output_unit': 'mL'}}

    # FIXME: fix upstream in the CLIF-MIMIC mapping Google Sheet, then drop these patches.
    # Only unambiguous 1:1 fixes for values that are not valid in 2.1 either.
    value_patches = {
        'patient_assessments_raw_gcs': {'assessment_category': {'gcs_eyes': 'gcs_eye'}},
        'patient_assessments_raw_gcs_only': {'assessment_category': {'gcs_eyes': 'gcs_eye'}},
        'invasive_hemodynamics': {'measurement_category': {'cardiac_output_thermodilution': 'co_thermodilution'}},
        'medication_admin_continuous': {'med_category': {'aminocaproic': 'aminocaproic_acid'}},
    }
    return column_renames, constant_cols, value_patches


@app.cell
def _(load_schema, pa, pc, pd):
    # keep integer / boolean columns with nulls from turning into floats on the pandas round trip
    NULLABLE_TYPES = {
        pa.int8(): pd.Int8Dtype(),
        pa.int16(): pd.Int16Dtype(),
        pa.int32(): pd.Int32Dtype(),
        pa.int64(): pd.Int64Dtype(),
        pa.bool_(): pd.BooleanDtype(),
    }

    SCHEMA_ARROW_TYPES = {
        'VARCHAR': pa.string(),
        'DOUBLE': pa.float64(),
        'INT': pa.int32(),
        'DATE': pa.date32(),
        'DATETIME': pa.timestamp('us', tz='UTC'),
    }

    def cast_to_schema_type(arr, target):
        if arr.type == target:
            return arr
        if pa.types.is_integer(arr.type) and pa.types.is_integer(target):
            # any integer width is valid for INT
            return arr
        if pa.types.is_timestamp(arr.type) and pa.types.is_timestamp(target):
            # keep the source unit; only normalize the tz to UTC (same instants)
            return pc.cast(arr, pa.timestamp(arr.type.unit, tz='UTC'))
        if pa.types.is_float32(arr.type) and pa.types.is_float64(target):
            # go through the decimal string so float32 noise (0.1 -> 0.10000000149) is not kept
            return pc.cast(pc.cast(arr, pa.string()), target)
        return pc.cast(arr, target)

    def align_to_schema(tbl, schema, keep_own_columns=False):
        """Order, cast, NULL-fill and drop columns to match a 3.0 schema."""
        spec = {c['name']: c['data_type'] for c in schema['columns']}
        names = tbl.column_names if keep_own_columns else list(spec)
        cols = []
        for name in names:
            target = SCHEMA_ARROW_TYPES.get(spec.get(name))
            if name in tbl.column_names:
                arr = tbl[name]
                cols.append(arr if target is None else cast_to_schema_type(arr, target))
            else:
                cols.append(pa.chunked_array([pa.nulls(tbl.num_rows, target)]))
        return pa.table(cols, names=names)

    def restore_case(df, schema_table):
        """Map lowercased values back to their 2.1 permissible spelling, so the
        crosswalk renames (keyed on the 2.1 spelling, e.g. 'High Flow NC') apply."""
        for col in (load_schema(schema_table, '2.1') or {}).get('columns', []):
            allowed = col.get('permissible_values')
            if not allowed or col['name'] not in df.columns:
                continue
            # permissible values can be non-strings (e.g. tracheostomy 0/1)
            lookup = {v.lower(): v for v in allowed if isinstance(v, str)}
            df[col['name']] = df[col['name']].map(
                lambda v: lookup.get(v.lower(), v) if isinstance(v, str) else v
            )
        return df
    return NULLABLE_TYPES, align_to_schema, restore_case


@app.cell
def _(
    NULLABLE_TYPES,
    align_to_schema,
    column_renames,
    constant_cols,
    crosswalk_table_2_1_to_3_0,
    keep_own_columns,
    load_schema,
    pa,
    pq,
    restore_case,
    schema_parent,
    src_dir,
    value_patches,
):
    def convert_table(table_name):
        """Convert one 2.1 demo table to 3.0; returns (arrow table, crosswalk report)."""
        schema_table = schema_parent.get(table_name, table_name)
        df = pq.read_table(src_dir / f'clif_{table_name}.parquet').to_pandas(
            types_mapper=NULLABLE_TYPES.get
        )
        df = df.rename(columns=column_renames.get(table_name, {}))
        for col, value in constant_cols.get(table_name, {}).items():
            df[col] = value
        for col, patch in value_patches.get(table_name, {}).items():
            df[col] = df[col].map(lambda v: patch.get(v, v))
        if table_name in keep_own_columns:
            df = restore_case(df, schema_table)

        df, report = crosswalk_table_2_1_to_3_0(df, schema_table)

        tbl = align_to_schema(
            pa.Table.from_pandas(df, preserve_index=False),
            load_schema(schema_table, '3.0'),
            keep_own_columns=table_name in keep_own_columns,
        )
        report['dropped_columns'] = sorted(set(df.columns) - set(tbl.column_names))
        report['null_filled_columns'] = sorted(set(tbl.column_names) - set(df.columns))
        return tbl, report
    return (convert_table,)


@app.cell
def _(convert_table, out_dir, pq, table_rename, tables):
    crosswalk_reports = {}
    for _table_name in tables:
        _tbl, _report = convert_table(_table_name)
        _out_name = table_rename.get(_table_name, _table_name)
        pq.write_table(_tbl, out_dir / f'clif_{_out_name}.parquet')
        crosswalk_reports[_out_name] = {'source_table': _table_name, **_report}
        print(f'wrote clif_{_out_name}.parquet ({_tbl.num_rows} rows, {_tbl.num_columns} cols)')
    return (crosswalk_reports,)


@app.cell
def _(crosswalk_reports, json, pd, report_path):
    # the report holds category values + counts and column names only -- no IDs
    report_path.write_text(json.dumps(crosswalk_reports, indent=2, default=str))

    _rows = []
    for _table, _rep in crosswalk_reports.items():
        for _col, _col_rep in _rep['columns'].items():
            for _a in _col_rep['ambiguous']:
                _rows.append((_table, _col, 'ambiguous', _a['original'], None, _a['count']))
            for _u in _col_rep['unresolved']:
                _rows.append((_table, _col, 'unresolved', _u['original'], _u['produced'], _u['count']))
    unmapped_values = pd.DataFrame(
        _rows, columns=['table', 'column', 'kind', 'original', 'produced', 'count']
    )
    column_changes = pd.DataFrame(
        [
            (_t, ', '.join(_r['dropped_columns']), ', '.join(_r['null_filled_columns']))
            for _t, _r in crosswalk_reports.items()
            if _r['dropped_columns'] or _r['null_filled_columns']
        ],
        columns=['table', 'dropped_columns', 'null_filled_columns'],
    )
    print(f'\ncrosswalk report written to {report_path}')
    print('\nvalues not resolved to a 3.0 permissible value:')
    print(unmapped_values.to_string(index=False))
    print('\ncolumn changes:')
    print(column_changes.to_string(index=False))
    return column_changes, unmapped_values


@app.cell
def _(column_changes, mo, unmapped_values):
    mo.vstack([
        mo.md('### Values not resolved to a 3.0 permissible value'),
        unmapped_values,
        mo.md('### Dropped / NULL-filled columns'),
        column_changes,
    ])
    return


if __name__ == "__main__":
    app.run()
