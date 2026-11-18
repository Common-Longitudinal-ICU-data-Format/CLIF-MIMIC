import marimo

__generated_with = "0.21.1"
app = marimo.App()

with app.setup:
    # src/tables/chest_xray_alt.py
    import re
    import sys
    from pathlib import Path

    import marimo as mo
    import polars as pl

    # Opened as a notebook, sys.path starts at src/tables, and a dev dependency (auto-mix-prep)
    # installs its own top-level `src`; put the repo root first so `src` is this repo's.
    if sys.path[0] != str(Path(__file__).resolve().parents[2]):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

    from src.logging_config import get_logger, setup_logging
    from src.utils import MIMIC_CXR_BATCHES, MIMIC_CXR_META_DIR, mapping_path_finder, save_to_rclif

    logger = get_logger('tables.chest_xray')

    MIMIC_TZ = 'US/Eastern'

    # Constant categories for this table (always chest x-ray); set to None to take them from the mapping.
    CONSTANTS = {'modality_category': 'xr', 'region_category': 'chest'}

    CLIF_COLUMNS = [
        'patient_id', 'source_dicom_path', 'processed_image_path', 'accession',
        'order_dttm', 'capture_dttm', 'series_dttm', 'report_dttm', 'order_name',
        'manufacturer_name', 'model_name', 'modality_name', 'modality_category',
        'region_name', 'region_category', 'study_location_category', 'report_text',
        'series_description', 'patient_orientation', 'view_position',
    ]


@app.cell
def _():
    # p10 = pl.read_parquet("/Volumes/healthcare-allocate/Dema/CXR/mimic_cxr_meta/mimic_cxr_meta_p10.parquet")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # CLIF `chest_xray` from MIMIC-CXR

    Builds the table from the DICOM header metadata that
    `dev/chest_xray/mimic_cxr_stream_metadata.py` extracts (one row per DICOM, `path` + one column
    per DICOM field, all text). No DICOM is read here.

    - **Config**: the metadata folder is `mimic_cxr_meta_dir` in the current workspace of
      `config/config.json` (default `<repo>/output/mimic_cxr`), and `mimic_cxr_batches` lists the
      folders to use (e.g. `["p18", "p19"]`; empty = every folder extracted so far).
    - **Mapping**: `data/mappings/mimic-to-clif-mappings - chest_xray.csv` (`dicom_field` ->
      `clif_column`).
    - **Output**: `output/<clif_output_dir_name>/clif_chest_xray.parquet`, like every other table.

    `main.py` builds this table through `main()`; the cells below run the same functions step by
    step and add date/time QA tables.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 0. Load the mapping and the extracted metadata

    Every `mimic_cxr_meta_pXX.parquet` in the metadata folder is stacked. A folder whose run has
    not finished yet (only `mimic_cxr_meta_pXX_parts/` exists) is read from its part files, so the
    table can be built on whatever has been extracted so far. Only the DICOM fields listed in the
    mapping are loaded; a field a folder lacks is null.
    """)
    return


@app.function
def load_mapping() -> pl.DataFrame:
    '''The dicom_field -> clif_column map, all text. Values are DICOM keywords (PatientID,
    StudyDate) and VR codes (DA/TM/DT), so they are stripped but their case is kept.'''
    mapping = pl.read_csv(mapping_path_finder('chest_xray'), infer_schema_length=0)
    return mapping.with_columns(pl.col(pl.String).str.strip_chars())


@app.function
def find_meta_files(meta_dir: str = MIMIC_CXR_META_DIR) -> dict:
    '''{batch: [parquet files]} for every MIMIC-CXR folder with extracted metadata in `meta_dir`:
    the combined mimic_cxr_meta_pXX.parquet when the run finished, else its part files.'''
    meta_dir = Path(meta_dir)
    files = {}
    for parts_dir in sorted(meta_dir.glob('mimic_cxr_meta_p*_parts')):
        batch = re.fullmatch(r'mimic_cxr_meta_(p\d+)_parts', parts_dir.name)
        parts = sorted(parts_dir.glob('meta_*.parquet'))
        if batch and parts:
            files[batch.group(1)] = parts
    for combined in sorted(meta_dir.glob('mimic_cxr_meta_p*.parquet')):
        batch = re.fullmatch(r'mimic_cxr_meta_(p\d+)\.parquet', combined.name)
        if batch:
            files[batch.group(1)] = [combined]
    return dict(sorted(files.items()))


@app.function
def load_meta(mapping: pl.DataFrame, meta_dir: str = MIMIC_CXR_META_DIR, batches=MIMIC_CXR_BATCHES) -> pl.DataFrame:
    '''One row per DICOM: `path`, `batch`, and one text column per mapping dicom_field.
    `batches` limits the MIMIC-CXR folders used (e.g. ['p18', 'p19']); empty = all extracted so far.'''
    files = find_meta_files(meta_dir)
    if batches:
        not_found = sorted(set(batches) - set(files))
        if not_found:
            logger.warning(f'no extracted metadata for {not_found} in {meta_dir}; skipped')
        files = {b: f for b, f in files.items() if b in batches}
    if not files:
        raise FileNotFoundError(
            f'no mimic_cxr_meta_pXX.parquet (or _parts folder) in {meta_dir}; run '
            'dev/chest_xray/mimic_cxr_stream_metadata.py or set mimic_cxr_meta_dir in config.json'
        )
    # `path` is always loaded; `jpeg_path` is a placeholder row (processed_image_path is derived from `path`).
    fields = [f for f in mapping['dicom_field'].drop_nulls().unique(maintain_order=True)
              if f not in ('path', 'jpeg_path')]
    wanted = ['path'] + fields

    frames = []
    for batch, paths in files.items():
        if len(paths) > 1 or paths[0].parent.name.endswith('_parts'):
            logger.warning(f'{batch}: extraction not finished, reading {len(paths)} part files')
        n_rows = 0
        for p in paths:
            cols = [c for c in wanted if c in pl.read_parquet_schema(p)]
            frame = pl.read_parquet(p, columns=cols).with_columns(pl.all().cast(pl.String))
            frames.append(frame.with_columns(pl.lit(batch).alias('batch')))
            n_rows += frame.height
        logger.info(f'{batch}: {n_rows} DICOMs')
    meta = pl.concat(frames, how='diagonal')

    # Fields no folder has: keep them as null columns so the build and the QA cells still run.
    absent = [f for f in fields if f not in meta.columns]
    if absent:
        logger.warning(f'DICOM fields in the mapping but not in the metadata (left null): {absent}')
        meta = meta.with_columns([pl.lit(None).cast(pl.String).alias(f) for f in absent])

    n_dup = meta.height - meta['path'].n_unique()
    if n_dup:
        raise ValueError(f'{n_dup} duplicated path values across {list(files)}')
    logger.info(f'loaded {meta.height} DICOMs x {len(fields)} fields from {list(files)}')
    # Null = the DICOM header has no such field; '' = the field is there but blank.
    return meta.select(wanted + ['batch'])


@app.cell
def _():
    mapping = load_mapping()
    mapping.filter(pl.col('clif_column').is_not_null())
    return (mapping,)


@app.cell
def _(mapping):
    meta = load_meta(mapping)
    meta.group_by('batch').agg(pl.len().alias('n_dicoms'), pl.col('PatientID').n_unique().alias('n_patients')).sort('batch')
    return (meta,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## QA: Study vs Series vs Acquisition date/time agreement

    Per DICOM, checks whether `StudyDate == SeriesDate == AcquisitionDate` and
    `StudyTime == SeriesTime == AcquisitionTime`. Each date (`DA`, YYYYMMDD) + time (`TM`,
    HHMMSS[.ffff]) pair is combined into one timestamp; where the three disagree, the gap table
    buckets the spread (max − min of the three timestamps). Rows with any of the six fields
    missing/unparseable are counted separately (`all_present` false, `gap_sec` null).
    """)
    return


@app.cell
def _(meta):
    # Combine each DICOM DA(YYYYMMDD) + TM(HHMMSS[.ffff]) pair into a naive datetime (whole seconds).
    # Null/empty raw fields stay null end-to-end (a missing time is "incomparable", NOT midnight),
    # so equality flags and gap_sec are null for those rows rather than reporting a spurious gap.
    def _da(c):  # DICOM date -> YYYYMMDD (null stays null)
        return pl.col(c).cast(pl.String).str.strip_chars().str.slice(0, 8)

    def _tm(c):  # DICOM time -> normalized HHMMSS (null/empty stays null)
        _t = pl.col(c).cast(pl.String).str.strip_chars()
        _h = _t.str.split('.').list.first().str.replace_all(':', '', literal=True).str.pad_end(6, '0').str.slice(0, 6)
        return pl.when(_t.is_null() | (_t.str.len_chars() == 0)).then(None).otherwise(_h)

    def _dt(date_col, time_col):
        return (pl.concat_str([_da(date_col), _tm(time_col)])
                .str.to_datetime(format='%Y%m%d%H%M%S', strict=False))  # null if either part null

    _fields = ['StudyDate', 'SeriesDate', 'AcquisitionDate', 'StudyTime', 'SeriesTime', 'AcquisitionTime']
    dt_check = (
        meta.select('path', 'PatientID', *_fields)
        .with_columns(
            _dt('StudyDate', 'StudyTime').alias('study_dt'),
            _dt('SeriesDate', 'SeriesTime').alias('series_dt'),
            _dt('AcquisitionDate', 'AcquisitionTime').alias('acq_dt'),
            pl.all_horizontal([pl.col(f).is_not_null() for f in _fields]).alias('all_present'),
        )
        .with_columns(
            # date-only agreement across the three (null if any date missing)
            ((_da('StudyDate') == _da('SeriesDate')) & (_da('SeriesDate') == _da('AcquisitionDate'))).alias('dates_equal'),
            # time-only agreement across the three (null if any time missing)
            ((_tm('StudyTime') == _tm('SeriesTime')) & (_tm('SeriesTime') == _tm('AcquisitionTime'))).alias('times_equal'),
            # spread of the three full timestamps, only when all six fields are present
            pl.when(pl.col('all_present'))
              .then((pl.max_horizontal('study_dt', 'series_dt', 'acq_dt')
                     - pl.min_horizontal('study_dt', 'series_dt', 'acq_dt')).dt.total_seconds())
              .otherwise(None).alias('gap_sec'),
        )
        .with_columns((pl.col('gap_sec') == 0).alias('dttm_all_equal'))
    )
    dt_check
    return (dt_check,)


@app.cell
def _(dt_check):
    # Summary: how many DICOMs have the three dates / times / full datetimes in agreement.
    _n = dt_check.height
    dt_agreement = pl.DataFrame({
        'check': ['StudyDate = SeriesDate = AcquisitionDate',
                  'StudyTime = SeriesTime = AcquisitionTime',
                  'full Study = Series = Acquisition datetime',
                  'any of the 6 date/time fields missing/unparseable'],
        'n': [
            dt_check.filter(pl.col('dates_equal')).height,
            dt_check.filter(pl.col('times_equal')).height,
            dt_check.filter(pl.col('dttm_all_equal')).height,
            dt_check.filter(~pl.col('all_present')).height,
        ],
    }).with_columns(
        pl.lit(_n).alias('n_total'),
        (100 * pl.col('n') / max(_n, 1)).round(1).alias('pct'),
    )
    dt_agreement
    return


@app.cell
def _(dt_check):
    # Time-gap table for DICOMs whose three timestamps are NOT identical (gap_sec > 0).
    _g = pl.col('gap_sec')
    gap_table = (
        dt_check.filter(_g.is_not_null() & (_g > 0))
        .with_columns(
            pl.when(_g <= 1).then(pl.lit('1) <=1s'))
              .when(_g <= 10).then(pl.lit('2) 1-10s'))
              .when(_g <= 60).then(pl.lit('3) 10-60s'))
              .when(_g <= 600).then(pl.lit('4) 1-10min'))
              .when(_g <= 3600).then(pl.lit('5) 10-60min'))
              .when(_g <= 86400).then(pl.lit('6) 1-24h'))
              .otherwise(pl.lit('7) >1d')).alias('gap_bucket')
        )
        .group_by('gap_bucket')
        .agg(
            pl.len().alias('n_cxr'),
            pl.col('PatientID').n_unique().alias('n_patients'),
            _g.min().round(1).alias('min_sec'),
            _g.max().round(1).alias('max_sec'),
        )
        .sort('gap_bucket')
    )
    _ndiff = dt_check.filter(_g > 0).height
    print(f'{_ndiff} of {dt_check.height} DICOMs have a Study/Series/Acquisition timestamp gap > 0')
    gap_table
    return


@app.cell
def _(dt_check):
    # Which PAIR drives the gap? gap_sec above is the full spread (max - min of the three) and
    # does not say which pair differs. These are the three pairwise gaps (abs seconds, on the
    # combined date+time timestamp), over DICOMs where all six fields are present.
    pair_gaps = dt_check.filter(pl.col('all_present')).with_columns(
        (pl.col('study_dt')  - pl.col('series_dt')).dt.total_seconds().abs().alias('study_vs_series_sec'),
        (pl.col('series_dt') - pl.col('acq_dt')).dt.total_seconds().abs().alias('series_vs_acq_sec'),
        (pl.col('study_dt')  - pl.col('acq_dt')).dt.total_seconds().abs().alias('study_vs_acq_sec'),
    )
    pair_summary = pl.DataFrame({
        'pair': ['Study vs Series', 'Series vs Acquisition', 'Study vs Acquisition'],
        'n_differ': [
            pair_gaps.filter(pl.col('study_vs_series_sec') > 0).height,
            pair_gaps.filter(pl.col('series_vs_acq_sec')   > 0).height,
            pair_gaps.filter(pl.col('study_vs_acq_sec')    > 0).height,
        ],
        'median_sec_when_diff': [
            pair_gaps.filter(pl.col('study_vs_series_sec') > 0)['study_vs_series_sec'].median(),
            pair_gaps.filter(pl.col('series_vs_acq_sec')   > 0)['series_vs_acq_sec'].median(),
            pair_gaps.filter(pl.col('study_vs_acq_sec')    > 0)['study_vs_acq_sec'].median(),
        ],
        'max_sec': [
            pair_gaps['study_vs_series_sec'].max(),
            pair_gaps['series_vs_acq_sec'].max(),
            pair_gaps['study_vs_acq_sec'].max(),
        ],
    })
    pair_summary
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 1. Build the `chest_xray` table

    Uses the `mapping` to assemble the CLIF columns from `meta` (all polars).

    - `source_dicom_path` is the DICOM's path in MIMIC-CXR (`files/pXX/pXXXXXXXX/sXXXXXXXX/<id>.dcm`)
      and `processed_image_path` the same path with `.jpg`, which is where MIMIC-CXR-JPG keeps the
      rendered image. `modality_category` / `region_category` come from `CONSTANTS` unless those
      are `None` (so map Modality/BodyPartExamined to the extra `modality_name` / `region_name`
      columns, not the category columns).
    - Datetime columns: map a date field to `<x>_date` and a time field to `<x>_time` (or use a single
      `<x>_dttm`); both feed the combined UTC column `<x>_dttm` (naive local time localized to
      `MIMIC_TZ`, then converted to UTC). A single `DT` field also works.
    - Any other column takes exactly one DICOM field.
    """)
    return


@app.function
def dicom_datetime_expr(date_col, time_col, tz):
    '''polars expr: combine DICOM DA (YYYYMMDD) + TM (HHMMSS/HHMM/HH; sub-second dropped) -> UTC
    datetime at whole-second resolution (prints as YYYY-MM-DD HH:MM:SS+00:00).'''
    date = pl.col(date_col).cast(pl.String).str.strip_chars().str.slice(0, 8)
    if time_col is None:
        hms = pl.lit('000000')
    else:
        t = pl.col(time_col).cast(pl.String).str.strip_chars().fill_null('')
        head = t.str.split('.').list.first().str.replace_all(':', '', literal=True)
        hms = pl.when(head.str.len_chars() == 0).then(pl.lit('000000')).otherwise(
            head.str.pad_end(6, '0').str.slice(0, 6))
    base = pl.concat_str([date, hms]).str.to_datetime(format='%Y%m%d%H%M%S', strict=False)
    return base.dt.replace_time_zone(tz, ambiguous='earliest', non_existent='null').dt.convert_time_zone('UTC')


@app.function
def dicom_dt_expr(dt_col, tz):
    '''polars expr: parse a DICOM DT (YYYYMMDDHHMMSS...) field -> UTC datetime.'''
    return (pl.col(dt_col).cast(pl.String).str.strip_chars().str.slice(0, 14)
            .str.to_datetime(format='%Y%m%d%H%M%S', strict=False)
            .dt.replace_time_zone(tz, ambiguous='earliest', non_existent='null').dt.convert_time_zone('UTC'))


@app.function
def target_column(label):
    '''Resolve a mapping clif_column label to its output column:
    `<x>_date` / `<x>_time` both feed the combined `<x>_dttm`; everything else is unchanged.'''
    if label.endswith('_date') or label.endswith('_time'):
        return label.rsplit('_', 1)[0] + '_dttm'
    return label


@app.function
def build_chest_xray(df, mapping, clif_columns=CLIF_COLUMNS, tz=MIMIC_TZ, constants=CONSTANTS):
    m = mapping.filter(pl.col('clif_column').fill_null('') != '')

    # Group mapped rows by their resolved target column (a date+time pair -> one *_dttm).
    auto = {'source_dicom_path', 'processed_image_path'}   # filled from `path`, not from a DICOM field
    groups, order = {}, []
    for r in m.select(['dicom_field', 'clif_column', 'VR']).to_dicts():
        target = target_column(r['clif_column'])
        if target not in groups:
            groups[target] = []
            order.append(target)
        groups[target].append((r['dicom_field'], r['clif_column'], (r['VR'] or '').strip()))

    bad = sorted(set(groups) - set(clif_columns))
    if bad:
        raise ValueError(f'unknown clif_column values in mapping: {bad}')
    needed = [fld for t in groups if t not in auto for (fld, _, _) in groups[t]]
    missing = sorted(set(needed) - set(df.columns))
    if missing:
        raise ValueError(f'mapped DICOM fields not in metadata table: {missing}')

    const_cols = {c for c, v in constants.items() if v is not None}

    # `path` is pXX/pXXXXXXXX/sXXXXXXXX/<id>.dcm; MIMIC-CXR and MIMIC-CXR-JPG both keep it under files/.
    source_path = pl.concat_str([pl.lit('files/'), pl.col('path')])
    exprs = [
        source_path.alias('source_dicom_path'),
        source_path.str.replace(r'\.dcm$', '.jpg').alias('processed_image_path'),
    ]
    for target in order:
        if target in auto or target in const_cols:     # auto-filled or overridden by a constant
            continue
        items = groups[target]                          # list of (dicom_field, label, VR)
        if target.endswith('_dttm'):
            da = [f for (f, label, vr) in items if vr == 'DA' or label.endswith('_date')]
            tm = [f for (f, label, vr) in items if vr == 'TM' or label.endswith('_time')]
            dt = [f for (f, label, vr) in items if vr == 'DT']
            if dt and not (da or tm):
                exprs.append(dicom_dt_expr(dt[0], tz).alias(target))
            elif len(da) == 1 and len(tm) <= 1:
                exprs.append(dicom_datetime_expr(da[0], tm[0] if tm else None, tz).alias(target))
            else:
                raise ValueError(f'{target}: expected one date field and at most one time field, got {[i[0] for i in items]}')
        else:
            if len(items) != 1:
                raise ValueError(f'{target}: expected exactly one DICOM field, got {[i[0] for i in items]}')
            # De-identification blanks some fields (e.g. Manufacturer): a blank is a missing value.
            exprs.append(pl.col(items[0][0]).cast(pl.String).str.strip_chars().replace('', None).alias(target))

    for c, v in constants.items():
        if v is not None:
            exprs.append(pl.lit(v).cast(pl.String).alias(c))

    out = df.select(exprs)

    # Unmapped columns: typed nulls (VARCHAR -> String, DATETIME -> tz-aware Datetime).
    add = [
        (pl.lit(None).cast(pl.Datetime('us', 'UTC')) if c.endswith('_dttm') else pl.lit(None).cast(pl.String)).alias(c)
        for c in clif_columns if c not in out.columns
    ]
    if add:
        out = out.with_columns(add)
    return out.select(clif_columns)


@app.cell
def _(mapping, meta):
    chest_xray = build_chest_xray(meta, mapping)
    print(f'chest_xray: {chest_xray.height} rows x {chest_xray.width} columns')

    _h = max(chest_xray.height, 1)
    summary = pl.DataFrame({
        'column': chest_xray.columns,
        'dtype': [str(dt) for dt in chest_xray.dtypes],
        'non_null': [chest_xray.height - chest_xray[c].null_count() for c in chest_xray.columns],
        'n_unique': [chest_xray[c].n_unique() for c in chest_xray.columns],
    }).with_columns((100 * pl.col('non_null') / _h).round(1).alias('pct_non_null'))
    summary
    return (chest_xray,)


@app.cell
def _(chest_xray):
    chest_xray
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## QC: field coverage, DICOM header -> `chest_xray`

    One row per mapping row, then one per CLIF column that no DICOM field feeds (constants, unmapped).

    - `n_with_field`: DICOMs whose header has the field at all (it may be blank).
    - `n_with_value`: DICOMs where the field has a non-blank value.
    - `n_in_table`: rows of the final table with a value in `clif_column`. For a `*_dttm` column
      this is counted once per contributing field (its date row and its time row show the same
      number), and it is below `n_with_value` when a value does not parse.
    """)
    return


@app.function
def qc_field_coverage(meta: pl.DataFrame, mapping: pl.DataFrame, chest_xray: pl.DataFrame) -> pl.DataFrame:
    '''Per DICOM field: how many DICOMs have it, how many have a value, and how many rows of the
    final table have a value in the CLIF column it feeds. Counts only.'''
    def in_table(col):
        return chest_xray.height - chest_xray[col].null_count() if col in chest_xray.columns else None

    rows, fed = [], set()
    for r in mapping.select('dicom_field', 'clif_column').to_dicts():
        field, target = r['dicom_field'], target_column(r['clif_column']) if r['clif_column'] else None
        values = meta[field] if field in meta.columns else None      # `jpeg_path` is not a DICOM field
        rows.append({
            'dicom_field': field,
            'clif_column': target,
            'n_with_field': None if values is None else meta.height - values.null_count(),
            'n_with_value': None if values is None else int((values.str.strip_chars().str.len_chars() > 0).sum()),
            'n_in_table': in_table(target),
        })
        fed.add(target)
    rows += [{'dicom_field': None, 'clif_column': c, 'n_with_field': None, 'n_with_value': None,
              'n_in_table': in_table(c)} for c in chest_xray.columns if c not in fed]

    n = max(meta.height, 1)
    return pl.DataFrame(rows, schema={
        'dicom_field': pl.String, 'clif_column': pl.String,
        'n_with_field': pl.Int64, 'n_with_value': pl.Int64, 'n_in_table': pl.Int64,
    }).with_columns(
        pl.lit(meta.height).alias('n_dicoms'),
        (100 * pl.col('n_with_field') / n).round(1).alias('pct_with_field'),
        (100 * pl.col('n_with_value') / n).round(1).alias('pct_with_value'),
        (100 * pl.col('n_in_table') / n).round(1).alias('pct_in_table'),
    )


@app.cell
def _(chest_xray, mapping, meta):
    field_coverage = qc_field_coverage(meta, mapping, chest_xray)
    field_coverage
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 2. Save

    `save_chest_xray` writes `clif_chest_xray.parquet` to the CLIF output folder. `main()` is the
    whole build in one call (what `main.py` runs); the button below saves the table built above.
    """)
    return


@app.function
def save_chest_xray(chest_xray: pl.DataFrame):
    # Arrow-backed pandas columns keep an all-null text column typed as text in the parquet file.
    save_to_rclif(chest_xray.to_pandas(use_pyarrow_extension_array=True), 'chest_xray')
    logger.info(f'saved {chest_xray.height} rows x {chest_xray.width} columns to clif_chest_xray.parquet')


@app.function
def main():
    '''Build and save the table (main.py runs this as `chest_xray._main`).'''
    logger.info('starting to build clif chest_xray table -- ')
    mapping = load_mapping()
    chest_xray = build_chest_xray(load_meta(mapping), mapping)
    save_chest_xray(chest_xray)
    logger.info('output saved to a parquet file, everything completed for the chest_xray table!')


@app.cell
def _():
    # save_button = mo.ui.run_button(label='Save clif_chest_xray.parquet')
    # save_button
    return


@app.cell
def _():
    # if save_button.value:
    #     save_chest_xray(chest_xray)
    return


@app.cell
def _(chest_xray):
    save_chest_xray(chest_xray)
    return


@app.cell
def _():
    pl.read_parquet("/Users/dema/WD/CLIF-MIMIC/output/rclif-2.1/clif_chest_xray.parquet")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
