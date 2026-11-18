import marimo

__generated_with = "0.21.1"
app = marimo.App()

with app.setup:
    # src/tables/chest_xray.py
    import re
    import sys
    import zipfile
    from pathlib import Path

    import marimo as mo
    import polars as pl

    # Opened as a notebook, sys.path starts at src/tables, and a dev dependency (auto-mix-prep)
    # installs its own top-level `src`; put the repo root first so `src` is this repo's.
    if sys.path[0] != str(Path(__file__).resolve().parents[2]):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

    from src.logging_config import get_logger, setup_logging
    from src.utils import (
        CLIF_PATIENT_PATH,
        MIMIC_CXR_BATCHES,
        MIMIC_CXR_META_DIR,
        MIMIC_CXR_REPORTS_ZIP,
        clif_table_pathfinder,
        convert_tz_to_utc,
        mapping_path_finder,
        save_to_rclif,
    )

    logger = get_logger('tables.chest_xray')

    # Constant categories for this table (always x-ray); set to None to take them from the mapping.
    # region_category is not a constant: it is derived from region_name (see region_category_expr).
    CONSTANTS = {'modality_category': 'xr'}

    # Columns of the saved table, in order.
    CLIF_COLUMNS = [
        'patient_id', 'source_dicom_path', 'processed_image_path', 'image_type', 'accession',
        'requesting_service', 'order_dttm', 'capture_dttm', 'acquisition_dttm', 'series_dttm', 'report_dttm',
        'manufacturer_name', 'model_name', 'modality_name', 'modality_category',
        'region_name', 'region_category', 'study_location_category',
        'series_description', 'patient_orientation', 'view_position', 'report_text',
    ]

    # The study number links a DICOM to its report (sXXXXXXXX.txt). It is built from the mapping like
    # a CLIF column, used for the join, then dropped: it is not part of the saved table.
    REPORT_KEY = 'StudyID'
    BUILD_COLUMNS = [c for c in CLIF_COLUMNS if c != 'report_text'] + [REPORT_KEY]


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # CLIF `chest_xray` from MIMIC-CXR

    Builds the table from the DICOM header metadata that
    `scripts/mimic_cxr_stream_metadata.py` extracts (one row per DICOM, `path` + one column
    per DICOM field, all text), keeps the patients in the CLIF `patient` table, and adds each
    study's report text. No DICOM is read here.

    - **Config**: every path comes from the current workspace of `config/config.json`.
        - `mimic_cxr_meta_dir`: the metadata folder (default `<repo>/output/mimic_cxr`).
        - `mimic_cxr_batches`: the folders to use (e.g. `["p18", "p19"]`; empty = every folder
          extracted so far).
        - `mimic_cxr_reports_zip`: MIMIC-CXR's `mimic-cxr-reports.zip` (empty = `report_text` is
          left null).
        - `clif_patient_path`: the CLIF patient table to limit to (empty = `clif_patient.parquet`
          in the CLIF output folder, i.e. the one this repo builds).
    - **Mapping**: `data/mappings/mimic-to-clif-mappings - chest_xray.csv` (`dicom_field` ->
      `clif_column`).
    - **Output**: `output/<clif_output_dir_name>/clif_chest_xray.parquet`, like every other table.

    `main.py` builds this table through `main()`; the cells below run the same functions step by
    step and add QA tables.

    ## Before you build: extract the DICOM metadata

    This table needs the DICOM headers of MIMIC-CXR, not the images, so you do **not** have to
    download the DICOMs (several hundred GB). `scripts/mimic_cxr_stream_metadata.py` reads each
    header straight from PhysioNet and keeps only Parquet files on disk. Run it once per top-level
    folder (`p10` ... `p19`) from the repo root; it needs `wget`, a credentialed PhysioNet account
    with access to MIMIC-CXR:

    ```
    uv run python scripts/mimic_cxr_stream_metadata.py --user <physionet-username> --batch p10
    ```

    - It can be stopped and re-run at any time and carries on where it left off; add `--limit 20`
      for a quick trial.
    - A folder can take hours. On a Mac, put `caffeinate -i` in front of the command so the
      computer does not go to sleep while it runs (it works the same with `--user` or
      `--local-dir`):

      ```
      caffeinate -i uv run python scripts/mimic_cxr_stream_metadata.py --user <physionet-username> --batch p10
      ```

      `caffeinate` only prevents sleep: keep the terminal window open (or run inside `tmux` /
      `screen`, which is also what to use on Linux or an HPC node). If the run does get
      interrupted, re-run the same command and it carries on.
    - Run one or two folders at a time. Each run already opens 4 connections to PhysioNet
      (`--workers`), and starting more folders at once slows down the images per second of every
      run rather than finishing sooner.
    - Output goes to `<repo>/output/mimic_cxr`, which is also the default `mimic_cxr_meta_dir`. If
      you pass `--out-dir`, set `mimic_cxr_meta_dir` to the same folder.
    - A folder that is still running can already be built from: its part files are used.
    - **If you already have the DICOMs on disk**, read them locally instead: no PhysioNet login,
      `wget` or network needed, and the output is the same.

      ```
      uv run python scripts/mimic_cxr_stream_metadata.py --local-dir /path/to/mimic-cxr/2.1.0 --batch p10
      ```

      `--local-dir` is the folder that holds MIMIC-CXR's `files/` folder. It expects the `.dcm`
      files as downloaded, under their original names and folders
      (`files/p10/p10000032/s50414267/<id>.dcm`); only the header of each file is read, not the
      pixel data. Only the folder you pass as `--batch` has to be there. Every `.dcm` found in it
      is used, so an incomplete download gives an incomplete table: keep
      `cxr-record-list.csv.gz` next to `files/` and the script reports how many listed images
      are not on disk.

    The report text is a separate, small download: `mimic-cxr-reports.zip` from the MIMIC-CXR
    project page on PhysioNet (see `mimic_cxr_reports_zip` above).
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
            'scripts/mimic_cxr_stream_metadata.py or set mimic_cxr_meta_dir in config.json'
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
    ## QA: every extracted DICOM field

    All fields in the extracted metadata, not only the mapped ones: the share of DICOMs whose
    header has the field, the share with a non-blank value, and the most common values. Use it to
    decide what to add to the mapping.
    """)
    return


@app.cell
def _():
    _files = find_meta_files()
    if MIMIC_CXR_BATCHES:
        _files = {b: f for b, f in _files.items() if b in MIMIC_CXR_BATCHES}
    _raw = pl.concat([pl.read_parquet(p) for paths in _files.values() for p in paths], how='diagonal_relaxed')
    _n = max(_raw.height, 1)
    _text = _raw.select(pl.all().cast(pl.String).str.strip_chars())

    def _samples(c, k=5):
        _v = _text[c].filter(_text[c].str.len_chars() > 0)
        return _v.value_counts(sort=True)[c].head(k).to_list()

    field_check = pl.DataFrame({
        'field': _raw.columns,
        'pct_with_field': [round(100 * (_raw.height - _raw[c].null_count()) / _n, 1) for c in _raw.columns],
        'pct_with_value': [round(100 * (_text[c].str.len_chars() > 0).sum() / _n, 1) for c in _raw.columns],
        'n_unique': [_text[c].filter(_text[c].str.len_chars() > 0).n_unique() for c in _raw.columns],
        'sample_vals': [_samples(c) for c in _raw.columns],
    })
    field_check
    return


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
      rendered image. `modality_category` comes from `CONSTANTS` unless it is `None` (so map
      Modality to the extra `modality_name` column, not the category column).
    - `region_category` is `chest` when `region_name` (the DICOM `BodyPartExamined`) contains the
      word "chest" in any letter case, and `other` otherwise, including when `region_name` is
      missing.
    - Datetime columns: map a date field to `<x>_date` and a time field to `<x>_time` (or use a single
      `<x>_dttm`); both feed the combined UTC column `<x>_dttm`. A single `DT` field also works. The local
      time is converted to UTC by `convert_tz_to_utc` in `src/utils.py`, the same function every
      other CLIF table uses.
    - Any other column takes exactly one DICOM field.
    - The result also carries `StudyID` (`REPORT_KEY`), the key the reports are joined on in step 3.
    """)
    return


@app.function
def dicom_datetime_expr(date_col, time_col):
    '''polars expr: combine DICOM DA (YYYYMMDD) + TM (HHMMSS/HHMM/HH; sub-second dropped) -> local
    datetime (no timezone yet) at whole-second resolution.'''
    date = pl.col(date_col).cast(pl.String).str.strip_chars().str.slice(0, 8)
    if time_col is None:
        hms = pl.lit('000000')
    else:
        t = pl.col(time_col).cast(pl.String).str.strip_chars().fill_null('')
        head = t.str.split('.').list.first().str.replace_all(':', '', literal=True)
        hms = pl.when(head.str.len_chars() == 0).then(pl.lit('000000')).otherwise(
            head.str.pad_end(6, '0').str.slice(0, 6))
    return pl.concat_str([date, hms]).str.to_datetime(format='%Y%m%d%H%M%S', strict=False)


@app.function
def dicom_dt_expr(dt_col):
    '''polars expr: parse a DICOM DT (YYYYMMDDHHMMSS...) field -> local datetime (no timezone yet).'''
    return (pl.col(dt_col).cast(pl.String).str.strip_chars().str.slice(0, 14)
            .str.to_datetime(format='%Y%m%d%H%M%S', strict=False))


@app.function
def local_to_utc(local: pl.Series) -> pl.Series:
    '''MIMIC local time -> UTC, with the repo's shared convert_tz_to_utc (it works on pandas).'''
    utc = pl.Series(local.name, convert_tz_to_utc(local.to_pandas()))
    return utc.cast(pl.Datetime('us', 'UTC'))


@app.function
def region_category_expr(region_col='region_name'):
    '''polars expr: 'chest' when the region name (DICOM BodyPartExamined) contains "chest" in any
    letter case, else 'other' (also when the region name is missing).'''
    is_chest = pl.col(region_col).str.to_lowercase().str.contains('chest', literal=True)
    return pl.when(is_chest).then(pl.lit('chest')).otherwise(pl.lit('other'))


@app.function
def target_column(label):
    '''Resolve a mapping clif_column label to its output column:
    `<x>_date` / `<x>_time` both feed the combined `<x>_dttm`; everything else is unchanged.'''
    if label.endswith('_date') or label.endswith('_time'):
        return label.rsplit('_', 1)[0] + '_dttm'
    return label


@app.function
def build_chest_xray(df, mapping, clif_columns=BUILD_COLUMNS, constants=CONSTANTS):
    '''One row per DICOM with the columns in `clif_columns`, assembled from `df` by the mapping.'''
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
                exprs.append(dicom_dt_expr(dt[0]).alias(target))
            elif len(da) == 1 and len(tm) <= 1:
                exprs.append(dicom_datetime_expr(da[0], tm[0] if tm else None).alias(target))
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
    # The datetime columns are local time so far.
    out = out.with_columns([local_to_utc(out[c]) for c in out.columns if out.schema[c] == pl.Datetime])

    # Unmapped columns: typed nulls (VARCHAR -> String, DATETIME -> tz-aware Datetime).
    add = [
        (pl.lit(None).cast(pl.Datetime('us', 'UTC')) if c.endswith('_dttm') else pl.lit(None).cast(pl.String)).alias(c)
        for c in clif_columns if c not in out.columns
    ]
    if add:
        out = out.with_columns(add)
    # region_category follows region_name, unless the mapping or a constant already set it.
    if 'region_category' in clif_columns and 'region_category' not in (set(groups) | const_cols):
        out = out.with_columns(region_category_expr().alias('region_category'))
    return out.select(clif_columns)


@app.cell
def _(mapping, meta):
    built = build_chest_xray(meta, mapping)
    print(f'built: {built.height} DICOMs, {built["patient_id"].n_unique()} patients')
    return (built,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 2. Keep the patients in the CLIF `patient` table

    MIMIC-CXR covers more patients than MIMIC-IV. Only DICOMs whose `patient_id` is in the CLIF
    `patient` table are kept: `clif_patient_path` in the config when set, else the
    `clif_patient.parquet` this repo builds in the CLIF output folder.
    """)
    return


@app.function
def filter_to_clif_patients(df: pl.DataFrame, patient_path: str = CLIF_PATIENT_PATH) -> pl.DataFrame:
    '''Keep the rows whose patient_id is in the CLIF patient table at `patient_path`
    (empty = clif_patient.parquet in the CLIF output folder).'''
    path = Path(patient_path or clif_table_pathfinder('patient'))
    if not path.is_file():
        raise FileNotFoundError(
            f'CLIF patient table not found at {path}; build it first (set "patient": 1 under '
            'clif_tables in config.json) or set clif_patient_path in config.json'
        )
    patient_ids = pl.read_parquet(path, columns=['patient_id'])['patient_id'].cast(pl.String).unique()
    out = df.filter(pl.col('patient_id').is_in(patient_ids.implode()))
    logger.info(
        f'kept {out.height} of {df.height} DICOMs ({out["patient_id"].n_unique()} of '
        f'{df["patient_id"].n_unique()} patients) that are in {path}'
    )
    return out


@app.cell
def _(built):
    in_clif = filter_to_clif_patients(built)
    print(f'in CLIF patient: {in_clif.height} DICOMs, {in_clif["patient_id"].n_unique()} patients')
    return (in_clif,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 3. Add the report text

    `mimic-cxr-reports.zip` holds one `sXXXXXXXX.txt` per study. Each report is read straight from
    the zip (`mimic_cxr_reports_zip` in the config) and joined on the study number, so every DICOM
    of a study gets the same `report_text`. Without a zip the column is left null. `StudyID` is
    dropped here, which leaves exactly `CLIF_COLUMNS`.
    """)
    return


@app.function
def load_reports(zip_path: str = MIMIC_CXR_REPORTS_ZIP):
    '''One row per study report in mimic-cxr-reports.zip: `StudyID`, `report_text`.
    None (with a warning) when no zip is configured or the file is missing.'''
    if not zip_path:
        logger.warning('mimic_cxr_reports_zip is not set in config.json; report_text is left null')
        return None
    if not Path(zip_path).is_file():
        logger.warning(f'no reports zip at {zip_path} (mimic_cxr_reports_zip); report_text is left null')
        return None
    rows = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if name.endswith('.txt'):
                rows.append((name, zf.read(name).decode('utf-8', errors='replace')))
    reports = (
        pl.DataFrame(rows, schema={'report_path': pl.String, 'report_text': pl.String}, orient='row')
        # .../pXX/pXXXXXXXX/sXXXXXXXX.txt -> the study number
        .select(pl.col('report_path').str.extract(r'/s(\d+)\.txt$', 1).alias(REPORT_KEY), 'report_text')
        .filter(pl.col(REPORT_KEY).is_not_null())
    )
    logger.info(f'loaded {reports.height} reports from {zip_path}')
    return reports


@app.function
def add_reports(df: pl.DataFrame, reports=None) -> pl.DataFrame:
    '''Add `report_text` (many DICOMs -> one report, joined on the study number; null when
    `reports` is None), drop the join key and return the final CLIF columns.'''
    if reports is None:
        out = df.with_columns(pl.lit(None).cast(pl.String).alias('report_text'))
    else:
        out = df.join(reports.select(REPORT_KEY, 'report_text'), on=REPORT_KEY, how='left', validate='m:1')
        logger.info(f'{out["report_text"].null_count()} of {out.height} DICOMs have no report')
    return out.select(CLIF_COLUMNS)


@app.cell
def _(in_clif):
    chest_xray = add_reports(in_clif, load_reports())
    print(f'chest_xray: {chest_xray.height} rows x {chest_xray.width} columns, '
          f'{chest_xray["report_text"].null_count()} without a report')
    chest_xray
    return (chest_xray,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 4. Save

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
    built = build_chest_xray(load_meta(mapping), mapping)
    chest_xray = add_reports(filter_to_clif_patients(built), load_reports())
    save_chest_xray(chest_xray)
    logger.info('output saved to a parquet file, everything completed for the chest_xray table!')


@app.cell
def _():
    save_button = mo.ui.run_button(label='Save clif_chest_xray.parquet')
    save_button
    return (save_button,)


@app.cell
def _(chest_xray, save_button):
    if save_button.value:
        save_chest_xray(chest_xray)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## QC: column coverage of the final `chest_xray` table

    One row per column of the table built above.

    - `n_with_value`: rows with a value (not null, and not blank for text columns).
    - `n_missing`: rows without one.
    - `n_unique`: distinct values, nulls excluded.
    """)
    return


@app.function
def qc_column_coverage(chest_xray: pl.DataFrame) -> pl.DataFrame:
    '''Per column of the final table: how many rows have a value. Counts only.'''
    def n_with_value(col):
        values = chest_xray[col]
        if values.dtype == pl.String:                 # a blank string is not a value
            return int((values.str.strip_chars().str.len_chars() > 0).sum())
        return chest_xray.height - values.null_count()

    n = max(chest_xray.height, 1)
    return pl.DataFrame({
        'column': chest_xray.columns,
        'dtype': [str(dt) for dt in chest_xray.dtypes],
        'n_rows': [chest_xray.height] * chest_xray.width,
        'n_with_value': [n_with_value(c) for c in chest_xray.columns],
        'n_unique': [chest_xray[c].drop_nulls().n_unique() for c in chest_xray.columns],
    }, schema_overrides={'n_rows': pl.Int64, 'n_with_value': pl.Int64, 'n_unique': pl.Int64}).with_columns(
        (pl.col('n_rows') - pl.col('n_with_value')).alias('n_missing'),
        (100 * pl.col('n_with_value') / n).round(1).alias('pct_with_value'),
    )


@app.cell
def _(chest_xray):
    chest_xray
    return


@app.cell
def _(chest_xray):
    column_coverage = qc_column_coverage(chest_xray)
    column_coverage
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
