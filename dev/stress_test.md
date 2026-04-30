# Stress-Test Dataset Utilities

Two paired marimo notebooks for fanning out a CLIF dataset to a larger size and verifying the result. Both are dual-mode: open in the marimo IDE for interactive exploration, or invoke as plain `python` scripts for batch runs.

## Overview

"Stress testing" here means producing an enlarged copy of an existing CLIF parquet dataset by duplicating each row `n_copies` times and appending a single-letter suffix to ID columns so the duplicated rows stay uniquely keyed. The schema, value distributions, and per-encounter structure are preserved — only row counts scale. Use this to load-test downstream pipelines (clifpy, SOFA computation, wide-dataset construction, etc.) without needing additional source data.

The end-to-end workflow is:

1. Run the ETL to produce `output/clif-mimic-1.2.0/`.

2. Run `generate_stress_test_data.py` to fan it out into `output/clif-mimic-stress-test/`.

3. Run `profile_cohort_size.py output/clif-mimic-stress-test` to confirm row and unique-ID counts scaled as expected (target ≈ original × `n_copies`).

## `generate_stress_test_data.py`

Duplicates every row in each `clif_*.parquet` `n_copies` times via a single DuckDB `CROSS JOIN` per table, appending suffixes `A`, `B`, `C`, ... to the relevant ID columns so rows remain uniquely keyed across copies.

ID columns suffixed per table family:

- **`hospitalization_id` only** — `adt`, `crrt_therapy`, `ecmo_mcs`, `hospital_diagnosis`, `input`, `labs`, `medication_admin_continuous`, `medication_admin_intermittent`, `output`, `patient_assessments`, `patient_assessments_raw_gcs`, `patient_assessments_raw_gcs_only`, `patient_procedures`, `position`, `respiratory_support`, `respiratory_support_bfill_processed`, `vitals`

- **`patient_id` only** — `code_status`, `patient`

- **Both `patient_id` and `hospitalization_id`** — `hospitalization`

- **`patient_id`, `hospitalization_id`, and `organism_id`** — `microbiology_culture`

### Run as a marimo notebook

```sh
uv run marimo edit dev/generate_stress_test_data.py --watch
```

### Run as a CLI script

```sh
uv run python dev/generate_stress_test_data.py \
    --n-copies 4 \
    --input-dir output/clif-mimic-1.2.0 \
    --output-dir output/clif-mimic-stress-test
```

Defaults:

- `--n-copies 4`

- `--input-dir output/clif-mimic-1.2.0`

- `--output-dir output/clif-mimic-stress-test`

`--n-copies` is capped at 26 (one suffix per letter of the alphabet).

## `profile_cohort_size.py`

Reports per-table row counts plus unique `hospitalization_id` and `patient_id` counts for every `clif_*.parquet` in a directory. Use it after the ETL or after stress-test generation to verify the dataset's scale.

### Run as a marimo notebook

```sh
uv run marimo edit dev/profile_cohort_size.py --watch
```

### Run as a CLI script

```sh
uv run python dev/profile_cohort_size.py [DATA_DIR]
```

`DATA_DIR` defaults to `output/clif-mimic-1.2.0` if omitted.
