# CLIF-MIMIC: rules for Claude

## Data access (hard rules)

This repo works with credentialed MIMIC patient data. Claude must not read it.

- **Never read real data.** Do not open, load, query, sample, summarise or print any real data file. This includes:
  - parquet files anywhere (MIMIC source tables, extracted MIMIC-CXR metadata, CLIF output tables in `output/`, and copies on Box, `/Volumes` or Downloads);
  - MIMIC csv / csv.gz tables and DICOM files;
  - the MIMIC-CXR report text (`mimic-cxr-reports.zip`, its `.txt` files, and any `report_text` column).
- **No exceptions for "safe" reads.** Counts only, schema only, a single row, `head()`, value counts, "in memory, nothing saved", and comparisons against an existing output are all reads. Do not do them.
- **Do not run code that loads real data.** That means no `main.py`, no table `main()` functions, no notebook cells, and no ad-hoc scripts pointed at the paths in `config/config.json`.
- **Never write data-derived values to disk.** No row counts, patient counts, IDs or column values from real data in notes, memory, todo files, commit messages or code comments.

## How to verify code instead

- Build small synthetic inputs (fake parquet, fake DICOMs, fake reports zip with made-up text) in the scratchpad and run the functions on those, passing the paths as arguments.
- Use static checks: `uv run marimo check <file>`, `uv run python -c "import main"`, `--help` on scripts, `uv lock --check`.
- When a result on real data is needed, give the user the exact command or code to run and ask them to report back. Say plainly that it was not verified on real data.

## If a task seems to need real data

Stop and ask first. A question about the data is not permission to inspect it.

## What is fine to read

- Code, config files (paths and switches), mapping CSVs in `data/mappings/`, mCIDE CSVs in `data/mcide/`, docs, and test fixtures in `tests/fixtures/`.
- File and folder names (for example, checking that a path exists), but not file contents.
