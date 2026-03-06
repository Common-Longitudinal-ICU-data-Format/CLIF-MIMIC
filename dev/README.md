# dev/ — Development Notebooks

This directory contains interactive development notebooks for prototyping CLIF table transformations before they are formalized into Hamilton DAG modules in `src/tables/`.

## Notebook Types

- **Marimo notebooks (`.py`)** — current standard. Can be edited in the marimo IDE (`make mo`) or any text editor.

- **Jupyter notebooks (`.ipynb`)** — legacy format used for earlier tables. New development should use marimo.

## Dev Notebook Pattern

Each dev notebook follows a consistent cell structure organized by level-1 markdown headers. See `io_dev.py` for the most thoroughly documented example.

**Return convention**: only return variables that downstream cells need. Use `return (var1, var2)` tuple syntax.

### Setup (no header, cells 1-5)

| Cell | Purpose | Key code |
|------|---------|----------|
| 1 | CWD check | `os.getcwd()` |
| 2 | Module import trigger | `from src import utils` (loads config) |
| 3 | Imports | `marimo as mo`, `pandas`, `duckdb`, utilities from `src.utils` |
| 4 | Placeholder | Commented-out `resave_mimic_table_from_csv_to_parquet()` |
| 5 | Demo data | Load `tests/clif_patient.parquet` and `tests/clif_hospitalization.parquet` |

### `# Query`

For exploring MIMIC items and deciding how to map them. This section should NOT contain anything needed by the production ETL pipeline.

- Use `search_mimic_items(keyword)` to discover relevant MIMIC item IDs from the `d_items` table.

- Use `fetch_mimic_events(item_ids)` to pull raw events for inspection.

- Use `mo.sql()` with DuckDB to inspect raw MIMIC data, check value distributions, and draw histograms.

### `# ETL`

Production-pipeline-ready transforms. This section maps 1:1 to Hamilton DAG nodes in `src/tables/`. Only code that belongs in the production pipeline should go here.

- Load the mapping CSV: `load_mapping_csv("table_name")` → reads from `data/mappings/mimic-to-clif-mappings - {table_name}.csv`.

- Filter to items with `decision == "TO MAP"` for the main transform.

- Use `mo.sql()` with DuckDB for the main transform query (see DuckDB conventions below).

- Apply `convert_tz_to_utc()` for datetime columns — MIMIC stores timestamps in US/Eastern, CLIF requires UTC.

- Cast columns to match CLIF schema types (`hadm_id` → `hospitalization_id` as VARCHAR, etc.).

### `# QA`

Validation checks and exploratory analysis on the ETL outputs.

- **Null checks**: count nulls in each column

- **Category validation**: verify `*_category` values are in the permissible mCIDE list (from `data/mcide/`)

- **Range checks**: volume/value percentiles, negative value counts

- **Duplicate checks**: where applicable

- **Exploratory EDA**: deeper analysis (e.g., net urine output co-occurrence, time-gap distributions)

### `# Demo`

Demo data creation and save.

- Subset to demo cohort: `INNER JOIN hosp_demo USING (hospitalization_id)`

- Save calls **commented out** by default — uncomment to persist: `save_to_rclif(df, 'table_name')`

## DuckDB SQL Conventions

All SQL in marimo cells uses DuckDB syntax via `mo.sql()`:

```sql
FROM table_name
JOIN other_table USING (id)
SELECT
    -- Comments go inside the query string
    a
    , b: a + 1
    , c: a + b
WHERE condition
ORDER BY a, b, c
```

Key rules:

- **Clause order**: `FROM` → `JOIN` → `SELECT` → `WHERE` → `ORDER BY`

- **Prefix aliases** with `:` (e.g., `net_uo: vol_out - vol_in`)

- **Comments** inside the quoted SQL string, not before it

- **F-string interpolation** for dynamic paths: `f"FROM '{mimic_table_pathfinder('outputevents')}' oe"`

## Key Utilities (`src/utils.py`)

| Function | Purpose |
|----------|---------|
| `mimic_table_pathfinder(table)` | Path to MIMIC parquet/CSV file |
| `load_mapping_csv(name)` | Load mapping from `data/mappings/` |
| `convert_tz_to_utc(series)` | Convert US/Eastern → UTC |
| `search_mimic_items(kw)` | Search `d_items` by keyword with stats |
| `save_to_rclif(df, name)` | Save DataFrame to CLIF output dir |
| `read_from_rclif(name)` | Read existing CLIF parquet |
| `fetch_mimic_events(item_ids)` | Fetch events by item ID list |

## Development Lifecycle

1. **Prototype** in `dev/{table}_dev.py` — explore data, define mappings, build transforms, run QA

2. **Refactor** into Hamilton DAG at `src/tables/{table}.py` — functions become DAG nodes

3. **Add Pandera schema** validation with `@tag(property="test")` decorator

4. **Write tests** in `tests/test_{table}.py` with fixture data

5. **Add table to config** (`config/config_template.json`) and update `CHANGELOG.md`

## Running Notebooks

```sh
# Launch marimo editor with watch mode
make mo

# Or directly for a specific notebook
uv run marimo edit dev/{notebook}.py --watch
```
