# MIMIC-IV to CLIF ETL Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10.5+](https://img.shields.io/badge/python-3.10.5+-blue.svg)](https://www.python.org/downloads/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![CLIF 2.1.0](https://img.shields.io/badge/CLIF-2.1.0-green.svg)](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0)
[![MIMIC-IV 3.1](https://img.shields.io/badge/MIMIC--IV-3.1-orange.svg)](https://physionet.org/content/mimiciv/)

This repository provides an ETL pipeline to transform the [MIMIC-IV](https://mimic.mit.edu/) database into the [Common Longitudinal ICU data Format (CLIF)](https://clif-consortium.github.io/website/). The latest release is v1.0.0 (October 2025) and transforms MIMIC-IV 3.1 into CLIF 2.1.0.

## Table of Contents

- [Getting started](#getting-started)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
  - [Add configuration](#add-configuration)
  - [Confirm version](#confirm-version)
  - [Run the pipeline](#run-the-pipeline)
- [Output](#output)
- [Resources](#resources)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

> [!NOTE]
> This project is being submitted to PhysioNet. All tables will be available for direct download for MIMIC-credentialed users when the submission is approved. For any future releases, we will upload the download-ready CLIF tables to the PhysioNet [project page](https://physionet.org/content/mimic-iv-ext-clif), but in the event of any lag, please refer to this repository for the code to generate the most up-to-date version

To run the pipeline, first review the [change log](CHANGELOG.md) to find the latest or preferred version; then follow the instructions in the [Usage](#usage) section below to generate the dataset.

For mapping decisions, see [the MIMIC-to-CLIF mapping spreadsheet](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/edit?usp=sharing) for details.

For issues encountered and decisions made during the mapping process, see the [ISSUESLOG](ISSUESLOG.md).

## Prerequisites

Before running the pipeline, ensure you have:

- PhysioNet credentials: [Access to the MIMIC-IV dataset](https://mimic.mit.edu/docs/gettingstarted/) on PhysioNet

- MIMIC-IV 3.1 CSV files downloaded from PhysioNet on your machine

- Python 3.10.5+ and Git installed on your machine

- Disk space:
  - ~30 GB for MIMIC-IV 3.1 CSV files (compressed)
  - ~15 GB for MIMIC-IV Parquet conversion
  - ~1 GB for CLIF output tables

## Usage
If you are an existing user, please `git pull` the relevant branch and refer to the [change log](CHANGELOG.md) for the updated CLIF tables that need to be re-generated.

If you are a new user, fork your own copy of this repository, and `git clone` to your local directory. 

### Add configuration

#### Required

Copy `config/config_template.json` to `config/config.json` and customize the following settings:

1. On the backend, the pipeline requires a copy of the MIMIC data in the parquet format for much faster processing. 
    - If you have already created a parquet copy of MIMIC before, you can set `"create_mimic_parquet_from_csv": 0` and provide the *absolute* path at which you store your MIMIC parquet files, at `"mimic_parquet_dir"`.
    - otherwise, if you do not have a copy of MIMIC in parquet yet, set `"create_mimic_parquet_from_csv": 1` and change the `"mimic_csv_dir"` under `"default"` to the *absolute* path at which you store the compressed csv files (.csv.gz) you downloaded from PhysioNet. By default, if you leave `"mimic_parquet_dir"` as a blank `"`, the program would create a `/parquet` subdirectory under your `"mimic_csv_dir"`. Optionally, you can also elect to store it anywhere else and the program would create a directory at the alternative path you provided. 

2. Specify the CLIF tables you want in the next run, by setting the value of tables you want to be 1 (otherwise 0) under `"clif_tables"`.
    - For example, to recreate two tables (`vitals` and `labs`) that were recently updated:

    ```json
    {
        "clif_tables": {
            "patient": 0,
            "hospitalization": 0,
            "adt": 0,
            "vitals": 1,
            "labs": 1,
            "patient_assessments": 0,
            "respiratory_support": 0,
            "medication_admin_continuous": 0,
            "position": 0
        }
    }
    ```

#### Optional
3. To enable working across multiple devices or workspaces, you can add more "workspace" along with their respective csv and parquet directory paths. For more details, you can refer to the example below or `/config/config_example.json` for how I personally specify file paths under three different workspace set-up: "local," "hpc," and "local_test." This would allow you to seamlessly switch between different devices or environments without having to update file paths every time you do so. Whenever you switch, you just need to update the name of the `"current_workspace"` accordingly, e.g. specify that `"current_workspace": "hpc"` as long as you have specified a set of directory paths under a key of the same name, i.e. `"hpc": {...}`. 

The following example shows two workspaces: "local" and "hpc", with current workspace set to "local":

- Since I had already created MIMIC parquet files in my HPC environment, I left `"mimic_csv_dir"` as blank `""` and only provided the location of my parquet files at `"mimic_parquet_dir"`.

- For my local device, I elected to convert from CSV by specifying their location at `"mimic_csv_dir"`, while leaving `"mimic_parquet_dir"` blank to use the default setting (creates `/parquet` subdirectory under the CSV directory).

```json
{
    "current_workspace": "local",
    "hpc": {
        "mimic_csv_dir": "",
        "mimic_parquet_dir": "/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1/parquet"
    },
    "local": {
        "mimic_csv_dir": "/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1",
        "mimic_parquet_dir": ""
    }
}
```

4. You can also store multiple versions of the CLIF table outputs by customizing `clif_output_dir_name`. If you leave it blank with `""`, the program would default to naming it `f"rclif-{CLIF_VERSION}"`. Using this default is recommended if you want to access and store multiple CLIF versions at the same time. 

### Confirm version

After navigating to the project directory, ensure you are on the correct branch:

- `main` - for the latest stable version
- `release/<x.x.x>` - for a beta version

To switch to a specific branch (e.g., `release/0.2.0`):

```bash
# Fetch information on all remote branches
git fetch

# Switch to branch release/0.2.0
git switch release/0.2.0

# Verify current branch
git branch
```


### Run the pipeline

#### Option 1: using `uv` (recommended)

[uv](https://docs.astral.sh/uv/) is a fast Python package manager that simplifies dependency management.

1. Install uv (if not already installed):

   ```bash
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Windows (PowerShell)
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

2. Run the pipeline (once on the correct branch):

   ```bash
   uv run python main.py
   ```

#### Option 2: using conventional Python

On the correct branch, run the following line by line:

```bash
# Create a virtual environment
python3 -m venv .venv/

# Activate the virtual environment
source .venv/bin/activate

# Install the dependencies
pip install -r requirements.txt

# Run the pipeline
python3 main.py
```

## Output

After running the pipeline, you'll find the following outputs:

### CLIF Tables

Generated Parquet files will be in `output/rclif-2.1.0/` (or your custom output directory name).

| Table | Typical Size |
|-------|------|
| `vitals` | ~265 MB |
| `labs` | ~344 MB |
| `patient_assessments` | ~137 MB |
| `medication_admin_continuous` | ~84 MB |
| `medication_admin_intermittent` | ~48 MB |
| `adt` | ~33 MB |
| `respiratory_support` | ~29 MB |
| `position` | ~24 MB |
| `hospital_diagnosis` | ~20 MB |
| `hospitalization` | ~16 MB |
| `patient_procedures` | ~8.2 MB |
| `crrt_therapy` | ~4.3 MB |
| `code_status` | ~1.1 MB |
| `patient` | ~3 MB |

### Logs

- `output/logs/clif_mimic_all.log` - All INFO+ messages
- `output/logs/clif_mimic_errors.log` - Warnings and errors only

## Resources

- 📚 [CLIF Website & Data Dictionary](https://clif-icu.com/data-dictionary)

- 🗺️ [MIMIC-to-CLIF Mapping Spreadsheet](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/)

- 📝 [Change Log](CHANGELOG.md)

- 📋 [Issues Log](ISSUESLOG.md)


## Contributing

To contribute to this open-source project, feel free to:

1. Open an issue for bugs or feature requests

2. Follow branch naming conventions (e.g., `new-table/dialysis`, `fix/vitals-mapping`)

3. Submit a pull request for review

## License

This project is licensed under the [MIT License](LICENSE).

> [!IMPORTANT]
> The MIMIC-IV dataset is subject to the [PhysioNet Data Use Agreement](https://physionet.org/content/mimiciv/). Users must obtain access through PhysioNet before processing.