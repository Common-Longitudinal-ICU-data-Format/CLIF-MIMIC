# MIMIC-IV to CLIF ETL Pipeline

This repository provides the ETL pipeline to transform the MIMIC-IV database into the Common Longitudinal ICU data Format (CLIF). The latest release is v1.0.0, released in October 2025, and transforms MIMIC-IV 3.1 into CLIF 2.1.0. 

## Getting started

> *Note: This project is being submitted to PhysioNet and all the tables will be available for direct download for MIMIC-credentialed users when the submission is approved. For any future releases, we will upload the download-ready CLIF tables to the PhysioNet [project page](https://physionet.org/content/mimic-iv-ext-clif), but in the event of any lag, please refer to this repository for the code to generate the most up-to-date version.*

To access this dataset, make sure you have acquired [access to the MIMIC-IV dataset](https://mimic.mit.edu/docs/gettingstarted/) on PhysioNet.

To run the pipeline once you are credentialed on PhysioNet, first review the [change log](CHANGELOG.md) to find the latest or preferred version; then follow the instructions in the [Usage](#usage) section below to generate the dataset.

For mapping decisions, see [the MIMIC-to-CLIF google spreadsheet](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/edit?usp=sharing.) for details.

For issues encountered and decisions made during the mapping process, see the [ISSUESLOG](ISSUESLOG.md).


## Usage
If you are an existing user, please `git pull` the relevant branch and refer to the [change log](CHANGELOG.md) for the updated CLIF tables that need to be re-generated.

If you are a new user, fork your own copy of this repository, and `git clone` to your local directory. 

### Add configuration

#### Required

Find `/config/config_template.json`, rename it to `config.json`, and customize a few things in this file:

1. On the backend, the pipeline requires a copy of the MIMIC data in the parquet format for much faster processing. 
    - If you have already created a parquet copy of MIMIC before, you can set `"create_mimic_parquet_from_csv": 0` and provide the *absolute* path at which you store your MIMIC parquet files, at `"mimic_parquet_dir"`.
    - otherwise, if you do not have a copy of MIMIC in parquet yet, set `"create_mimic_parquet_from_csv": 1` and change the `"mimic_csv_dir"` under `"default"` to the *absolute* path at which you store the compressed csv files (.csv.gz) you downloaded from PhysioNet. By default, if you leave `"mimic_parquet_dir"` as a blank `"`, the program would create a `/parquet` subdirectory under your `"mimic_csv_dir"`. Optionally, you can also elect to store it anywhere else and the program would create a directory at the alternative path you provided. 

2. Specify the CLIF tables you want in the next run, by setting the value of tables you want to be 1 (otherwise 0) under `"clif_tables"`. 
    - for example, specify the json object in the following way to recreate two tables (`vitals` and `labs`) that were recently updated:

```python
{   ...
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

The following example shows I have specified two sets of paths corresponding to two workspaces I work at: "local" and "hpc", and I'm currently operating in my "local" workspace. 
- Since I had already created MIMIC parquet files in my HPC environment, I left `"mimic_csv_dir"` as blank `""` and only provided the location of my parquet files, at `"mimic_parquet_dir"`.
- Meanwhile, I didn't copy these parquet files from my HPC environment to my local device, so I elected to convert them again from csv by specifying their local location at `"mimic_csv_dir"`, while leaving `"mimic_parquet_dir"` blank to indicate I'm happy to use the default setting which is to create a subdirectory `/parquet` under the csv directory, i.e. at `"/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1/parquet"`.

```python
{   "current_workspace": "local",
    "hpc": {
        "mimic_csv_dir": "",
        "mimic_parquet_dir":"/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1/parquet"
    },
    "local": {
        "mimic_csv_dir": "/some/absolute/path/to/your/project/root/CLIF-MIMIC/data/mimic-data/mimic-iv-3.1",
        "mimic_parquet_dir": ""
    }
```

4. You can also store multiple versions of the CLIF table outputs by customizing `clif_output_dir_name`. If you leave it blank with `""`, the program would default to naming it `f"rclif-{CLIF_VERSION}"`. Using this default is recommended if you want to access and store multiple CLIF versions at the same time. 

### Confirm version
After navigating to the project directory, first make sure you are on the correct branch -- which should be `main` if you are using a stable version; or a branch by the corresponding version name, e.g. branch `release/0.2.0`, if you are using a beta version.

To switch to branch `release/0.2.0`, for example, run:
```sh
# collect information on all the remote branches available from the Github repo
git fetch 
# switch to branch release/0.2.0
git switch release/0.2.0
```
- to double check, you can run `git branch` to check that the intended branch is correctly identified in the highlighted output.


### Run the pipeline

#### Option 1: use `uv` (recommended)

[uv](https://docs.astral.sh/uv/) is a fast Python package manager that simplifies dependency management.

1. Install uv (if not already installed):

   ```bash
   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Windows (PowerShell)
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

2. Once you are on the correct branch, run:
    ```bash
    uv run python main.py
    ```


#### Option 2: use conventional Python

On the correct branch, run the following *line by line*:
```sh
# create a virtual environment
python3 -m venv .venv/

# activate the virtual environment
source .venv/bin/activate

# install the dependencies
pip install -r requirements.txt

# run the pipeline
python3 main.py
```

## Contributing
To contribute to this open-source project, feel free to :
1. Open an issue for any bug or new data request.
2. Follow some branch naming conventions (e.g. `new-table/dialysis`).
3. Submit a pull request (PR) for review.

## License
This project is licensed under the MIT License. 

Note: The MIMIC-IV dataset is subject to the PhysioNet data use agreement, and users must obtain access through PhysioNet before processing.