# Changelog

| MIMIC version | CLIF version | Latest CLIF-MIMIC release | Status |
|-----------------|-----------------|-------------------|---------------------|
| IV-3.1 | [2.1.0](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0) | [v1.0.0](#latest-v100---2025-10-27) | 🧩 partial (✅ stable on the already-released tables) |
| IV-3.1 | [2.0.0](https://clif-icu.com/data-dictionary/data-dictionary-2.0.0) | [v0.1.0](#v010---2025-05-01) | ✅ stable |

## LATEST! \[v1.0.0\] - 2025-10-27

### Readme

This is a major update that introduces new tables under the CLIF 2.1.0 version, as well as new fields and improved data quality for existing tables. We recommend rebuilding all CLIF tables to ensure you are using the latest version.

### New

-   add new CLIF 2.1 tables: [`medication_admin_intermittent`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#medication-admin-intermittent), [`code_status`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#code-status), [`hospital_diagnosis`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#hospital-diagnosis), [`patient_procedures`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#patient-procedures)

-   add new fields `med_route_name`, `med_route_category`, `mar_action_name`, `mar_action_category` to the two medication administration tables.

-   include [out-of-hospital death date](https://mimic.mit.edu/docs/iv/modules/hosp/patients/#dod) as `death_dttm` in the `patient` table. (Previously only the more precise [in-hospital death date-time](https://mimic.mit.edu/docs/iv/modules/hosp/admissions/#admittime-dischtime-deathtime) was used.) [#58](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF/issues/58)

-   populate `hospital_id` as 'mimic' and `hospital_type` as 'academic' in the `adt` table.

-   add `lab_order_name` and `lab_order_category` to the `labs` table

### Changed

-   the mappings of a few ventilator modes are updated to more precise `mode_category`, reducing the number of rows assigned to 'Other'. See the [mapping spreadsheet](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/edit?gid=1126032073#gid=1126032073) for the latest mappings and the `status` and `note` columns for the changes.
-   To avoid having an entirely null field, `lab_order_dttm` is now populated with the same [`charttime`](https://mimic.mit.edu/docs/iv/modules/hosp/labevents/#charttime) field in MIMIC's `labevents` table that was previously mapped to `lab_collect_dttm` only.

### Fixed

-   whitespaces in MIMIC source strings are handled more gracefully to recognize 'Trach mask ' as correctly implying that a tracheostomy had been performed. This fix moves up the time of tracheostomy for patients on 'Trach mask ' which translates to more – and earlier incidence of – rows of `tracheostomy` = 1 in the `respiratory_support` table.
-   the medication tables are overhauled with improved data quality, mostly driven by the development of the dedicated [`medication_admin_intermittent`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#medication-admin-intermittent) table which helps identify and reassign intermittent medication administrations that were previously mislabeled as continuous.



### Future

-   update the [`ecmo_mcs`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#ecmo-mcs) table to CLIF 2.1 standard

-   add the new [`microbiology_culture`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#microbiology-culture) table

-   add the new [`microbiology_nonculture`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#microbiology-nonculture) table

-   add the new [`microbiology_susceptibility`](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#microbiology-susceptibility) table

## \[v0.2.0\] - 2025-05-13

### Readme

This is a beta release for the latest CLIF 2.1.0 version, which introduces several new tables and data elements. It is in the beta stage because CLIF's 2.1.0 version currently remains WIP (see CLIF's [maturity levels](https://clif-consortium.github.io/website/maturity.html)). As a result, this beta release will live on a separate `release/0.2.0` branch until CLIF 2.1.0 is stablized.

To access this version, follow the steps in the [README](README.md#run-the-pipeline) to check out the `release/0.2.0` branch. The newly updated tables that need to be regenerated (or generated for the first time) are: `patient`, `hospitalization`, `adt`, `ecmo_mcs`, `crrt_therapy`.

### New

-   add the new [`crrt_therapy`](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.1.0.html#crrt-therapy) table
-   add the new [`ecmo_mcs`](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.1.0.html#ecmo_mcs) table
-   add the new `location_type` field in the `adt` table ([#1](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/1))
-   populate the `language_category` field with newly developed mapping in the in the `patient` table
-   populate the `admission_type_category` field with newly developed mapping in the `hospitalization` table

### Fixed

-   add mCIDE files locally in the repo to replace directly reading from raw.githubusercontent which may incur a device-specific SSL certificate error for some users ([#15](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/15))

## \[v0.1.0\] - 2025-05-01

### Readme

This is the last CLIF-MIMIC release for [CLIF's 2.0.0 version](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.0.0.html). It is a major update that introduces new data elements as well as fixes that yield much cleaner data with fewer nulls.

That said, the CLIF-MIMIC release for [CLIF 2.1.0](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.1.0.html) is scheduled next week, so **unless you are interested in obtaining the new SBT data elements right now, you are welcomed to wait for the next release, where more data elements will be added** (and are currently withheld to align with 2.0.0's schema).

If you are using this release, please update all CLIF tables as they are all impacted by the time-zone update. You should also re-install or update the virtual environment following the [README](README.md#run-the-pipeline) since new packages were added to validate data quality.

### New

-   add Spontaneous Breathing Trial (SBT) data elements to the CLIF `patient_assessments` table (specifically `sbt_delivery_pass_fail` and `sbt_fail_reason`)
-   add outlier labs values parsed from MIMIC comments
-   add [`hamilton`](https://hamilton.dagworks.io/en/latest/) and [`pandera`](https://www.union.ai/pandera) as light-weight back-end frameworks for pipeline orchestration and data quality validation respectively

### Changed

-   convert all date-time fields (ending in `dttm`) from UTC−05:00 (U.S. Eastern Time Zone) to UTC, following a [CLIF-wide design](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF/issues/40) to standardize timezone representation

### Fixed

-   remove unintended nulls from the `labs` table ([#2](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/2)), `patient` table ([#3](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/3)), `respiratory_support` table ([#5](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/5)), `hospitalization` table ([#4](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/4))

## \[v0.0.1\] - 2025-01-31

### Readme

CLIF tables updated: `respiratory_support`, `patient`

### Changed

-   update the `device_category` mapping of "T-piece" from "IMV" to "Others" pending further review

### Fixed

-   correct typo in the config files that mistakenly suggested the latest CLIF version is 2.1 (it should be 2.0)
-   remove two duplicate rows in the CLIF `patient` output table

## \[v0.0.0\] - 2025-01-21

First release!