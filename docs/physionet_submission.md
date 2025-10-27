# Title
> *Your title should be no longer than 200 characters. Avoid acronyms and abbreviations where possible. Also avoid leading with "The". Only letters, numbers, spaces, underscores, and hyphens are allowed.*
> - *If your dataset is derived from MIMIC and you would like to use the MIMIC acronym, please include the letters "Ext" (for example, MIMIC-IV-Ext-YOUR-DATASET"). Ext may either indicate "extracted" (e.g. a derived subset) or "extended" (e.g. annotations), depending on your use case.*
> - *If the dataset is derived from another dataset, the title must make this clear.*

MIMIC-IV-Ext-CLIF

# Abstract
> *A brief description of the resource and the context in which it was created.*
>
> *Your abstract must be no longer than 250 words. The focus should be on the resource being shared. If the resource was generated as part of a scientific investigation, relevant information may be provided to facilitate reuse. References should not be included. The abstract should also include a high-level description of the data as well as an overview of the key aims of the project. The abstract may appear in search indexes independently of the full project metadata, so providing detailed information about the content is important.*

MIMIC-IV-Ext-CLIF is a publicly accessible dataset derived from MIMIC-IV v3.1, transformed into the Common Longitudinal ICU data Format (CLIF). CLIF is an open-source critical care database schema developed by a consortium of 10+ US healthcare institutions to standardize intensive care unit (ICU) data for multi-center research. While CLIF has demonstrated value in federated research settings, access has been limited to consortium members with institutional electronic health record (EHR) systems. This dataset addresses that gap by providing CLIF-formatted data derived from the publicly available MIMIC-IV database, enabling researchers worldwide to utilize the CLIF format without requiring institutional EHR access.

The dataset contains [TODO: N distinct patients] and [TODO: N hospitalizations] from Beth Israel Deaconess Medical Center spanning [TODO: date range]. We transform MIMIC-IV into 15 CLIF tables including core demographics (patient, hospitalization), clinical monitoring (vitals, labs), respiratory support, medication administration, and specialized ICU interventions (ECMO, continuous renal replacement therapy). The transformation employs a transparent, reproducible ETL pipeline with open-source mapping decisions documented in an accessible spreadsheet. All transformations adhere to CLIF 2.1.0 specifications, including standardized minimum Common ICU Data Elements (mCIDE) with controlled vocabularies and UTC datetime formatting.

This resource enables algorithm validation, CLIF implementation benchmarking, critical care phenotyping research, and multi-center study prototyping. By providing CLIF-formatted data from MIMIC-IV, we accelerate adoption of standardized critical care data formats and facilitate reproducible research in critical illness.



# Background
> *Your background should provide the reader with an introduction to the resource. The section should offer context in which the resource was created and outline your motivations for sharing.*

## Why CLIF is useful

Critical illness, or acute organ failure requiring life support, threatens over five million American lives annually. Electronic health record (EHR) data are a source of granular information that could generate crucial insights into the nature and optimal treatment of critical illness. However, data management, security, and standardization are barriers to large-scale critical illness EHR studies.

A consortium of critical care physicians and data scientists from nine US healthcare systems developed the Common Longitudinal Intensive Care Unit (ICU) data Format (CLIF), an open-source database format that harmonizes a minimum set of ICU Data Elements (mCIDE) for use in critical illness research. The consortium demonstrated CLIF's value through federated analyses involving over 100,000 critically ill patients across diverse health systems, showcasing its potential for mortality prediction validation, clinical subphenotyping, and reproducible multi-center research.

However, CLIF implementation requires substantial data science and clinical expertise, and access has been limited to consortium institutions with EHR infrastructure. This creates a barrier for researchers who could benefit from CLIF's standardized format but lack institutional resources.

## Why MIMIC-IV-Ext-CLIF is useful

MIMIC-IV-Ext-CLIF fulfills the vision articulated in the CLIF consortium's foundational work: providing open access to CLIF-formatted data to broaden adoption beyond consortium institutions. This dataset serves multiple critical research needs:

**Reproducibility and Validation**: Researchers can replicate CLIF-based algorithms and validate findings from consortium studies using a publicly accessible benchmark dataset. This addresses a key challenge in critical care research where validation studies often cannot access the original data sources.

**CLIF Implementation Development**: Institutions developing their own CLIF pipelines can use this dataset as a reference implementation, comparing their ETL outputs against a verified CLIF transformation. The open-source mapping decisions (documented in an accessible spreadsheet) provide transparency into clinical judgment required for MIMIC-to-CLIF transformations.

**Algorithm Development Without EHR Access**: Researchers without access to institutional EHR data can develop and test critical care algorithms using standardized CLIF format, enabling participation in critical illness research beyond well-resourced academic medical centers.

**Multi-Center Study Prototyping**: Teams planning CLIF-based multi-center studies can prototype analyses, test hypotheses, and refine methods using MIMIC-IV-Ext-CLIF before deploying to federated institutional databases.

**Educational Resource**: The combination of CLIF-formatted data, documented mapping decisions, and open-source ETL code serves as an educational resource for training data scientists and clinician-researchers in standardized critical care data formats.

By bridging the gap between proprietary institutional EHR data and open research, MIMIC-IV-Ext-CLIF democratizes access to standardized critical care data and accelerates the development of reproducible, generalizable critical illness research.


# Methods
> *The "Methods" and "Technical Implementation" sections provide details of the procedures used to create your resource including, but not limited to, how the data was collected, any measurement devices, etc. For software, the section may cover aspects such as development process, software design, and description of algorithms. For data, the section may include details such as experimental design, data acquisition, and data processing.*

The dataset is created following a four-step process: (1) search, (2) map, (3) code, and (4) validate.

## (1) Search

Extensive search of the original MIMIC-IV database is conducted to identify candidate data elements for each CLIF table. The search employs case-insensitive keyword matching against MIMIC table and column names, supplemented by consultation of extensive online documentation from both the official MIMIC website and the GitHub community.

To systematically evaluate candidates, we developed utility functions that automatically generate comprehensive listings of MIMIC data elements with relevant statistics:

- For **numeric variables**: minimum, mean, median, maximum values, and distribution percentiles

- For **categorical variables**: distinct categories, frequency counts, and prevalence

- For **temporal variables**: earliest and latest timestamps, missing value patterns

For example, when mapping vital signs, we identified over 200 distinct `itemid` values in MIMIC's `chartevents` table corresponding to heart rate measurements (e.g., "Heart Rate", "Heart rate (bpm)", "Arterial BP [Systolic]"). These statistics enable clinical reviewers to assess data quality, identify potential duplicates, and make informed mapping decisions.

## (2) Map

Candidate MIMIC data elements, enriched with their statistical profiles, are comprehensively documented in an [open-access spreadsheet](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/edit?usp=sharing) and mapped to CLIF mCIDE categories by expert physician-scientists and data scientists. Each MIMIC `itemid` or data element is reviewed and assigned a decision label:

- **"TO MAP, AS IS"**: Direct mapping to CLIF category without transformation

- **"TO MAP, CONVERT UOM"**: Mapping requires unit of measurement conversion (e.g., mg/dL to mmol/L)

- **"NO MAPPING"**: No appropriate CLIF category exists

- **"MAPPED ELSEWHERE"**: Element is captured in a different CLIF table or field

- **"ALREADY MAPPED"**: Previously reviewed and mapped

- **"UNSURE"**: Requires additional clinical or technical review

- **"NOT AVAILABLE"**: Data element not present or insufficient in MIMIC-IV

This open-access documentation approach prioritizes transparency, allowing clinicians and researchers to evaluate mapping decisions without requiring programming expertise. The spreadsheet also tracks review status, reviewer identities, and actionable next steps.

**CLIF mCIDE Dual Naming**: For each mapping, we preserve both the MIMIC-specific terminology (in `*_name` fields) and the standardized CLIF category (in `*_category` fields). For example, MIMIC's `itemid` 220045 ("Heart Rate") maps to `vital_category = "Heart Rate"` while `vital_name` preserves the original MIMIC label. This dual representation enables both standardization and traceability.

## (3) Code

Mapping decisions documented in the spreadsheet are implemented as modular Python scripts using modern data engineering frameworks:

**Core Technologies**:

- **pandas** (v2.3.3+): DataFrame transformations and data manipulation

- **duckdb** (v1.3.0+): In-memory SQL query engine for efficient data filtering and joins

- **Hamilton** (v1.88.0+): Directed Acyclic Graph (DAG) orchestration for reproducible ETL pipelines

- **Pandera** (v0.26.1+): Schema validation against CLIF 2.1.0 specifications

- **fastparquet & pyarrow** (v2024.11.0+, v21.0.0+): Efficient Parquet file I/O

**ETL Architecture**:

Each CLIF table is built using a **Hamilton DAG pattern** where:

- Functions represent transformation steps (nodes in the DAG)

- Function parameters define dependencies (edges in the DAG)

- The Hamilton driver executes transformations in dependency order

This approach ensures modularity, testability, and reproducibility. For example, the `labs` table pipeline includes functions for loading mappings, constructing ID-to-category dictionaries, fetching MIMIC events, filtering outliers, removing duplicates, and validating schemas—each independently testable.

**Mapping-as-Code**: The pipeline uses exported CSV copies of the mapping spreadsheet as the "source of truth," avoiding error-prone hard-coding. When a mapping decision changes (e.g., updating an `itemid` from "TO MAP, AS IS" to "NO MAPPING"), re-running the pipeline automatically incorporates the change without code modification.

**Special Handling**:

- **Medication tables**: When building both continuous and intermittent medication administration tables, a unified module processes both efficiently to avoid redundant MIMIC table loads

- **Datetime standardization**: All timestamps are converted from MIMIC's UTC-5 (US Eastern Time) to UTC using timezone-aware transformations, ensuring CLIF's temporal consistency requirement

- **Null handling**: Columns where all values are null (e.g., `lab_order_dttm` not available in MIMIC) are explicitly set to `pd.NA` to distinguish from missing data

## (4) Validate

Validation occurs at multiple levels to ensure CLIF 2.1.0 compliance and data quality:

**Schema Validation with Pandera**:

Each CLIF table includes a Pandera `DataFrameSchema` specifying:

- **Column data types**: String, integer, float, datetime with timezone

- **Nullability constraints**: Which fields permit missing values

- **Value constraints**: Permissible mCIDE categories (e.g., `lab_category` must match values from CLIF's `clif_lab_categories.csv`)

- **Custom checks**: Domain-specific validations (e.g., "all-null" checks for optional fields)

Tables failing schema validation generate detailed error reports identifying non-compliant rows and failure reasons.

**Quality Checks**:

- **Duplicate removal**: Systematic deduplication based on composite keys (e.g., hospitalization_id + timestamp + category)

- **Outlier filtering**: Removal of physiologically impossible values (e.g., negative creatinine, heart rate > 300 bpm)

- **Temporal consistency**: Validation that event timestamps fall within hospitalization windows

- **Referential integrity**: Verification that all `hospitalization_id` values exist in the hospitalization table

**Hamilton Testing Framework**:

Functions tagged with `@tag(property="test")` define executable test nodes that validate intermediate transformations. These tests run within the DAG execution, enabling continuous validation during pipeline development.

The validated output consists of 15 CLIF 2.1.0 tables in Parquet format, each conforming to the published CLIF data dictionary specifications.



# Data Description
> *Content description: Your content (data, software, model) description should describe the resource in detail, outlining how files are structured, file formats, and a description of what the files contain. We also suggest including summary statistics where appropriate (e.g. total number of distinct patients, number of files, types of signals, over what time span was the data collected, etc.).*

## Dataset Overview

**Format**: Apache Parquet (.parquet) - columnar storage format optimized for analytical queries

**CLIF Version**: 2.1.0

**Source**: MIMIC-IV v3.1 (Beth Israel Deaconess Medical Center, Boston, MA)

**Temporal Coverage**: [TODO: Date range from MIMIC-IV, likely 2008-2019]

**Population**: [TODO: N distinct patients], [TODO: N hospitalizations]

**DateTime Format**: All timestamps in UTC (YYYY-MM-DD HH:MM:SS+00:00)

## File Structure

Dataset consists of 15 CLIF tables, each stored as a separate Parquet file:

```
mimic-iv-ext-clif/
├── clif_patient.parquet
├── clif_hospitalization.parquet
├── clif_adt.parquet
├── clif_vitals.parquet
├── clif_labs.parquet
├── clif_respiratory_support.parquet
├── clif_patient_assessments.parquet
├── clif_medication_admin_continuous.parquet
├── clif_medication_admin_intermittent.parquet
├── clif_position.parquet
├── clif_ecmo_mcs.parquet
├── clif_crrt_therapy.parquet
├── clif_code_status.parquet
├── clif_hospital_diagnosis.parquet
└── clif_patient_procedures.parquet
```

## Table Descriptions

### Core Identifier Tables

**`clif_patient.parquet`** (~3 MB, [TODO: N rows])

Patient-level demographics and outcomes. One row per unique patient.

Key fields:

- `patient_id` (string): De-identified patient identifier

- `race_name`, `race_category`: Race as documented (MIMIC-specific) and standardized CLIF category

- `ethnicity_name`, `ethnicity_category`: Ethnicity documentation and category

- `sex_name`, `sex_category`: Biological sex

- `birth_date` (date): Date of birth (year only for patients >89, per HIPAA)

- `death_dttm` (datetime, UTC): Hospital death timestamp if applicable

- `language_name`, `language_category`: Primary language

**`clif_hospitalization.parquet`** (~16 MB, [TODO: N rows])

Hospital and ICU admission/discharge information. One row per hospitalization.

Key fields:

- `patient_id` (string): Links to patient table

- `hospitalization_id` (string): Unique hospitalization identifier (used as foreign key across all tables)

- `hospital_admit_dttm`, `hospital_discharge_dttm` (datetime, UTC): Hospital admission and discharge times

- `icu_admit_dttm`, `icu_discharge_dttm` (datetime, UTC): ICU admission and discharge times

- `admission_type_name`, `admission_type_category`: Type of admission (emergency, elective, etc.)

- `discharge_location_name`, `discharge_location_category`: Discharge disposition

**`clif_adt.parquet`** (~33 MB, [TODO: N rows])

Admission-Discharge-Transfer events tracking patient location throughout hospitalization.

Key fields:

- `hospitalization_id` (string): Links to hospitalization table

- `in_dttm`, `out_dttm` (datetime, UTC): Location entry and exit times

- `location_name`, `location_category`: Physical location (ICU, ward, etc.)

- `location_type`: CLIF 2.1.0 addition classifying location types

### Clinical Monitoring Tables

**`clif_vitals.parquet`** (~265 MB, [TODO: N rows])

Vital sign measurements throughout hospitalization.

Key fields:

- `hospitalization_id` (string)

- `recorded_dttm` (datetime, UTC): Measurement timestamp

- `vital_name`, `vital_category`: Vital sign type (Heart Rate, Blood Pressure Systolic/Diastolic/Mean, SpO2, Respiratory Rate, Temperature, Glasgow Coma Scale)

- `vital_value` (string): Measurement value as recorded

- `vital_value_numeric` (float): Numeric conversion for quantitative analysis

- `reference_unit`: Unit of measurement (bpm, mmHg, %, breaths/min, °C/°F, score)

**`clif_labs.parquet`** (~344 MB, [TODO: N rows])

Laboratory test results including chemistry, hematology, and blood gas measurements.

Key fields:

- `hospitalization_id` (string)

- `lab_collect_dttm` (datetime, UTC): Specimen collection time

- `lab_result_dttm` (datetime, UTC): Result available time

- `lab_name`, `lab_category`: Test name and standardized category (e.g., "Hemoglobin", "Creatinine", "pH")

- `lab_value` (string), `lab_value_numeric` (float): Results

- `reference_unit`: Units (g/dL, mg/dL, mmol/L, etc.)

- `lab_order_dttm`, `lab_order_name`, `lab_order_category`: Order information (set to NA for MIMIC-IV)

- `lab_specimen_name`, `lab_specimen_category`: Specimen type (set to NA for MIMIC-IV)

- `lab_loinc_code`: LOINC codes (set to NA for MIMIC-IV)

**`clif_patient_assessments.parquet`** (~137 MB, [TODO: N rows])

Clinical assessment scores and scales.

Key fields:

- `hospitalization_id` (string)

- `assessment_dttm` (datetime, UTC): Assessment time

- `assessment_name`, `assessment_category`: Score type (Glasgow Coma Scale components, RASS, Pain Scale, Spontaneous Breathing Trial)

- `assessment_value` (string), `assessment_value_numeric` (float): Score values

### Respiratory Support Table

**`clif_respiratory_support.parquet`** (~29 MB, [TODO: N rows])

Mechanical ventilation modes, settings, and measurements.

Key fields:

- `hospitalization_id` (string)

- `recorded_dttm` (datetime, UTC)

- `device_name`, `device_category`: Respiratory support device (Invasive Mechanical Ventilation, Non-Invasive Ventilation, High-Flow Nasal Cannula, etc.)

- `mode_name`, `mode_category`: Ventilation mode (CMV, SIMV, PSV, CPAP, etc.)

- `tracheostomy_present` (boolean)

- Ventilator settings and measurements (PEEP, FiO2, tidal volume, respiratory rate, peak pressure, plateau pressure)

### Medication Administration Tables

**`clif_medication_admin_continuous.parquet`** (~84 MB, [TODO: N rows])

Continuous infusion medications (vasopressors, sedation, inotropes, insulin).

Key fields:

- `hospitalization_id` (string)

- `admin_dttm` (datetime, UTC): Administration timestamp

- `med_name`, `med_category`: Medication name and standardized category

- `med_route`: Route of administration

- `med_dose`, `med_dose_unit`: Dosage and units

**`clif_medication_admin_intermittent.parquet`** (~48 MB, [TODO: N rows])

Bolus and scheduled medication administrations.

Key fields:

- Similar to continuous table but for intermittent administrations

- Includes antibiotics, analgesics, anticoagulants, and other intermittent therapies

### Advanced Support Tables

**`clif_ecmo_mcs.parquet`** (~882 KB, [TODO: N rows])

Extracorporeal Membrane Oxygenation (ECMO) and Mechanical Circulatory Support devices.

Key fields:

- `hospitalization_id` (string)

- `recorded_dttm` (datetime, UTC)

- `device_name`, `device_category`: Support device type (VA-ECMO, VV-ECMO, IABP, Impella, etc.)

- Device-specific settings and measurements

**`clif_crrt_therapy.parquet`** (~4.3 MB, [TODO: N rows])

Continuous Renal Replacement Therapy (CRRT) sessions and settings (CLIF 2.1.0 addition).

Key fields:

- `hospitalization_id` (string)

- `recorded_dttm` (datetime, UTC)

- `therapy_name`, `therapy_category`: CRRT modality (CVVH, CVVHD, CVVHDF, SCUF)

- Therapy settings (blood flow rate, dialysate flow rate, replacement fluid rate)

### Procedures and Status Tables

**`clif_position.parquet`** (~24 MB, [TODO: N rows])

Patient positioning (prone, supine, etc.) - critical for ARDS management.

Key fields:

- `hospitalization_id` (string)

- `recorded_dttm` (datetime, UTC)

- `position_name`, `position_category`: Position type

**`clif_code_status.parquet`** (~1.1 MB, [TODO: N rows])

Code status and advance directive documentation (CLIF 2.1.0 addition).

Key fields:

- `hospitalization_id` (string)

- `recorded_dttm` (datetime, UTC)

- `code_status_name`, `code_status_category`: Resuscitation preferences

**`clif_hospital_diagnosis.parquet`** (~20 MB, [TODO: N rows])

ICD-9-CM and ICD-10-CM diagnosis codes.

Key fields:

- `hospitalization_id` (string)

- `diagnosis_code`, `diagnosis_code_type`: ICD code and version

- `diagnosis_name`: Diagnosis description

- `diagnosis_priority`: Primary vs. secondary diagnoses

**`clif_patient_procedures.parquet`** (~8.2 MB, [TODO: N rows])

ICD procedure codes and HCPCS codes.

Key fields:

- `hospitalization_id` (string)

- `procedure_dttm` (datetime, UTC): Procedure time (when available)

- `procedure_code`, `procedure_code_type`: ICD or HCPCS code and type

- `procedure_name`: Procedure description

## Data Completeness

**Available CLIF tables** (15 total): All core demographic, clinical monitoring, respiratory support, medication administration, and specialized ICU intervention tables available in MIMIC-IV.

**Unavailable CLIF tables** (data not in MIMIC-IV):

- Microbiology tables (culture, sensitivity, non-culture)

- Medication orders (only administrations available)

- Provider information (de-identified in MIMIC-IV)

- Therapy session/details

- Intake/output flowsheets

- Admission diagnosis table (diagnoses available but not linked to admission timing)

## Summary Statistics

- **Patients**: [TODO: N unique patient_id values]

- **Hospitalizations**: [TODO: N unique hospitalization_id values]

- **ICU encounters**: [TODO: subset of hospitalizations with ICU admit times]

- **Temporal span**: [TODO: earliest to latest date across all tables]

- **Total file size**: [TODO: sum of all parquet files, approximately 1.4 GB based on listed sizes]

- **Mechanical ventilation**: [TODO: N hospitalizations with invasive mechanical ventilation]

- **Mortality**: [TODO: N patients with death_dttm, N hospitalizations ending in death]

- **Table row counts**: See individual table descriptions above

[TODO: Additional population characteristics - age distribution, sex distribution, race/ethnicity breakdown]




# Usage Notes
> *This section should provide the reader with information relevant to reuse. Why is this data useful for the community?*
> - *In particular we suggest discussing: (1) how the data has already been used (citing relevant papers); (2) the reuse potential of the dataset; (3) known limitations that users should be aware of when using the resource; and (4) any complementary code or datasets that might be of interest to the user community.*

### (1) How the data has already been used

[TODO: Cite CLIF consortium concept paper when published]

[TODO: Cite any validation studies or analyses that have used this dataset]

This dataset is intended to serve as a reference implementation and public benchmark for CLIF-formatted data. Researchers are encouraged to share publications using this dataset to build a body of reproducible critical care research.

### (2) Reuse potential of the dataset

This dataset offers substantial reuse potential across multiple research domains:

**Algorithm Validation and Benchmarking**:

- Validate critical care prediction models (mortality, sepsis, ARDS, aki) against a standardized dataset

- Benchmark CLIF implementations from different institutions by comparing outputs

- Replicate findings from CLIF consortium studies using publicly accessible data

- Test algorithm robustness across temporal subsets or patient subgroups

**CLIF Pipeline Development**:

- Reference implementation for institutions developing their own MIMIC-to-CLIF ETL pipelines

- Verification dataset for testing CLIF schema compliance

- Training resource for data engineers learning CLIF format specifications

- Quality assurance comparison for institutional CLIF transformations

**Critical Care Phenotyping and Subgroup Discovery**:

- Disease trajectory modeling (e.g., temperature trajectories, respiratory failure progression)

- Unsupervised clustering to identify clinical subphenotypes

- Time-series analysis of longitudinal ICU data

- Treatment response heterogeneity assessment

**Multi-Center Study Prototyping**:

- Develop and refine analytical pipelines before deploying to federated institutional databases

- Test hypotheses and estimate effect sizes for power calculations

- Prototype cohort definitions and inclusion/exclusion criteria

- Validate data quality checks and outlier handling strategies

**Machine Learning Model Development**:

- Train predictive models using standardized ICU data elements

- Develop feature engineering approaches for longitudinal critical care data

- Test model interpretability methods on complex temporal data

- Build foundational models for critical illness (with MIMIC-IV's diverse data)

**Educational Applications**:

- Teaching critical care data science and informatics

- Training clinician-researchers in standardized data formats

- Demonstrating ETL best practices and data quality validation

- Workshops on reproducible critical care research methods

### (3) Known issues or limitations

Users should be aware of the following limitations when using this dataset:

#### Potential undercounting in race and ethnicity mapping

The race- and ethnicity-related fields in CLIF's `patient` table are sourced from MIMIC-IV's `race` field in the `admissions` table. MIMIC's `race` field provides a granular breakdown that merges what are historically documented separately as "race" and "ethnicity." Examples include "HISPANIC/LATINO - PUERTO RICAN", "SOUTH AMERICAN", "BLACK/CAPE VERDEAN", "WHITE - BRAZILIAN", and "ASIAN - ASIAN INDIAN".

Because patients no longer answer two separate questions, an Asian Hispanic patient would now be documented as "MULTIPLE RACE/ETHNICITY" instead of "Asian" for race and "Hispanic" for ethnicity. This could lead to undercounting of both categories in the final mapped result. If this patient chooses either category over the other, it would also lead to undercounting of the unchosen category.

In rarer cases, categories like "South American" which do not fit contemporary U.S. racial and ethnic classification schemas are mapped to uninformative "Other" or "Unknown" categories, since we cannot determine if the patient is of Spanish origin (which would classify them as "Hispanic").

Each of these edge cases accounts for approximately 0.1% of the patient population. For detailed discussion, see: https://github.com/MIT-LCP/mimic-code/issues/1236

#### Single-institution data limitations

This dataset derives from a single academic medical center (Beth Israel Deaconess Medical Center) and may not generalize to:

- Community hospitals or non-academic settings

- Institutions with different case mixes or patient demographics

- Healthcare systems with different documentation practices

- Regions with different population characteristics

Users should validate findings across multiple datasets when possible.

#### MIMIC-specific mapping decisions

Not all CLIF permissible values are represented in MIMIC-IV data:

- Some `*_category` values may have zero or very few instances

- Certain clinical practices specific to other institutions may not appear

- Temporal trends reflect BIDMC practices during the data collection period

The open-access mapping spreadsheet documents all MIMIC-to-CLIF decisions for transparency.

#### Datetime conversion and temporal edge cases

All timestamps are converted from MIMIC's UTC-5 (US Eastern Time) to UTC:

- Daylight saving time transitions are handled, but edge cases may exist

- Original MIMIC timestamps should be consulted for temporal analyses requiring extreme precision

- Some timestamps in MIMIC may have been entered with time zone assumptions that create artifacts

#### Missing CLIF tables

Several CLIF 2.1.0 tables are not available because MIMIC-IV does not contain the source data:

- Microbiology culture, sensitivity, and non-culture results (de-identified in MIMIC-IV)

- Medication orders (only administrations are recorded)

- Provider information (de-identified)

- Detailed therapy sessions (e.g., physical therapy, occupational therapy)

- Intake/output flowsheets

- Admission diagnosis timing

Users requiring these data elements should access them from institutional CLIF implementations.

#### Data quality considerations

While extensive quality checks are performed, users should be aware:

- Physiologically implausible values are filtered, but clinical judgment may differ on thresholds

- Missing data patterns reflect clinical documentation practices, not data processing errors

- Duplicate removal is systematic but may not capture all clinically redundant entries

- Some MIMIC data inconsistencies (e.g., copy-forward in respiratory flowsheets) persist in CLIF format

### (4) Complementary code or datasets

Researchers using MIMIC-IV-Ext-CLIF may benefit from these complementary resources:

**Code Repositories**:

- **CLIF-MIMIC ETL pipeline** (this repository): Open-source code for transforming MIMIC-IV to CLIF

  * GitHub: [TODO: Add repository URL]

  * Includes mapping spreadsheets, Hamilton DAG implementations, Pandera schemas

- **CLIF consortium website**: Data dictionaries, quality control scripts, analysis examples

  * https://clif-consortium.github.io/website/

- **MIMIC Code Repository**: Community-contributed code for MIMIC analysis

  * https://github.com/MIT-LCP/mimic-code

**Datasets**:

- **MIMIC-IV**: Original source dataset with additional tables not in CLIF format

  * https://physionet.org/content/mimiciv/

- **MIMIC-IV on FHIR**: MIMIC-IV in FHIR format for healthcare interoperability research

  * https://physionet.org/content/mimic-iv-fhir/

- **CLIF consortium data** (federated access): Contact CLIF consortium for multi-center analyses

**Documentation**:

- **CLIF data dictionary**: Complete specifications for all CLIF 2.1.0 tables and mCIDE

  * https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.1.0.html

- **MIMIC-to-CLIF mapping spreadsheet**: Transparent mapping decisions

  * https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/

- **MIMIC-IV documentation**: Source data documentation

  * https://mimic.mit.edu/docs/iv/


# Release Notes
>*Important notes about the current release, and changes from previous versions.*

## Initial Release (v0.2.0-beta)

This is the initial public release of MIMIC-IV-Ext-CLIF, providing MIMIC-IV v3.1 data transformed into CLIF 2.1.0 format.

**CLIF Tables Included** (15 total):

- **Core**: patient, hospitalization, adt

- **Clinical Monitoring**: vitals, labs, patient_assessments, position

- **Respiratory**: respiratory_support

- **Medications**: medication_admin_continuous, medication_admin_intermittent

- **Advanced Support**: ecmo_mcs (CLIF 2.1.0 addition), crrt_therapy (CLIF 2.1.0 addition)

- **Status & Procedures**: code_status (CLIF 2.1.0 addition), hospital_diagnosis, patient_procedures

**Key Features**:

- **Hamilton DAG-based ETL**: Reproducible, modular transformation pipeline with dependency-based execution

- **Pandera schema validation**: All tables validated against CLIF 2.1.0 specifications

- **Open-source mapping decisions**: Transparent MIMIC-to-CLIF mappings documented in accessible spreadsheet

- **UTC datetime standardization**: All timestamps converted from MIMIC's UTC-5 to UTC per CLIF requirements

- **Dual naming convention**: Preserves both MIMIC-specific terms (`*_name`) and standardized CLIF categories (`*_category`)

**Known Issues**:

- Race/ethnicity mapping may undercount some categories due to MIMIC's merged race/ethnicity field (affects ~0.1% of population)

- Some optional CLIF fields set to NA where MIMIC-IV data unavailable (e.g., lab_order_dttm, lab_specimen_name)

- Datetime conversion edge cases possible at daylight saving time transitions

For detailed limitations, see Usage Notes section above.

**Future Releases**:

- Additional validation against CLIF consortium quality benchmarks

- Potential inclusion of additional MIMIC-IV data elements as CLIF schema evolves

- Performance optimizations for large-scale analyses

# Acknowledgements
> *Thank the people who helped with the research but did not qualify for authorship. In addition, provide any funding information.*

[TODO: Acknowledge CLIF Consortium members and leadership]

We gratefully acknowledge the CLIF Consortium, a collaboration of critical care physicians and data scientists from nine US healthcare institutions, for developing the Common Longitudinal ICU data Format and providing the foundational framework this work builds upon.

We thank the MIMIC-IV team at MIT Laboratory for Computational Physiology and Beth Israel Deaconess Medical Center for creating and maintaining the MIMIC-IV database, without which this public CLIF implementation would not be possible.

We acknowledge PhysioNet for hosting and distributing critical care datasets that enable reproducible research worldwide.

[TODO: Add specific funding sources if applicable - e.g., NIH grants, institutional support]

[TODO: Acknowledge individual contributors to mapping decisions and code development]

# Conflicts Of Interest
> *A statement on potential conflicts of interest is required. If the authors have no conflicts of interest, the section should say "The author(s) have no conflicts of interest to declare".*

The author(s) have no conflicts of interest to declare.

# Ethics
> *Please provide a statement on the ethics of your work. Think about the project impact and briefly highlight both benefits and risks. Please also add relevant institutional review details here, for example:*
> - *Clinical trial data: Please specify trial registration number and registry name.*

## Ethical Approval and De-identification

This dataset is derived from MIMIC-IV v3.1, which was collected and de-identified with approval from the Institutional Review Boards of Beth Israel Deaconess Medical Center (Boston, MA) and the Massachusetts Institute of Technology (Cambridge, MA). The requirement for individual patient consent was waived because the project did not impact clinical care and all data were de-identified in compliance with the Health Insurance Portability and Accountability Act (HIPAA) Safe Harbor method.

The transformation from MIMIC-IV to CLIF format does not re-identify any patients or introduce new privacy risks. All patient identifiers, dates, and protected health information remain de-identified in accordance with MIMIC-IV's original de-identification procedures. The CLIF transformation preserves these de-identification properties while reorganizing data into a standardized format.

## Benefits and Impact

**Benefits**:

- **Democratizes access to standardized critical care data**: Enables researchers worldwide to use CLIF format without requiring institutional EHR access or consortium membership

- **Accelerates reproducible research**: Provides public benchmark dataset for validating critical care algorithms and findings

- **Enhances transparency**: Open-source ETL code and mapping decisions allow scrutiny and improvement of data transformation methods

- **Educational value**: Serves as training resource for critical care data science and standardized data formats

- **Potential for improved patient care**: Facilitates development of validated prediction models and clinical decision support tools that may ultimately improve critical illness outcomes

**Risks and Mitigation**:

- **Potential for misinterpretation**: Single-institution data may not generalize to all settings. Mitigation: Comprehensive documentation of limitations and encouragement of multi-dataset validation

- **Re-identification risk** (minimal): While MIMIC-IV's de-identification is robust, combinations of rare features could theoretically narrow identification. Mitigation: Adherence to PhysioNet's data use agreement and responsible data handling practices

- **Algorithmic bias propagation**: Models trained on this data may perpetuate biases present in source EHR system. Mitigation: Transparent documentation of demographic distributions and known limitations

- **Misuse for unapproved purposes**: Data governed by PhysioNet's data use agreement restricting use to research, education, and healthcare operations

## Responsible Use Guidelines

Users of this dataset must:

1. Complete required PhysioNet credentialing and sign the data use agreement

2. Use data only for approved research, educational, or healthcare quality improvement purposes

3. Not attempt to re-identify any individuals

4. Acknowledge data source and cite this dataset appropriately in publications

5. Validate findings across multiple datasets when feasible, recognizing single-institution limitations

6. Consider fairness and bias implications when developing predictive models

7. Report any potential data quality issues or privacy concerns to dataset maintainers

## Data Use Agreement

Access to this dataset requires agreement to the PhysioNet Credentialed Health Data License, which includes provisions for:

- Protecting patient privacy and confidentiality

- Using data only for approved purposes

- Not attempting re-identification

- Proper data security measures

- Acknowledgment and citation requirements

Users must complete PhysioNet's credentialing process, which includes CITI "Data or Specimens Only Research" training or equivalent.


# Example MIMIC-based Submissions
- [Symile-MIMIC: a multimodal clinical dataset of chest X-rays, electrocardiograms, and blood labs from MIMIC-IV](https://physionet.org/content/symile-mimic/1.0.0/)
- [MIMIC-IV-Ext-CEKG: A Process-Oriented Dataset Derived from MIMIC-IV for Enhanced Clinical Insights](https://physionet.org/content/mimic-iv-ext-cekg/1.0.0/)
- [MIMIC-IV on FHIR](https://physionet.org/content/mimic-iv-fhir/2.1/)
