# MIMIC-IV-Ext-CLIF
> *Your title should be no longer than 200 characters. Avoid acronyms and abbreviations where possible. Also avoid leading with "The". Only letters, numbers, spaces, underscores, and hyphens are allowed.* 
> 
> *If your dataset is derived from MIMIC and you would like to use the MIMIC acronym, please include the letters "Ext" (for example, MIMIC-IV-Ext-YOUR-DATASET"). Ext may either indicate "extracted" (e.g. a derived subset) or "extended" (e.g. annotations), depending on your use case.*
> 
> *If the dataset is derived from another dataset, the title must make this clear.*


Zewei Liao, Shan Guleria, Kevin Smith, Rachel Baccile, Kaveri Chhikara, Dema Therese, Vaishvik Chaudhari, Michael Burkhart, Brett Beaulieu-Jones, Snigdha Jain, Katie Connell, Kevin Buell, Juan C. Rojas, Patrick G. Lyons, Sivasubramanium V. Bhavani, Catherine A. Gao, Anna K. Barker, Chad H. Hochberg, Nicholas E. Ingraham, William F. Parker


# Abstract
> *A brief description of the resource and the context in which it was created.*
>
> *Your abstract must be no longer than 250 words. The focus should be on the resource being shared. If the resource was generated as part of a scientific investigation, relevant information may be provided to facilitate reuse. References should not be included. The abstract should also include a high-level description of the data as well as an overview of the key aims of the project. The abstract may appear in search indexes independently of the full project metadata, so providing detailed information about the content is important.*

MIMIC-IV-Ext-CLIF is a derived dataset of MIMIC-IV v3.1, transformed into the Common Longitudinal ICU data Format (CLIF). CLIF is an open-source critical care data model developed by a consortium of 10+ US academic medical centers to standardize intensive care unit (ICU) data for multi-center research. While CLIF has demonstrated value in federated research settings, access has been limited to consortium members with institutional electronic health record (EHR) data. This dataset addresses that gap by providing CLIF-formatted data derived from the open-access deidentified MIMIC-IV dataset, enabling researchers worldwide to utilize the CLIF format without requiring institutional EHR access.

MIMIC-IV-Ext-CLIF contains 14 CLIF tables as of the latest CLIF 2.1.0 version, covering core demographics (`patient`, `hospitalization`), clinical monitoring (`vitals`, `labs`), respiratory support, medication administration, and specialized ICU interventions (e.g. continuous renal replacement therapy). The transformation employs a reproducible ETL pipeline with transparent mapping decisions documented in user-friendly spreadsheets. 

# Background
> *Your background should provide the reader with an introduction to the resource. The section should offer context in which the resource was created and outline your motivations for sharing.*

Each year, more than five million Americans suffer from critical illness, or acute organ failure that necessitates life-sustaining interventions. While electronic health records (EHRs) contain granular data that could inform better understanding and management of critical illness, large-scale EHR research is hampered by challenges related to data handling, security, and standardization. [1]

To address these issues, a consortium of critical care clinicians and data scientists from over ten U.S. health systems created the Common Longitudinal Intensive Care Unit (ICU) data Format (CLIF). CLIF is an open-source data model that harmonizes a minimum set of ICU Data Elements (mCIDE) to support research in critical care. Its effectiveness has been demonstrated in federated studies analyzing data from over 100,000 ICU patients across multiple health systems, highlighting its utility in reproducible multi-center research, from mortality prediction validation to clinical subphenotyping. [1]
 
However, CLIF implementation requires substantial data science and clinical expertise, while access has been limited to consortium institutions with EHR infrastructure. This creates a barrier for researchers who could benefit from CLIF's standardized format but lack institutional resources. MIMIC-IV-Ext-CLIF closes this gap by providing a freely accessible CLIF-formatted dataset from MIMIC-IV [2], opening up CLIF access to researchers without institutional EHR access, who can now develop code against the MIMIC-IV-Ext-CLIF dataset and scale their studies across the CLIF consortium.


# Methods
> *The "Methods" and "Technical Implementation" sections provide details of the procedures used to create your resource including, but not limited to, how the data was collected, any measurement devices, etc. For software, the section may cover aspects such as development process, software design, and description of algorithms. For data, the section may include details such as experimental design, data acquisition, and data processing.*

The dataset is created following a four-step process: (1) query, (2) map, (3) program, and (4) validate.

## (1) Query

Extensive query of the original MIMIC-IV database was performed to identify candidate data elements for each CLIF table. The query employs case-insensitive keyword matching against MIMIC tables and fields, in tandem with consultation of the rich online documentation on both the official MIMIC website and the GitHub community.

To systematically evaluate candidates, we developed utility functions that automatically generate comprehensive listings of MIMIC data elements with relevant statistics: minimum, mean, median, maximum values for numeric variables, and distinct categories and their frequency counts for categorical variables.

## (2) Map

Candidate MIMIC data elements, along with their statistical summaries, are comprehensively documented in an [open-access spreadsheet](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/edit?usp=sharing) where they are reviewed and mapped to CLIF mCIDE categories by a group of one to three expert physician-scientists and one to two data scientists. In most cases, a common decision label is assigned to each MIMIC `item` or data element reviewed:

- TO MAP, AS IS: Direct mapping to CLIF category without transformation

- TO MAP, CONVERT UOM: Mapping requires unit of measurement conversion (e.g., mg/dL to mmol/L)

- NO MAPPING: This MIMIC data element has no appropriate CLIF counterpart and should not be mapped

- MAPPED ELSEWHERE: This data element is captured in a different CLIF table or field

- UNSURE: Requires additional clinical or technical review

- NOT AVAILABLE: CLIF data element not present or insufficient in MIMIC-IV

This open-access documentation approach prioritizes transparency, allowing clinicians and researchers to evaluate mapping decisions without requiring programming expertise. The spreadsheet also tracks review status, reviewer identities, and actionable next steps.

For each mapping, we preserve both the MIMIC-specific terminology (in `*_name` fields) and the standardized CLIF category (in `*_category` fields). For example, MIMIC's `itemid` 220045 ("Heart Rate") maps to `vital_category = "heart_rate"` while `vital_name` preserves the original MIMIC label. This dual representation enables both standardization and traceability.

## (3) Program

Mapping decisions documented in the spreadsheet are implemented as modular Python scripts using modern data engineering frameworks. Each CLIF table is built using the Hamilton DAG pattern to ensure modularity, testability, and reproducibility. The pipeline uses exported CSV copies of the mapping spreadsheet as the "source of truth," avoiding error-prone hard-coding. When a mapping decision changes (e.g., updating "TO MAP, AS IS" to "NO MAPPING"), re-running the pipeline automatically incorporates the change without code modification. The ETL pipeline is implemented in the [CLIF-MIMIC GitHub repository](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/tree/main) and is publicly available for review and reuse.

## (4) Validate

Validation occurs at multiple levels to ensure CLIF 2.1.0 compliance and data quality.

The `pandera` framework is deployed to validate the schema of each transformed CLIF table, checking for compliance in data types, nullability, and permissible mCIDE categories. These validation are accomanieid by more complex and comprehensive checks using tools in the CLIF ecosystem such as CLIF TableOne [7] and CLIF Lighthouse [8] against CLIF consortium-wide quality benchmarks.

For complex transformations such as flattening the timestamps in the medication administration tables, unit tests are written to ensure the robustness of the transformation.

# Data Description
> *Content description: Your content (data, software, model) description should describe the resource in detail, outlining how files are structured, file formats, and a description of what the files contain. We also suggest including summary statistics where appropriate (e.g. total number of distinct patients, number of files, types of signals, over what time span was the data collected, etc.).*

The dataset consists of 14 CLIF tables derived from MIMIC-IV v3.1, each stored as a separate Parquet file. For detailed descriptions of each CLIF table, see the [CLIF data dictionary](https://clif-icu.com/data-dictionary).

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
├── clif_crrt_therapy.parquet
├── clif_code_status.parquet
├── clif_hospital_diagnosis.parquet
└── clif_patient_procedures.parquet
```

# Usage Notes
> *This section should provide the reader with information relevant to reuse. Why is this data useful for the community?*
> - *In particular we suggest discussing: (1) how the data has already been used (citing relevant papers); (2) the reuse potential of the dataset; (3) known limitations that users should be aware of when using the resource; and (4) any complementary code or datasets that might be of interest to the user community.*

### Reuse potential

As an open-access implemnentation of the CLIF format, this dataset offers substantial reuse potential for researchers both within or outside the CLIF consortium. For researchers already with CLIF-formatted insitutational data, this dataset can serve as a validation dataset for code development and project prototyping. For researchers currently building their CLIF ETL pipelines, this dataset can serve as reference implementation in orchestrating certain CLIF-specific transformations. For researchers without CLIF-formatted institutional data, this dataset provides a low-barrior entry point to the CLIF format whereby code developed against this dataset can be scaled across the entire CLIF consortium, and any researcher can reproduce findings from any CLIF consortium studies using this open-access implementation.

See references [4-6] for published studies and manuscripts that have already used this dataset. 


### Known issues or limitations

For a detailed listing of issues encountered and decisions made when mapping MIMIC-IV to CLIF, see the [ISSUESLOG](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/blob/main/ISSUESLOG.md).

### Complementary resources

[MIMIC-IV-Ext-CLIF ETL pipeline GitHub repository](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/tree/main)

[MIMIC-to-CLIF mapping spreadsheet](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/)

[CLIF data dictionary](https://clif-icu.com/data-dictionary)

[CLIF website](https://clif-icu.com/)


# Release Notes
>*Important notes about the current release, and changes from previous versions.*

Detailed release notes can be found in the [CHANGELOG](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/blob/main/CHANGELOG.md) of this project's GitHub repository.


# Acknowledgements
> *Thank the people who helped with the research but did not qualify for authorship. In addition, provide any funding information.*

We thank the MIMIC team at MIT Laboratory for Computational Physiology and Beth Israel Deaconess Medical Center for creating and maintaining the MIMIC-IV database, without which this public CLIF implementation would not be possible. We acknowledge PhysioNet for hosting and distributing critical care datasets that enable reproducible research worldwide.

Dr. Lyons is supported by NIH/NCI K08CA270383. Dr. Rojas is supported by NIH/NIDA R01DA051464 and the Robert Wood Johnson Foundation and has received consulting fees from Truveta. Dr. Buell is supported by an institutional research training grant (NIH/NHLBI T32 HL007605). Dr. Bhavani is supported by NIH/NIGMS K23GM144867. Dr. Gao is supported by NIH/NHLBI K23HL169815, a Parker B. Francis Opportunity Award, and an American Thoracic Society Unrestricted Grant. Dr. Hochberg is supported by NIH/NHLBI K23HL169743. Dr. Ingraham is supported by NIH/NHLBI K23HL166783. Dr. Barker is supported by an institutional research training grant (NIH/NHLBI T32 HL 007749). Dr. Parker is supported by NIH K08HL150291, R01LM014263, and the Greenwall Foundation. 


# Conflicts Of Interest
> *A statement on potential conflicts of interest is required. If the authors have no conflicts of interest, the section should say "The author(s) have no conflicts of interest to declare".*

The author(s) have no conflicts of interest to declare.

# Ethics
> *Please provide a statement on the ethics of your work. Think about the project impact and briefly highlight both benefits and risks. Please also add relevant institutional review details here, for example:*
> - *Clinical trial data: Please specify trial registration number and registry name.*

MIMIC-IV-Ext-CLIF is derived from MIMIC-IV and is covered by the same IRB. 

# References

1. Rojas JC, Lyons PG, Chhikara K, Chaudhari V, Bhavani SV, Nour M, et al. A common longitudinal intensive care unit data format (CLIF) for critical illness research. Intensive Care Med [Internet]. 2025 Mar 1 [cited 2025 Nov 11];51(3):556–69. Available from: https://doi.org/10.1007/s00134-025-07848-7
2. Johnson AEW, Bulgarelli L, Shen L, Gayles A, Shammout A, Horng S, et al. MIMIC-IV, a freely accessible electronic health record dataset. Sci Data [Internet]. 2023 Jan 3 [cited 2025 Nov 11];10(1):1. Available from: https://www.nature.com/articles/s41597-022-01899-x
3. Common-Longitudinal-ICU-data-Format/CLIF-MIMIC [Internet]. Common Longitudinal ICU data Format (CLIF); 2025 [cited 2025 Nov 11]. Available from: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC
4. Ingraham N e., Chhikara K, Eddington C, Ortiz A c., Schmid B, Weissman G e., et al. The Association of Sex and Height With Low-tidal Volume Ventilation in a Multi-center Cohort of Critically Ill Adults. Am J Respir Crit Care Med [Internet]. 2025 May [cited 2025 Nov 11];211(Abstracts):A7695–A7695. Available from: https://www.atsjournals.org/doi/abs/10.1164/ajrccm.2025.211.Abstracts.A7695
5. Amagai S, Chaudhari V, Chhikara K, Ingraham NE, Hochberg CH, Barker AK, et al. The Epidemiology of ICU Readmissions Across Ten Health Systems. Critical Care Explorations [Internet]. 2025 Nov [cited 2025 Nov 11];7(11):e1341. Available from: https://journals.lww.com/ccejournal/fulltext/2025/11000/the_epidemiology_of_icu_readmissions_across_ten.1.aspx
6. Patel B k., Chhikara K, Liao Z, Ingraham N e., Eddington C, Jain S, et al. Identifying Windows of Opportunity for Early Mobilization of Mechanically Ventilated Patients: Multi-center Comparative Analysis of Clinical Trial and Consensus Guideline Eligibility Criteria. Am J Respir Crit Care Med [Internet]. 2025 May [cited 2025 Nov 11];211(Abstracts):A2870–A2870. Available from: https://www.atsjournals.org/doi/abs/10.1164/ajrccm.2025.211.Abstracts.A2870
7. Common-Longitudinal-ICU-data-Format/CLIF-TableOne [Internet]. Common Longitudinal ICU data Format (CLIF); 2025 [cited 2025 Nov 11]. Available from: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-TableOne
8. Common-Longitudinal-ICU-data-Format/CLIF-Lighthouse [Internet]. Common Longitudinal ICU data Format (CLIF); 2025 [cited 2025 Nov 11]. Available from: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-Lighthouse



# Example MIMIC-based Submissions
- [Symile-MIMIC: a multimodal clinical dataset of chest X-rays, electrocardiograms, and blood labs from MIMIC-IV](https://physionet.org/content/symile-mimic/1.0.0/)
- [MIMIC-IV-Ext-CEKG: A Process-Oriented Dataset Derived from MIMIC-IV for Enhanced Clinical Insights](https://physionet.org/content/mimic-iv-ext-cekg/1.0.0/)
- [MIMIC-IV on FHIR](https://physionet.org/content/mimic-iv-fhir/2.1/)
