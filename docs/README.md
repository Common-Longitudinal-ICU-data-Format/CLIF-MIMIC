# MIMIC-IV-Ext-CLIF

MIMIC-IV-Ext-CLIF is a derived dataset that transforms MIMIC-IV v3.1 into the Common Longitudinal ICU data Format (CLIF), an open-source critical care data model developed by a consortium of 10+ US academic medical centers. This dataset enables researchers worldwide to utilize the standardized CLIF format without requiring institutional EHR access.

## Dataset Contents

This dataset contains 14 CLIF tables (version 2.1.0) derived from MIMIC-IV v3.1, stored as Parquet files:

```
mimic-iv-ext-clif/
├── README.md
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

## Resources

- CLIF Data Dictionary: https://clif-icu.com/data-dictionary/data-dictionary-2.1.0
- ETL Pipeline & Source Code: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC
- MIMIC-to-CLIF Mappings: https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/
- Change Log: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/blob/main/CHANGELOG.md
- Issues Log: https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/blob/main/ISSUESLOG.md
- PhysioNet Project Page: https://physionet.org/content/mimic-iv-ext-clif

## License

This derived dataset is licensed under the MIT License. The source MIMIC-IV dataset is subject to the [PhysioNet Data Use Agreement](https://physionet.org/content/mimiciv/view-dua/3.1/).
