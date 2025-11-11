# ISSUES LOG

## `patient` table

### non-unique race and ethnicity values across encounters

In MIMIC-IV, patient race and ethnicity are documented at each encounter and the same patient can be documented with different race and ethnicity values across encounters. To ensure each patient has a unique value for `race_category` and `ethnicity_category` in CLIF's `patient` table, we consider values other than "unknown" or "other" to be informative, and choose the highest-frequency informative value for a patient across all encounters. When there is a tie, we choose the most recent. 

For example, if a patient's documented race, from least to most recent, is `asian, white, asian, white, other`, we would map this patient's race to `white`, because after excluding the uninformative `other`, `asian` and `white` are tied at a frequency of 2, but `white` is the most recent.

### Potential undercounting in race and ethnicity mapping

The race- and ethnicity-related fields in CLIF's `patient` table are sourced from MIMIC-IV's `race` field in the `admissions` table. MIMIC's `race` field provides a granular breakdown that merges what were historically documented separately as "race" and "ethnicity." Examples include "HISPANIC/LATINO - PUERTO RICAN", "SOUTH AMERICAN", "BLACK/CAPE VERDEAN", "WHITE - BRAZILIAN", and "ASIAN - ASIAN INDIAN".

Because patients no longer answer two separate questions, an Asian Hispanic patient would now be documented as "MULTIPLE RACE/ETHNICITY" instead of "Asian" for race and "Hispanic" for ethnicity. This could lead to undercounting of both categories in the final mapped result. If this patient chooses either category over the other, it would also lead to undercounting of the unchosen category.

In rarer cases, categories like "South American" which do not fit contemporary U.S. racial and ethnic classification schemas are mapped to uninformative "Other" or "Unknown" categories, since we cannot determine if the patient is of Spanish origin (which would classify them as "Hispanic").

Each of these edge cases accounts for approximately 0.1% of the patient population. See discussion: https://github.com/MIT-LCP/mimic-code/issues/1236

## `hospitalization` table

### `admission_type_category`
The current mapping to the `admission_type_category` field is known to have questionable congruence and is being actively investigated and subject to change.

## `labs` table

### `lab_order_dttm`
There are three date-time fields in CLIF's `labs` table: `lab_order_dttm`, `lab_collect_dttm`, `lab_result_dttm`. In MIMIC, only two date-time fields are available: [`charttime`](https://mimic.mit.edu/docs/iv/modules/hosp/labevents/#charttime) and [`storetime`](https://mimic.mit.edu/docs/iv/modules/hosp/labevents/#charttime). `charttime` is said to capture "usually the time at which the specimen was acquired" and therefore mapped to `lab_collect_dttm`, while `storetime` is "when the information would have been available to care providers" and therefore mapped to `lab_result_dttm`. This leaves `lab_order_dttm` null which has caused issue in previous projects. Therefore, in the interest of not having an entirely null field, starting from the 1.0.0 release,`lab_order_dttm` is populated with the same [`charttime`](https://mimic.mit.edu/docs/iv/modules/hosp/labevents/#charttime) from MIMIC that was previously mapped to `lab_collect_dttm` only.


## `medication_admin_*` tables

### `med_route_category`
In MIMIC-IV, information about the route of medication administration is dispersed across multiple fields: `ordercategoryname`, `secondaryordercategoryname`, `ordercomponenttypedescription`, `ordercategorydescription`, `category`. In [most cases](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/edit?gid=2143004591#gid=2143004591), a combination of one or more of these fields are enough to determine the `med_route_category` in CLIF. In [rarer cases](https://docs.google.com/spreadsheets/d/1QhybvnlIuNFw0t94JPE6ei2Ei6UgzZAbGgwjwZCTtxE/edit?gid=1471893996#gid=1471893996) where these are not enough, we also take into account the particular medication. For example, for the same `ordercategoryname` = '11-Prophylaxis (Non IV)', we know 'Heparin Sodium (Prophylaxis)' would have to be administered intramuscularly (`med_route_category` = 'im'), while 'Pantoprazole (Protonix)' would have to be administered enterally (`med_route_category` = 'enteral'). The only 2 exceptions so far are 'Insulin - Humalog' (`itemid = 223262`) and 'Naloxone (Narcan)' (`itemid = 222021`) which are only marked to be `05-Med Bolus` and are currently mapped to `iv` but theorectically they can be administered through IM or inhalation in very rare cases.

## `patient_assessments` table

### `cam_loc`

One of the MIMIC-IV items currently mapped to CLIF's `cam_loc` field is shown to have poor consistency. For a detailed discussion, see [#17](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/17).

