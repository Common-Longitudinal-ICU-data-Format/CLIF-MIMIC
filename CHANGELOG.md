# Changelog
Current CLIF version: 2.0

Current MIMIC-IV version: 3.1

## [Planned]
- [add](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF-MIMIC/issues/1) mapping for `location_type` 

## [v0.0.1] - 2025-01-31
### Overview
- updated CLIF tables: `respiratory_support`, `patient`.

### Changed
- the `device_category` mapping of "T-piece" was changed from "IMV" to "Others" pending further review

### Fixed
- typo in the config files that mistakenly suggested the latest CLIF version is 2.1 (it should be 2.0)
- remove two duplicate rows in the CLIF `patient` table output.


## [v0.0.0] - 2025-01-21
First release.