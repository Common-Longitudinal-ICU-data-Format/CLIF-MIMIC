# testing
resp_device_rank.index("IMV")

# check all duplicates are dropped
find_duplicates(resp_events_clean)

resp_wider_cleaned
# check whether there are still duplicates that remain
(resp_wider_cleaned.columns.value_counts() > 1).sum()
resp_wider_cleaned


# check for mapping -- looks like it's fine
resp_wider_cleaned.value_counts(["device_name", "device_category"], dropna = False)
# check for mapping
resp_counts = resp_wider_cleaned.value_counts(["mode_name", "mode_category", "device_category"], dropna = False).reset_index()
resp_counts
# check for mapping
mode_device_counts = resp_wider_cleaned.value_counts(["mode_category", "device_category"], dropna = False)
mode_device_counts


resp_fcf[
    resp_fcf.duplicated(subset = ["hospitalization_id", "recorded_dttm"], keep = False)
]