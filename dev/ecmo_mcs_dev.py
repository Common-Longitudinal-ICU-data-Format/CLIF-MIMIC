import marimo

__generated_with = "0.20.2"
app = marimo.App(width="columns", sql_output="polars")


@app.cell(column=0)
def _():
    import os
    os.getcwd()
    return


@app.cell
def _():
    from src import utils

    return


@app.cell
def _():
    import marimo as mo
    import numpy as np
    import pandas as pd
    import logging
    import duckdb
    from src.utils import (
        construct_mapper_dict, fetch_mimic_events, load_mapping_csv,
        get_relevant_item_ids, find_duplicates, rename_and_reorder_cols,
        save_to_rclif, convert_and_sort_datetime, setup_logging,
        search_mimic_items, mimic_table_pathfinder,
        resave_mimic_table_from_csv_to_parquet, read_from_rclif,
        convert_tz_to_utc
    )

    return load_mapping_csv, mimic_table_pathfinder, mo, pd, search_mimic_items


@app.cell
def _():
    # resave_mimic_table_from_csv_to_parquet(table='chartevents')
    return


@app.cell
def _(pd):
    pt_demo = pd.read_parquet('tests/clif_patient.parquet')
    hosp_demo = pd.read_parquet('tests/clif_hospitalization.parquet')
    return (hosp_demo,)


@app.cell
def _(mo):
    mo.md(r"""
    # Query
    """)
    return


@app.cell
def _(search_mimic_items):
    search_mimic_items(kw='ECMO')
    return


@app.cell
def _(search_mimic_items):
    search_mimic_items(kw='Impella')
    return


@app.cell
def _():
    # Sample ECMO events: circuit config, flow, speed, sweep, FiO2
    # ecmo_sample = fetch_mimic_events(item_ids=[229268, 229270, 229277, 229278, 229280])
    return


@app.cell
def _(mo):
    mo.md(r"""
    # ETL
    """)
    return


@app.cell
def _(load_mapping_csv):
    ecmo_mapping_raw = load_mapping_csv("ecmo_mcs")
    ecmo_mapping_raw
    return (ecmo_mapping_raw,)


@app.cell
def _(ecmo_mapping_raw, mo):
    ecmo_mapping = mo.sql(
        f"""
        -- Filter to items with a variable mapping. device_context is provided
        -- directly in the mapping CSV (normalizes MIMIC categories so device
        -- identification and measurement items share the same grouping key).
        FROM ecmo_mapping_raw
        SELECT itemid, variable, label, category, device_context
        WHERE variable IS NOT NULL AND TRIM(variable) != ''
        """
    )
    return (ecmo_mapping,)


@app.cell
def _(mo):
    device_lookup = mo.sql(
        f"""
        -- Lookup table mapping (itemid, charted_value) -> standardized CLIF categories.
        -- Only device-identification items (e.g., Circuit Configuration, Type of Catheter)
        -- appear here. The CSV stores 'NA' for non-ECMO devices; convert to SQL NULL.
        FROM 'data/mappings/mimic-to-clif-mappings - ecmo_mcs_device.csv'
        SELECT itemid, "label", "value", device_category, mcs_group
            , ecmo_config: NULLIF(ecmo_configuration_category, 'NA')
        	--, ecmo_configuration_category
        -- RESUME: complete value column?
        """
    )
    return (device_lookup,)


@app.cell
def _(ecmo_mapping, mimic_table_pathfinder, mo):
    all_events = mo.sql(
        f"""
        -- Base CTE: fetch every chartevent row whose itemid is in the mapping.
        -- INNER JOIN filters to only mapped items; device_context comes along
        -- so downstream CTEs can group by physical device.
        FROM '{mimic_table_pathfinder("chartevents")}' ce
        INNER JOIN ecmo_mapping m ON ce.itemid = m.itemid
        SELECT
            ce.hadm_id, ce.charttime, ce.itemid, ce.value, ce.valuenum
            , m.variable, m.label, m.device_context
        WHERE ce.hadm_id IS NOT NULL
        """
    )
    return (all_events,)


@app.cell
def _(all_events, mo):
    _df = mo.sql(
        f"""
        -- QA check on duplication
        SELECT *
        FROM all_events
        WHERE variable != 'device_name'
            AND (hadm_id, charttime, variable, device_context) IN (
            SELECT hadm_id, charttime, variable, device_context
            FROM all_events
            GROUP BY hadm_id, charttime, variable, device_context
            HAVING COUNT(DISTINCT valuenum) > 1
        )
        ORDER BY hadm_id, charttime, variable, device_context;
        """
    )
    return


@app.cell
def _(all_events, mo):
    _df = mo.sql(
        f"""
        -- QA check on duplication
        SELECT *
        FROM all_events
        WHERE variable != 'device_name'
            AND (hadm_id, charttime, variable) IN (
            SELECT hadm_id, charttime, variable
            FROM all_events
            GROUP BY hadm_id, charttime, variable
            HAVING COUNT(DISTINCT valuenum) > 1
        )
        ORDER BY hadm_id, charttime, variable;
        """
    )
    return


@app.cell
def _(all_events, device_lookup, mo):
    device_events = mo.sql(
        f"""
        -- Items where variable = 'device_name' identify WHICH device is in use
        -- (e.g., Circuit Configuration -> "VV", Type of Catheter -> "5.5").
        -- LEFT JOIN with device_lookup translates the charted text value into
        -- standardized device_category, mcs_group, and ecmo_configuration_category.
        FROM all_events ae
        LEFT JOIN device_lookup dl ON ae.itemid = dl.itemid AND TRIM(ae.value) = TRIM(dl.value)
        SELECT
            ae.hadm_id, ae.charttime, ae.device_context
            , device_name: dl.label || ' = ' || ae.value
            , device_category: dl.device_category
            , mcs_group: dl.mcs_group
            , ecmo_configuration_category: dl.ecmo_config
        WHERE ae.variable = 'device_name'
        """
    )
    return (device_events,)


@app.cell
def _(all_events, mo):
    _df = mo.sql(
        f"""
        -- experiment w/o group by
        FROM all_events ae
        SELECT
            -- NOTE: keep raw MIMIC names (hadm_id, charttime) in intermediates;
            -- rename to CLIF standard only in the final join SELECT to keep
            -- JOIN conditions readable and preserve source lineage.
            ae.hadm_id, ae.charttime, ae.device_context
            -- Blood flow in L/min (numeric)
            , flow: CASE WHEN ae.variable = 'flow' THEN ae.valuenum END
            -- Gas sweep rate: stored as text in MIMIC, extract numeric portion
            , sweep_set: CASE WHEN ae.variable = 'sweep_set'
                THEN TRY_CAST(REGEXP_EXTRACT(ae.value, '[\\d\\.]+') AS FLOAT) END
            -- FiO2: MIMIC stores as % (0-100), divide by 100 -> fraction (0-1)
            , fdo2_set: CASE WHEN ae.variable = 'fdo2_set' THEN ae.valuenum / 100.0 END
            -- Control parameters: the mapping CSV encodes category after the colon
            -- e.g., "control_parameter_category:rpm" -> category = "rpm"
            , control_parameter_name: CASE WHEN ae.variable LIKE 'control_parameter_category:%' THEN ae.label END
            , control_parameter_category: CASE WHEN ae.variable LIKE 'control_parameter_category:%'
                THEN SPLIT_PART(ae.variable, ':', 2) END
            -- RPM items are numeric; Impella power is text (P0-P9) -> extract digit
            , control_parameter_value: CASE
                WHEN ae.variable = 'control_parameter_category:rpm' THEN ae.valuenum
                WHEN ae.variable = 'control_parameter_category:impella_power'
                    THEN TRY_CAST(REGEXP_EXTRACT(ae.value, '\\d+') AS FLOAT)
                END
        WHERE ae.variable != 'device_name'
        ORDER BY 1, 2, 3
        -- GROUP BY 1, 2, 3
        """
    )
    return


@app.cell
def _(all_events, mo):
    measurement_events = mo.sql(
        f"""
        -- Pivot all non-device items into wide format: one row per
        -- (hadm_id, charttime, device_context) with columns for each measurement.
        -- MAX() is safe because at most one item per variable fires per group.
        FROM all_events ae
        SELECT
            -- NOTE: keep raw MIMIC names (hadm_id, charttime) in intermediates;
            -- rename to CLIF standard only in the final join SELECT to keep
            -- JOIN conditions readable and preserve source lineage.
            ae.hadm_id, ae.charttime, ae.device_context
            -- Blood flow in L/min (numeric)
            , flow: MAX(CASE WHEN ae.variable = 'flow' THEN ae.valuenum END)
            -- Gas sweep rate: stored as text in MIMIC, extract numeric portion
            , sweep_set: MAX(CASE WHEN ae.variable = 'sweep_set'
                THEN TRY_CAST(REGEXP_EXTRACT(ae.value, '[\\d\\.]+') AS FLOAT) END)
            -- FiO2: MIMIC stores as % (0-100), divide by 100 -> fraction (0-1)
            , fdo2_set: MAX(CASE WHEN ae.variable = 'fdo2_set' THEN ae.valuenum / 100.0 END)
            -- Control parameters: the mapping CSV encodes category after the colon
            -- e.g., "control_parameter_category:rpm" -> category = "rpm"
            , control_parameter_name: MAX(CASE WHEN ae.variable LIKE 'control_parameter_category:%' THEN ae.label END)
            , control_parameter_category: MAX(CASE WHEN ae.variable LIKE 'control_parameter_category:%'
                THEN SPLIT_PART(ae.variable, ':', 2) END)
            -- RPM items are numeric; Impella power is text (P0-P9) -> extract digit
            , control_parameter_value: MAX(CASE
                WHEN ae.variable = 'control_parameter_category:rpm' THEN ae.valuenum
                WHEN ae.variable = 'control_parameter_category:impella_power'
                    THEN TRY_CAST(REGEXP_EXTRACT(ae.value, '\\d+') AS FLOAT)
                END)
        WHERE ae.variable != 'device_name'
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
        """
    )
    return (measurement_events,)


@app.cell
def _(device_events, measurement_events, mo):
    clif_ecmo_mcs_raw = mo.sql(
        f"""
        -- FULL OUTER JOIN: keeps measurement rows without a device event (common for
        -- Centrimag, legacy Hemodynamics items) and device events without measurements.
        -- Fallback CASE WHENs provide default device_category/mcs_group inferred from
        -- device_context when no device-identification event exists at that timestamp.
        FROM measurement_events m
        FULL OUTER JOIN device_events d
            ON m.hadm_id = d.hadm_id 
            AND m.charttime = d.charttime 
            AND m.device_context = d.device_context
        SELECT
            hospitalization_id: CAST(COALESCE(m.hadm_id, d.hadm_id) AS VARCHAR)
            , recorded_dttm: CAST(COALESCE(m.charttime, d.charttime) AS TIMESTAMP)
            , _device_context: COALESCE(m.device_context, d.device_context)
            , device_name: COALESCE(d.device_name, _device_context)
            -- Fallback: when no device event matched, infer device_category from context
            , device_category: -- d.device_category
            COALESCE(d.device_category, CASE
                WHEN COALESCE(m.device_context, d.device_context) = 'ecmo' THEN 'ecmo_other_unspec'
            	   WHEN COALESCE(m.device_context, d.device_context) = 'hm2' THEN 'heartmate_2'
                WHEN COALESCE(m.device_context, d.device_context) = 'rvad' THEN 'rvad_other_unspec'
                WHEN COALESCE(m.device_context, d.device_context) = 'centrimag_lv' THEN 'centrimag_lv'
                WHEN COALESCE(m.device_context, d.device_context) = 'centrimag_rv' THEN 'centrimag_rv'
                WHEN COALESCE(m.device_context, d.device_context) = 'heartware' THEN 'heartware'
                -- WHEN COALESCE(m.device_context, d.device_context) = 'impella_l' THEN 'impella_lv_other_unspec'
                WHEN COALESCE(m.device_context, d.device_context) = 'impella_r' THEN 'impella_rp'
    
                -- WHEN COALESCE(m.device_context, d.device_context) = 'iabp' THEN 'iabp'
    
                END)
            -- Fallback: infer mcs_group from context (e.g., centrimag_lv -> temporary_lvad)
            , mcs_group: -- d.mcs_group
            COALESCE(d.mcs_group, CASE
                WHEN COALESCE(m.device_context, d.device_context) = 'ecmo' THEN 'ecmo'
                WHEN COALESCE(m.device_context, d.device_context) in ('hm2', 'durable_vad', 'heartware') THEN 'durable_lvad' -- there is no durable_rvad
                WHEN COALESCE(m.device_context, d.device_context) = 'rvad' THEN 'temporary_rvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'centrimag_lv' THEN 'temporary_lvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'centrimag_rv' THEN 'temporary_rvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'impella_l' THEN 'impella_lvad'
                WHEN COALESCE(m.device_context, d.device_context) = 'impella_r' THEN 'temporary_rvad'
    
                -- WHEN COALESCE(m.device_context, d.device_context) = 'iabp' THEN 'iabp'
                END)
            , d.ecmo_configuration_category
            , m.control_parameter_name
            , m.control_parameter_category
            , m.control_parameter_value
            , m.flow
            , m.sweep_set
            , m.fdo2_set
        -- WHERE hospitalization_id in ('25324698', '27261448')
        ORDER BY hospitalization_id, recorded_dttm
        """
    )
    return (clif_ecmo_mcs_raw,)


@app.cell(disabled=True)
def _(clif_ecmo_mcs_raw):
    clif_ecmo_mcs_utc = clif_ecmo_mcs_raw
    # clif_ecmo_mcs_utc["recorded_dttm"] = convert_tz_to_utc(
    #     pd.to_datetime(clif_ecmo_mcs_utc["recorded_dttm"])
    # )
    clif_ecmo_mcs_utc
    return (clif_ecmo_mcs_utc,)


@app.cell
def _(clif_ecmo_mcs_utc, mo):
    clif_ecmo_mcs = mo.sql(
        f"""
        -- Remove outliers: null out values outside physiological range
        FROM clif_ecmo_mcs_utc
        SELECT * REPLACE(
            CASE WHEN flow NOT BETWEEN 0 AND 10 THEN NULL ELSE flow END AS flow
            , CASE WHEN sweep_set NOT BETWEEN 0 AND 15 THEN NULL ELSE sweep_set END AS sweep_set
            , CASE WHEN fdo2_set NOT BETWEEN 0 AND 1 THEN NULL ELSE fdo2_set END AS fdo2_set
        )
        """
    )
    return (clif_ecmo_mcs,)


@app.cell(column=1)
def _(mo):
    mo.md(r"""
    # QA
    """)
    return


@app.cell
def _(clif_ecmo_mcs, mo):
    _df = mo.sql(
        f"""
        -- Null check across all columns
        FROM clif_ecmo_mcs
        SELECT
            n_total: COUNT(*)
            , n_null_hosp_id: COUNT(*) - COUNT(hospitalization_id)
            , n_null_dttm: COUNT(*) - COUNT(recorded_dttm)
            , n_null_device_name: COUNT(*) - COUNT(device_name)
            , n_null_device_cat: COUNT(*) - COUNT(device_category)
            , n_null_mcs_group: COUNT(*) - COUNT(mcs_group)
            , n_null_ecmo_config: COUNT(*) - COUNT(ecmo_configuration_category)
            , n_null_ctrl_param_name: COUNT(*) - COUNT(control_parameter_name)
            , n_null_ctrl_param_cat: COUNT(*) - COUNT(control_parameter_category)
            , n_null_ctrl_param_val: COUNT(*) - COUNT(control_parameter_value)
            , n_null_flow: COUNT(*) - COUNT(flow)
            , n_null_sweep: COUNT(*) - COUNT(sweep_set)
            , n_null_fdo2: COUNT(*) - COUNT(fdo2_set)
        """
    )
    return


@app.cell
def _(clif_ecmo_mcs, mo):
    _df = mo.sql(
        f"""
        -- device_name x device_category x mcs_group crosstab
        FROM clif_ecmo_mcs
        SELECT device_name, device_category, mcs_group
            , n: COUNT(*)
        GROUP BY device_name, device_category, mcs_group
        ORDER BY n DESC
        """
    )
    return


@app.cell
def _(clif_ecmo_mcs, pd):
    # Validate device_category against mCIDE permissible list
    _valid_device_cats = pd.read_csv("data/mcide/clif_ecmo_mcs_device_category.csv")["device_category"].tolist()
    _actual = set(clif_ecmo_mcs["device_category"].dropna().unique())
    _invalid = _actual - set(_valid_device_cats)
    print(f"Invalid device_category values: {_invalid}" if _invalid else "All device_category values are valid!")

    # Validate mcs_group
    _valid_mcs_groups = pd.read_csv("data/mcide/clif_ecmo_mcs_mcs_group.csv")["mcs_group"].tolist()
    _actual_mcs = set(clif_ecmo_mcs["mcs_group"].dropna().unique())
    _invalid_mcs = _actual_mcs - set(_valid_mcs_groups)
    print(f"Invalid mcs_group values: {_invalid_mcs}" if _invalid_mcs else "All mcs_group values are valid!")

    # Validate ecmo_configuration_category
    _valid_configs = pd.read_csv("data/mcide/clif_ecmo_mcs_configuration_category.csv")["ecmo_configuration_category"].tolist()
    _actual_configs = set(clif_ecmo_mcs["ecmo_configuration_category"].dropna().unique())
    _invalid_configs = _actual_configs - set(_valid_configs)
    print(f"Invalid ecmo_configuration_category values: {_invalid_configs}" if _invalid_configs else "All ecmo_configuration_category values are valid!")

    # Validate control_parameter_category
    _valid_ctrl_cats = pd.read_csv("data/mcide/clif_ecmo_mcs_control_param_category.csv")["control_parameter_category"].tolist()
    _actual_ctrl = set(clif_ecmo_mcs["control_parameter_category"].dropna().unique())
    _invalid_ctrl = _actual_ctrl - set(_valid_ctrl_cats)
    print(f"Invalid control_parameter_category values: {_invalid_ctrl}" if _invalid_ctrl else "All control_parameter_category values are valid!")
    return


@app.cell
def _(clif_ecmo_mcs, mo):
    _df = mo.sql(
        f"""
        -- Numeric distribution checks for flow, sweep, fdo2, and control params
        FROM clif_ecmo_mcs
        SELECT
            flow_min: MIN(flow)
            , flow_p25: QUANTILE_CONT(flow, 0.25)
            , flow_median: MEDIAN(flow)
            , flow_p75: QUANTILE_CONT(flow, 0.75)
            , flow_max: MAX(flow)
            , sweep_min: MIN(sweep_set)
            , sweep_median: MEDIAN(sweep_set)
            , sweep_max: MAX(sweep_set)
            , fdo2_min: MIN(fdo2_set)
            , fdo2_median: MEDIAN(fdo2_set)
            , fdo2_max: MAX(fdo2_set)
            , ctrl_min: MIN(control_parameter_value)
            , ctrl_median: MEDIAN(control_parameter_value)
            , ctrl_max: MAX(control_parameter_value)
        """
    )
    return


@app.cell
def _(clif_ecmo_mcs, mo):
    _df = mo.sql(
        f"""
        -- ecmo_configuration_category distribution (ECMO devices only)
        FROM clif_ecmo_mcs
        SELECT ecmo_configuration_category
            , n: COUNT(*)
        WHERE mcs_group = 'ecmo'
        GROUP BY ecmo_configuration_category
        ORDER BY n DESC
        """
    )
    return


@app.cell
def _(clif_ecmo_mcs, mo):
    _df = mo.sql(
        f"""
        -- control_parameter_category distribution with value stats
        FROM clif_ecmo_mcs
        SELECT control_parameter_category
            , control_parameter_name
            , n: COUNT(*)
            , avg_val: ROUND(AVG(control_parameter_value), 1)
            , median_val: ROUND(MEDIAN(control_parameter_value), 1)
        WHERE control_parameter_category IS NOT NULL
        GROUP BY control_parameter_category, control_parameter_name
        ORDER BY n DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Demo
    """)
    return


@app.cell
def _(clif_ecmo_mcs, hosp_demo, mo):
    clif_demo_ecmo_mcs = mo.sql(
        f"""
        -- Subset to demo cohort
        FROM clif_ecmo_mcs p
        INNER JOIN hosp_demo d USING (hospitalization_id)
        SELECT p.*
        """
    )
    return


@app.cell
def _():
    # save_to_rclif(df=clif_ecmo_mcs, table_name='ecmo_mcs')
    # save_to_rclif(df=clif_demo_ecmo_mcs, table_name='demo_ecmo_mcs')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
