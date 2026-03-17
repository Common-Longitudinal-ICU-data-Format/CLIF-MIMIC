import marimo

__generated_with = "0.20.2"
app = marimo.App(width="columns", sql_output="pandas")


@app.cell
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

    return (
        convert_tz_to_utc,
        fetch_mimic_events,
        load_mapping_csv,
        mimic_table_pathfinder,
        mo,
        pd,
        search_mimic_items,
    )


@app.cell
def _():
    # resave_mimic_table_from_csv_to_parquet(table = 'outputevents')
    return


@app.cell
def _(pd):
    pt_demo = pd.read_parquet('tests/clif_patient.parquet')
    hosp_demo = pd.read_parquet('tests/clif_hospitalization.parquet')
    return (hosp_demo,)


@app.cell
def _(search_mimic_items):
    search_mimic_items(kw='Ureteral Stent')
    return


@app.cell
def _(fetch_mimic_events):
    stents = fetch_mimic_events(item_ids=[226557, 226558])
    return (stents,)


@app.cell
def _(stents):
    import matplotlib.pyplot as plt

    # Facet by 'label': create a subplot for each unique label
    unique_labels = stents['label'].dropna().unique()
    n_labels = len(unique_labels)

    fig, axes = plt.subplots(n_labels, 1, figsize=(8, 4 * n_labels), sharex=True)
    # axes may be a single Axes if n_labels==1, so make it always iterable
    if n_labels == 1:
        axes = [axes]

    for ax, label in zip(axes, unique_labels):
        values = stents[stents['label'] == label]["value"].dropna()
        ax.hist(
            values,
            bins=30,
            alpha=0.7,
            histtype='stepfilled'
        )
        ax.set_title(f"Distribution of 'value' for label: {label}")
        ax.set_ylabel("Frequency")
    axes[-1].set_xlabel("Value")
    plt.tight_layout()
    plt.show()
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Urine Output Item Mapping

    Loaded from `data/mappings/mimic-to-clif-mappings - output.csv`.

    - `TO MAP`: include in CLIF output table
    - `SPECIAL`: item 227488 (GU Irrigant Volume In) — input fluid, needed for net UO exploration
    """)
    return


@app.cell
def _(load_mapping_csv):
    output_mapping = load_mapping_csv("output")
    output_mapping
    return (output_mapping,)


@app.cell
def _(output_mapping):
    mapping_to_map = output_mapping[output_mapping["decision"] == "TO MAP"]
    mapped_item_ids = mapping_to_map["itemid"].tolist()
    all_item_ids = output_mapping["itemid"].tolist()
    return all_item_ids, mapped_item_ids, mapping_to_map


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Load Raw Output Events
    """)
    return


@app.cell
def _(all_item_ids, mimic_table_pathfinder, mo):
    _item_ids_str = ','.join(map(str, all_item_ids))
    raw_output = mo.sql(
        f"""
        -- Load all urine output events (including GU irrigant) from MIMIC outputevents
        FROM '{mimic_table_pathfinder("outputevents")}' oe
        LEFT JOIN '{mimic_table_pathfinder("d_items")}' d USING (itemid)
        SELECT oe.hadm_id
            , oe.stay_id
            , oe.charttime
            , oe.itemid
            , d.label
            , oe.value
            , oe.valueuom
        WHERE oe.itemid IN ({_item_ids_str})
        """
    )
    return (raw_output,)


@app.cell
def _(mo, raw_output):
    _df = mo.sql(
        f"""
        -- Summary stats by item
        FROM raw_output
        SELECT itemid
            , label
            , n: COUNT(*)
            , n_null_value: COUNT(*) - COUNT(value)
            , n_negative: COUNT(CASE WHEN value < 0 THEN 1 END)
            , n_zero: COUNT(CASE WHEN value = 0 THEN 1 END)
            , min_val: ROUND(MIN(value), 2)
            , median_val: ROUND(MEDIAN(value), 2)
            , max_val: ROUND(MAX(value), 2)
        GROUP BY itemid, label
        ORDER BY n DESC
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Transform to CLIF Output Schema

    Target columns: `hospitalization_id`, `recorded_dttm`, `output_name`, `output_category`, `output_group`, `output_volume`

    This transform uses only `TO MAP` items (excludes the SPECIAL GU irrigant input).
    Negative and zero volumes are filtered out per CLIF schema requirement.
    """)
    return


@app.cell
def _(mapped_item_ids, mapping_to_map, mimic_table_pathfinder, mo):
    _item_ids_str = ','.join(map(str, mapped_item_ids))
    clif_output_raw = mo.sql(
        f"""
        -- Transform MIMIC outputevents to CLIF output schema (urine scope)
        FROM '{mimic_table_pathfinder("outputevents")}' oe
        INNER JOIN mapping_to_map m ON oe.itemid = m.itemid
        SELECT
            hospitalization_id: CAST(oe.hadm_id AS VARCHAR)
            , recorded_dttm: CAST(oe.charttime AS TIMESTAMP)
            , output_name: m.label
            , output_category: m.output_category
            , output_group: m.output_group
            , output_volume: CAST(oe.value AS FLOAT)
        WHERE oe.itemid IN ({_item_ids_str})
            AND oe.hadm_id IS NOT NULL
            AND oe.value IS NOT NULL
            AND oe.value > 0
        """
    )
    return (clif_output_raw,)


@app.cell
def _(clif_output_raw, convert_tz_to_utc, pd):
    clif_output_utc = clif_output_raw
    clif_output_utc["recorded_dttm"] = convert_tz_to_utc(
        pd.to_datetime(clif_output_utc["recorded_dttm"])
    )
    clif_output_utc
    return (clif_output_utc,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Net Urine Output Exploration

    The MIMIC reference query computes net urine output by:

    1. Negating item 227488 (GU Irrigant Volume In) values
    2. Summing all values at the same `stay_id` + `charttime`

    This gives: `net UO = irrigant/urine volume out - irrigant volume in`

    **Question**: Is this feasible for the CLIF output table, given that:

    - CLIF keeps individual rows (not aggregated)

    - CLIF requires `output_volume > 0`

    Let's explore how often irrigant items co-occur and what the net values look like.
    """)
    return


@app.cell
def _(mo, raw_output):
    gu_events = mo.sql(
        f"""
        -- Isolate GU irrigant-related events (items 227488 and 227489)
        FROM raw_output
        SELECT hadm_id
            , charttime
            , itemid
            , label
            , value
        WHERE itemid IN (227488, 227489)
        ORDER BY hadm_id, charttime
        """
    )
    return (gu_events,)


@app.cell
def _(gu_events, mo):
    gu_co_occurrence = mo.sql(
        f"""
        -- How often do irrigant in (227488) and out (227489) co-occur at the same hadm_id + charttime?
        FROM gu_events
        SELECT hadm_id
            , charttime
            , has_in: MAX(CASE WHEN itemid = 227488 THEN 1 ELSE 0 END)
            , has_out: MAX(CASE WHEN itemid = 227489 THEN 1 ELSE 0 END)
            , vol_in: SUM(CASE WHEN itemid = 227488 THEN value ELSE 0 END)
            , vol_out: SUM(CASE WHEN itemid = 227489 THEN value ELSE 0 END)
            , net_uo: SUM(CASE WHEN itemid = 227488 THEN -value ELSE value END)
        GROUP BY hadm_id, charttime
        ORDER BY hadm_id, charttime
        """
    )
    return (gu_co_occurrence,)


@app.cell
def _(gu_co_occurrence, mo):
    _df = mo.sql(
        f"""
        -- Summary of co-occurrence patterns
        FROM gu_co_occurrence
        SELECT
            cooccur_pattern: CASE
                WHEN has_in = 1 AND has_out = 1 THEN 'both_in_and_out'
                WHEN has_in = 1 AND has_out = 0 THEN 'in_only'
                WHEN has_in = 0 AND has_out = 1 THEN 'out_only'
            END
            , n_rows: COUNT(*)
            , n_netuo_is_negative: COUNT(CASE WHEN net_uo < 0 THEN 1 END)
            , n_netuo_is_zero: COUNT(CASE WHEN net_uo = 0 THEN 1 END)
            , avg_net_uo: ROUND(AVG(net_uo), 1)
            , median_net_uo: ROUND(MEDIAN(net_uo), 1)
        GROUP BY cooccur_pattern
        ORDER BY n_rows DESC
        """
    )
    return


@app.cell
def _(gu_co_occurrence, mo):
    _df = mo.sql(
        f"""
        -- Distribution of net UO values (when both in and out present)
        FROM gu_co_occurrence
        SELECT
            min_net: MIN(net_uo)
            , p5_net: QUANTILE_CONT(net_uo, 0.05)
            , p25_net: QUANTILE_CONT(net_uo, 0.25)
            , median_net: MEDIAN(net_uo)
            , p75_net: QUANTILE_CONT(net_uo, 0.75)
            , p95_net: QUANTILE_CONT(net_uo, 0.95)
            , max_net: MAX(net_uo)
        WHERE has_in = 1 AND has_out = 1
        """
    )
    return


@app.cell
def _(gu_co_occurrence, mo):
    gu_time_gaps = mo.sql(
        f"""
        -- For each unmatched out-event, find the most recent preceding in-event
        -- Orphans (no prior input in that hadm_id) have time_in = NULL
        WITH
        out_only AS (
            FROM gu_co_occurrence
            SELECT hadm_id, charttime AS time_out
            WHERE has_out = 1 AND has_in = 0
        ),
        has_in_times AS (
            FROM gu_co_occurrence
            SELECT hadm_id, charttime AS time_in
            WHERE has_in = 1
        )
        FROM out_only o
        ASOF JOIN has_in_times i ON o.hadm_id = i.hadm_id AND o.time_out >= i.time_in
        SELECT o.hadm_id, i.time_in, o.time_out
            , gap_hrs: ROUND(EXTRACT(EPOCH FROM (o.time_out - i.time_in)) / 3600, 2)
        ORDER BY o.hadm_id, o.time_out
        """
    )
    return (gu_time_gaps,)


@app.cell
def _(gu_time_gaps, mo):
    _df = mo.sql(
        f"""
        -- Summary stats of time gaps for unmatched GU events
        FROM gu_time_gaps
        SELECT
            n_pairs: COUNT(*)
            , n_orphan: COUNT(CASE WHEN gap_hrs IS NULL THEN 1 END)
            , n_within_1hr: COUNT(CASE WHEN gap_hrs <= 1 THEN 1 END)
            , n_within_2hr: COUNT(CASE WHEN gap_hrs <= 2 THEN 1 END)
            , p25_gap_hrs: ROUND(QUANTILE_CONT(gap_hrs, 0.25) FILTER (WHERE gap_hrs IS NOT NULL), 2)
            , median_gap_hrs: ROUND(MEDIAN(gap_hrs) FILTER (WHERE gap_hrs IS NOT NULL), 2)
            , p75_gap_hrs: ROUND(QUANTILE_CONT(gap_hrs, 0.75) FILTER (WHERE gap_hrs IS NOT NULL), 2)
            , p95_gap_hrs: ROUND(QUANTILE_CONT(gap_hrs, 0.95) FILTER (WHERE gap_hrs IS NOT NULL), 2)
            , max_gap_hrs: ROUND(MAX(gap_hrs), 2)
        """
    )
    return


@app.cell
def _(gu_time_gaps, mo):
    import altair as alt
    _gaps_non_null = gu_time_gaps.dropna(subset=["gap_hrs"])
    chart = mo.ui.altair_chart(
        alt.Chart(_gaps_non_null).mark_bar().encode(
            alt.X("gap_hrs:Q", bin=alt.Bin(maxbins=50), title="Gap (hours)"),
            alt.Y("count()", title="Count"),
        ).properties(title="Distribution of Time Gaps for Unmatched GU Irrigant In/Out Events")
    )
    chart
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Net UO Exploration Notes

    *After inspecting the results above, record observations here:*

    - How many charttimes have both irrigant in and out?

    - What fraction of net UO values are negative (output_volume constraint violation)?

    - Is the netting approach viable for CLIF, or should we keep raw volumes only?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Quality Assurance
    """)
    return


@app.cell
def _(clif_output_utc, mo):
    _df = mo.sql(
        f"""
        -- Null check across all columns
        FROM clif_output_utc
        SELECT
            n_total: COUNT(*)
            , n_null_hosp_id: COUNT(*) - COUNT(hospitalization_id)
            , n_null_dttm: COUNT(*) - COUNT(recorded_dttm)
            , n_null_name: COUNT(*) - COUNT(output_name)
            , n_null_category: COUNT(*) - COUNT(output_category)
            , n_null_group: COUNT(*) - COUNT(output_group)
            , n_null_volume: COUNT(*) - COUNT(output_volume)
        """
    )
    return


@app.cell
def _(clif_output_utc, mo):
    _df = mo.sql(
        f"""
        -- Category and name distribution
        FROM clif_output_utc
        SELECT output_category
            , output_name
            , n: COUNT(*)
            , avg_volume: ROUND(AVG(output_volume), 1)
            , median_volume: ROUND(MEDIAN(output_volume), 1)
        GROUP BY output_category, output_name
        ORDER BY n DESC
        """
    )
    return


@app.cell
def _(clif_output_utc, mo):
    _df = mo.sql(
        f"""
        -- Volume range check (should all be positive)
        FROM clif_output_utc
        SELECT
            min_vol: MIN(output_volume)
            , p25_vol: QUANTILE_CONT(output_volume, 0.25)
            , median_vol: MEDIAN(output_volume)
            , p75_vol: QUANTILE_CONT(output_volume, 0.75)
            , p95_vol: QUANTILE_CONT(output_volume, 0.95)
            , max_vol: MAX(output_volume)
            , n_negative: COUNT(CASE WHEN output_volume <= 0 THEN 1 END)
        """
    )
    return


@app.cell
def _(clif_output_utc):
    # Validate against permissible CLIF output categories (urine group)
    VALID_URINE_CATEGORIES = [
        "urethral", "external_urinary_catheter", "indwelling_urinary_catheter",
        "suprapubic_cathether", "nephrostomy", "urostomy", "urinary_other",
    ]
    invalid_cats = set(clif_output_utc["output_category"].unique()) - set(VALID_URINE_CATEGORIES)
    print(f"Invalid categories found: {invalid_cats}" if invalid_cats else "All output_category values are valid!")

    invalid_groups = set(clif_output_utc["output_group"].unique()) - {"urine"}
    print(f"Invalid groups found: {invalid_groups}" if invalid_groups else "All output_group values are valid!")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Demo Data Subset
    """)
    return


@app.cell
def _(clif_output_utc, hosp_demo, mo):
    clif_demo_output = mo.sql(
        f"""
        FROM clif_output_utc p
        INNER JOIN hosp_demo d USING (hospitalization_id)
        SELECT p.*
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Save
    """)
    return


@app.cell
def _():
    # save_to_rclif(df=clif_output_utc, table_name='output')
    # save_to_rclif(df=clif_demo_output, table_name='demo_output')
    return


@app.cell
def _():
    return


@app.cell
def _(mimic_table_pathfinder, pd):
    ing_df = pd.read_parquet(mimic_table_pathfinder('ingredientevents'))
    d_items = pd.read_parquet(mimic_table_pathfinder('d_items'))
    return d_items, ing_df


@app.cell
def _(d_items, ing_df, mo):
    _df = mo.sql(
        f"""
        FROM ing_df
        LEFT JOIN d_items USING (itemid)
        """
    )
    return


@app.cell
def _(d_items, ing_df, mo):
    _df = mo.sql(
        f"""
        FROM ing_df
        LEFT JOIN d_items USING (itemid)
        SELECT label, category, COUNT(*) as n
        GROUP BY label, category
        ORDER BY n DESC
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
