import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium", sql_output="pandas")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import plotly.express as px

    return mo, pd, px


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Raw vs. Imputed GCS Comparison
    """)
    return


@app.cell
def _(mo):
    pa_raw = mo.sql(
        f"""
        FROM '/Users/wliao0504/code/clif/CLIF-MIMIC/output/clif-mimic-1.1.0/clif_patient_assessments_raw_gcs.parquet'
        """
    )
    return (pa_raw,)


@app.cell
def _(mo):
    pa_imputed = mo.sql(
        f"""
        FROM '/Users/wliao0504/code/clif/CLIF-MIMIC/output/clif-mimic-1.1.0/clif_patient_assessments.parquet'
        SELECT *
        WHERE assessment_category IN ('gcs_eyes', 'gcs_verbal', 'gcs_motor', 'gcs_total')
        """
    )
    return (pa_imputed,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Summary Stats
    """)
    return


@app.cell
def _(mo, pa_imputed, pa_raw):
    _df = mo.sql(
        f"""
        -- row counts per assessment_category
        WITH raw_counts AS (
            FROM pa_raw
            SELECT
                assessment_category
                , n: COUNT(*)
                , null_rate: ROUND(SUM(CASE WHEN numerical_value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
            GROUP BY assessment_category
        ),
        imputed_counts AS (
            FROM pa_imputed
            SELECT
                assessment_category
                , n: COUNT(*)
                , null_rate: ROUND(SUM(CASE WHEN numerical_value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
            GROUP BY assessment_category
        )
        FROM raw_counts r
        FULL OUTER JOIN imputed_counts i USING (assessment_category)
        SELECT
            assessment_category
            , raw_n: r.n
            , raw_null_pct: r.null_rate
            , imputed_n: i.n
            , imputed_null_pct: i.null_rate
        ORDER BY assessment_category
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Distribution Comparison
    """)
    return


@app.cell
def _(mo, pa_raw):
    dist_raw = mo.sql(
        f"""
        -- value distribution for raw GCS
        FROM pa_raw
        SELECT
            assessment_category
            , numerical_value
            , n: COUNT(*)
        WHERE numerical_value IS NOT NULL
        GROUP BY assessment_category, numerical_value
        ORDER BY assessment_category, numerical_value
        """
    )
    return (dist_raw,)


@app.cell
def _(mo, pa_imputed):
    dist_imputed = mo.sql(
        f"""
        -- value distribution for imputed GCS
        FROM pa_imputed
        SELECT
            assessment_category
            , numerical_value
            , n: COUNT(*)
        WHERE numerical_value IS NOT NULL
        GROUP BY assessment_category, numerical_value
        ORDER BY assessment_category, numerical_value
        """
    )
    return (dist_imputed,)


@app.cell
def _(dist_imputed, dist_raw, pd):
    dist_raw_labeled = dist_raw.assign(version="raw")
    dist_imputed_labeled = dist_imputed.assign(version="imputed")
    dist_combined = pd.concat([dist_raw_labeled, dist_imputed_labeled], ignore_index=True)
    return (dist_combined,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### gcs_total
    """)
    return


@app.cell
def _(dist_combined, px):
    fig_total = px.bar(
        dist_combined[dist_combined["assessment_category"] == "gcs_total"],
        x="numerical_value",
        y="n",
        color="version",
        barmode="group",
        title="gcs_total: raw vs imputed",
        labels={"numerical_value": "GCS Total Score", "n": "Count"},
    )
    fig_total.update_xaxes(dtick=1)
    fig_total
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Sub-scores
    """)
    return


@app.cell
def _(dist_combined, px):
    fig_sub = px.bar(
        dist_combined[dist_combined["assessment_category"] != "gcs_total"],
        x="numerical_value",
        y="n",
        color="version",
        barmode="group",
        facet_col="assessment_category",
        title="GCS sub-scores: raw vs imputed",
        labels={"numerical_value": "Score", "n": "Count"},
    )
    fig_sub.update_xaxes(dtick=1)
    fig_sub
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Missingness Analysis
    """)
    return


@app.cell
def _(mo, pa_imputed, pa_raw):
    mo.sql(
        f"""
        -- gcs_total missingness comparison
        WITH raw_miss AS (
            FROM pa_raw
            SELECT
                total_rows: COUNT(*)
                , null_rows: SUM(CASE WHEN numerical_value IS NULL THEN 1 ELSE 0 END)
                , null_pct: ROUND(null_rows * 100.0 / total_rows, 2)
            WHERE assessment_category = 'gcs_total'
        ),
        imputed_miss AS (
            FROM pa_imputed
            SELECT
                total_rows: COUNT(*)
                , null_rows: SUM(CASE WHEN numerical_value IS NULL THEN 1 ELSE 0 END)
                , null_pct: ROUND(null_rows * 100.0 / total_rows, 2)
            WHERE assessment_category = 'gcs_total'
        )
        SELECT
            raw_total: r.total_rows
            , raw_null: r.null_rows
            , raw_null_pct: r.null_pct
            , imputed_total: i.total_rows
            , imputed_null: i.null_rows
            , imputed_null_pct: i.null_pct
        FROM raw_miss r, imputed_miss i
        """
    )
    return


if __name__ == "__main__":
    app.run()
