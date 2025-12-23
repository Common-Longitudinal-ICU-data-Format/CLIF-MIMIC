import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo, pandas as pd, sys, os
    from src.utils import mimic_table_pathfinder, clif_table_pathfinder
    return clif_table_pathfinder, mimic_table_pathfinder, mo, pd


@app.cell
def _():
    import src.logging_config
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Item query
    """)
    return


@app.cell
def _(mimic_table_pathfinder, pd):
    d_labitems = pd.read_parquet(mimic_table_pathfinder('d_labitems'))
    d_labitems
    return


@app.cell
def _():
    kw = 'basophil'
    return (kw,)


@app.cell
def _(kw, mimic_table_pathfinder, mo, null):
    _df = mo.sql(
        f"""
        FROM "{mimic_table_pathfinder('labevents')}" e
        LEFT JOIN "{mimic_table_pathfinder('d_labitems')}" d
            USING (itemid)
        SELECT itemid, label
            , abbreviation: ''
            , fluid
            , category
            , n: COUNT(*)
            , value_instances: 'min: ' || ROUND(MIN(valuenum), 1) || '; mean: ' || ROUND(AVG(valuenum), 1) || '; median: ' || ROUND(MEDIAN(valuenum), 1) || '; max: ' || ROUND(MAX(valuenum), 1)
            , uom_instances: (SELECT STRING_AGG(
                    CONCAT(valueuom, ': ', valueuom_count), '; '
                    ORDER BY valueuom_count DESC)
                FROM (
                    SELECT valueuom, COUNT(*) AS valueuom_count
                    FROM "{mimic_table_pathfinder('labevents')}" AS e2
                    WHERE e2.itemid = e.itemid -- AND valueuom IS NOT NULL AND valueuom <> ''
                    GROUP BY valueuom
                ) AS valueuom_counts)
        WHERE LOWER(label) LIKE '%{kw}%'
        GROUP BY itemid, label, fluid, category
        ORDER BY n DESC
        -- LIMIT 10
        """
    )
    return


@app.cell
def _(mimic_table_pathfinder, mo, null):
    _df = mo.sql(
        f"""
        FROM "{mimic_table_pathfinder('labevents')}" e
        LEFT JOIN "{mimic_table_pathfinder('d_labitems')}" d
            USING (itemid)
        SELECT *
        WHERE itemid = 50934
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Load pipeline
    """)
    return


@app.cell
def _():
    from src.tables import labs
    return (labs,)


@app.cell
def _(labs):
    from hamilton import driver
    from hamilton.caching.stores.memory import InMemoryMetadataStore, InMemoryResultStore

    dr = (
        driver.Builder()
        .with_modules(labs)
        # .with_cache(
        #     result_store=InMemoryResultStore(),
        #     metadata_store=InMemoryMetadataStore(),
        # )
        .build()
    )

    g = dr.display_all_functions()
    g.render('dev/labs')
    return (dr,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Nodes
    """)
    return


@app.cell
def _(dr):
    labs_mapping = dr.execute(['labs_mapping'])['labs_mapping']
    labs_mapping
    return (labs_mapping,)


@app.cell
def _(dr):
    nodes = dr.execute(['le_labs_renamed_reordered'])
    return (nodes,)


@app.cell
def _(nodes):
    le_labs_renamed_reordered = nodes['le_labs_renamed_reordered']
    le_labs_renamed_reordered
    return (le_labs_renamed_reordered,)


@app.cell
def _(labs_mapping, le_labs_renamed_reordered, mo):
    _df = mo.sql(
        f"""
        FROM le_labs_renamed_reordered e
        LEFT JOIN labs_mapping m USING (itemid)
        SELECT e.hospitalization_id
            , e.lab_order_dttm, e.lab_collect_dttm, e.lab_result_dttm
            , e.lab_order_name, e.lab_order_category
            , e.lab_name, e.lab_category
            , lab_value_numeric: CASE
            	WHEN decision = 'TO MAP, CONVERT UOM' THEN lab_value_numeric * conversion_multiplier
            	ELSE lab_value_numeric END
            , lab_value: lab_value_numeric::STRING
            , reference_unit: e.target_uom
            , e.lab_specimen_name
            , e.lab_specimen_category
            , e.lab_loinc_code
            , e.itemid
            , m.decision
        	, m.conversion_multiplier
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Test
    """)
    return


@app.cell
def _(labs):
    # validate schema
    labs._test()
    return


@app.cell
def _(clif_table_pathfinder, mo, null):
    v100_lab_cats = mo.sql(
        f"""
        FROM "{clif_table_pathfinder('labs_v100')}"
        SELECT lab_category
        	, COUNT(*) as n
        GROUP BY lab_category
        ORDER BY n DESC
        """
    )
    return (v100_lab_cats,)


@app.cell
def _(clif_table_pathfinder, mo, null):
    v110_lab_cats = mo.sql(
        f"""
        FROM "{clif_table_pathfinder('labs')}"
        SELECT lab_category
        	, COUNT(*) as n
        GROUP BY lab_category
        ORDER BY n DESC
        """
    )
    return (v110_lab_cats,)


@app.cell
def _(mo, v100_lab_cats, v110_lab_cats):
    comparison = mo.sql(
        f"""
        FROM v100_lab_cats l
        FULL JOIN v110_lab_cats r USING (lab_category)
        SELECT lab_category
            , l.n as old_n
            , r.n as new_n
        	, delta: new_n - COALESCE(old_n, 0)
        ORDER BY delta DESC, old_n DESC
        """
    )
    return


@app.cell
def _(clif_table_pathfinder, mo, null):
    _df = mo.sql(
        f"""
        FROM "{clif_table_pathfinder('labs')}" l
        -- WHERE '_absolute' in lab_category OR '_percent' IN lab_category AND lab_name = 'Eosinophil Count'
        WHERE lab_category = 'eosinophils_absolute'
        LIMIT 100
        """
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
