import marimo

__generated_with = "0.19.2"
app = marimo.App(width="columns", sql_output="pandas")


@app.cell
def _():
    import marimo as mo
    import numpy as np
    import pandas as pd
    import re
    import logging, duckdb, os
    from Levenshtein import distance as levenshtein_distance, jaro_winkler
    from src.utils import fetch_mimic_events, load_mapping_csv, \
        get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
        convert_and_sort_datetime, setup_logging, search_mimic_items, mimic_table_pathfinder, \
        resave_mimic_table_from_csv_to_parquet, convert_tz_to_utc
    return (
        convert_tz_to_utc,
        jaro_winkler,
        levenshtein_distance,
        mimic_table_pathfinder,
        mo,
        pd,
        re,
        save_to_rclif,
    )


@app.cell
def _(mimic_table_pathfinder):
    mimic_mc_path = mimic_table_pathfinder("microbiologyevents")
    return (mimic_mc_path,)


@app.cell
def _(mimic_mc_path, mo, null):
    mimic_mc = mo.sql(
        f"""
        SELECT *
        FROM '{mimic_mc_path}'
        LIMIT 10
        """
    )
    return


@app.cell
def _(mimic_mc_path, mo, null):
    organism_names = mo.sql(
        f"""
        FROM '{mimic_mc_path}'
        SELECT org_name as organism_name, org_itemid as itemid, COUNT(*) as n
        GROUP BY org_name, org_itemid
        ORDER BY n DESC
        """
    )
    return (organism_names,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Mapping pipeline
    """)
    return


@app.cell
def _(pd):
    # Load CLIF organism categories
    clif_categories_path = "data/mcide/clif_microbiology_culture_organism_categories.csv"
    clif_categories = pd.read_csv(clif_categories_path)
    return (clif_categories,)


@app.cell
def _(jaro_winkler, levenshtein_distance, pd, re):
    def split_to_subcomponents(s: str) -> set:
        """Split string by space/punctuation, lowercase, filter empty strings."""
        if pd.isna(s) or s is None:
            return set()
        # Split by space, underscore, comma, hyphen, slash, parentheses
        parts = re.split(r'[\s_,\-/()]+', str(s).lower().strip())
        # Filter out empty strings and common noise words
        noise_words = {'', 'formerly', 'not'}
        # Normalize 'species' and 'spp' to 'sp' for exact matching
        normalized = []
        for p in parts:
            if p in noise_words or not p:
                continue
            if p in ('species', 'spp'):
                normalized.append('sp')
            else:
                normalized.append(p)
        return set(normalized)

    def fuzzy_match(s1: str, s2: str, algo: str = "jaro_winkler") -> bool:
        """Check if two strings are a fuzzy match (tolerates end typos, not beginning)."""
        if algo == "jaro_winkler":
            # Jaro-Winkler: 1.0 = exact match, higher weight on prefix matching
            # Threshold 0.9 allows ~2 char difference at end for typical organism names
            return jaro_winkler(s1, s2) >= 0.9
        if algo == "levenshtein":
            return levenshtein_distance(s1, s2) <= 2
        raise ValueError(f"Unknown algo: {algo}")

    def find_fuzzy_bijection(source_set: set, target_set: set, algo: str = "jaro_winkler") -> bool:
        """Check if each source subcomponent can fuzzy-match to a unique target subcomponent."""
        if len(source_set) != len(target_set):
            return False
        # Try to find a bijection using greedy matching
        _remaining_targets = set(target_set)
        for _src in source_set:
            _matched = False
            for _tgt in list(_remaining_targets):
                if fuzzy_match(_src, _tgt, algo):
                    _remaining_targets.remove(_tgt)
                    _matched = True
                    break
            if not _matched:
                return False
        return True

    def find_fuzzy_subset(source_set: set, target_set: set, algo: str = "jaro_winkler") -> bool:
        """Check if each target subcomponent can fuzzy-match to some source subcomponent."""
        for _tgt in target_set:
            if not any(fuzzy_match(_src, _tgt, algo) for _src in source_set):
                return False
        return True

    def genus_fuzzy_matches_source(genus: str, source_set: set, algo: str = "jaro_winkler") -> bool:
        """Check if genus fuzzy matches at least one source subcomponent."""
        return any(fuzzy_match(genus, _src, algo) for _src in source_set)
    return (
        find_fuzzy_bijection,
        find_fuzzy_subset,
        genus_fuzzy_matches_source,
        split_to_subcomponents,
    )


@app.cell
def _(clif_categories, split_to_subcomponents):
    # Pre-compute target subcomponents for all organism_categories
    all_clif_categories = list(clif_categories["organism_category"].unique())
    category_subcomponents = {_cat: split_to_subcomponents(_cat) for _cat in all_clif_categories}
    # Pre-compute genus (first element of category split by '_')
    category_genus = {_cat: _cat.split('_')[0] for _cat in all_clif_categories}

    print(f"Loaded {len(all_clif_categories)} organism categories")
    return all_clif_categories, category_genus, category_subcomponents


@app.cell
def _(
    all_clif_categories,
    category_genus,
    category_subcomponents,
    find_fuzzy_bijection,
    find_fuzzy_subset,
    genus_fuzzy_matches_source,
    pd,
    split_to_subcomponents,
):
    def match_organism_to_category(organism_name: str) -> tuple:
        """
        Match an organism name to a CLIF category.

        Returns: (category, match_type)
        - match_type: 'exact_bijection', 'fuzzy_bijection', 'exact_subset', 'fuzzy_subset', 'unmatched'
        """
        # Handle NULL/empty → no_growth
        if pd.isna(organism_name) or organism_name is None or str(organism_name).strip() == "":
            return ("no_growth", "exact_bijection")

        _source_set = split_to_subcomponents(organism_name)

        if not _source_set:
            return ("no_growth", "exact_bijection")

        _best_match = None
        _best_type = None
        _best_len = 0  # Prefer longer (more specific) matches

        for _cat in all_clif_categories:
            _target_set = category_subcomponents[_cat]
            _genus = category_genus[_cat]

            if not _target_set:
                continue

            # Genus must fuzzy match at least one source subcomponent
            if not genus_fuzzy_matches_source(_genus, _source_set):
                continue

            # PRIORITY 1: Exact bijection
            if _source_set == _target_set:
                if len(_target_set) > _best_len or _best_type != "exact_bijection":
                    _best_match = _cat
                    _best_type = "exact_bijection"
                    _best_len = len(_target_set)
                continue

            # PRIORITY 2: Fuzzy bijection (same size, each can fuzzy match)
            if _best_type not in ("exact_bijection",) and find_fuzzy_bijection(_source_set, _target_set):
                if len(_target_set) > _best_len or _best_type not in ("exact_bijection", "fuzzy_bijection"):
                    _best_match = _cat
                    _best_type = "fuzzy_bijection"
                    _best_len = len(_target_set)
                continue

            # PRIORITY 3: Exact subset (target is subset of source)
            if _best_type not in ("exact_bijection", "fuzzy_bijection") and _target_set.issubset(_source_set):
                if len(_target_set) > _best_len or _best_type not in ("exact_bijection", "fuzzy_bijection", "exact_subset"):
                    _best_match = _cat
                    _best_type = "exact_subset"
                    _best_len = len(_target_set)
                continue

            # PRIORITY 4: Fuzzy subset (each target can fuzzy match to some source)
            if _best_type not in ("exact_bijection", "fuzzy_bijection", "exact_subset") and find_fuzzy_subset(_source_set, _target_set):
                if len(_target_set) > _best_len:
                    _best_match = _cat
                    _best_type = "fuzzy_subset"
                    _best_len = len(_target_set)

        if _best_match:
            return (_best_match, _best_type)

        return (None, "unmatched")
    return (match_organism_to_category,)


@app.cell
def _(all_clif_categories, match_organism_to_category, organism_names, pd):
    # Build the organism_mapping table with FULL JOIN logic
    _results = []
    _matched_sources = set()
    _matched_targets = set()

    for _, _row in organism_names.to_pandas().iterrows():
        _org_name = _row["organism_name"]
        _itemid = _row["itemid"]
        _n = _row["n"]

        _category, _match_type = match_organism_to_category(_org_name)

        if _category is not None:
            _matched_sources.add(_org_name)
            _matched_targets.add(_category)

        _results.append({
            "organism_name": _org_name,
            "itemid": _itemid,
            "n": _n,
            "organism_category": _category if _category else "NO_MAPPING",
            "match_type": _match_type,
        })

    # Add NOT_AVAILABLE rows for target categories with no source match
    _unmatched_targets = set(all_clif_categories) - _matched_targets
    for _cat in _unmatched_targets:
        _results.append({
            "organism_name": "NOT_AVAILABLE",
            "itemid": None,
            "n": 0,
            "organism_category": _cat,
            "match_type": "unmatched",
        })

    organism_mapping = pd.DataFrame(_results)

    # Add genus column (first element of organism_category split by '_')
    organism_mapping["genus"] = organism_mapping["organism_category"].apply(
        lambda x: x.split('_')[0] if pd.notna(x) and x not in ("NO_MAPPING",) else None
    )

    # Add n_src_organism_name: count of unique organism_names per category
    _category_counts = organism_mapping[organism_mapping["organism_name"] != "NOT_AVAILABLE"].groupby("organism_category")["organism_name"].nunique().reset_index()
    _category_counts.columns = ["organism_category", "n_src_organism_name"]
    organism_mapping = organism_mapping.merge(_category_counts, on="organism_category", how="left")
    organism_mapping["n_src_organism_name"] = organism_mapping["n_src_organism_name"].fillna(0).astype(int)

    # Import validated column from fixture CSV (preserves validation status across iterations)
    _validated_path = "tests/fixtures/mimic-to-clif-mappings - microbiology_culture.csv"
    _validated_df = pd.read_csv(_validated_path, usecols=["itemid", "organism_category", "validated"])
    # Join on both organism_name and organism_category (validation is for a specific mapping pair)
    organism_mapping = organism_mapping.merge(_validated_df, on=["itemid", "organism_category"], how="left")

    # Sort: genus with most n first, then organism_category within genus
    _category_total_n = organism_mapping.groupby("organism_category")["n"].sum().reset_index()
    _category_total_n.columns = ["organism_category", "_total_n"]
    organism_mapping = organism_mapping.merge(_category_total_n, on="organism_category", how="left")

    _genus_total_n = organism_mapping.groupby("genus")["n"].sum().reset_index()
    _genus_total_n.columns = ["genus", "_genus_total_n"]
    organism_mapping = organism_mapping.merge(_genus_total_n, on="genus", how="left")

    organism_mapping = organism_mapping.sort_values(
        by=["_genus_total_n", "genus", "organism_category", "match_type", "n"],
        ascending=[False, True, True, True, False]
    ).drop(columns=["_total_n", "_genus_total_n"]).reset_index(drop=True)
    return (organism_mapping,)


@app.cell
def _(clif_categories, organism_mapping, pd):
    # Coverage validation report

    # Categories with matches
    _matched_categories = set(organism_mapping["organism_category"].dropna().unique())
    _all_categories = set(clif_categories["organism_category"].unique())

    # Categories without any matches
    _unmatched_categories = _all_categories - _matched_categories
    unmatched_categories_df = pd.DataFrame({
        "organism_category": sorted(_unmatched_categories),
        "status": "NOT_AVAILABLE"
    })

    # Organism names without matches
    _unmatched_organisms = organism_mapping[organism_mapping["match_type"] == "unmatched"]

    # Summary stats
    _coverage_stats = {
        "total_organism_names": len(organism_mapping),
        "matched_organism_names": len(organism_mapping[organism_mapping["match_type"] != "unmatched"]),
        "unmatched_organism_names": len(_unmatched_organisms),
        "total_categories": len(_all_categories),
        "matched_categories": len(_matched_categories),
        "unmatched_categories": len(_unmatched_categories),
        "match_type_breakdown": organism_mapping["match_type"].value_counts().to_dict()
    }

    print("=== Coverage Report ===")
    print(f"Organism names: {_coverage_stats['matched_organism_names']}/{_coverage_stats['total_organism_names']} matched")
    print(f"Categories: {_coverage_stats['matched_categories']}/{_coverage_stats['total_categories']} have matches")
    print(f"\nMatch type breakdown:")
    for _mt, _count in _coverage_stats["match_type_breakdown"].items():
        print(f"  {_mt}: {_count}")
    return (unmatched_categories_df,)


@app.cell
def _(all_clif_categories, organism_mapping):
    # Validation: Check that ALL 542 categories appear in organism_mapping
    _n_categories_expected = len(all_clif_categories)
    _n_categories_in_mapping = organism_mapping["organism_category"].nunique()

    # Exclude NO_MAPPING from the count since it's a placeholder
    _real_categories = organism_mapping[organism_mapping["organism_category"] != "NO_MAPPING"]["organism_category"].nunique()

    print(f"=== Validation ===")
    print(f"Expected categories: {_n_categories_expected}")
    print(f"Unique categories in mapping (excl NO_MAPPING): {_real_categories}")
    print(f"Total unique values in organism_category column: {_n_categories_in_mapping}")

    # This assertion validates that ALL CLIF categories are in the mapping
    assert _real_categories == _n_categories_expected, \
        f"Expected {_n_categories_expected} categories in mapping, got {_real_categories}"

    print("✓ All categories accounted for!")
    return


@app.cell
def _(organism_mapping, unmatched_categories_df):
    # Display results for review
    print("Sample of organism_mapping:")
    print(organism_mapping.head(20).to_string())
    print(f"\n\nUnmatched categories ({len(unmatched_categories_df)}):")
    print(unmatched_categories_df.to_string())
    return


@app.cell
def _(organism_mapping):
    # Export mapping to CSV (uncomment to save)
    organism_mapping.to_csv(
        "data/mappings/mimic-to-clif-mappings - microbiology_culture.csv",
        index=False
    ) 
    return


@app.cell
def _(organism_mapping):
    organism_mapping
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # `fluid_category`
    """)
    return


@app.cell
def _(mimic_mc_path, mo, null):
    _df = mo.sql(
        f"""
        FROM '{mimic_mc_path}'
        SELECT spec_itemid
            , spec_type_desc as fluid_name
            , test_name
            , COUNT(*) as n
        GROUP BY spec_itemid, spec_type_desc, test_name
        ORDER BY n DESC
        """
    )
    return


@app.cell
def _(mimic_mc_path, mo, null):
    _df = mo.sql(
        f"""
        FROM '{mimic_mc_path}'
        SELECT *
        WHERE spec_type_desc = 'SWAB'
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Pipeline
    """)
    return


@app.cell
def _():
    fluid_mapping_path = 'data/mappings/mimic-to-clif-mappings - fluid_category.csv'
    return (fluid_mapping_path,)


@app.cell
def _(fluid_mapping_path, mimic_mc_path, mo, null):
    mc_df = mo.sql(
        f"""
        FROM '{mimic_mc_path}' me
        LEFT JOIN '{fluid_mapping_path}' fluid_mapping
            ON TRIM(me.spec_type_desc) = TRIM(fluid_mapping.fluid_name)
        SELECT patient_id: me.subject_id::STRING
            , hospitalization_id: me.hadm_id::STRING
            , organism_id: me.microevent_id::STRING
            , order_dttm: COALESCE(me.charttime, me.chartdate)
            , collect_dttm: COALESCE(me.charttime, me.chartdate)
            , result_dttm: COALESCE(me.storetime, me.storedate)
            , fluid_name: me.spec_type_desc
            , fluid_category: fluid_mapping.fluid_category
            , method_name: me.test_name
            -- , method_category: method_mapping.method_category
            , organism_name: me.org_name
            --, organism_category: organism_mapping.organism_category; NULL -> 'no_growth'
            --, organism_group: organism_mapping.organism_group
            --, lab_loinc_code: NULL
        WHERE hospitalization_id IS NOT NULL
        """
    )
    return (mc_df,)


@app.cell
def _(convert_tz_to_utc, mc_df):
    mc_df['order_dttm'] = convert_tz_to_utc(mc_df['order_dttm'])
    mc_df['collect_dttm'] = convert_tz_to_utc(mc_df['collect_dttm'])
    mc_df['result_dttm'] = convert_tz_to_utc(mc_df['result_dttm'])
    return


@app.cell
def _(mc_df):
    mc_df
    return


@app.cell
def _(mc_df, save_to_rclif):
    save_to_rclif(df=mc_df, table_name='microbiology_culture')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
