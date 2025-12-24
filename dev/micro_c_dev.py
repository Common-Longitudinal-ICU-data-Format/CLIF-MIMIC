import marimo

__generated_with = "0.18.4"
app = marimo.App(width="columns")


@app.cell
def _():
    import marimo as mo
    import numpy as np
    import pandas as pd
    import re
    import logging, duckdb, os
    from rapidfuzz import fuzz, process
    from src.utils import fetch_mimic_events, load_mapping_csv, \
        get_relevant_item_ids, find_duplicates, rename_and_reorder_cols, save_to_rclif, \
        convert_and_sort_datetime, setup_logging, search_mimic_items, mimic_table_pathfinder, \
        resave_mimic_table_from_csv_to_parquet
    return (
        fuzz,
        mimic_table_pathfinder,
        mo,
        pd,
        process,
        re,
        search_mimic_items,
    )


@app.cell
def _():
    # resave_mimic_table_from_csv_to_parquet(table = 'hcpcsevents')
    return


@app.cell
def _(search_mimic_items):
    search_mimic_items('oxygen', for_labs=True)
    return


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


@app.cell
def _(pd):
    # Load CLIF organism categories
    clif_categories_path = "data/mcide/clif_microbiology_culture_organism_categories.csv"
    clif_categories = pd.read_csv(clif_categories_path)
    return (clif_categories,)


@app.cell
def _(pd, re):
    def normalize_organism_name(name: str) -> str:
        """Normalize organism name for matching: lowercase, strip quantifiers and CFU counts."""
        if pd.isna(name) or name is None:
            return ""

        name = str(name).lower().strip()

        # Remove common quantifiers/prefixes
        quantifiers = [
            r"^few\s+", r"^rare\s+", r"^many\s+", r"^moderate\s+",
            r"^isolated from broth only\s+",
            r"^\d+,?\d*\s*-\s*\d+,?\d*\s*cfu/ml\s+",  # e.g., "1,000 - 10,000 cfu/ml"
            r"^>=?\d+,?\d*\s*cfu/ml\s+",  # e.g., ">=100,000 cfu/ml"
            r"^<\d+,?\d*\s*cfu/ml\s+",  # e.g., "<1,000 cfu/ml"
            r"^\d+,?\d*\s*cfu/gram of tissue\s+",  # e.g., "60,000 cfu/gram of tissue"
        ]

        for pattern in quantifiers:
            name = re.sub(pattern, "", name)

        return name.strip()

    def match_organism_to_category(organism_name: str, categories_df) -> tuple:
        """
        Match an organism name to a CLIF category.

        Returns: (category, match_type, score)
        - match_type: 'species', 'genus', 'fuzzy', 'special', 'unmatched'
        - score: 1.0 for exact matches, fuzzy score otherwise
        """
        normalized = normalize_organism_name(organism_name)

        # Handle NULL/empty → no_growth
        if not normalized:
            return ("no_growth", "special", 1.0)

        # Build list of (category, parts, is_sp) for matching
        category_info = []
        for cat in categories_df["organism_category"].unique():
            parts = cat.split("_")
            is_sp = parts[-1] == "sp" if len(parts) > 1 else False
            category_info.append((cat, parts, is_sp))

        # First pass: find species-level matches (all parts must match)
        species_matches = []
        for cat, parts, is_sp in category_info:
            if is_sp:
                continue  # Skip genus-level for first pass

            # Check if ALL parts appear in the organism name
            all_match = all(part in normalized for part in parts)
            if all_match:
                species_matches.append(cat)

        if species_matches:
            # Prefer longest match (most specific)
            best = max(species_matches, key=len)
            return (best, "species", 1.0)

        # Second pass: genus-level matches (*_sp categories)
        genus_matches = []
        for cat, parts, is_sp in category_info:
            if not is_sp:
                continue

            # For genus_sp, just the genus needs to match
            genus = parts[0]
            if genus in normalized:
                genus_matches.append(cat)

        if genus_matches:
            best = max(genus_matches, key=len)
            return (best, "genus", 1.0)

        # No match found
        return (None, "unmatched", 0.0)
    return (match_organism_to_category,)


@app.cell
def _(
    clif_categories,
    fuzz,
    match_organism_to_category,
    organism_names,
    pd,
    process,
):
    # Build the organism_mapping table
    results = []

    for _, row in organism_names.to_pandas().iterrows():
        org_name = row["organism_name"]
        itemid = row["itemid"]
        n = row["n"]

        category, match_type, score = match_organism_to_category(org_name, clif_categories)

        # If unmatched, try fuzzy matching against organism_name_examples
        if match_type == "unmatched":
            # Get all examples
            examples = []
            for _, cat_row in clif_categories.iterrows():
                if pd.notna(cat_row["organism_name_examples"]):
                    for ex in str(cat_row["organism_name_examples"]).split(","):
                        ex = ex.strip().lower()
                        if ex:
                            examples.append((ex, cat_row["organism_category"]))

            if examples:
                normalized = org_name.lower().strip() if pd.notna(org_name) else ""
                if normalized:
                    # Find best fuzzy match
                    example_texts = [ex[0] for ex in examples]
                    match_result = process.extractOne(
                        normalized,
                        example_texts,
                        scorer=fuzz.token_set_ratio
                    )
                    if match_result and match_result[1] >= 70:  # 70% threshold
                        matched_example = match_result[0]
                        # Find the category for this example
                        for ex_text, ex_cat in examples:
                            if ex_text == matched_example:
                                category = ex_cat
                                match_type = "fuzzy"
                                score = match_result[1] / 100.0
                                break

        results.append({
            "organism_name": org_name,
            "itemid": itemid,
            "n": n,
            "organism_category": category,
            "match_type": match_type,
            "match_score": score
        })

    organism_mapping = pd.DataFrame(results)
    return (organism_mapping,)


@app.cell
def _(clif_categories, organism_mapping, pd):
    # Coverage validation report

    # Categories with matches
    matched_categories = set(organism_mapping["organism_category"].dropna().unique())
    all_categories = set(clif_categories["organism_category"].unique())

    # Categories without any matches
    unmatched_categories = all_categories - matched_categories
    unmatched_categories_df = pd.DataFrame({
        "organism_category": sorted(unmatched_categories),
        "status": "NOT_AVAILABLE"
    })

    # Organism names without matches
    unmatched_organisms = organism_mapping[organism_mapping["match_type"] == "unmatched"]

    # Summary stats
    coverage_stats = {
        "total_organism_names": len(organism_mapping),
        "matched_organism_names": len(organism_mapping[organism_mapping["match_type"] != "unmatched"]),
        "unmatched_organism_names": len(unmatched_organisms),
        "total_categories": len(all_categories),
        "matched_categories": len(matched_categories),
        "unmatched_categories": len(unmatched_categories),
        "match_type_breakdown": organism_mapping["match_type"].value_counts().to_dict()
    }

    print("=== Coverage Report ===")
    print(f"Organism names: {coverage_stats['matched_organism_names']}/{coverage_stats['total_organism_names']} matched")
    print(f"Categories: {coverage_stats['matched_categories']}/{coverage_stats['total_categories']} have matches")
    print(f"\nMatch type breakdown:")
    for mt, count in coverage_stats["match_type_breakdown"].items():
        print(f"  {mt}: {count}")
    return (unmatched_categories_df,)


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
def _(organism_mapping):
    organism_mapping['organism_category'].nunique()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
