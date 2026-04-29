"""
Category & Manager Level Case Rate Report Generator

Automates the process of merging case management data with resolution data,
computing enriched categories via cross-referencing, and generating
category-level and manager-level aggregation reports with rate calculations.

Saves ~1.5 hours/week of manual Excel pivot table work.

Usage:
    python andon_rate.py [--data-dir PATH] [--output PATH]

Defaults:
    --data-dir  ./data
    --output    <data-dir>/Category_Manager_Level_Rate.xlsx
"""

import argparse
import glob
import os
import sys

import pandas as pd


def find_latest_file(directory, prefix):
    """Find the most recent file matching the given prefix."""
    pattern = os.path.join(directory, f"{prefix}*.csv")
    files = glob.glob(pattern)
    if not files:
        sys.exit(f"ERROR: No files matching '{prefix}*.csv' found in {directory}")
    latest = max(files, key=os.path.getmtime)
    print(f"  Using: {os.path.basename(latest)}")
    return latest


def load_data(data_dir):
    """Load the most recent case and resolution CSV files."""
    case_file = find_latest_file(data_dir, "case_data_")
    resolve_file = find_latest_file(data_dir, "resolution_data_")

    case_df = pd.read_csv(case_file, dtype=str)
    resolve_df = pd.read_csv(resolve_file, dtype=str)

    case_df.columns = case_df.columns.str.strip()
    resolve_df.columns = resolve_df.columns.str.strip()
    case_df["Case ID"] = case_df["Case ID"].str.strip()
    case_df["Requester"] = case_df["Requester"].str.strip()
    resolve_df["Case ID"] = resolve_df["Case ID"].str.strip()
    resolve_df["Associate"] = resolve_df["Associate"].str.strip()

    return case_df, resolve_df


def add_cross_check_and_category(case_df, resolve_df):
    """
    Enrich case data with category from resolution data.
    Merge on (Case ID, Associate) — keeps first match per pair.
    Falls back to original skill field when no match found.
    """
    resolve_dedup = resolve_df.drop_duplicates(subset=["Case ID", "Associate"], keep="first")

    case_df = case_df.merge(
        resolve_dedup[["Case ID", "Associate", "Category"]],
        left_on=["Case ID", "Requester"],
        right_on=["Case ID", "Associate"],
        how="left",
        suffixes=("", "_resolve"),
    )

    case_df.rename(columns={"Category_resolve": "Cross Check"}, inplace=True)
    case_df.drop(columns=["Associate"], inplace=True, errors="ignore")

    case_df["Enriched Category"] = case_df["Cross Check"].fillna(case_df["Requester Skill"])

    return case_df


def build_category_agg(case_df, resolve_df):
    """
    Category-level aggregation:
    - Resolution count by category
    - Case count by enriched category
    - Invalid case count by enriched category
    - Computes case rate and invalid rate
    """
    resolve_counts = resolve_df.groupby("Category").size().rename("Resolve")
    case_counts = case_df.groupby("Enriched Category").size().rename("Case Count")
    invalid_counts = (
        case_df[case_df["Gap"].str.strip() == "Invalid Andon"]
        .groupby("Enriched Category")
        .size()
        .rename("Invalid Case Count")
    )

    all_categories = sorted(set(resolve_counts.index) | set(case_counts.index))

    cat_agg = pd.DataFrame(index=all_categories)
    cat_agg.index.name = "Category"
    cat_agg["Resolve"] = resolve_counts.reindex(cat_agg.index, fill_value=0)
    cat_agg["Case Count"] = case_counts.reindex(cat_agg.index, fill_value=0)
    cat_agg["Invalid Case Count"] = invalid_counts.reindex(cat_agg.index, fill_value=0)
    cat_agg["Case Rate"] = (cat_agg["Case Count"] / cat_agg["Resolve"]).fillna(0).round(4)
    cat_agg["Invalid Rate"] = (cat_agg["Invalid Case Count"] / cat_agg["Case Count"]).fillna(0).round(4)

    return cat_agg.reset_index()


def build_manager_level(case_df, resolve_df):
    """
    Manager-level aggregation:
    - Resolution count by supervisor
    - Case count by requester supervisor
    - Invalid case count by requester supervisor
    - Computes case rate and invalid rate per manager
    """
    resolve_mgr = resolve_df.groupby("Supervisor").size().rename("Resolve Count")
    case_mgr = case_df.groupby("Requester Supervisor").size().rename("Case Count")
    invalid_mgr = (
        case_df[case_df["Gap"].str.strip() == "Invalid Andon"]
        .groupby("Requester Supervisor")
        .size()
        .rename("Invalid Case Count")
    )

    all_managers = sorted(set(resolve_mgr.index) | set(case_mgr.index))

    mgr_df = pd.DataFrame(index=all_managers)
    mgr_df.index.name = "Manager"
    mgr_df["Resolve Count"] = resolve_mgr.reindex(mgr_df.index, fill_value=0)
    mgr_df["Case Count"] = case_mgr.reindex(mgr_df.index, fill_value=0)
    mgr_df["Invalid Case Count"] = invalid_mgr.reindex(mgr_df.index, fill_value=0)
    mgr_df["Case Rate"] = (mgr_df["Case Count"] / mgr_df["Resolve Count"]).fillna(0).round(4)
    mgr_df["Invalid Rate"] = (mgr_df["Invalid Case Count"] / mgr_df["Case Count"]).fillna(0).round(4)

    return mgr_df.reset_index()


def write_output(case_df, cat_agg, manager_level, output_path):
    """Write all sheets to the output Excel file."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        case_df.to_excel(writer, sheet_name="Source_Data", index=False)
        cat_agg.to_excel(writer, sheet_name="Category_Agg", index=False)
        manager_level.to_excel(writer, sheet_name="Manager_Level_Rate", index=False)

    print(f"\nOutput saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Category & Manager Level Case Rate Report")
    parser.add_argument("--data-dir", default=os.path.join(".", "data"),
                        help="Directory containing CSV files")
    parser.add_argument("--output", default=None, help="Output Excel file path")
    args = parser.parse_args()

    if args.output is None:
        args.output = os.path.join(args.data_dir, "Category_Manager_Level_Rate.xlsx")

    print("=== Category & Manager Level Rate Report ===\n")

    case_df, resolve_df = load_data(args.data_dir)
    print(f"  Case rows: {len(case_df)}, Resolve rows: {len(resolve_df)}\n")

    case_df = add_cross_check_and_category(case_df, resolve_df)
    matched = case_df["Cross Check"].notna().sum()
    print(f"  Cross-check matched: {matched}/{len(case_df)} rows")
    print(f"  Fallback to original skill: {len(case_df) - matched} rows\n")

    cat_agg = build_category_agg(case_df, resolve_df)
    print(f"  Unique categories: {len(cat_agg)}")

    manager_level = build_manager_level(case_df, resolve_df)
    print(f"  Unique managers: {len(manager_level)}")

    write_output(case_df, cat_agg, manager_level, args.output)


if __name__ == "__main__":
    main()
