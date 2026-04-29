"""
Weekly Business Review (WBR) Report ETL Pipeline

Automates weekly WBR report generation by merging case management and
resolution tracking data, filtering by region and category, and producing
a multi-sheet Excel workbook with aggregated metrics.

Saves ~2 hours/week of manual Excel work.
"""

import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime

# --- Configuration (update these for your environment) ---
INPUT_DIR = os.environ.get("WBR_DATA_DIR", os.path.join(".", "data"))
OUTPUT_FILE = os.path.join(INPUT_DIR, "WBR_Output.xlsx")

# Regional team members (anonymized)
REGIONAL_RESOLVERS = {r.lower() for r in [
    "resolver_01", "resolver_02", "resolver_03", "resolver_04",
    "resolver_05", "resolver_06", "resolver_07", "resolver_08", "resolver_09"
]}

# Case categories to include in regional filter
CASE_CATEGORY_FILTER = {
    "Missing Inbound - Region A",
    "Missing Inbound - Region B",
    "High Value Proactive - Regional",
    "High Value Investigation - Regional",
    "Missing Inbound - Region C",
    "High Priority Proactive - Regional",
    "Missing Inbound - Region D",
    "Missing Inbound - Region E",
    "High Value Investigation - Regional Alt Lang 1",
    "High Value Investigation - Regional Alt Lang 2",
}

RESOLVE_CATEGORY_FILTER = {
    "Missing Inbound - Region B",
    "Missing Inbound - Region D",
    "Missing Inbound - Region A",
    "Missing Inbound - Region E",
    "Missing Inbound - Region C",
    "High Priority Proactive - Regional",
    "High Value Investigation - Regional",
    "Low Value Investigation - Regional",
    "High Value Investigation - Regional Alt Lang 1",
    "Low Value Investigation - Regional Alt Lang 2",
    "High Value Investigation - Regional Alt Lang 2",
    "Low Value Investigation - Regional Alt Lang 1",
}


def get_latest_file(directory, prefix):
    """Find the most recently modified file matching a prefix pattern."""
    pattern = os.path.join(directory, f"{prefix}*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files matching '{prefix}*.csv' in {directory}")
    return max(files, key=os.path.getmtime)


def load_sources():
    """Load and clean the two source CSV files."""
    case_file = get_latest_file(INPUT_DIR, "case_data_")
    resolve_file = get_latest_file(INPUT_DIR, "resolution_data_")
    print(f"  Case file:    {os.path.basename(case_file)}")
    print(f"  Resolve file: {os.path.basename(resolve_file)}")

    case_df = pd.read_csv(case_file)
    resolve_df = pd.read_csv(resolve_file)

    case_df.columns = case_df.columns.str.strip()
    resolve_df.columns = resolve_df.columns.str.strip()

    for col in ["Case ID", "Requester", "Resolver", "Requester Skill", "Gap", "Site"]:
        if col in case_df.columns:
            case_df[col] = case_df[col].astype(str).str.strip()

    for col in ["Case ID", "Associate", "Supervisor", "Category"]:
        if col in resolve_df.columns:
            resolve_df[col] = resolve_df[col].astype(str).str.strip()

    return case_df, resolve_df


def add_cross_check_and_category(case_df, resolve_df):
    """
    Cross-reference case data with resolution data to enrich category info.
    Matches on (Case ID + Associate) and falls back to the original skill field.
    """
    resolve_keys = resolve_df["Case ID"] + "|" + resolve_df["Associate"].str.lower()
    resolve_cat = resolve_df["Category"]

    lookup = {}
    for key, cat in zip(resolve_keys, resolve_cat):
        if key not in lookup:
            lookup[key] = cat

    case_keys = case_df["Case ID"] + "|" + case_df["Requester"].str.lower()
    case_df["Cross Check"] = case_keys.map(lookup)
    case_df["Enriched Category"] = case_df["Cross Check"].fillna(case_df["Requester Skill"])

    return case_df


def build_regional_cases(case_df):
    """Filter cases to regional team and relevant categories."""
    mask = (
        case_df["Resolver"].str.lower().isin(REGIONAL_RESOLVERS)
        & case_df["Enriched Category"].isin(CASE_CATEGORY_FILTER)
    )
    regional = case_df.loc[mask].copy()
    print(f"  Regional case rows: {len(regional)}")

    regional["Date"] = pd.to_datetime(regional["Date"], errors="coerce")
    regional["Skill"] = "Investigation"
    regional["week_no"] = regional["Date"].dt.isocalendar().week.astype("int64")

    return pd.DataFrame({
        "Case ID":           regional["Case ID"].values,
        "Gap":               regional["Gap"].values,
        "Question":          regional["Question"].values,
        "Date":              regional["Date"].values,
        "Site":              regional["Site"].values,
        "Skill":             regional["Skill"].values,
        "Requester":         regional["Requester"].values,
        "Category":          regional["Enriched Category"].values,
        "Resolver":          regional["Resolver"].values,
        "Advisor Comments":  regional["Advisor Comments"].values,
        "week_no":           regional["week_no"].values,
    })


def build_invalid_cases(regional_cases):
    """Extract cases flagged as invalid."""
    mask = regional_cases["Gap"].str.lower() == "invalid andon"
    invalid = regional_cases.loc[mask].copy()
    print(f"  Invalid case rows: {len(invalid)}")
    return invalid


def build_aggregations(regional_cases, invalid_cases):
    """Count cases and invalid cases per associate."""
    case_agg = (
        regional_cases.groupby("Requester", as_index=False)["Case ID"]
        .count()
        .rename(columns={"Requester": "Associate", "Case ID": "Case_Count"})
    )
    invalid_agg = (
        invalid_cases.groupby("Requester", as_index=False)["Case ID"]
        .count()
        .rename(columns={"Requester": "Associate", "Case ID": "Invalid_Case_Count"})
    )
    return case_agg, invalid_agg


def build_resolve_data(resolve_df):
    """Filter resolution data and aggregate by associate."""
    mask = (
        resolve_df["Category"].isin(RESOLVE_CATEGORY_FILTER)
        & (resolve_df["Supervisor"] != "")
        & (resolve_df["Supervisor"] != "nan")
        & resolve_df["Supervisor"].notna()
    )
    filtered = resolve_df.loc[mask].copy()
    print(f"  Resolve data rows: {len(filtered)}")

    resolve_agg = (
        filtered.groupby("Associate", as_index=False)["Case ID"]
        .count()
        .rename(columns={"Case ID": "Resolve_Count"})
    )
    return filtered, resolve_agg


def build_wbr_report(resolve_agg, case_agg, invalid_agg, resolve_filtered, case_df):
    """
    Build the final WBR report sheet combining all aggregations.
    Computes case counts, resolve counts, and invalid counts per associate.
    """
    today = datetime.now()
    iso_year, iso_week, _ = today.isocalendar()
    prev_week = iso_week - 1 if iso_week > 1 else datetime(iso_year - 1, 12, 28).isocalendar()[1]

    wbr = resolve_agg[["Associate"]].copy()
    wbr["week_no"] = prev_week

    case_map = case_agg.set_index("Associate")["Case_Count"]
    wbr["Sum of case_count"] = wbr["Associate"].map(case_map).fillna(0).astype(int)

    resolve_map = resolve_agg.set_index("Associate")["Resolve_Count"]
    wbr["Sum of resolve_count"] = wbr["Associate"].map(resolve_map).fillna(0).astype(int)

    supervisor_map = resolve_filtered.drop_duplicates("Associate").set_index("Associate")["Supervisor"]
    wbr["Manager"] = wbr["Associate"].map(supervisor_map)

    wbr["Skip Manager"] = ""
    wbr["Category"] = "Investigation-Regional"
    wbr["Current Skill"] = "Investigation-Regional"
    wbr["Role"] = "Production"
    wbr["Tenure"] = ""

    invalid_map = invalid_agg.set_index("Associate")["Invalid_Case_Count"]
    wbr["Invalid Case Count"] = wbr["Associate"].map(invalid_map).fillna(0).astype(int)

    site_df = case_df[["Requester", "Site"]].drop_duplicates("Requester")
    site_df["_key"] = site_df["Requester"].str.lower()
    site_map = site_df.set_index("_key")["Site"]
    wbr["Site"] = wbr["Associate"].str.lower().map(site_map)

    return wbr[[
        "week_no", "Associate", "Sum of case_count", "Sum of resolve_count",
        "Manager", "Skip Manager", "Category", "Current Skill", "Role",
        "Tenure", "Invalid Case Count", "Site"
    ]]


def write_output(regional_cases, invalid_cases, case_agg, invalid_agg,
                 resolve_filtered, resolve_agg, wbr):
    """Write all result sheets to a single Excel workbook."""
    print(f"  Writing to {OUTPUT_FILE}...")
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        regional_cases.to_excel(writer, sheet_name="Regional_Cases", index=False)
        invalid_cases.to_excel(writer, sheet_name="Invalid_Cases", index=False)
        case_agg.to_excel(writer, sheet_name="Case_Agg", index=False)
        invalid_agg.to_excel(writer, sheet_name="Invalid_Agg", index=False)
        resolve_filtered.to_excel(writer, sheet_name="Resolve_Data", index=False)
        resolve_agg.to_excel(writer, sheet_name="Resolve_Agg", index=False)
        wbr.to_excel(writer, sheet_name="WBR_Report", index=False)


def run_pipeline():
    print("=" * 60)
    print("  WBR Report ETL Pipeline")
    print("=" * 60)

    print("\n[1/8] Loading source files...")
    case_df, resolve_df = load_sources()

    print("[2/8] Cross-referencing categories...")
    case_df = add_cross_check_and_category(case_df, resolve_df)

    print("[3/8] Filtering regional cases...")
    regional_cases = build_regional_cases(case_df)

    print("[4/8] Extracting invalid cases...")
    invalid_cases = build_invalid_cases(regional_cases)

    print("[5/8] Building aggregations...")
    case_agg, invalid_agg = build_aggregations(regional_cases, invalid_cases)

    print("[6/8] Processing resolution data...")
    resolve_filtered, resolve_agg = build_resolve_data(resolve_df)

    print("[7/8] Building WBR report...")
    wbr = build_wbr_report(resolve_agg, case_agg, invalid_agg, resolve_filtered, case_df)

    print("[8/8] Writing output...")
    write_output(regional_cases, invalid_cases, case_agg, invalid_agg,
                 resolve_filtered, resolve_agg, wbr)

    print("\n Done! Output sheets:")
    for name in ["Regional_Cases", "Invalid_Cases", "Case_Agg",
                  "Invalid_Agg", "Resolve_Data", "Resolve_Agg", "WBR_Report"]:
        print(f"  - {name}")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
