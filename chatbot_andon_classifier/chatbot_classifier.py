"""
Advisor Comment Classifier & Case Analytics Pipeline

Automates the classification of free-text advisor comments into standardized
categories using fuzzy string matching, then generates multi-dimensional
pivot reports by resolver, manager, skip-level manager, and case category.

Techniques used:
- Fuzzy matching (SequenceMatcher) for noisy text classification
- Regex-based hashtag extraction and normalization
- Multi-level pivot table generation
- Contribution percentage calculations per category

Saves ~1.5 hours/week of manual comment categorization and reporting.
"""

import pandas as pd
import re
import os
from difflib import SequenceMatcher
from pathlib import Path

# --- Configuration ---
FOLDER = Path(os.environ.get("PROJECT_DATA_DIR", os.path.join(".", "data")))
OUTPUT_FILE = FOLDER / "cleaned_report.xlsx"

VALID_HASHTAGS = [
    "#Review_needed",
    "#Tool_not_used",
    "#Tool_used_confirmation",
    "#Tool_used_incorrect_prompt",
    "#Tool_error"
]
VALID_LOWER = {v.lower(): v for v in VALID_HASHTAGS}

# --- Category Consolidation Mapping ---
CATEGORY_MAPPING = {
    "Non-Investigation": [
        "Fee Correction Automated", "Manual Warehouse Damage", "Manual Order Review",
        "Outreach - Regional Alt Lang", "Fee Correction",
        "Manual Carrier Damage", "Customer Return Escalation",
        "Warehouse Lost Automated - FE", "Warehouse Damage Automated",
        "Manual Warehouse Lost", "Manual Customer Refund", "Refunds",
        "Warehouse Lost Automated - SOP", "Manual Warehouse Removal",
        "Manual Warehouse Disposal", "Customer Return Escalation - Regional",
        "Warehouse Lost Automated - Regional", "Top Defective Seller",
        "Outreach - Regional Calldown", "Outreach - Tollgate",
        "Return Verification", "Service Fee"
    ],
    "Overall Investigation": [
        "HV Investigation MX", "Missing Inbound - US", "High Priority Proactive - US",
        "HV Investigation AU", "High Priority - Regional", "HV Investigation JP",
        "HV Investigation US", "High Priority - US",
        "Investigation Ineligible", "HV Investigation Regional",
        "HV Investigation MX - Alt Lang",
        "Missing Inbound - CA", "HV Investigation BR", "Missing",
        "Missing Inbound - Region A", "High Priority Proactive - CA",
        "HV Investigation MX - Spanish", "High Priority Proactive - Regional",
        "HV Investigation SA", "HV Investigation AE",
        "Missing Inbound - Region C",
        "Missing Inbound - Region B", "High Priority - CA",
        "HV Investigation TR",
        "Missing Inbound - Region E", "HV Investigation PL",
        "HV Investigation Regional - Alt Lang 1", "Missing Inbound - Region D",
        "HV Investigation Regional - Alt Lang 2", "HV Investigation AU - Alt Lang",
        "HV Investigation BR - Portuguese", "HV Investigation EG"
    ],
    "Distribution": [
        "Replenish", "Manual Missing Inbound - Distribution",
        "Warehousing & Distribution", "Replenish Snowball"
    ],
    "Audit": [
        "Proactive Cost Audit"
    ],
    "Disputes": [
        "Defect Disputes Regional", "Defect Disputes"
    ]
}

# Build reverse lookup: category value -> parent group
CATEGORY_REVERSE = {}
for group, values in CATEGORY_MAPPING.items():
    for v in values:
        CATEGORY_REVERSE[v.strip().lower()] = group


# --- Auto-pick most recent file ---
def load_input_file():
    files = sorted(
        list(FOLDER.glob("*.csv")) + list(FOLDER.glob("*.xlsx")),
        key=os.path.getmtime, reverse=True
    )
    files = [f for f in files if f.name != "cleaned_report.xlsx"]
    if not files:
        raise FileNotFoundError(f"No .csv or .xlsx files found in {FOLDER}")
    input_file = files[0]
    print(f"Processing: {input_file.name}")
    return pd.read_csv(input_file) if input_file.suffix == ".csv" else pd.read_excel(input_file)


# --- Fuzzy Matching ---
def fuzzy_match(text, threshold=0.80):
    """Match input text against valid hashtags using sequence similarity."""
    text_lower = text.lower().strip()
    best_score, best_tag = 0, None
    for valid_lower, valid_original in VALID_LOWER.items():
        score = SequenceMatcher(None, text_lower, valid_lower).ratio()
        if score > best_score:
            best_score, best_tag = score, valid_original
    return best_tag if best_score >= threshold else None


def clean_comment(val):
    """
    Classify a free-text comment into a standardized hashtag category.
    Handles: missing values, duplicate hashtags, typos, missing # prefix.
    """
    if pd.isna(val) or str(val).strip() == "":
        return "Blanks"
    text = str(val).strip()

    # Normalize: collapse multiple # into one
    cleaned = re.sub(r'[#]+', '#', text).strip()

    # No # present — try adding one and matching
    if '#' not in cleaned:
        candidate = '#' + cleaned
        if candidate.lower() in VALID_LOWER:
            return VALID_LOWER[candidate.lower()]
        matched = fuzzy_match(candidate)
        return matched if matched else "Other comments"

    # Try longest valid hashtag match from start of text
    cleaned_lower = cleaned.lower()
    best_match = None
    for valid_lower, valid_original in VALID_LOWER.items():
        if cleaned_lower.startswith(valid_lower):
            if best_match is None or len(valid_lower) > len(best_match[0]):
                best_match = (valid_lower, valid_original)

    if best_match:
        return best_match[1]

    # Handle duplicates: e.g. "#Tool_not_used  #Tool_not_used"
    tokens = re.split(r'\s{2,}', cleaned)
    tokens = [t.strip() for t in tokens if t.strip().startswith('#')]
    if tokens:
        unique = list(set(t.lower() for t in tokens))
        if len(unique) == 1 and unique[0] in VALID_LOWER:
            return VALID_LOWER[unique[0]]

    # Extract first hashtag token and fuzzy match
    first_tag_match = re.match(r'(#[\w]+(?:[\s][\w]+)*)', cleaned)
    if first_tag_match:
        raw_tag = first_tag_match.group(1).strip().rstrip('-\u2013\u2014:.')
        if raw_tag.lower() in VALID_LOWER:
            return VALID_LOWER[raw_tag.lower()]
        matched = fuzzy_match(raw_tag)
        if matched:
            return matched

    return "Other comments"


def map_category(val):
    """Map a category value to its consolidated parent group."""
    if pd.isna(val) or str(val).strip() == "":
        return "Unmapped"
    return CATEGORY_REVERSE.get(str(val).strip().lower(), "Unmapped")


def run_pipeline():
    print("=" * 60)
    print("  Advisor Comment Classifier Pipeline")
    print("=" * 60)

    # Load data
    df = load_input_file()

    # Classify comments
    col_idx = df.columns.get_loc("Advisor Comments") + 1
    df.insert(col_idx, "Cleaned_Comments", df["Advisor Comments"].apply(clean_comment))

    # Map categories
    df["Category_Group"] = df["Enriched Category"].apply(map_category)

    # Aggregation: comment distribution
    case_counts = df.groupby("Cleaned_Comments")["Case ID"].count().reset_index(name="Case Count")

    # Pivot: by resolver
    pivot_resolver = pd.pivot_table(
        df, values="Case ID", index="Resolver",
        columns="Cleaned_Comments", aggfunc="count", fill_value=0
    )
    pivot_resolver["Grand Total"] = pivot_resolver.sum(axis=1)

    # Pivot: by skip-level manager
    pivot_skip = pd.pivot_table(
        df, values="Case ID", index="Skip Manager",
        columns="Cleaned_Comments", aggfunc="count", fill_value=0
    )
    pivot_skip["Grand Total"] = pivot_skip.sum(axis=1)
    pivot_skip.loc["Grand Total"] = pivot_skip.sum()

    # Pivot: by direct manager
    pivot_manager = pd.pivot_table(
        df, values="Case ID", index="Requester Supervisor",
        columns="Cleaned_Comments", aggfunc="count", fill_value=0
    )
    pivot_manager["Grand Total"] = pivot_manager.sum(axis=1)
    pivot_manager.loc["Grand Total"] = pivot_manager.sum()

    # Category-level breakdown with contribution %
    cat_rows = []
    for group in CATEGORY_MAPPING.keys():
        cat_df = df[df["Category_Group"] == group]
        total = len(cat_df)
        cat_rows.append({"Category Group": group, "Case Count": total, "Contribution": ""})
        for comment in VALID_HASHTAGS + ["Blanks", "Other comments"]:
            count = len(cat_df[cat_df["Cleaned_Comments"] == comment])
            pct = f"{(count / total * 100):.2f}%" if total > 0 else "0.00%"
            cat_rows.append({"Category Group": comment, "Case Count": count, "Contribution": pct})

    cat_df = pd.DataFrame(cat_rows)

    # Export
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Cleaned Data", index=False)
        case_counts.to_excel(writer, sheet_name="Case Counts", index=False)
        pivot_resolver.to_excel(writer, sheet_name="By Resolver")
        pivot_skip.to_excel(writer, sheet_name="By Skip Level")
        pivot_manager.to_excel(writer, sheet_name="By Manager")
        cat_df.to_excel(writer, sheet_name="By Category", index=False)

    print(f"\nDone! Output: {OUTPUT_FILE}")
    print("Sheets: Cleaned Data, Case Counts, By Resolver, By Skip Level, By Manager, By Category")


if __name__ == "__main__":
    run_pipeline()
