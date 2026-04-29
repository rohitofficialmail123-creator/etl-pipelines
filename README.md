# ETL Pipelines Portfolio

**4 Python ETL pipelines** built to automate weekly operational reporting, reducing **7+ hours/week** of manual Excel-based data processing.

These pipelines were developed to solve real business problems — transforming raw CSV/Excel exports from case management and resolution tracking systems into structured, multi-sheet analytical reports used by operations leadership for weekly business reviews.

---

## Pipelines Overview

| # | Pipeline | What It Does | Time Saved | Key Techniques |
|---|----------|-------------|------------|----------------|
| 1 | [WBR Automation](./wbr_automation/) | Merges case and resolution data, filters by region/category, builds weekly business review report | ~2 hrs/week | Multi-source join, cross-referencing, ISO week calculation, multi-sheet Excel output |
| 2 | [Andon Rate Report](./andon_rate_report/) | Computes case rates and invalid rates at category and manager levels | ~1.5 hrs/week | Category-level & manager-level aggregation, rate calculations, fallback enrichment logic |
| 3 | [FW WBR Pipeline](./fw_wbr_pipeline/) | Processes case review queue data with queue/category filters, computes TP90/TP99 stats and age distributions | ~2 hrs/week | Statistical metrics (TP90, TP99), age-group bucketing, Excel formatting preservation, multi-variant sheet generation |
| 4 | [Comment Classifier](./chatbot_andon_classifier/) | Classifies free-text advisor comments into standardized categories using fuzzy matching | ~1.5 hrs/week | Fuzzy string matching (SequenceMatcher), regex text normalization, multi-level pivot tables, contribution % analysis |

---

## Architecture Pattern

All four pipelines follow the same **Extract → Transform → Load** pattern:

```
┌─────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  CSV/Excel   │────▶│  Python Transform     │────▶│  Multi-sheet    │
│  Exports     │     │  (pandas + openpyxl)  │     │  Excel Report   │
└─────────────┘     └──────────────────────┘     └─────────────────┘
     Extract              Transform                    Load
```

**Data flow:**
1. **Extract** — Auto-detect and load the most recent source files from a configurable directory
2. **Transform** — Clean, merge, filter, aggregate, and compute derived metrics
3. **Load** — Write structured output to a multi-sheet Excel workbook

---

## Technical Highlights

### Data Quality & Cleaning
- Whitespace normalization across all string columns
- Null/NaN handling with configurable fallback logic
- Deduplication on composite keys before joins

### Cross-Source Enrichment
- Case data enriched with resolution categories via composite key matching (Case ID + Associate)
- Fallback to original classification when no cross-reference match exists

### Text Classification (Comment Classifier)
- Fuzzy matching using `SequenceMatcher` with configurable similarity threshold (0.80)
- Handles real-world noise: duplicate hashtags, missing `#` prefix, typos, trailing punctuation
- Multi-pass classification: exact match → prefix match → dedup check → fuzzy match → fallback

### Statistical Computation (FW WBR)
- Average, TP90, TP99 percentile calculations per metric column
- Age-group distribution bucketing (0-10, 10-20, etc.)
- Average processing time difference between date columns

### Aggregation Patterns
- Group-by aggregations at multiple dimensions (associate, manager, skip-level, category)
- Pivot tables with grand totals and contribution percentages
- Rate calculations (case rate, invalid rate) with zero-division handling

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| **Python 3.8+** | Core language |
| **pandas** | Data manipulation, aggregation, pivot tables |
| **openpyxl** | Excel read/write with formatting preservation |
| **numpy** | Percentile calculations (TP90, TP99) |
| **difflib** | Fuzzy string matching for text classification |
| **argparse** | CLI configuration for flexible execution |

---

## Project Structure

```
etl_pipelines/
├── README.md
├── requirements.txt
├── .gitignore
├── wbr_automation/
│   └── wbr_automation.py
├── andon_rate_report/
│   └── andon_rate.py
├── fw_wbr_pipeline/
│   └── fw_wbr.py
└── chatbot_andon_classifier/
    └── chatbot_classifier.py
```

---

## Setup & Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run any pipeline (example)
python wbr_automation/wbr_automation.py

# Andon rate report with custom paths
python andon_rate_report/andon_rate.py --data-dir ./my_data --output ./output/report.xlsx
```

### Configuration

Pipelines read source files from a configurable directory. Set via:
- **Environment variable**: `WBR_DATA_DIR` or `PROJECT_DATA_DIR`
- **CLI argument**: `--data-dir PATH` (where supported)
- **Default**: `./data/` in the current directory

---

## Business Impact

These pipelines replaced a fully manual process where analysts would:
1. Open multiple CSV exports in Excel
2. Manually VLOOKUP/INDEX-MATCH across files
3. Create pivot tables by hand
4. Copy-paste results into formatted report templates
5. Recalculate statistics manually each week

**Before:** ~7+ hours/week of repetitive manual work across 4 reports  
**After:** ~5 minutes total — run each script, review output

---

## Author

Data Engineering & Automation | Python, SQL, pandas

---

## License

MIT
