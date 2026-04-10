# Universal Data Ingestion Platform — Technical Reference

**Version:** 1.2 | **Domain covered:** Trade Credit Payments (`trade`)

---

## Table of Contents

1. [What Is This Project](#1-what-is-this-project)
2. [Technology Stack & Libraries](#2-technology-stack--libraries)
3. [Directory Structure](#3-directory-structure)
4. [Architecture Overview](#4-architecture-overview)
5. [End-to-End Pipeline Flow](#5-end-to-end-pipeline-flow)
6. [Component: File Parser](#6-component-file-parser)
7. [Component: Column Mapper](#7-component-column-mapper)
8. [Component: Pipeline Orchestrator](#8-component-pipeline-orchestrator)
9. [Component: DQ Engine](#9-component-dq-engine)
10. [Component: Profile Store](#10-component-profile-store)
11. [Component: Output Writer & Lineage Writer](#11-component-output-writer--lineage-writer)
12. [Configuration System](#12-configuration-system)
13. [Canonical Model & Lookup Table](#13-canonical-model--lookup-table)
14. [System Tables Schema](#14-system-tables-schema)
15. [UI Layer (app.py)](#15-ui-layer-apppy)
16. [Output Files Reference](#16-output-files-reference)
17. [Adding a New Domain](#17-adding-a-new-domain)
18. [Tuning & Troubleshooting](#18-tuning--troubleshooting)
19. [Security Model](#19-security-model)
20. [Glossary](#20-glossary)

---

## 1. What Is This Project

A **multi-format, multi-file data ingestion platform** for financial data contributors. It accepts raw source files in any format, maps their columns to a standard canonical schema, runs data quality checks, and produces clean standardised output files alongside full audit trails.

### Core problems it solves

| Problem | Solution |
|---------|----------|
| Every contributor uses different column names | Column mapping engine (exact → fuzzy → LLM) |
| Different file formats per contributor | Universal file parser (CSV, TSV, XLSX, JSON, XML, PDF, DOCX, ZIP) |
| Manual re-mapping for every recurring file | Mapping Profile system — save once, reuse automatically |
| No audit trail for data decisions | Column lineage, archive lineage, DQ exceptions |
| Hollow/corrupt canonical output | `direct_tables` guard — tables need ≥2 direct columns |
| Contributor files bundled in archives | Nested ZIP extraction with security checks |

---

## 2. Technology Stack & Libraries

### Core runtime
| Library | Version | Used for |
|---------|---------|----------|
| **Python** | 3.10+ | Runtime |
| **pandas** | ≥2.0 | DataFrames — parsing, transformation, output CSVs |
| **gradio** | ≥4.44 | Web UI — file upload, scorecard, tabs, HTML components |
| **python-dotenv** | ≥1.0 | Loading `.env` for API keys |

### File parsing
| Library | Used for |
|---------|----------|
| **openpyxl** | Reading/writing `.xlsx` files; read-only mode for header-only profiling |
| **xlrd** | Legacy `.xls` file support |
| **python-docx** | Extracting text tables from `.docx` Word documents |
| **pdfplumber** | Extracting text and tables from text-based PDFs |
| **lxml** | XML parsing; backend for `pandas.read_xml()` |
| **beautifulsoup4 + html5lib** | HTML table parsing via `pandas.read_html()` |
| **chardet** | Byte-level encoding detection for files without BOM |
| **pytesseract + Pillow** | *(optional)* OCR for scanned/image PDFs |

### Matching & mapping
| Library | Used for |
|---------|----------|
| **difflib** (stdlib) | `SequenceMatcher.ratio()` for fuzzy column name matching |
| **hashlib** (stdlib) | SHA-256 fingerprinting for profile identity |
| **re** (stdlib) | Column name normalisation, markdown fence stripping |

### LLM integrations *(optional)*
| Library | Provider |
|---------|---------|
| **anthropic** | Claude (Opus, Sonnet, Haiku) |
| **openai** | GPT-4o, o3 |
| **google-genai** | Gemini 2.0 / 2.5 |

### Standard library (key modules)
| Module | Used for |
|--------|----------|
| `uuid` | Job IDs, lineage IDs, exception IDs |
| `json` | Config files, profile store, DQ report, job summary |
| `csv` | Header-only reading in `_detect_columns_only` |
| `zipfile` | ZIP extraction with nested archive support |
| `os / pathlib` | Directory and path management |
| `datetime / timezone` | UTC timestamps on all records |
| `dataclasses` | `MappingResult`, `ParsedFile`, `Profile`, `MatchResult` |

---

## 3. Directory Structure

```
Data_Ingestion/
│
├── app.py                          # Gradio UI — all tabs, event wiring, UI functions
├── pipeline.py                     # Orchestrator — runs all engine steps end-to-end
├── requirements.txt
│
├── engine/
│   ├── __init__.py
│   ├── file_parser.py              # Parses any file format → ParsedFile objects
│   ├── column_mapper.py            # Maps source columns → canonical columns
│   ├── dq_engine.py                # Data quality checks on canonical tables
│   ├── output_writer.py            # Writes all output files to disk
│   ├── lineage_writer.py           # Builds COLUMN_LINEAGE / ARCHIVE_LINEAGE / RECORD_EXCEPTIONS DFs
│   └── profile_store.py            # Mapping profile save/match/load system
│
├── domains/
│   └── trade/
│       ├── trade_system_config.json    # All tunable parameters for the trade domain
│       ├── trade_canonical_model.json  # Schema definition — tables, columns, types, mandatory flags
│       ├── trade_lookup_table.csv      # Source column alias → canonical column mapping table
│       └── trade_prompt.txt            # LLM prompt template for column disambiguation
│
├── profiles/
│   └── trade/
│       ├── index.json              # Lightweight index of all saved profiles (always loaded)
│       └── <fp8>.json              # Full profile — columns + overrides + cached scorecard rows
│
├── global_system_tables.json       # Schema for RECORD_EXCEPTIONS, COLUMN_LINEAGE, ARCHIVE_LINEAGE, JOB_SUMMARY
├── global_prompt.txt               # Shared LLM instructions prepended to domain prompt
│
├── output/
│   └── <YYYYMMDD>_<job_id>/        # One folder per job run
│       ├── canonical_trade_trd_customer_<job_id>.csv
│       ├── canonical_trade_trd_invoice_<job_id>.csv
│       ├── exceptions_trade_<job_id>.csv
│       ├── column_lineage_<job_id>.csv
│       ├── archive_lineage_<job_id>.csv   (if ZIP was involved)
│       ├── dq_trade_<job_id>.json
│       └── job_summary_<job_id>.json
│
├── test_data/                      # Sample files for testing all scenarios
└── sample_trade_pack_v1/v2/        # Realistic multi-format contributor packs
```

---

## 4. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                          Gradio UI (app.py)                      │
│  Tab 1: Upload & Run  |  Tab 2-4: Review  |  Tab 5: Profiles    │
└─────────────────────────────┬───────────────────────────────────┘
                               │
                         pipeline.py
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                       │
  file_parser.py        column_mapper.py         profile_store.py
  (parse all formats)   (exact/fuzzy/LLM/        (fingerprint, match,
                         override/propagation)    save, cache scorecard)
        │                      │
        └──────────┬───────────┘
                   │
             dq_engine.py
             (mandatory nulls, type,
              duplicates, referential
              integrity, business rules)
                   │
        ┌──────────┴──────────┐
        │                     │
  output_writer.py      lineage_writer.py
  (canonical CSVs,      (COLUMN_LINEAGE,
   exceptions CSV,       ARCHIVE_LINEAGE,
   DQ JSON,              RECORD_EXCEPTIONS)
   job summary JSON)
```

### Key design principles

- **Config-driven, not code-driven**: all thresholds, schemas, null values, business rules live in JSON/CSV config files. No pipeline code changes needed for a new domain.
- **Per-file processing**: even in a multi-file batch, each file is parsed, mapped, and DQ-checked independently. Blocking is evaluated per-file.
- **Immutable job output**: each run creates a new dated folder with `job_id` in every filename. Nothing is overwritten.
- **Audit-first**: every mapping decision is recorded in COLUMN_LINEAGE with method, confidence, and LLM reasoning.

---

## 5. End-to-End Pipeline Flow

```
User uploads files → clicks Analyze Mapping
        │
        ▼
1. load_config()
   Reads: trade_system_config.json, trade_canonical_model.json, trade_lookup_table.csv

        │
        ▼
2. Profile check (per file)
   For each file:
     compute SHA-256 fingerprint of sorted column names
     check against profiles/trade/index.json
     EXACT match + cached mappings? → restore scorecard, skip mapper
     PARTIAL match (≥ configured threshold)? → show amber banner
     No match → proceed to mapper

        │
        ▼
3. parse_input_file()   [engine/file_parser.py]
   Detects format → reads DataFrame → returns ParsedFile(dataframe, source_filename, ...)

        │
        ▼
4. map_columns()   [engine/column_mapper.py]
   For each source column:
     a. Check user_overrides → USER OVERRIDE (conf=100)
     b. Normalise name → check lookup table → EXACT LOOKUP (conf=100)
     c. Fuzzy match against all lookup aliases → FUZZY MATCH (conf ≈80-91)
     d. If fuzzy conf < llm_disambiguation_required_below → LLM disambiguation
     e. No match → LLM fallback (if enabled)
     f. Still no match → NO MATCH
   After all columns: apply shared-key propagation rules

        │
        ▼
5. UI review (Analyze step ends here)
   User reviews scorecard, edits Selected_Target inline,
   adds overrides via dropdowns, saves profile

        │ (user clicks Run Pipeline)
        ▼
6. _process_parsed_file()   [pipeline.py]
   re-maps each file with final user_overrides applied
   computes direct_tables (tables with ≥2 non-propagated columns)
   calls _build_canonical_tables() — builds output DataFrames

        │
        ▼
7. run_dq_checks()   [engine/dq_engine.py]
   Per canonical table:
     - mandatory null check
     - type validation (numeric, date)
     - duplicate key detection
     - referential integrity
     - business rules
   Returns (cleaned_tables, exceptions_list, dq_report)

        │
        ▼
8. build_column_lineage_df()   [engine/lineage_writer.py]
   One row per source column per file — full mapping audit trail

        │
        ▼
9. write_all_outputs()   [engine/output_writer.py]
   Writes canonical CSVs, exceptions CSV, DQ JSON, job summary JSON,
   column lineage CSV, archive lineage CSV (if applicable)
```

---

## 6. Component: File Parser

**File:** [engine/file_parser.py](engine/file_parser.py)

### What it does
Accepts any supported file path and returns a list of `ParsedFile` objects (one per sheet/sub-file). Handles encoding detection, archive extraction, and normalisation.

### ParsedFile dataclass
```python
@dataclass
class ParsedFile:
    dataframe: pd.DataFrame       # the parsed data
    source_filename: str          # original filename (e.g. "customers.csv")
    source_file_format: str       # "CSV", "XLSX", "JSON", etc.
    source_contributor_id: str    # extracted from filename or config
    archive_lineage_id: str|None  # set if file came from inside a ZIP
```

### Supported formats

| Format | How parsed | Notes |
|--------|-----------|-------|
| CSV/TSV | `csv.Sniffer` for delimiter detection, then `pandas.read_csv` | Tries UTF-8-sig → UTF-16 → UTF-8 → CP1252 → Latin-1 |
| XLSX | `pandas.read_excel(engine='openpyxl')` | All sheets extracted |
| XLS | `pandas.read_excel(engine='xlrd')` | Legacy format |
| JSON | `pandas.read_json` or `json.load` | Handles arrays of objects and nested keys |
| XML | `pandas.read_xml(parser='lxml')` | Flattens to tabular |
| HTML | `pandas.read_html(flavor='bs4')` | Extracts all `<table>` elements |
| DOCX | `python-docx` table extraction | Each Word table → separate ParsedFile |
| PDF (text) | `pdfplumber` page/table extraction | Falls back to text-line parsing |
| PDF (scanned) | `pytesseract` OCR → text → parse | Requires Tesseract installed |
| ZIP | `zipfile.ZipFile` recursive extraction | Zip-slip protection, encrypted ZIP detection, depth/count limits |
| Pipe/semicolon delimited | Detected by `csv.Sniffer` | Treated same as CSV |

### Archive security checks
Before extracting any ZIP entry, the parser checks:
- **Zip-slip**: resolved extraction path must stay within the temp directory
- **Encrypted**: `entry.flag_bits & 0x1` — blocked if encrypted
- **Blocked extensions**: `.exe`, `.dll`, `.js`, `.bat`, etc. — configurable
- **File size**: `max_single_extracted_file_mb` (default 500 MB)
- **Count limit**: `max_files_per_archive` (default 5000)
- **Nesting depth**: `max_archive_nesting_depth` (default 5)

### `_detect_columns_only(file_path, cfg)`
Lightweight header-only reader used by the profile system. Reads only the first row to get column names without loading the full file:
- CSV: `csv.reader` with 2-row limit
- XLSX: `openpyxl` read-only mode, iterates first row only
- JSON: `json.load` → first object keys
- Others: falls back to full parse

---

## 7. Component: Column Mapper

**File:** [engine/column_mapper.py](engine/column_mapper.py)

### MappingResult dataclass
One per source column per file. Written to COLUMN_LINEAGE.
```python
@dataclass
class MappingResult:
    lineage_id: str
    mapping_reference_id: str
    job_id: str
    domain: str
    source_filename: str
    source_column_name: str        # original: "Invoice Date"
    source_column_normalised: str  # normalised: "invoice_date"
    canonical_table: str|None      # "TRD_INVOICE"
    canonical_column: str          # "Invoice_Date" or "UNMAPPED"
    match_method: str              # "EXACT LOOKUP" / "FUZZY MATCH" / "LLM (...)" / "USER OVERRIDE" / "NO MATCH"
    confidence_score: int          # 0–100
    was_mandatory: bool
    met_threshold: bool
    llm_reasoning: str|None
    lookup_variant_matched: str|None
    archive_lineage_id: str|None
    is_propagated: bool
    insert_timestamp: str
```

### Step 1 — Column normalisation
Applied before any matching:
```
"Invoice Date"   → lowercase → "invoice date"
                 → spaces→_  → "invoice_date"
"Invoice-Date"   → hyphens→_ → "invoice_date"
"  invoice__id " → collapse_ → "invoice_id"
                 → strip_    → "invoice_id"
```
All normalisation steps are configurable individually in `trade_system_config.json` under `matching.normalisation`.

### Step 2 — Exact lookup
Checks `trade_lookup_table.csv` — a CSV with columns:
`source_alias | canonical_table | canonical_column`

If the normalised source column matches any `source_alias` exactly → `EXACT LOOKUP`, confidence = `exact_confidence` (default 100).

### Step 3 — Fuzzy match
Uses `difflib.SequenceMatcher(None, normalised_source, alias).ratio()`:
- Compares against **all** `source_alias` values in the lookup table
- Picks the highest similarity score
- Accepts if score ≥ `fuzzy_min_similarity` (default 0.70)
- Confidence = `int(score × 100 × fuzzy_confidence_multiplier)` (default multiplier 0.91)
- Maximum fuzzy confidence ≈ 91 (deliberately below exact)

### Step 4 — LLM disambiguation
Triggered when fuzzy confidence < `llm_disambiguation_required_below` (default 70):
- Sends source column + all canonical options to LLM
- LLM returns `{canonical_table, canonical_column, confidence, reasoning}` as JSON
- Accepted if LLM confidence ≥ `confidence_accept_threshold` (default 55)
- Three providers: Claude (`_llm_map_claude`), OpenAI (`_llm_map_openai`), Gemini (`_llm_map_gemini`)
- All return `(table, column, confidence, reasoning, method_label)`
- Responses wrapped in `try/except json.JSONDecodeError` — invalid JSON falls through gracefully

### Step 5 — LLM fallback
For columns with NO exact/fuzzy match at all:
- Runs if `llm_provider != "None"` and `apply_to == "all" or "unmatched_only"`
- Same flow as disambiguation

### Step 6 — User override
Highest priority — checked first. Format: `{source_col: (canonical_table, canonical_column)}`.
Applied via UI (inline scorecard edit or guardrail dropdowns). Confidence = 100.

### Step 7 — Propagation
After all columns are mapped:
- Reads `shared_key_propagation_rules` from config
- If any source column resolved to a shared-key canonical column (e.g. `Account_Number`), that same source column is also mapped to the shared key in the other canonical table
- Propagated rows have `is_propagated = True`
- Propagated rows do NOT count toward `direct_tables` — prevents hollow rows

---

## 8. Component: Pipeline Orchestrator

**File:** [pipeline.py](pipeline.py)

### `run_pipeline(file_paths, domain, user_overrides, config_dir, log_callback)`
Main entry point called by the UI. Returns `(job_id, output_dir, log_text, job_status)`.

### `_process_parsed_file(pf, cfg, canonical_model, lookup, job_id, domain, user_overrides, config_dir)`
Processes one `ParsedFile`:
1. Calls `map_columns()` to get `MappingResult` list
2. Computes `direct_tables` — tables that have ≥2 non-propagated mapped columns:
   ```python
   direct_count: dict[str, int] = {}
   for r in mapping_results:
       if r.canonical_table and r.canonical_column != "UNMAPPED" and not r.is_propagated:
           direct_count[r.canonical_table] = direct_count.get(r.canonical_table, 0) + 1
   direct_tables = {tbl for tbl, cnt in direct_count.items() if cnt >= 2}
   ```
3. Calls `_build_canonical_tables(df, mapping_results, direct_tables, ...)`

### `_build_canonical_tables(df, mapping_results, direct_tables, ...)`
Builds output DataFrames, one per active canonical table:
- Skips tables not in `direct_tables` (hollow row prevention)
- For each row in source DataFrame: fills canonical columns from `col_map`
- Uses `src_row.get(src_col)` — safe even if column name has encoding drift
- Adds audit/lineage columns: `Source_Filename`, `Mapping_Reference_ID`, `Record_Status`, `Insert_Timestamp`, etc.

### Multi-file batch handling
- All files share one `job_id` and one output folder
- Each file is processed independently: `_process_parsed_file` is called per file
- Canonical DataFrames are collected then `pd.concat()`-ed per table (empty DataFrames filtered before concat)
- Blocking is per-file: `Job_Status = BLOCKED` only if ALL files blocked
- Otherwise: `SUCCESS` or `SUCCESS_WITH_EXCEPTIONS`

---

## 9. Component: DQ Engine

**File:** [engine/dq_engine.py](engine/dq_engine.py)

### `run_dq_checks(canonical_tables, source_col_maps, cfg, job_id, domain)`
Runs all checks against each canonical table. Returns `(cleaned_tables, exceptions_df, dq_report)`.

### Check 1 — Mandatory null check
For each mandatory canonical column:
- A value is null if it is in `null_values` (from config) or is `None` / empty string
- Failure → RECORD_EXCEPTIONS row with type `MANDATORY_NULL`

### Check 2 — Type validation
For numeric columns (`type: "numeric"` in canonical model):
- Strips currency symbols / commas (configurable in `type_validation.numeric.strip_chars`)
- Attempts `float()` conversion
- Failure → `TYPE_MISMATCH` exception

For date columns (`type: "date"`):
- Tries 14 date formats: `%Y-%m-%d`, `%d/%m/%Y`, `%d-%b-%Y`, `%Y%m%d`, etc.
- Failure → `TYPE_MISMATCH` exception

### Check 3 — Duplicate key detection
Configured in `quality.duplicate_detection.keys`:
- Per canonical table, one or more columns form the business key
- Identifies rows with duplicate key combinations → `DUPLICATE_KEY` exception

### Check 4 — Referential integrity
Configured in `quality.referential_integrity.rules`:
- `TRD_INVOICE.Account_Number` must exist in `TRD_CUSTOMER.Account_Number`
- Cross-table check across the canonical output (not the source)
- Failure → `REFERENTIAL_INTEGRITY_FAIL` exception, severity FAIL

### Check 5 — Business rules
Defined in `quality.business_rules`:

| Rule ID | Description | Severity |
|---------|-------------|----------|
| `INV_DUE_GE_INV_DATE` | Due_Date ≥ Invoice_Date | FAIL |
| `PAID_LE_INVOICE` | Paid_Amount ≤ Invoice_Amount | FAIL |
| `CURRENCY_ISO4217` | Currency must be valid ISO 4217 code | WARN |

### DQ report structure
```json
{
  "job_id": "...",
  "domain": "trade",
  "generated_at": "2026-04-10T...",
  "tables": {
    "TRD_CUSTOMER": {
      "total_rows": 50,
      "columns": {
        "Account_Number": {
          "null_count": 0,
          "fill_rate": 100.0,
          "type_errors": 0
        }
      }
    }
  },
  "exception_summary": {
    "MANDATORY_NULL": 2,
    "TYPE_MISMATCH": 1
  }
}
```

---

## 10. Component: Profile Store

**File:** [engine/profile_store.py](engine/profile_store.py)

### Purpose
Stores column mapping decisions for a file shape so they can be auto-applied when the same (or similar) file arrives again — skipping the mapping engine entirely on exact matches.

### Profile identity — fingerprinting
```python
def fingerprint(columns: list[str]) -> str:
    normalised = sorted(c.strip().lower() for c in columns)
    return hashlib.sha256("|".join(normalised).encode()).hexdigest()
```
- Order-independent: `Tax_ID` in col 1 or col 10 → same fingerprint
- Case-independent: `TAX_ID` and `tax_id` → same fingerprint
- File-name-independent: `customers_jan.csv` and `customers_feb.csv` with same headers → same profile

### Storage layout
```
profiles/trade/
    index.json         # {fingerprint: {name, column_count, use_count, last_used}}
    3617d3b4.json      # full profile (first 8 chars of fingerprint = filename)
    ae8a1b04.json
    ...
```

### Profile dataclass
```python
@dataclass
class Profile:
    fingerprint: str         # full SHA-256 hex
    name: str                # user-defined
    domain: str
    columns: list[str]       # sorted, lowercased source column names
    overrides: dict[str,str] # {source_col: "TABLE.Column"} — manual corrections only
    use_count: int
    created_at: str          # ISO date
    last_used: str           # ISO date
    mappings: list[dict]     # full scorecard rows — enables engine-skip on EXACT match
```

### Matching tiers

| Tier | Condition | Result |
|------|-----------|--------|
| EXACT | SHA-256 fingerprint in index | Load full profile; if `mappings` present, restore scorecard and skip engine |
| PARTIAL | Jaccard overlap ≥ `profile_partial_threshold` (default 0.70) | Show amber banner; user clicks "Apply Suggested" to load overrides |
| NONE | Below threshold | Run engine from scratch |

### Jaccard overlap calculation
```
Jaccard = |intersection| / |union|

incoming = {account_number, company_name, tax_id, currency}    (4 cols)
saved    = {account_number, company_name, tax_id, due_date}    (4 cols)
intersection = {account_number, company_name, tax_id}  → 3
union        = {account_number, company_name, tax_id, currency, due_date} → 5
Jaccard = 3/5 = 0.60  → below 0.70 → NONE
```

### Partial match scan performance
Pre-filter before computing Jaccard:
```python
if abs(saved_count - incoming_count) > max(incoming_count * 0.5, 3):
    skip  # don't even load the .json file
```
At 1000 profiles, typically only 10–20 are loaded for Jaccard computation.

### Configuring the threshold
In `domains/trade/trade_system_config.json`:
```json
"matching": {
    "profile_partial_threshold": 0.70
}
```
- `0.70` = current default (70% overlap)
- `1.00` = disables partial matching entirely
- `0.50` = looser — suggests profiles with only 50% column overlap

### Saving a profile
From UI: after reviewing scorecard → enter name → click **Save as Profile**:
1. Full scorecard rows saved in `mappings` (enables engine-skip next time)
2. Manual corrections saved in `overrides`
3. For multi-file batches: one profile per distinct file shape, names auto-suffixed with filename
4. Overrides filtered to only columns that exist in the file (prevents stale cross-file overrides)

### Stale entry cleanup
If `index.json` references a fingerprint whose `.json` file no longer exists on disk, the stale entry is automatically removed from the index when encountered.

---

## 11. Component: Output Writer & Lineage Writer

### Output Writer — [engine/output_writer.py](engine/output_writer.py)

| Function | Output file |
|----------|------------|
| `write_canonical_tables()` | `canonical_trade_trd_<table>_<job_id>.csv` |
| `write_exceptions()` | `exceptions_trade_<job_id>.csv` |
| `write_dq_report()` | `dq_trade_<job_id>.json` |
| `write_job_summary()` | `job_summary_<job_id>.json` |
| `write_column_lineage()` | `column_lineage_<job_id>.csv` |
| `write_archive_lineage()` | `archive_lineage_<job_id>.csv` (only if ZIP involved) |

All files land in `output/<YYYYMMDD>_<job_id>/`.

### Lineage Writer — [engine/lineage_writer.py](engine/lineage_writer.py)

**`build_column_lineage_df(mapping_results)`**
One row per source column per file. Captures the full mapping audit: method, confidence, LLM reasoning, lookup variant matched, archive provenance.

**`build_archive_lineage_df(archive_lineage_rows)`**
One row per file extracted from a ZIP. Records parent archive, root archive, nesting level, extraction status.

**`merge_exceptions(dq_exceptions, canonical_tables)`**
Combines DQ engine exceptions into the `RECORD_EXCEPTIONS` DataFrame with full provenance.

---

## 12. Configuration System

All behaviour is controlled by two JSON files per domain. No code changes needed to tune the system.

### `trade_system_config.json` — key sections

```
_metadata          version, domain_key
domain             name, description, canonical_tables list
llm                provider, model per provider, thresholds, timeout
matching           fuzzy similarity, confidence multipliers, normalisation toggles,
                   propagation rules, profile_partial_threshold, auto_block flag
quality            mandatory_threshold, null_values list, type_validation,
                   referential_integrity rules, duplicate_detection keys,
                   business_rules list
output             file name prefixes, preview_rows, max_exceptions_display
contributor_overrides   per-contributor threshold/null overrides keyed by contributor ID
ingestion          archive settings, allowed types, security limits
file_type_coverage structured/semi-structured/document/archive lists
text_extraction    encoding order, unicode normalisation, OCR settings
system_tables      scope, reference file
```

### `trade_canonical_model.json` — structure
```json
{
  "_metadata": { "version": "...", "domain": "trade" },
  "TRD_CUSTOMER": {
    "business_columns": {
      "Account_Number": { "type": "string", "mandatory": true },
      "Company_Name":   { "type": "string", "mandatory": true },
      "Government_ID":  { "type": "string", "mandatory": false }
    }
  },
  "TRD_INVOICE": {
    "business_columns": {
      "Invoice_Number":  { "type": "string",  "mandatory": true },
      "Invoice_Amount":  { "type": "numeric", "mandatory": true },
      "Invoice_Date":    { "type": "date",    "mandatory": true }
    }
  }
}
```

### `trade_lookup_table.csv` — structure
```
source_alias,canonical_table,canonical_column
account_number,TRD_CUSTOMER,Account_Number
acct_no,TRD_CUSTOMER,Account_Number
company_name,TRD_CUSTOMER,Company_Name
invoice_date,TRD_INVOICE,Invoice_Date
inv_date,TRD_INVOICE,Invoice_Date
```
Each alias is normalised the same way as source columns before comparison. Multiple aliases can map to the same canonical column — add as many rows as needed.

---

## 13. Canonical Model & Lookup Table

### Canonical tables (trade domain)

**TRD_CUSTOMER** — one row per trade credit customer account
| Column | Type | Mandatory |
|--------|------|-----------|
| Account_Number | string | Yes |
| Company_Name | string | Yes |
| DUNS_Number | string | No |
| Government_ID | string | No |
| Address_1 | string | No |
| Address_2 | string | No |
| City | string | No |
| State | string | No |
| Postcode | string | No |
| Country_Code | string | No |

**TRD_INVOICE** — one row per invoice
| Column | Type | Mandatory |
|--------|------|-----------|
| Account_Number | string | Yes (shared key) |
| Invoice_Number | string | Yes |
| Invoice_Date | date | Yes |
| Invoice_Amount | numeric | Yes |
| Invoice_Type | string | No |
| Due_Date | date | No |
| Paid_Amount | numeric | No |
| Paid_Date | date | No |
| Currency | string | No |
| Payment_Terms | string | No |

### Shared key propagation
`Account_Number` is a shared key — it links TRD_CUSTOMER to TRD_INVOICE. When a source column resolves to `Account_Number` in either table, it is also propagated to the other table.

**Hollow row prevention**: a file must have ≥2 directly mapped (non-propagated) columns for a canonical table to be written. A customer file that only has `Account_Number` will NOT produce TRD_INVOICE rows even though `Account_Number` propagates there.

---

## 14. System Tables Schema

Defined in `global_system_tables.json`. Present in every domain.

### RECORD_EXCEPTIONS
One row per DQ failure per source record.

| Column | Description |
|--------|-------------|
| Exception_ID | UUID, auto-generated |
| Job_ID | Links to job |
| Source_Filename | Original file |
| Source_Row_Index | 1-based row number in source file |
| Source_Column_Name | Exact column header from source |
| Canonical_Table / Column | Where it was mapped to |
| Raw_Value | Exact failing value as string |
| Exception_Type | `MANDATORY_NULL`, `TYPE_MISMATCH`, `DUPLICATE_KEY`, `REFERENTIAL_INTEGRITY_FAIL`, `BUSINESS_RULE_FAIL`, etc. |
| Reason | Human-readable explanation |
| Insert_Timestamp | UTC |

### COLUMN_LINEAGE
One row per source column per file.

| Column | Description |
|--------|-------------|
| Lineage_ID | UUID |
| Mapping_Reference_ID | Groups all columns for one job |
| Source_Column_Name | Original header |
| Source_Column_Normalised | After normalisation |
| Canonical_Table / Column | Mapped to |
| Match_Method | EXACT LOOKUP / FUZZY MATCH / LLM (...) / USER OVERRIDE / NO MATCH |
| Confidence_Score | 0–100 |
| Was_Mandatory | bool |
| Met_Threshold | bool |
| LLM_Reasoning | One-line explanation (LLM only) |
| Archive_Lineage_ID | FK if file came from ZIP |

### ARCHIVE_LINEAGE
One row per file extracted from a ZIP.

| Column | Description |
|--------|-------------|
| Archive_Lineage_ID | UUID, FK referenced by COLUMN_LINEAGE |
| Source_Filename | Extracted file name |
| Parent_Archive | Immediate containing ZIP |
| Root_Archive | Originally uploaded ZIP |
| Nested_Level | 0=direct, 1=top-level ZIP, 2=nested, etc. |
| Extraction_Status | SUCCESS / BLOCKED_EXTENSION / BLOCKED_ENCRYPTED / etc. |

### JOB_SUMMARY
One row per job run.

| Column | Description |
|--------|-------------|
| Job_ID | UUID |
| Job_Status | SUCCESS / SUCCESS_WITH_EXCEPTIONS / BLOCKED / FAILED |
| Files_Processed / Files_Failed | Counts |
| Total_Source_Rows | Input rows |
| Total_Canonical_Rows_Written | Output rows |
| Columns_Mapped_Exact/Fuzzy/LLM/Unmapped | Breakdown |
| Job_Start/End_Timestamp | UTC |
| Config_Version | Version from system_config |

---

## 15. UI Layer (app.py)

Built with **Gradio 4.x**. Six tabs.

### Tab 1 — Upload & Run
Main workflow tab. Key functions:

| Function | Trigger | What it does |
|----------|---------|-------------|
| `analyze_mappings()` | Analyze Mapping btn | Parses files, checks profiles, runs mapper (if needed), returns scorecard |
| `apply_scorecard_overrides()` | Save Overrides btn | Reads edited scorecard, extracts rows where Selected_Target ≠ Suggested_Target |
| `add_override_from_dropdowns()` | Add Override btn | Adds a single guardrail-selected override to active set |
| `clear_all_overrides()` | Clear All btn | Resets override text box |
| `ui_apply_suggested_profile()` | Apply Suggested btn | Loads partial-match profile's overrides into text box |
| `ui_save_profile()` | Save as Profile btn | Saves columns + overrides + full scorecard to profiles/trade/ |
| `run_pipeline_ui()` | Run Pipeline btn | Calls `run_pipeline()`, returns output paths for download |

### Profile banner states
| State | Colour | Shown when |
|-------|--------|-----------|
| Green | `#1a7a4a` | EXACT profile match — overrides pre-loaded, engine skipped (if mappings cached) |
| Amber | `#b07d00` | PARTIAL match — suggestion available |
| Hidden | — | No match |

### Scorecard columns
`Source_File` | `Source_Column` | `Suggested_Target` | `Selected_Target` *(editable)* | `Match_Method` | `Confidence_Score` | `Was_Mandatory` | `Met_Threshold` | `Is_Propagated` | `LLM_Reasoning`

### Tabs 2–4 — DQ Review, Exceptions, Lineage
Read-only display of DQ report, exceptions CSV, column lineage CSV from the last run.

### Tab 5 — Profiles
Lists all saved profiles. Delete by name. Refresh loads from disk.

### Tab 6 — Downloads
Download buttons for all output files from the last run.

---

## 16. Output Files Reference

### `canonical_trade_trd_customer_<job_id>.csv`
Clean, standardised customer records. Columns = all TRD_CUSTOMER business columns + lineage audit columns (`Source_Filename`, `Mapping_Reference_ID`, `Record_Status`, `Record_Version`, `Insert_Timestamp`, `Inserted_By_Job`, etc.)

### `canonical_trade_trd_invoice_<job_id>.csv`
Clean invoice records. Same pattern.

### `exceptions_trade_<job_id>.csv`
All DQ failures. One row per failed check per source record. Use `Exception_Type` to filter.

### `column_lineage_<job_id>.csv`
Full mapping audit. Join to canonical tables via `Mapping_Reference_ID`. Shows exactly how every column was mapped, what method was used, confidence score, LLM reasoning.

### `archive_lineage_<job_id>.csv`
Present only when a ZIP was uploaded. Shows every file extracted, its nesting level, and extraction status.

### `dq_trade_<job_id>.json`
Summary statistics per canonical table per column: null counts, fill rates, type error counts. Used for dashboards.

### `job_summary_<job_id>.json`
High-level job outcome: file counts, row counts, mapping method breakdown, exception counts, timestamps.

---

## 17. Adding a New Domain

1. **Create directory**: `domains/<domain_name>/`

2. **Create `<domain>_system_config.json`**: copy from `domains/trade/` and update:
   - `domain.name`, `domain.description`, `domain.canonical_tables`
   - Add/remove business rules
   - Keep `matching`, `quality`, `ingestion`, `text_extraction` sections as-is (edit thresholds as needed)

3. **Create `<domain>_canonical_model.json`**: define your tables and columns:
   ```json
   {
     "MY_TABLE": {
       "business_columns": {
         "Column_A": {"type": "string", "mandatory": true},
         "Column_B": {"type": "numeric", "mandatory": false}
       }
     }
   }
   ```

4. **Create `<domain>_lookup_table.csv`**: map all known source aliases:
   ```
   source_alias,canonical_table,canonical_column
   col_a,MY_TABLE,Column_A
   column a,MY_TABLE,Column_A
   ```

5. **Create `<domain>_prompt.txt`**: LLM prompt template. Use `{domain}`, `{source_col}`, `{options_text}`, `{lookup_context}` placeholders.

6. **Create profiles directory**: `profiles/<domain_name>/` — empty, created automatically on first save.

7. **Add to UI**: in `app.py`, add the new domain to the `domain_dd` dropdown choices.

---

## 18. Tuning & Troubleshooting

### Matching thresholds

| Config key | Default | Effect |
|-----------|---------|--------|
| `matching.fuzzy_min_similarity` | 0.70 | Lower = more fuzzy matches but more false positives |
| `matching.fuzzy_confidence_multiplier` | 0.91 | Scales down fuzzy confidence vs exact (keep < 1.0) |
| `matching.llm_disambiguation_required_below` | 70 | Lower = less LLM disambiguation |
| `llm.confidence_accept_threshold` | 55 | Lower = accept weaker LLM suggestions |
| `quality.mandatory_threshold` | 80 | Minimum confidence to count a mandatory column as mapped |
| `matching.profile_partial_threshold` | 0.70 | Lower = more partial profile suggestions |

### Common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Column maps to wrong canonical | Lookup table has incorrect alias | Add/fix entry in `trade_lookup_table.csv` |
| Column never matches | Alias missing from lookup | Add row to lookup table |
| Hollow rows in canonical output | File has only propagated columns for that table | Expected behaviour — file doesn't actually contain that table's data |
| Profile not matching on re-run | Profile was saved from a batch (combined column set) | Delete old profile, run single file, save profile |
| Partial match showing for identical file | Profile saved with extra/missing column | Check `profiles/trade/<fp>.json` columns vs current file headers |
| LLM not called | `llm.provider` is "None" in config | Set provider to "Claude"/"OpenAI"/"Gemini" |
| LLM returns invalid JSON | LLM response format issue | Now caught by `json.JSONDecodeError` and falls back gracefully |
| Pipeline hangs on LLM | Network/API timeout | Check `llm.timeout_seconds` — default 30s; now passed to all providers |
| ZIP extraction blocked | Encrypted, oversized, or blocked extension | Check `ingestion` section; files appear in archive_lineage with BLOCKED_* status |

### DQ threshold tuning
```json
"quality": {
    "mandatory_threshold": 80,    // increase for stricter mandatory checks
    "dq_pass_fill_rate": 95,      // fill rate % considered "passing"
    "dq_warn_fill_rate": 70       // fill rate % triggering warning
}
```

### Adding a new null value
```json
"null_values": ["", "null", "N/A", "TBD", "UNKNOWN"]
```
Add any string that should be treated as null for mandatory checks and fill rate calculations.

### Adding a new business rule
```json
"business_rules": [
    {
        "rule_id": "MY_RULE",
        "description": "Column X must be >= 0",
        "severity": "FAIL"
    }
]
```
Then add the evaluation logic in `dq_engine.py` `_check_business_rules()` checking for `rule_id == "MY_RULE"`.

---

## 19. Security Model

### File upload security
- ZIP entries validated against: zip-slip, encrypted content, blocked extensions, file size, archive depth, file count
- All extraction paths resolved with `os.path.realpath()` and checked to remain within the temp directory
- Blocked entries logged to ARCHIVE_LINEAGE with specific `Extraction_Status` code

### API keys
- LLM API keys loaded from environment variables via `python-dotenv`
- Keys never logged, never stored in config files
- Required env vars: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GEMINI_API_KEY` / `GOOGLE_API_KEY`

### Config path validation
- `load_config()` constructs paths as `os.path.join(config_dir, "domains", domain)`
- Use validated, server-controlled `domain` values; do not expose raw user input as domain name

### Profile data
- Stored as plain JSON files on disk under `profiles/<domain>/`
- Contains source column names and mapping decisions only — no source data values

---

## 20. Glossary

| Term | Meaning |
|------|---------|
| **Canonical table** | Standardised output table (e.g. TRD_CUSTOMER, TRD_INVOICE) |
| **Canonical column** | Standardised output column name within a canonical table |
| **Source column** | Raw column name as it appeared in the contributor's file |
| **Alias** | Alternative source column name that maps to the same canonical column |
| **Mapping Reference ID** | UUID grouping all column mappings for one job; FK in canonical output |
| **Fingerprint** | SHA-256 hash of a file's sorted, lowercased column names; profile identity |
| **Direct column** | Column mapped to a canonical table directly (not via propagation) |
| **Propagated column** | Column added to a second table because it is a shared key |
| **direct_tables** | Set of canonical tables with ≥2 direct columns; only these produce output rows |
| **Hollow row** | A canonical row with only propagated columns and no business data |
| **EXACT match** | 100% column fingerprint match against a saved profile |
| **PARTIAL match** | Jaccard overlap ≥ threshold; profile is suggested but not auto-applied |
| **Jaccard overlap** | `|intersection| / |union|` — fraction of columns shared between two sets |
| **Job ID** | UUID4 identifying a single pipeline run |
| **Job Status** | SUCCESS / SUCCESS_WITH_EXCEPTIONS / BLOCKED / FAILED |
| **BLOCKED** | All mandatory columns either unmapped or below confidence threshold |
| **LLM disambiguation** | LLM called to resolve a fuzzy match with low confidence |
| **LLM fallback** | LLM called when no exact or fuzzy match exists at all |
| **Mandatory threshold** | Minimum confidence (default 80) for a mandatory column to count as mapped |
| **Fill rate** | % of non-null values in a column; `(non_null / total) * 100` |
| **Zip-slip** | Archive attack where extracted path escapes the target directory via `../` sequences |
| **Contributor** | The organisation submitting data files |
| **Domain** | A subject area with its own canonical schema (e.g. trade, company_registration) |
