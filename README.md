# Universal Data Ingestion & Normalisation Platform
**Version:** POC 1.2 | **Domain:** Trade Credit Payments

A config-driven pipeline that ingests contributor files in any format, maps columns to a canonical model using exact/fuzzy/LLM matching, runs data quality checks, and produces standardised outputs with full lineage.

---

## Table of Contents
1. [Project Structure](#1-project-structure)
2. [Architecture Overview](#2-architecture-overview)
3. [End-to-End Flow](#3-end-to-end-flow)
4. [Setup](#4-setup)
5. [Running the UI (Gradio)](#5-running-the-ui-gradio)
6. [Running via CLI](#6-running-via-cli)
7. [Multi-File Batch Ingestion](#7-multi-file-batch-ingestion)
8. [Mapping Profiles](#8-mapping-profiles)
9. [LLM Configuration](#9-llm-configuration)
10. [Config Files Reference](#10-config-files-reference)
11. [Output Files Reference](#11-output-files-reference)
12. [Testing with Sample Files](#12-testing-with-sample-files)
13. [Adding a New Domain](#13-adding-a-new-domain)
14. [Troubleshooting](#14-troubleshooting)

---

For a focused explanation of mapping logic and confidence scoring, see:
- `UI_DOCUMENTATION.md` (Column Mapper: Exact -> Fuzzy -> LLM -> Override -> Propagation)

## 1. Project Structure

```
Data_Ingestion/
│
├── app.py                          ← Gradio UI (run this to open the browser app)
├── pipeline.py                     ← CLI orchestrator (run this from terminal)
├── requirements.txt
├── .env.example                    ← Copy to .env and add API keys
│
├── engine/                         ← Core engine modules (no domain logic here)
│   ├── file_parser.py              ← ZIP unpacking, file detection, parsing
│   ├── column_mapper.py            ← Exact / fuzzy / LLM / override mapping
│   ├── dq_engine.py                ← Data quality checks
│   ├── lineage_writer.py           ← Builds lineage & exception DataFrames
│   ├── output_writer.py            ← Writes all output files
│   └── profile_store.py            ← Mapping profile save / match / apply
│
├── global_system_tables.json       ← Global system tables (shared by all domains)
├── global_prompt.txt               ← Fallback LLM prompt (used when no domain prompt exists)
│
├── domains/                        ← One subfolder per domain
│   └── trade/
│       ├── trade_system_config.json    ← Thresholds, LLM settings, DQ rules
│       ├── trade_canonical_model.json  ← Canonical tables & column specs
│       ├── trade_lookup_table.csv      ← 206 source column → canonical mappings
│       └── trade_prompt.txt            ← Domain-specific LLM prompt (overrides global)
│
├── profiles/                       ← Saved mapping profiles (auto-created)
│   └── trade/
│       ├── index.json              ← Lightweight index, always loaded
│       └── <fingerprint8>.json     ← Full profile per file shape
│
├── output/                         ← All job outputs land here (auto-created)
│   └── <YYYYMMDD>_<job_id>/        ← Per-job subfolder
├── test_data/                      ← Test files (CSV, TSV, JSON, XLSX — see §12)
└── sample_trade_pack_v1/           ← Pre-built sample files for testing
```

---

## 2. Architecture Overview

```
Uploaded File(s)  ← 1 to N files in a single interaction
      │
      ▼  Profile check per file (exact/partial/none)
┌─────────────────┐
│ profile_store   │  Fingerprints columns, matches saved profiles,
│                 │  auto-applies overrides on exact match
└────────┬────────┘
         │  pre-seeded overrides (or none)
         ▼  (outer loop — one parse call per uploaded file)
┌─────────────────┐
│   file_parser   │  Detects file type, unpacks ZIPs (nested),
│                 │  decodes encoding, parses to DataFrame
└────────┬────────┘
         │  DataFrame(s)
         ▼
┌─────────────────┐
│  column_mapper  │  1. Exact lookup  (lookup_table.csv)
│                 │  2. Fuzzy match   (difflib, ~91% max)
│                 │  3. LLM fallback  (Claude / OpenAI / Gemini)
│                 │  4. User override (from UI or --override flag)
│                 │  5. Shared-key propagation (Account_Number)
└────────┬────────┘
         │  MappingResult list + mapping_reference_id
         ▼
┌─────────────────┐
│  direct_tables  │  Only tables with ≥2 directly-mapped columns
│  filter         │  receive rows — prevents hollow records from
│                 │  shared-key propagation bleeding across tables
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    dq_engine    │  Mandatory null checks
│                 │  Type validation & coercion
│                 │  Duplicate key detection
│                 │  Referential integrity
│                 │  Business rules
└────────┬────────┘
         │  Coerced canonical DataFrames + exceptions list
         ▼
┌─────────────────────────────────┐
│  lineage_writer + output_writer │  Writes all outputs to /output
└─────────────────────────────────┘
         │
         ▼
  canonical CSVs  +  exceptions CSV  +  column lineage CSV
  archive lineage CSV  +  DQ report JSON  +  job summary JSON
          (all files share one Job_ID — unified lineage)
```

**Design principle:** The engine has zero domain-specific logic. All trade behaviour lives in the three config files. To add a new domain, create three new config files — no code changes.

---

## 3. End-to-End Flow

Each pipeline run executes these steps (for each uploaded file in batch order):

| Step | What happens |
|------|-------------|
| 1 | Receive one or more uploaded file paths; generate a single `Job_ID` for the batch |
| 2 | **Per-file profile check** — fingerprint column headers, match against `profiles/<domain>/index.json` |
| 3 | EXACT profile match → pre-seed overrides, log `[PROFILE] EXACT MATCH`, skip manual re-analysis |
| 4 | For each file: detect if direct file or ZIP archive |
| 5 | If ZIP: recursively unpack per safety policy (depth, size, extension, zip-slip) |
| 6 | Validate each entry: extension + MIME + size + blocked-extension check |
| 7 | Parse file → normalised DataFrame (CSV/TSV/Excel/JSON/XML/HTML/DOCX/PDF) |
| 8 | Normalise column names (lowercase, underscores, strip specials) |
| 9 | Run column mapping pipeline: exact → fuzzy → LLM → override → propagation |
| 10 | Compute `direct_tables` (tables with ≥2 non-propagated columns); skip hollow-row tables |
| 11 | Compute mandatory coverage; **block commit** if mandatory threshold not met |
| 12 | Project source rows into canonical tables (TRD_CUSTOMER, TRD_INVOICE) |
| 13 | Run DQ: mandatory nulls, type validation, duplicates, RI, business rules |
| 14 | Write canonical output CSVs |
| 15 | Write DQ report JSON |
| 16 | Write RECORD_EXCEPTIONS CSV |
| 17 | Write COLUMN_LINEAGE + ARCHIVE_LINEAGE CSVs |
| 18 | Write JOB_SUMMARY JSON; return status |

**Job statuses:**
- `SUCCESS` — all records written, zero exceptions
- `SUCCESS_WITH_EXCEPTIONS` — records written, some DQ failures logged; also set when some (but not all) files in a batch are blocked
- `BLOCKED` — **all** files in the batch had unresolved mandatory columns; no canonical output written
- `FAILED` — no files could be parsed at all

---

## 4. Setup

### Prerequisites
- Python 3.10+
- pip

### Install dependencies

```bash
cd Data_Ingestion
pip install -r requirements.txt
```

### (Optional) Configure LLM API keys

```bash
copy .env.example .env       # Windows
cp .env.example .env         # Mac/Linux

# Edit .env and add the key for your chosen provider
# ANTHROPIC_API_KEY=sk-ant-...
# OPENAI_API_KEY=sk-...
# GOOGLE_API_KEY=...
```

Keys are loaded automatically on startup. You can also enter them directly in the UI.

---

## 5. Running the UI (Gradio)

```bash
python app.py
```

Opens at **http://localhost:7860** in your browser automatically.

```bash
python app.py --port 7861          # Custom port
python app.py --share              # Public share link (useful for demos)
python app.py --no-browser         # Start without opening browser
```

### UI Walkthrough

**Tab 1 — Upload & Run**

| Field | Description |
|-------|-------------|
| Upload file(s) | Drag-drop or browse. Select **one or more files**. Accepts CSV, XLSX, XLS, JSON, XML, TXT, DOCX, PDF, ZIP. All files processed under one `Job_ID`. |
| Domain | `trade` (more domains added via config). One domain applies to the whole batch. |
| LLM Provider | `None` / `Claude` / `OpenAI` / `Gemini` |
| API Key | Appears when a provider is selected. Auto-populated from `.env` if set. |
| Apply LLM to | `Unmatched only` (recommended) or `All columns` |
| Advanced Settings | Sliders for mandatory threshold, fuzzy similarity, LLM accept threshold |
| Column Overrides | Force-map a source column: one per line as `Source Col = TBL.Column`. Applies across all files in the batch. |

**Two-step workflow:**
1. Click **1) Analyze Mapping** — checks for a saved profile first; if matched, overrides are pre-seeded automatically. Then parses all files and shows a combined scorecard with a `Source_File` column per row.
2. Review the scorecard. Edit **Selected_Target** cells inline to override any mapping, or use the guardrail dropdowns for typo-safe selection. Click **2) Run Pipeline**.

**Profile detection banner (appears automatically after Analyze):**
- Green banner — exact profile match, overrides pre-loaded, pipeline ready immediately
- Amber banner — partial match (≥70% columns), click "Apply Suggested Profile Overrides" to load, then re-run Analyze

**Adjust Mapping:**
- Edit `Selected_Target` in the scorecard table directly (power users who know the schema)
- Use **Source column** + **Canonical target** dropdowns then **Add Override** (typo-safe — only valid targets shown)
- Click **Save Overrides from Scorecard** to persist inline edits to the override state
- Click **Clear All Overrides** to reset

**Save as Profile:**
- After any run, enter a name and click **Save Profile** — one profile per file shape is saved under `profiles/<domain>/`
- On the next run with matching columns, overrides auto-apply

**Tab 2 — Results**
- Status badge with job metadata and mapping stats
- DQ report: fill rates per column, exception breakdown
- TRD_CUSTOMER and TRD_INVOICE preview tables
- Exceptions table

**Tab 3 — Lineage**
- Column Lineage: every source column, match method, confidence score, LLM reasoning
- Archive Lineage: populated when input is a ZIP

**Tab 4 — Run Log**
- Full pipeline log including profile match messages

**Tab 5 — Profiles**
- Lists all saved profiles (name, column count, overrides saved, uses, last used)
- Refresh and delete by 8-char ID

**Tab 6 — Downloads**
- TRD_CUSTOMER CSV, TRD_INVOICE CSV, Exceptions CSV, Column Lineage CSV, DQ Report JSON, Archive Lineage CSV, Job Summary JSON

---

## 6. Running via CLI

```bash
# Single file (backward compatible)
python pipeline.py path/to/file.csv

# Multi-file batch — all files processed under one Job_ID
python pipeline.py customer.csv invoice.xlsx --domain trade

# ZIP + flat file together
python pipeline.py batch.zip standalone.csv --domain trade

# Full options
python pipeline.py file1.csv file2.xlsx \
  --domain trade \
  --config-dir . \
  --output-dir ./output \
  --contributor-id CONTRIB001 \
  --preview

# Force a column mapping (applies to all files in the batch)
python pipeline.py file.csv --override "Billing Reference=TRD_INVOICE.Invoice_Number"

# Multiple overrides
python pipeline.py customer.csv invoice.csv \
  --override "Billing Ref=TRD_INVOICE.Invoice_Number" \
  --override "Customer Code=TRD_CUSTOMER.Account_Number"
```

**Exit codes:** `0` = SUCCESS or SUCCESS_WITH_EXCEPTIONS, `1` = BLOCKED or FAILED

### Profile log messages in CLI output

Every run logs the profile check result per file:

```
Checking mapping profiles...
  [PROFILE] EXACT MATCH for 'invoices_jan.csv': "Monthly Invoice Report"
            (ID: a3f2b1c4, 7 previous use(s), 2 override(s))
    Override active: billing_ref -> TRD_INVOICE.Invoice_Number
    Override active: customer_code -> TRD_CUSTOMER.Account_Number

  [PROFILE] PARTIAL MATCH for 'customers_q2.csv': "Customer Master" (85% overlap)
            — not auto-applied

  [PROFILE] No profile match for 'new_format.csv' — fresh analysis
```

---

## 7. Multi-File Batch Ingestion

Multiple separate files can be uploaded in a single interaction and processed under one shared `Job_ID`.

### How it works

1. **Single `Job_ID`** is generated at the start of the job. Every row in `COLUMN_LINEAGE`, `RECORD_EXCEPTIONS`, `ARCHIVE_LINEAGE`, and `JOB_SUMMARY` references this one ID.
2. **Files are parsed sequentially** in upload order.
3. **Mapping and DQ run per parsed sub-file.** Each file is mapped and quality-checked independently.
4. **Hollow-row prevention.** A file only writes rows to a canonical table if it has ≥2 non-propagated columns mapping to that table. This prevents customer files from writing hollow rows into TRD_INVOICE (and vice versa) via the shared `Account_Number` key.
5. **Outputs are unified.** After all files complete, canonical DataFrames are merged, lineage rows are accumulated, and a single set of output artefacts is written to one job folder.

### Unified outputs

| Output | Behaviour |
|--------|-----------|
| Canonical CSVs | One per canonical table; rows from all source files merged, each tagged with `Source_Filename` |
| Exceptions CSV | All exceptions from all files in one file |
| Column Lineage CSV | All mapping decisions from all files; all rows share `Job_ID` |
| Archive Lineage CSV | Populated if any uploaded file was a ZIP |
| DQ Report JSON | Merged statistics across all files |
| Job Summary JSON | Aggregate counts: `Files_Processed`, `Total_Source_Rows`, etc. |

### Blocking behaviour (per-file)

- If one file is blocked, the other files still produce canonical output.
- `Job_Status = BLOCKED` only if **all** files in the batch are blocked.
- If some files are blocked but others succeed → `Job_Status = SUCCESS_WITH_EXCEPTIONS`.

### Edge cases

| Scenario | Behaviour |
|----------|-----------|
| One file fails to parse | Logged as failed; remaining files continue |
| All files fail to parse | `Job_Status = FAILED`; `Files_Processed = 0` |
| ZIP + flat file together | ZIP extracted (archive lineage written); flat file parsed directly |
| Same column name in two files | Each file mapped independently; `COLUMN_LINEAGE` rows distinguished by `Source_Filename` |
| Customer file + invoice file | Each writes rows only to its own target table (≥2 column threshold) |

### Constraints

- All files in a batch use the **same domain config**.
- One **override set** applies to all files (matched on source column name).
- Files are processed **sequentially**.

---

## 8. Mapping Profiles

Profiles eliminate repetitive manual override work. The first time you process a file shape, save a profile. Every subsequent run with matching columns auto-applies your corrections.

### How profiles work

Each file's column headers are **fingerprinted** (SHA-256 of sorted, lowercased column names). This means:
- Column order doesn't matter — `[invoice_number, account_number]` and `[account_number, invoice_number]` are the same profile
- Filename doesn't matter — `invoices_jan.csv` and `invoices_feb.csv` with the same columns hit the same profile
- Case doesn't matter — `Invoice_Number` and `invoice_number` are treated identically

### Directory structure

```
profiles/
  trade/
    index.json          ← tiny index, always in memory — O(1) exact lookup
    a3f2b1c4.json       ← full profile (loaded only on a match)
    d7e9f012.json
  finance/              ← ready when you add a finance domain
    gl_transactions.json
```

The `profiles/<domain>/` path follows the same `domain` arg as `domains/<domain>/` — no extra config.

### Three matching tiers

| Tier | Condition | Behaviour |
|------|-----------|-----------|
| **EXACT** | Column fingerprint matches 100% | Overrides auto-applied silently. Green banner shown. Pipeline ready immediately — no analysis step needed. |
| **PARTIAL** | ≥70% column overlap (Jaccard) | Amber banner + "Apply Suggested Profile Overrides" button. Human decides. |
| **NONE** | Below threshold or no profiles | Normal fresh analysis, no banner. |

### Profile matching is per-file

In a multi-file batch, each file is matched independently:
- `customers_exact.csv` → matches "Customer Master" profile → auto-applied
- `invoices_exact.csv` → matches "Monthly Invoice Report" profile → auto-applied
- Both sets of overrides are merged before the mapper runs

Profiles are also **saved per-file** in a multi-file batch. If you upload two files and save a profile named "Monthly Batch":
- `Monthly Batch — customers_exact.csv` saved (10 columns)
- `Monthly Batch — invoices_exact.csv` saved (11 columns)

Each can then be matched independently by future uploads.

### What a profile stores

Only **human corrections** — the overrides you made after seeing the engine's suggestions. The engine still runs its full exact/fuzzy/LLM pipeline on every job. The profile just pre-seeds the corrections so you don't redo them manually.

```json
{
  "name": "Monthly Invoice Report",
  "domain": "trade",
  "columns": ["account_number", "due_date", "invoice_amount", ...],
  "overrides": {
    "billing_ref": "TRD_INVOICE.Invoice_Number",
    "cust_code": "TRD_CUSTOMER.Account_Number"
  },
  "use_count": 7,
  "created_at": "2026-01-15",
  "last_used": "2026-04-05"
}
```

### How search scales

The `index.json` is a dictionary keyed by fingerprint — exact lookup is O(1) regardless of how many profiles exist. Partial scan uses a column-count pre-filter: if your file has 11 columns, any profile with fewer than 6 or more than 16 is skipped without loading the file. At 1,000 profiles, you load at most 10–20 candidates.

### Saving a profile from the UI

1. Upload files and run **Analyze Mapping**
2. Make any override corrections
3. Enter a name in the **Profile name** field
4. Click **Save Profile**

### Managing profiles

The **Profiles tab** lists all saved profiles sorted by use count. You can refresh the list or delete a profile by its 8-char ID.

### Saving a profile from CLI

Profiles are saved from the UI. CLI runs consume profiles automatically (the profile check runs at startup and logs the result) but do not save new ones.

---

## 9. LLM Configuration

LLM is **disabled by default** (`provider = "None"` in `trade_system_config.json`).

### Enable via UI
Select a provider from the dropdown. Enter your API key or let it load from `.env`.

### Enable via config file
Edit `trade_system_config.json`:
```json
"llm": {
    "provider": "Claude",
    "model": { "Claude": "claude-sonnet-4-6" },
    "confidence_accept_threshold": 55,
    "apply_to": "unmatched_only"
}
```

### When does LLM fire?
| Scenario | Behaviour |
|----------|-----------|
| Source column has no exact or fuzzy match | LLM called (if `apply_to = "unmatched_only"`) |
| Fuzzy match confidence < `llm_disambiguation_required_below` (70) | LLM called to confirm or override |
| LLM response confidence < `confidence_accept_threshold` (55) | Column falls through to `NO MATCH` |

### How Confidence Score Works

| Stage | How the score is set |
|-------|----------------------|
| Exact lookup | Always `100` |
| Fuzzy match | `int(similarity_ratio × 100 × 0.91)` — capped at ~91, below exact |
| LLM | Model self-reports 0–100 in its JSON response |
| User override / Profile override | Always `100` |
| No match | `0` |

### Supported Models

| Provider | Tier | Model ID |
|----------|------|----------|
| **Claude** | Best | `claude-opus-4-6` |
| Claude | Balanced | `claude-sonnet-4-6` |
| Claude | Economy | `claude-haiku-4-5-20251001` |
| **OpenAI** | Best | `gpt-4o` |
| OpenAI | Economy | `gpt-4o-mini` |
| **Gemini** | Best | `gemini-2.5-pro` |
| Gemini | Balanced | `gemini-2.0-flash` |

---

## 10. Config Files Reference

### `trade_system_config.json`

| Section | Key settings |
|---------|-------------|
| `llm` | provider, model, thresholds |
| `matching` | fuzzy_min_similarity (0.7), mandatory_threshold logic, shared-key propagation rules |
| `quality` | mandatory_threshold (80), fill rate thresholds, null value list, type validation, RI rules, duplicate keys, business rules |
| `ingestion` | ZIP settings, blocked extensions, size limits |
| `output` | File name prefixes, preview row counts |

### `trade_canonical_model.json`

```
TRD_CUSTOMER             TRD_INVOICE
─────────────            ─────────────
Account_Number *         Account_Number *  ──FK──► TRD_CUSTOMER
DUNS_Number              DOE *
Government_ID            Invoice_Number *
Company_Name *           Invoice_Date *
Country_Code             Due_Date *
Address_1                Invoice_Amount *
Address_2                Payment_Terms *
City                     Paid_Date
State                    Paid_Amount
Postcode                 Invoice_Type
                         Currency

* = mandatory
```

### `trade_lookup_table.csv`
206 rows. Format: `source_variation, canonical_table, canonical_column`

### `global_system_tables.json`
Defines 4 engine-owned system tables: `RECORD_EXCEPTIONS`, `COLUMN_LINEAGE`, `ARCHIVE_LINEAGE`, `JOB_SUMMARY`.

---

## 11. Output Files Reference

```
output/
└── 20260308_<job_id>/
    ├── canonical_trade_trd_customer_<job_id>.csv
    ├── canonical_trade_trd_invoice_<job_id>.csv
    ├── exceptions_trade_<job_id>.csv
    ├── column_lineage_<job_id>.csv
    ├── archive_lineage_<job_id>.csv   (only if input was ZIP)
    ├── dq_trade_<job_id>.json
    └── job_summary_<job_id>.json
```

| File | Contents |
|------|----------|
| `canonical_trade_trd_customer_<job_id>.csv` | Normalised customer records |
| `canonical_trade_trd_invoice_<job_id>.csv` | Normalised invoice records |
| `exceptions_trade_<job_id>.csv` | All record-level DQ failures |
| `column_lineage_<job_id>.csv` | Full column mapping audit trail |
| `archive_lineage_<job_id>.csv` | ZIP extraction log |
| `dq_trade_<job_id>.json` | DQ report: fill rates, exception counts |
| `job_summary_<job_id>.json` | Job outcome: row counts, mapping stats, status |

### Exception types
| Type | Meaning |
|------|---------|
| `MANDATORY_NULL` | A mandatory column is null in a source record |
| `TYPE_MISMATCH` | Value cannot be parsed to declared type |
| `PARSE_ERROR` | File could not be parsed |
| `ENCODING_ERROR` | File encoding could not be determined |
| `ARCHIVE_ERROR` | ZIP entry was blocked or could not be extracted |
| `UNSUPPORTED_FILE_TYPE` | File extension not supported |
| `UNMAPPED_MANDATORY` | Mandatory source column could not be mapped |
| `LOW_CONFIDENCE_MAPPING` | Mapped but confidence below mandatory threshold |
| `REFERENTIAL_INTEGRITY_FAIL` | TRD_INVOICE.Account_Number not in TRD_CUSTOMER |
| `DUPLICATE_KEY` | Duplicate composite key detected |
| `BUSINESS_RULE_FAIL` | Due_Date < Invoice_Date, Paid > Invoice, invalid currency |

---

## 12. Testing with Sample Files

### test_data/ — generated test files

```bash
# All exact column names — clean 2-table batch
python pipeline.py test_data/customers_exact.csv test_data/invoices_exact.csv --domain trade

# Fuzzy headers — all columns need fuzzy matching
python pipeline.py test_data/customers_fuzzy.csv test_data/invoices_fuzzy.csv --domain trade

# DQ issues — missing mandatory values, bad dates, unmapped columns
python pipeline.py test_data/invoices_with_issues.csv --domain trade

# Mixed customer + invoice columns in one file — multi-table mapping
python pipeline.py test_data/mixed_customers_invoices.csv --domain trade

# Multi-format batch — TSV + JSON
python pipeline.py test_data/customers_european.tsv test_data/invoices_apac.json --domain trade

# XLSX files
python pipeline.py test_data/customers_exact.xlsx test_data/invoices_exact.xlsx --domain trade
```

| Test file | Format | Tests |
|-----------|--------|-------|
| `customers_exact.csv` | CSV | All columns exact lookup |
| `customers_fuzzy.csv` | CSV | Headers like `Cust No`, `Buyer Name`, `ZIP Code` → fuzzy match |
| `customers_european.tsv` | TSV | European companies, `vat_number`, `municipality` |
| `invoices_exact.csv` | CSV | All columns exact lookup, 10 rows PAID/OPEN mix |
| `invoices_fuzzy.csv` | CSV | Headers like `Buyer Acct`, `Gross Amt`, `CCY` → fuzzy |
| `invoices_with_issues.csv` | CSV | Missing mandatory values, bad date → exceptions |
| `mixed_customers_invoices.csv` | CSV | Customer + invoice columns in one file |
| `invoices_apac.json` | JSON | Short aliases (`inv_num`, `acct_no`) → exact lookup |
| `customers_exact.xlsx` | XLSX | Exact column names |
| `invoices_exact.xlsx` | XLSX | Exact column names |
| `invoices_fuzzy_headers.xlsx` | XLSX | Fuzzy headers (`Client No`, `Billing Date`, `Gross Amt`) |

**Suggested multi-file combos:**
```bash
# Clean 2-table, no exceptions expected
python pipeline.py test_data/customers_exact.csv test_data/invoices_exact.csv

# All fuzzy — check scorecard confidence scores (~80-91)
python pipeline.py test_data/customers_fuzzy.csv test_data/invoices_fuzzy.csv

# Partial-block scenario — expect SUCCESS_WITH_EXCEPTIONS
python pipeline.py test_data/invoices_exact.csv test_data/invoices_with_issues.csv

# Multi-format batch
python pipeline.py test_data/customers_european.tsv test_data/invoices_apac.json
```

### sample_trade_pack_v1/ — pre-built samples

```bash
python pipeline.py sample_trade_pack_v1/trade_contrib_01_standard.csv --preview
python pipeline.py sample_trade_pack_v1/trade_contrib_02_tsv.tsv --preview
python pipeline.py sample_trade_pack_v1/trade_contrib_10_nested_zip.zip --preview
```

**What to verify in multi-file runs:**
- All `COLUMN_LINEAGE` rows share the same `Job_ID`
- Each row has the correct `Source_Filename`
- `JOB_SUMMARY.Files_Processed` equals the number of successfully parsed files
- Canonical CSVs contain rows from all files merged — no hollow rows

---

## 13. Adding a New Domain

No code changes required. Create a domain subfolder:

```
domains/
└── insurance/
    ├── insurance_system_config.json
    ├── insurance_canonical_model.json
    ├── insurance_lookup_table.csv
    └── insurance_prompt.txt            ← optional
```

Profiles for the new domain are automatically stored under `profiles/insurance/`.

Then run:
```bash
python pipeline.py your_file.csv --domain insurance
```

---

## 14. Troubleshooting

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError: gradio` | `pip install -r requirements.txt` |
| `ModuleNotFoundError: anthropic` | `pip install anthropic` |
| Port 7860 already in use | `python app.py --port 7861` |
| Job status: BLOCKED | All files in the batch had unresolved mandatory columns. Check Run Log — it lists which mandatory columns failed. Use Column Overrides or save a profile with the correct overrides. |
| Job status: SUCCESS_WITH_EXCEPTIONS (unexpected) | Some files may have been individually blocked. Check `RECORD_EXCEPTIONS` for `UNMAPPED_MANDATORY` and which `Source_Filename` they belong to. |
| All columns show NO MATCH | Check the source file has a header row. Try enabling LLM for unmatched columns. |
| Profile not matching | The file likely has a different column set. Check the Profiles tab — if column count differs by more than 50%, it won't match. Re-save after adding/removing columns. |
| Profile matched but overrides not applied | Profile is consumed at Analyze step in the UI. For CLI, user_overrides must be passed via `--override` flags — the log shows `Override active:` for each profile override in use. |
| ZIP blocked (BLOCKED_ENCRYPTED) | The ZIP is password-protected. Decrypt before uploading. |
| PDF extraction empty | Install `pdfplumber`: `pip install pdfplumber`. For scanned PDFs: `pip install pytesseract Pillow` |
| `.env` key not loading | Ensure `.env` is in the same folder as `app.py`. Format: `ANTHROPIC_API_KEY=sk-ant-...` (no quotes). |
| Hollow rows in canonical output | Fixed in v1.2 — only tables with ≥2 directly-mapped columns receive rows. If you see this in older output, re-run with the current version. |
