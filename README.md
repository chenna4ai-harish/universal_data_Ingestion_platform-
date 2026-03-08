# Universal Data Ingestion & Normalisation Platform
**Version:** POC 1.0 | **Domain:** Trade Credit Payments

A config-driven pipeline that ingests contributor files in any format, maps columns to a canonical model using exact/fuzzy/LLM matching, runs data quality checks, and produces standardised outputs with full lineage.

---

## Table of Contents
1. [Project Structure](#1-project-structure)
2. [Architecture Overview](#2-architecture-overview)
3. [End-to-End Flow](#3-end-to-end-flow)
4. [Setup](#4-setup)
5. [Running the UI (Gradio)](#5-running-the-ui-gradio)
6. [Running via CLI](#6-running-via-cli)
7. [LLM Configuration](#7-llm-configuration)
8. [Config Files Reference](#8-config-files-reference)
9. [Output Files Reference](#9-output-files-reference)
10. [Testing with Sample Files](#10-testing-with-sample-files)
11. [Adding a New Domain](#11-adding-a-new-domain)
12. [Troubleshooting](#12-troubleshooting)

---

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
│   └── output_writer.py            ← Writes all output files
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
├── output/                         ← All job outputs land here (auto-created)
│   └── <YYYYMMDD>_<job_id>/        ← Per-job subfolder (auto-created)
├── test_data/                      ← Test files created during development
└── sample_trade_pack_v1/           ← Pre-built sample files for testing
```

---

## 2. Architecture Overview

```
Uploaded File
      │
      ▼
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
```

**Design principle:** The engine has zero domain-specific logic. All trade behaviour lives in the three config files. To add a new domain, create three new config files — no code changes.

---

## 3. End-to-End Flow

Each pipeline run executes these 15 steps:

| Step | What happens |
|------|-------------|
| 1 | Receive uploaded file path |
| 2 | Detect if direct file or ZIP archive |
| 3 | If ZIP: recursively unpack per safety policy (depth, size, extension, zip-slip) |
| 4 | Validate each entry: extension + MIME + size + blocked-extension check |
| 5 | Parse file → normalised DataFrame (CSV/TSV/Excel/JSON/XML/HTML/DOCX/PDF) |
| 6 | Normalise column names (lowercase, underscores, strip specials) |
| 7 | Run column mapping pipeline: exact → fuzzy → LLM → override → propagation |
| 8 | Compute mandatory coverage; **block commit** if mandatory threshold not met |
| 9 | Project source rows into canonical tables (TRD_CUSTOMER, TRD_INVOICE) |
| 10 | Run DQ: mandatory nulls, type validation, duplicates, RI, business rules |
| 11 | Write canonical output CSVs |
| 12 | Write DQ report JSON |
| 13 | Write RECORD_EXCEPTIONS CSV |
| 14 | Write COLUMN_LINEAGE + ARCHIVE_LINEAGE CSVs |
| 15 | Write JOB_SUMMARY JSON; return status |

**Job statuses:**
- `SUCCESS` — all records written, zero exceptions
- `SUCCESS_WITH_EXCEPTIONS` — records written, some DQ failures logged
- `BLOCKED` — mandatory columns unmapped/below threshold; no canonical output written
- `FAILED` — file could not be parsed at all

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
# Copy the example env file
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
# Custom port
python app.py --port 7861

# Generate a public share link (useful for demos)
python app.py --share

# Start without opening browser
python app.py --no-browser
```

### UI Walkthrough

**Tab 1 — Upload & Run**

| Field | Description |
|-------|-------------|
| Upload file | Drag-drop or browse. Accepts CSV, XLSX, XLS, JSON, XML, TXT, DOCX, PDF, ZIP |
| Domain | `trade` (more domains can be added via config) |
| LLM Provider | `None` / `Claude` / `OpenAI` / `Gemini` — see [LLM Configuration](#7-llm-configuration) |
| API Key | Appears when a provider is selected. Auto-populated from `.env` if set |
| Apply LLM to | `Unmatched only` (recommended) or `All columns` |
| Advanced Settings | Sliders for mandatory threshold, fuzzy similarity, LLM accept threshold |
| Column Overrides | Force-map a source column: one per line as `Source Col = TBL.Column` |

Click **Run Pipeline** → results populate the other tabs.

**Tab 2 — Results**
- Status badge with job metadata and mapping stats
- DQ report: fill rates per column, exception breakdown
- TRD_CUSTOMER preview table
- TRD_INVOICE preview table
- Exceptions table

**Tab 3 — Lineage**
- Column Lineage: every source column, match method, confidence score, LLM reasoning
- Archive Lineage: populated when input is a ZIP (extraction path, nested level, status)

**Tab 4 — Run Log**
- Full pipeline log streamed from the engine

**Tab 5 — Downloads**
- TRD_CUSTOMER CSV, TRD_INVOICE CSV, Exceptions CSV, Column Lineage CSV, DQ Report JSON

---

## 6. Running via CLI

```bash
# Basic run
python pipeline.py path/to/file.csv

# Full options
python pipeline.py path/to/file.csv \
  --domain trade \
  --config-dir . \
  --output-dir ./output \
  --contributor-id CONTRIB001 \
  --preview

# ZIP batch
python pipeline.py path/to/batch.zip --domain trade

# Force a column mapping
python pipeline.py file.csv --override "Billing Reference=TRD_INVOICE.Invoice_Number"

# Multiple overrides
python pipeline.py file.csv \
  --override "Billing Ref=TRD_INVOICE.Invoice_Number" \
  --override "Customer Code=TRD_CUSTOMER.Account_Number"
```

**Exit codes:** `0` = SUCCESS or SUCCESS_WITH_EXCEPTIONS, `1` = BLOCKED or FAILED

---

## 7. LLM Configuration

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

### LLM Prompt Structure

Prompt templates are loaded from files at runtime. The resolution order is:

| Priority | File | When used |
|----------|------|-----------|
| 1st | `domains/<domain>/<domain>_prompt.txt` | Domain-specific prompt — customise per domain |
| 2nd | `global_prompt.txt` | Shared fallback when no domain prompt exists |
| 3rd | Built-in default | If neither file is present (no files required) |

This means you can give the trade domain a prompt with trade-specific hints while other domains fall back to the generic prompt — no code changes needed.

The engine sends the resolved prompt to the model for each column that needs LLM resolution. It is the same across all three providers (Claude / OpenAI / Gemini):

```
You are a data mapping assistant for the trade domain.
Source column: "billing_ref_no"
Available canonical columns:
  - TRD_CUSTOMER.Account_Number
  - TRD_CUSTOMER.Company_Name
  - TRD_CUSTOMER.DUNS_Number
  - TRD_INVOICE.Invoice_Number
  - TRD_INVOICE.Invoice_Date
  - TRD_INVOICE.Invoice_Amount
  ... (all business columns from the canonical model)

Task: identify which canonical column this source column most likely maps to.
Respond ONLY with valid JSON:
{"canonical_table": "...", "canonical_column": "...", "confidence": 0-100, "reasoning": "one sentence"}
If no match, use {"canonical_table": null, "canonical_column": "UNMAPPED", "confidence": 0, "reasoning": "..."}
```

Key points about the prompt:
- The **domain name** is injected so the model understands context (trade, insurance, etc.)
- The **full list of canonical columns** across all tables is always provided — the model picks from this enumerated set, not free-form
- The model is instructed to return **only valid JSON** with four fields; markdown fences are stripped if the model adds them
- The `confidence` field (0–100) is **self-reported by the model** — it reflects how certain the model is that the mapping is semantically correct

### How Confidence Score Works

| Stage | How the score is set |
|-------|----------------------|
| Exact lookup | Always `100` (defined in config as `exact_confidence`) |
| Fuzzy match | `int(similarity_ratio × 100 × 0.91)` — the 0.91 multiplier caps fuzzy at ~91, below exact |
| LLM | Model self-reports 0–100 in its JSON response |
| User override | Always `100` |
| No match | `0` |

After an LLM call the engine applies two threshold checks:

1. **Accept threshold** (`confidence_accept_threshold`, default 55): if the model returns a score below this, the column is treated as `NO MATCH` even if the model named a target. This guards against low-certainty hallucinations.
2. **Mandatory threshold** (`mandatory_threshold`, default 80): if a mapped column is mandatory and its confidence is below 80, the record is flagged `LOW_CONFIDENCE_MAPPING` in RECORD_EXCEPTIONS.

The `LLM_Reasoning` field in COLUMN_LINEAGE contains the one-sentence explanation the model provided, allowing reviewers to audit every LLM decision.

### LLM output in lineage
Every LLM-mapped column gets `Match_Method = "LLM (Claude)"` and a one-sentence `LLM_Reasoning` in the COLUMN_LINEAGE output.

### Supported Models (latest as of 2025)

Configure the model in `domains/trade/trade_system_config.json` under `llm.model`:

| Provider | Tier | Model ID | Input $/1M | Output $/1M |
|----------|------|----------|-----------|------------|
| **Claude** | Best | `claude-opus-4-6` | $15.00 | $75.00 |
| Claude | Balanced | `claude-sonnet-4-6` | $3.00 | $15.00 |
| Claude | Economy | `claude-haiku-4-5-20251001` | $0.80 | $4.00 |
| **OpenAI** | Best | `gpt-4o` | $2.50 | $10.00 |
| OpenAI | Economy | `gpt-4o-mini` | $0.15 | $0.60 |
| OpenAI | Reasoning | `o3` | $10.00 | $40.00 |
| **Gemini** | Best | `gemini-2.5-pro` | $1.25 | $10.00 |
| Gemini | Balanced | `gemini-2.0-flash` | $0.10 | $0.40 |
| Gemini | Economy | `gemini-2.0-flash-lite` | $0.075 | $0.30 |

> The config ships with the **best** model per provider. Swap to a lower tier to reduce cost with no code change.

### Cost per LLM Call (with lookup context in prompt)

Each call sends ~**1,084 input tokens** (lookup aliases + canonical column list + instructions) and receives ~**80 output tokens** (JSON response).

| Model | Cost per call |
|-------|-------------|
| claude-opus-4-6 | $0.02226 |
| claude-sonnet-4-6 | $0.00445 |
| claude-haiku-4-5 | $0.00119 |
| gpt-4o | $0.00351 |
| gpt-4o-mini | $0.00021 |
| gemini-2.0-flash | $0.00014 |

> **LLM is only called when a column fails both exact lookup AND fuzzy match.** With 206 lookup entries, well-named files typically trigger 0–3 LLM calls. Most jobs cost $0.00.

### Cost Scenarios (claude-opus-4-6 — worst-case pricing)

| Scenario | Cost |
|----------|------|
| Single file, all exact matches (typical) | $0.00 |
| Single file, 3 novel column names | $0.07 |
| Single file, worst case 20 columns all need LLM | $0.45 |
| ZIP with 10 files × 3 LLM calls each | $0.67 |
| 100 files/day × 3 LLM calls | $6.68/day |
| 1,000 files/day × 3 LLM calls | $66.78/day |
| 10,000 files/day × 3 LLM calls | $667.80/day |

For high-volume production, switch to `gemini-2.0-flash` ($0.00014/call) — cost drops **~160×** at comparable quality for this structured mapping task.

### Provider SDKs
```bash
pip install anthropic          # Claude
pip install openai             # OpenAI
pip install google-generativeai  # Gemini
```

---

## 8. Config Files Reference

### `trade_system_config.json`
Controls all runtime behaviour for the trade domain.

| Section | Key settings |
|---------|-------------|
| `llm` | provider, model, thresholds |
| `matching` | fuzzy_min_similarity (0.7), mandatory_threshold logic, shared-key propagation rules |
| `quality` | mandatory_threshold (80), fill rate thresholds, null value list, type validation, RI rules, duplicate keys, business rules |
| `ingestion` | ZIP settings, blocked extensions, size limits |
| `output` | File name prefixes, preview row counts |

### `trade_canonical_model.json`
Defines the target data model for the trade domain.

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

Each table also has `audit_columns` (Record_Status, Record_Version, timestamps, job IDs) and `lineage_columns` (Source_Filename, Source_Row_Index, Mapping_Reference_ID, etc.) — all system-generated.

### `trade_lookup_table.csv`
206 rows. Format: `source_variation, canonical_table, canonical_column`

Examples:
```
inv_no,TRD_INVOICE,Invoice_Number
billing_date,TRD_INVOICE,Invoice_Date
debtor_ref,TRD_CUSTOMER,Account_Number
gross_amount,TRD_INVOICE,Invoice_Amount
```

To add a new source variation, just add a row — no code change needed.

### `global_system_tables.json`
Defines 4 engine-owned system tables used across all domains:
- `RECORD_EXCEPTIONS` — one row per DQ failure per source record
- `COLUMN_LINEAGE` — one row per source column per job
- `ARCHIVE_LINEAGE` — one row per ZIP entry extracted
- `JOB_SUMMARY` — one row per pipeline job run

---

## 9. Output Files Reference

Each job creates its own subfolder under `./output/`:

```
output/
└── 20260308_a3f2c1d4-....<job_id>/
    ├── canonical_trade_trd_customer_<job_id>.csv
    ├── canonical_trade_trd_invoice_<job_id>.csv
    ├── exceptions_trade_<job_id>.csv
    ├── column_lineage_<job_id>.csv
    ├── archive_lineage_<job_id>.csv   (only if input was ZIP)
    ├── dq_trade_<job_id>.json
    └── job_summary_<job_id>.json
```

The subfolder is named `<YYYYMMDD>_<job_id>`, making it easy to sort by date and trace back to a specific run. All files for that job are self-contained inside the folder.

| File | Contents |
|------|----------|
| `canonical_trade_trd_customer_<job_id>.csv` | Normalised customer records |
| `canonical_trade_trd_invoice_<job_id>.csv` | Normalised invoice records |
| `exceptions_trade_<job_id>.csv` | All record-level DQ failures |
| `column_lineage_<job_id>.csv` | Full column mapping audit trail — includes `LLM_Reasoning` for LLM-mapped columns |
| `archive_lineage_<job_id>.csv` | ZIP extraction log (only if input was ZIP) |
| `dq_trade_<job_id>.json` | DQ report: fill rates, exception counts per table |
| `job_summary_<job_id>.json` | High-level job outcome: row counts, mapping stats, status |

### Exception types
| Type | Meaning |
|------|---------|
| `MANDATORY_NULL` | A mandatory column is null in a source record |
| `TYPE_MISMATCH` | Value cannot be parsed to declared type (numeric/date) |
| `PARSE_ERROR` | File could not be parsed |
| `ENCODING_ERROR` | File encoding could not be determined |
| `ARCHIVE_ERROR` | ZIP entry was blocked or could not be extracted |
| `UNSUPPORTED_FILE_TYPE` | File extension not supported in POC |
| `UNMAPPED_MANDATORY` | Mandatory source column could not be mapped |
| `LOW_CONFIDENCE_MAPPING` | Mapped but confidence below mandatory threshold |
| `REFERENTIAL_INTEGRITY_FAIL` | TRD_INVOICE.Account_Number not in TRD_CUSTOMER |
| `DUPLICATE_KEY` | Duplicate composite key detected |
| `BUSINESS_RULE_FAIL` | Due_Date < Invoice_Date, Paid > Invoice, invalid currency |

---

## 10. Testing with Sample Files

Sample files are in `sample_trade_pack_v1/`. Each tests a different scenario.

```bash
# Standard CSV — clean data, all columns present
python pipeline.py sample_trade_pack_v1/trade_contrib_01_standard.csv --preview

# TSV format
python pipeline.py sample_trade_pack_v1/trade_contrib_02_tsv.tsv --preview

# Pipe-delimited TXT
python pipeline.py sample_trade_pack_v1/trade_contrib_03_pipe.txt --preview

# JSON format
python pipeline.py sample_trade_pack_v1/trade_contrib_05_json.json --preview

# ZIP with nested ZIPs
python pipeline.py sample_trade_pack_v1/trade_contrib_10_nested_zip.zip --preview
```

### Quick test via CLI
```bash
# Should produce SUCCESS_WITH_EXCEPTIONS (intentional DQ issues in test data)
python pipeline.py test_data/sample_invoices.csv --preview

# Should unpack ZIP, block .exe, process 2 CSVs
python pipeline.py test_data/test_batch.zip
```

### Expected results for `sample_invoices.csv`
- Source rows: 5
- Canonical rows: 10 (5 customer + 5 invoice via shared-key propagation)
- Exceptions: 3
  - 1× `MANDATORY_NULL` — row 5 has empty Account_Number
  - 1× `MANDATORY_NULL` — same row, Account_Number also null in TRD_INVOICE
  - 1× `BUSINESS_RULE_FAIL` — row 4's Due_Date is before Invoice_Date

---

## 11. Adding a New Domain

No code changes required. Create a domain subfolder with four files (the prompt is optional):

```
domains/
└── insurance/
    ├── insurance_system_config.json    ← copy trade version, adjust rules
    ├── insurance_canonical_model.json  ← define your canonical tables
    ├── insurance_lookup_table.csv      ← source column → canonical mappings
    └── insurance_prompt.txt            ← (optional) domain-specific LLM prompt
```

**Prompt resolution for the new domain:**
- If `domains/insurance/insurance_prompt.txt` exists → used for all LLM calls in that domain
- Otherwise → `global_prompt.txt` at the root is used as the fallback
- If neither file exists → a built-in default prompt is used (no files required)

Then run:

```bash
python pipeline.py your_file.csv --domain insurance
```

The engine picks up the new config automatically.

---

## 12. LLM vs Machine Learning Model — Design Decision

### Why not train a custom ML model for column mapping?

A trained ML classifier (e.g. XGBoost, BERT fine-tune, or a custom embedding model) is an alternative to using an LLM API for the mapping step. Here is a full comparison:

---

#### Pros of a Trained ML Model

| Advantage | Detail |
|-----------|--------|
| **Zero inference cost** | After training, predictions are free — no per-call API charges |
| **Low latency** | Local model inference: sub-millisecond per column, vs ~1–3s for an LLM API round-trip |
| **No internet dependency** | Works fully offline / air-gapped environments |
| **No vendor lock-in** | No dependency on Anthropic, OpenAI, or Google availability |
| **Deterministic** | Same input always produces same output once trained |

---

#### Cons of a Trained ML Model — Why It Is a Poor Fit Here

| Problem | Why it matters |
|---------|---------------|
| **Cold-start / no data** | You need hundreds of labelled examples per canonical column before a model is useful. On day 1 there are none. The LLM approach works immediately. |
| **Short, cryptic column names** | Column names like `inv_amt_3`, `ref_no_b`, `grss_val` have almost no signal for a text classifier. The LLM reasons about semantics across its full training corpus. |
| **Long-tail vocabulary** | Each contributor uses unique abbreviations. A 206-row lookup table already covers the known ones. ML models are poor at generalising to unseen abbreviations outside the training set. |
| **Canonical model changes break the model** | Every time a new canonical column is added (e.g. `Tax_Code`), the ML model must be fully retrained and redeployed. The LLM handles it automatically — just add the column to the JSON config. |
| **Domain shift requires retraining** | A model trained on trade data learns nothing useful for insurance or healthcare. The LLM generalises across domains out of the box. |
| **No reasoning / auditability** | An ML classifier gives a probability but no explanation. The LLM fills the `LLM_Reasoning` field with a human-readable sentence — essential for data governance and lineage audits. |
| **MLOps overhead** | Requires GPU infrastructure, experiment tracking, model versioning, A/B testing pipeline, retraining schedules, drift monitoring. The LLM requires only an API key. |
| **Training data quality** | Mislabelled training examples corrupt the model silently. LLM behaviour is visible and debuggable via the prompt. |
| **Context-free prediction** | An ML model sees only the column name string. The LLM sees the column name, the full lookup alias table, all canonical columns, and domain hints simultaneously. |

---

#### Why the Current 4-Tier Approach Is Better Than Both Pure ML and Pure LLM

```
Tier 1 — Exact Lookup       ← handles ~85% of columns, zero cost, deterministic
Tier 2 — Fuzzy Match        ← handles ~10% of columns, zero cost, fast
Tier 3 — LLM               ← handles ~5% of truly novel columns, small API cost
Tier 4 — NO MATCH          ← routes to exception for human review
```

- **Tier 1 + 2 already cover the vast majority** of real-world columns. The 206-row lookup table was built exactly to capture all known variations. Only genuinely unseen column names reach tier 3.
- **The LLM is the fallback of last resort**, not the primary mechanism. This keeps costs near zero for typical files.
- A trained ML model would need to replace all four tiers to justify its complexity. Used only as a tier-3 fallback (exactly where LLM sits today), its accuracy would be *lower* than the LLM's because it lacks world knowledge and training data for rare cases.

---

#### When a Trained ML Model Would Make Sense

Only consider training a domain-specific model if **all** of the following are true:
1. You have **>10,000 labelled column mapping examples** per domain
2. The canonical model is **stable** (rarely changes)
3. You need **sub-millisecond inference** at very high scale (millions of files/hour)
4. You have **strict data sovereignty** requirements prohibiting external API calls
5. LLM costs at your volume are genuinely unacceptable (e.g. >$10,000/day)

For a POC, and for most production deployments at reasonable scale, the LLM API approach is the correct engineering choice.

---

## 13. Troubleshooting

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError: gradio` | `pip install -r requirements.txt` |
| `ModuleNotFoundError: anthropic` | `pip install anthropic` |
| Port 7860 already in use | `python app.py --port 7861` |
| Job status: BLOCKED | Check the Run Log tab — it lists which mandatory columns failed mapping. Use Column Overrides to force-map them. |
| All columns show NO MATCH | Check the source file has a header row. Verify column names are readable. Try enabling LLM for unmatched columns. |
| ZIP blocked (BLOCKED_ENCRYPTED) | The ZIP is password-protected. Decrypt it before uploading. |
| PDF extraction empty | Install `pdfplumber`: `pip install pdfplumber`. For scanned PDFs: `pip install pytesseract Pillow` |
| `.env` key not loading | Ensure `.env` is in the same folder as `app.py` / `pipeline.py`. Key format: `ANTHROPIC_API_KEY=sk-ant-...` (no quotes). |
| FutureWarning on concat | Harmless pandas warning. Does not affect output. |
