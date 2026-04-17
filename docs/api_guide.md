# REST API Guide

## Universal Data Ingestion & Normalisation Platform

**Audience:** First-time API users — no prior knowledge of the platform internals assumed.  
**What this covers:** What the API is, how to start it, every endpoint with real request/response examples, and common patterns for integrating it into your own system.

---

## Table of Contents

1. [What the API is (and isn't)](#1-what-the-api-is-and-isnt)
2. [How it relates to the Gradio UI](#2-how-it-relates-to-the-gradio-ui)
3. [Starting the server](#3-starting-the-server)
4. [The two-step workflow](#4-the-two-step-workflow)
5. [Endpoint reference](#5-endpoint-reference)
   - [GET /api/v1/health](#get-apiv1health)
   - [POST /api/v1/jobs/analyze](#post-apiv1jobsanalyze)
   - [POST /api/v1/jobs/run](#post-apiv1jobsrun)
   - [POST /api/v1/jobs/auto](#post-apiv1jobsauto)
   - [GET /api/v1/profiles](#get-apiv1profiles)
   - [DELETE /api/v1/profiles/{id}](#delete-apiv1profilesid)
6. [Automated pipeline — the full picture](#6-automated-pipeline--the-full-picture)
7. [Field reference — run response](#7-field-reference--run-response)
8. [Supplying column overrides](#8-supplying-column-overrides)
9. [Multi-file batches](#9-multi-file-batches)
10. [Using LLM-assisted mapping](#10-using-llm-assisted-mapping)
11. [HTTP status codes](#11-http-status-codes)
12. [Interactive docs (Swagger UI)](#12-interactive-docs-swagger-ui)
13. [Testing without writing real code](#13-testing-without-writing-real-code)
14. [How it was built](#14-how-it-was-built)

---

## 1. What the API is (and isn't)

The REST API lets you **submit files and receive structured data quality results and normalised output programmatically** — without using the browser UI.

You send a file (CSV, XLSX, JSON, XML, ZIP, etc.) via HTTP.  
The platform maps its columns to the canonical data model, runs data quality checks, and returns a JSON summary.  
All canonical output CSVs and DQ reports are written to the server's `output/` directory.

**What it is NOT:**
- It does not stream data back row by row (it runs the full pipeline, then returns)
- It does not store your uploaded file — it processes it and discards the temp copy
- It is not a database query API — it is a file ingestion API

---

## 2. How it relates to the Gradio UI

The API and the Gradio UI run **on the same port, in the same process, using the same engine**.

```
http://localhost:7861/ui        → Gradio browser UI  (unchanged)
http://localhost:7861/api/v1/   → REST API
http://localhost:7861/docs      → Interactive API docs (Swagger UI)
```

Both call exactly the same `run_pipeline()` function internally.  
A file processed via API uses the same config, lookup table, and profile store as the UI.  
Profiles saved in the UI are visible and usable by the API, and vice versa.

---

## 3. Starting the server

### Normal start (UI + API together)

```bash
python server.py
```

Output:
```
============================================================
  Data Ingestion Platform — Combined Server
============================================================
  Gradio UI  : http://127.0.0.1:7861/ui
  REST API   : http://127.0.0.1:7861/api/v1
  API docs   : http://127.0.0.1:7861/docs
============================================================
```

### Options

```bash
python server.py --port 7862          # use a different port
python server.py --no-browser         # don't auto-open the browser
python server.py --reload             # auto-reload when code changes (dev mode)
```

### API only (no UI)

If you only want the REST API with no Gradio UI overhead:

```bash
uvicorn api:standalone_app --port 7861
```

---

## 4. The two-step workflow

The API mirrors the same two-step workflow as the UI:

```
Step 1 — Analyze    POST /api/v1/jobs/analyze
          ↓
          Inspect the scorecard.
          Are all mandatory columns mapped correctly?
          Do you need to add any overrides?
          ↓
Step 2 — Run        POST /api/v1/jobs/run
          ↓
          Canonical CSVs, DQ report, lineage, and job summary written to output/
```

You can skip Step 1 and go straight to `/jobs/run` if:
- You've already analyzed this file shape before and saved a profile
- You know the file's columns will map cleanly (e.g. it's a known format)

---

## 5. Endpoint reference

---

### GET /api/v1/health

A quick liveness check. Use this to confirm the server is up and config is readable.

**Request:**
```bash
curl http://localhost:7861/api/v1/health
```

**Response:**
```json
{
  "status": "ok",
  "platform": "Universal Data Ingestion & Normalisation Platform",
  "config_version": "1.2",
  "config_dir": "C:\\Users\\...\\Data_Ingestion"
}
```

| Field | Meaning |
|---|---|
| `status` | `"ok"` if config loaded successfully; starts with `"config_error:"` if not |
| `config_version` | Version string from `trade_system_config.json` |
| `config_dir` | Absolute path to the config directory on the server |

---

### POST /api/v1/jobs/analyze

**Parse files and map columns — no output is written to disk.**

Use this to preview how the platform would map your file's columns before committing to a full run. The response includes a scorecard row for every source column, telling you what it would be mapped to and with what confidence.

**Form fields:**

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `files` | file(s) | Yes | — | One or more files to analyze |
| `domain` | string | No | `trade` | Domain key |
| `overrides` | JSON string | No | `{}` | Force specific columns to specific targets (see §7) |
| `llm_provider` | string | No | `None` | `None` / `Claude` / `OpenAI` / `Gemini` |
| `llm_api_key` | string | No | `""` | API key (only if provider is not None) |
| `llm_accept_threshold` | int (0-100) | No | `55` | Minimum LLM confidence to accept a mapping |
| `mandatory_threshold` | int (0-100) | No | `80` | Minimum confidence for mandatory columns |
| `fuzzy_min_similarity` | int (0-100) | No | `70` | Minimum similarity for fuzzy matching |
| `check_profiles` | bool | No | `true` | Whether to run profile matching first |

**Request:**
```bash
curl -X POST http://localhost:7861/api/v1/jobs/analyze \
  -F "files=@invoices.csv" \
  -F "domain=trade"
```

**Response:**
```json
{
  "analyze_job_id": "ANALYZE-49cf2b1c-5436-4a9f-82f9-b28f233f7df7",
  "domain": "trade",
  "files_analyzed": 1,
  "total_columns": 12,
  "blocked_mandatory_count": 0,
  "profile_match": {
    "tier": "EXACT",
    "profile_name": "Monthly Invoice Report",
    "profile_id": "c56e97e6",
    "overlap": 1.0
  },
  "scorecard": [
    {
      "source_file": "invoices.csv",
      "source_column": "account_number",
      "source_column_normalised": "account_number",
      "suggested_target": "TRD_CUSTOMER.Account_Number",
      "match_method": "EXACT LOOKUP",
      "confidence_score": 100,
      "was_mandatory": true,
      "met_threshold": true,
      "is_propagated": false,
      "llm_reasoning": null
    },
    {
      "source_file": "invoices.csv",
      "source_column": "Billing Ref",
      "source_column_normalised": "billing_ref",
      "suggested_target": "UNMAPPED",
      "match_method": "NO MATCH",
      "confidence_score": 0,
      "was_mandatory": false,
      "met_threshold": false,
      "is_propagated": false,
      "llm_reasoning": null
    }
  ]
}
```

**What to look at in the response:**

- `blocked_mandatory_count` — if this is > 0, a `/jobs/run` would be **BLOCKED**. You need to add overrides for those columns before running.
- `scorecard` rows where `was_mandatory: true` and `met_threshold: false` — these are your problem columns.
- `profile_match.tier` — `EXACT` means a saved profile matched and overrides were auto-applied. `PARTIAL` means a near-match was found but not applied automatically. `NONE` means fresh analysis.

**Scorecard field guide:**

| Field | Meaning |
|---|---|
| `source_column` | Column name as it appears in your file |
| `suggested_target` | What the engine mapped it to (`TABLE.Column` or `UNMAPPED`) |
| `match_method` | `EXACT LOOKUP` / `FUZZY MATCH` / `LLM (Claude)` / `USER OVERRIDE` / `NO MATCH` |
| `confidence_score` | 0–100. 100 = exact match or override. ~70–91 = fuzzy. LLM-reported for LLM. |
| `was_mandatory` | `true` if this canonical column is declared mandatory in the data model |
| `met_threshold` | `true` if confidence ≥ mandatory threshold (80 by default) |
| `is_propagated` | `true` if this mapping was created by shared-key propagation (not a direct match) |
| `llm_reasoning` | One-sentence explanation from the LLM (only when LLM was used) |

---

### POST /api/v1/jobs/run

**Run the full ingestion pipeline and write all output files.**

This is the main endpoint. It parses your files, maps columns, runs data quality checks, and writes canonical CSVs, exception logs, DQ reports, and lineage files to the server's `output/` directory.

**Form fields:** Same as `/jobs/analyze`, plus:

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `contributor_id` | string | No | `UNKNOWN` | Your organisation or system identifier — stamped into every canonical row |

**Request — minimal:**
```bash
curl -X POST http://localhost:7861/api/v1/jobs/run \
  -F "files=@invoices.csv" \
  -F "domain=trade"
```

**Request — with contributor ID and an override:**
```bash
curl -X POST http://localhost:7861/api/v1/jobs/run \
  -F "files=@invoices.csv" \
  -F "domain=trade" \
  -F "contributor_id=CONTRIB001" \
  -F 'overrides={"Billing Ref": "TRD_INVOICE.Invoice_Number"}'
```

**Response:**
```json
{
  "Job_ID": "e5579bb7-0d5a-4fff-a56d-d205fa20a2b5",
  "Job_Status": "SUCCESS",
  "Job_Narrative": "Job SUCCESS: no exceptions.",
  "Domain": "trade",
  "Source_Filename": "invoices.csv",
  "Total_Source_Rows": 10,
  "Total_Canonical_Rows_Written": 10,
  "Files_Processed": 1,
  "Files_Failed": 0,
  "Total_Exceptions": 0,
  "Exceptions_Blocking": 0,
  "Exceptions_DataQuality": 0,
  "Exceptions_Informational": 0,
  "Columns_Mapped_Exact": 12,
  "Columns_Mapped_Fuzzy": 0,
  "Columns_Mapped_LLM": 0,
  "Columns_Unmapped": 0,
  "Mandatory_Columns_Blocked": 0,
  "Blocked_Mandatory_Details": [],
  "Job_Start_Timestamp": "2026-04-17T12:02:25.998509+00:00",
  "Job_End_Timestamp": "2026-04-17T12:02:26.541872+00:00",
  "Config_Version": "1.2",
  "Canonical_Model_Version": "1.3",
  "output_files": {
    "canonical": {
      "TRD_INVOICE": "C:\\...\\output\\20260417_e5579bb7\\canonical_trade_trd_invoice_e5579bb7.csv"
    },
    "exceptions": null,
    "column_lineage": "C:\\...\\output\\20260417_e5579bb7\\column_lineage_e5579bb7.csv",
    "archive_lineage": null,
    "dq_report": "C:\\...\\output\\20260417_e5579bb7\\dq_trade_e5579bb7.json",
    "job_summary": "C:\\...\\output\\20260417_e5579bb7\\job_summary_e5579bb7.json"
  }
}
```

**`Job_Status` values:**

| Value | Meaning |
|---|---|
| `SUCCESS` | All files processed, no exceptions |
| `SUCCESS_WITH_EXCEPTIONS` | Processed successfully but data quality issues were found (DQ exceptions, some files blocked) |
| `BLOCKED` | Every file in the batch was blocked — mandatory columns could not be mapped. No canonical rows written. |
| `FAILED` | Pipeline could not run — config error, all files unparseable, etc. |

**The files in `output_files` are written on the server.** If you are running the server locally, you can open them directly. If the server is remote, you would need to add a download endpoint or share the output directory via a network share — this is a planned future addition.

---

### POST /api/v1/jobs/auto

**Profile-gated automated pipeline — no human interaction required.**

This is the endpoint for scheduled jobs, system-to-system integrations, and any workflow where you need a **zero-touch, fail-fast** guarantee:

- If the file's columns **exactly match** a saved profile → it is processed in full, canonical output is written.
- If the match is **partial or missing** → the file is skipped immediately with a clear message. No partial output is written.

This means you can safely run it unattended: it will never silently produce wrong mappings because a file shape has changed.

> **Prerequisite:** You must first process the file shape once through the UI (Analyze → inspect scorecard → Save Profile), or the file will always be rejected.

**Form fields:**

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `files` | file(s) | Yes | — | One or more source files (CSV, XLSX, JSON, XML, ZIP, …) |
| `domain` | string | No | `trade` | Domain key |
| `contributor_id` | string | No | `UNKNOWN` | Stamped into every canonical row for lineage |

**Request — single file:**
```bash
curl -X POST http://localhost:7861/api/v1/jobs/auto \
  -F "files=@invoices.csv" \
  -F "domain=trade" \
  -F "contributor_id=CONTRIB001"
```

**Request — batch of files:**
```bash
curl -X POST http://localhost:7861/api/v1/jobs/auto \
  -F "files=@invoices.csv" \
  -F "files=@customers.xlsx" \
  -F "domain=trade"
```

**Response — all accepted:**
```json
{
  "accepted_count": 2,
  "rejected_count": 0,
  "file_results": [
    {
      "file": "invoices.csv",
      "status": "ACCEPTED",
      "profile_tier": "EXACT",
      "profile_name": "Monthly Invoice Report",
      "profile_id": "c56e97e6",
      "overlap": 1.0,
      "message": "Exact profile match: \"Monthly Invoice Report\" (ID: c56e97e6)"
    },
    {
      "file": "customers.xlsx",
      "status": "ACCEPTED",
      "profile_tier": "EXACT",
      "profile_name": "Customer Master",
      "profile_id": "a3f2b811",
      "overlap": 1.0,
      "message": "Exact profile match: \"Customer Master\" (ID: a3f2b811)"
    }
  ],
  "job_summary": {
    "Job_ID": "e5579bb7-0d5a-4fff-a56d-d205fa20a2b5",
    "Job_Status": "SUCCESS",
    "Job_Narrative": "Job SUCCESS: no exceptions.",
    "Total_Canonical_Rows_Written": 25,
    "output_files": { "..." : "..." }
  }
}
```

**Response — some rejected:**
```json
{
  "accepted_count": 1,
  "rejected_count": 1,
  "file_results": [
    {
      "file": "invoices.csv",
      "status": "ACCEPTED",
      "profile_tier": "EXACT",
      "profile_name": "Monthly Invoice Report",
      "profile_id": "c56e97e6",
      "overlap": 1.0,
      "message": "Exact profile match: \"Monthly Invoice Report\" (ID: c56e97e6)"
    },
    {
      "file": "new_format.csv",
      "status": "REJECTED",
      "profile_tier": "PARTIAL",
      "profile_name": "Monthly Invoice Report",
      "profile_id": "c56e97e6",
      "overlap": 0.727,
      "message": "No exact profile match. Closest profile is \"Monthly Invoice Report\" with 72% column overlap — review in the UI and save a new profile before using the automated pipeline."
    }
  ],
  "job_summary": {
    "Job_Status": "SUCCESS",
    "..." : "..."
  }
}
```

**Response — all rejected (no pipeline run):**
```json
{
  "accepted_count": 0,
  "rejected_count": 1,
  "file_results": [
    {
      "file": "unknown.csv",
      "status": "REJECTED",
      "profile_tier": "NONE",
      "profile_name": null,
      "profile_id": null,
      "overlap": null,
      "message": "No saved profile found for this file's column layout. Use the UI to run Analyze Mapping, review the scorecard, and click 'Save Profile' before using the automated pipeline."
    }
  ],
  "job_summary": null
}
```

**`file_results` field guide:**

| Field | Meaning |
|---|---|
| `file` | Filename as uploaded |
| `status` | `ACCEPTED` or `REJECTED` |
| `profile_tier` | `EXACT` — 100% column overlap. `PARTIAL` — some columns matched. `NONE` — no profile found at all. |
| `profile_name` | Name of the best-matching profile (null if NONE) |
| `profile_id` | 8-char short ID of the best-matching profile (null if NONE) |
| `overlap` | Fraction of columns that matched (1.0 = exact, 0.0–0.99 = partial, null = none) |
| `message` | Human-readable outcome — safe to display to an operator |

**Decision logic:**

```
For each file:
  ┌─ EXACT match (overlap = 1.0)
  │     → ACCEPTED — added to pipeline run
  ├─ PARTIAL match (overlap < 1.0, best guess profile exists)
  │     → REJECTED — message tells operator to save a new profile via UI
  └─ NONE (no profile at all)
        → REJECTED — message tells operator to run Analyze first via UI

After checking all files:
  ┌─ Any accepted? → run_pipeline(accepted_files_only)
  └─ None accepted? → return immediately; job_summary is null
```

**ZIP archives:**
Each file inside a ZIP is checked independently. A ZIP containing three files where two match exactly and one doesn't will produce two `ACCEPTED` and one `REJECTED` entry, and the pipeline will run for the two accepted files only.

---

### GET /api/v1/profiles

List all saved mapping profiles for a domain. Profiles are automatically matched when you call `/jobs/analyze` or `/jobs/run`.

**Request:**
```bash
curl "http://localhost:7861/api/v1/profiles?domain=trade"
```

**Response:**
```json
{
  "domain": "trade",
  "profiles": [
    {
      "id": "c56e97e6",
      "fingerprint": "c56e97e61f2cb61cfdd82d60d52e551445b02a43dde52f6ad1eef77d22502758",
      "name": "Monthly Invoice Report",
      "column_count": 11,
      "override_count": 2,
      "use_count": 5,
      "last_used": "2026-04-17"
    }
  ]
}
```

| Field | Meaning |
|---|---|
| `id` | 8-character short ID — use this for the DELETE endpoint |
| `fingerprint` | Full SHA-256 fingerprint of the column set |
| `name` | Display name set when the profile was saved (via UI or future API) |
| `column_count` | Number of columns this profile covers |
| `override_count` | Number of manual override mappings stored in this profile |
| `use_count` | How many times this profile has been auto-applied |
| `last_used` | ISO date of last use |

---

### DELETE /api/v1/profiles/{id}

Delete a saved profile by its 8-character short ID (shown in the `id` field above and in the Gradio Profiles tab).

**Request:**
```bash
curl -X DELETE "http://localhost:7861/api/v1/profiles/c56e97e6?domain=trade"
```

**Response:**
```json
{
  "deleted": true,
  "profile_id": "c56e97e6",
  "name": "Monthly Invoice Report"
}
```

If the ID is not found:
```json
{
  "detail": "Profile 'c56e97e6' not found"
}
```
HTTP status: `404`.

---

## 6. Automated pipeline — the full picture

This section explains when to use `/jobs/auto` vs. the two-step workflow, how profiles work as the gate, and how to set up a reliable scheduled job.

### When to use each endpoint

| Situation | Recommended endpoint |
|---|---|
| New file format — first time you've seen it | `/jobs/analyze` then `/jobs/run` |
| Known file format, previously profiled | `/jobs/auto` |
| Scheduled overnight batch job | `/jobs/auto` |
| System-to-system integration (no human available) | `/jobs/auto` |
| Want to inspect mappings before committing | `/jobs/analyze` |
| Want to test overrides before running | `/jobs/analyze` with `overrides` |

### The profile requirement

`/jobs/auto` requires a **saved profile** for every file it processes. A profile is a fingerprint of a file's column set — it is saved the first time you process a file through the UI:

```
First time (manual):
  1. Upload file in Gradio UI → Analyze Mapping
  2. Review the scorecard — check all mandatory columns are mapped
  3. Add any overrides for columns the engine couldn't resolve
  4. Click "Save Profile" → give it a name
  → Profile saved. Future automated runs will recognise this file shape.

Subsequent runs (automated):
  POST /api/v1/jobs/auto  (no manual steps needed)
```

You can see all saved profiles at `GET /api/v1/profiles` or in the Gradio Profiles tab.

### What "exact match" means

The platform computes a SHA-256 fingerprint of the file's column headers (lowercased, sorted alphabetically). Two files match exactly if and only if they have **exactly the same set of column names** — the order of columns in the file doesn't matter, but every column must be present with the same name (case-insensitive).

If a file arrives with one extra column, one missing column, or a renamed column — it is a partial match, and `/jobs/auto` will reject it with a message like:
```
No exact profile match. Closest profile is "Monthly Invoice Report" with 90% column overlap
— review in the UI and save a new profile before using the automated pipeline.
```
This is intentional: the platform will not silently process a changed file with an old mapping.

### Scheduling with cron / Task Scheduler

**Linux / macOS cron:**
```bash
# Run every day at 2 AM — process all CSVs in /data/inbox/
0 2 * * * curl -X POST http://localhost:7861/api/v1/jobs/auto \
  -F "files=@/data/inbox/daily_invoices.csv" \
  -F "domain=trade" \
  -F "contributor_id=CRON_JOB" \
  >> /var/log/ingestion.log 2>&1
```

**Windows Task Scheduler (PowerShell):**
```powershell
$body = @{
    domain         = "trade"
    contributor_id = "TASK_SCHEDULER"
}
$file = Get-Item "C:\inbox\daily_invoices.csv"
$response = Invoke-RestMethod -Uri "http://localhost:7861/api/v1/jobs/auto" `
    -Method Post `
    -Form @{ files = $file; domain = "trade"; contributor_id = "TASK_SCHEDULER" }

if ($response.accepted_count -gt 0 -and $response.job_summary.Job_Status -in "SUCCESS","SUCCESS_WITH_EXCEPTIONS") {
    Write-Host "OK: $($response.accepted_count) file(s) processed"
    exit 0
} else {
    Write-Host "FAILED: $($response.rejected_count) file(s) rejected"
    $response.file_results | Where-Object status -eq "REJECTED" | ForEach-Object {
        Write-Host "  $($_.file): $($_.message)"
    }
    exit 1
}
```

### Using the CLI instead of the API

If the server is not running, you can call the automated pipeline directly from the command line using `auto_pipeline.py`:

```bash
# Single file
python auto_pipeline.py data/invoices.csv

# Batch
python auto_pipeline.py data/customers.csv data/invoices.xlsx

# ZIP archive (each inner file is checked independently)
python auto_pipeline.py data/batch.zip

# Full options
python auto_pipeline.py data/invoices.csv \
    --domain trade \
    --output-dir ./output \
    --contributor-id CONTRIB001
```

**Exit codes:**
- `0` — at least one file was accepted and processed successfully
- `1` — all files were rejected, or the pipeline failed

This is suitable for shell scripts and CI/CD pipelines where you want to fail the build if the file shape has changed unexpectedly.

### Python integration example

```python
import requests

BASE = "http://localhost:7861/api/v1"

def run_automated_ingestion(file_path: str, domain: str = "trade") -> bool:
    """
    Submit a file to the automated pipeline.
    Returns True if processed successfully, False if rejected or failed.
    """
    with open(file_path, "rb") as f:
        r = requests.post(
            f"{BASE}/jobs/auto",
            files={"files": (file_path, f)},
            data={"domain": domain, "contributor_id": "MY_SYSTEM"},
        )
    r.raise_for_status()
    result = r.json()

    # Print per-file outcome
    for fr in result["file_results"]:
        status_icon = "✓" if fr["status"] == "ACCEPTED" else "✗"
        print(f"  {status_icon} {fr['file']}: {fr['message']}")

    # Check overall outcome
    job_status = (result.get("job_summary") or {}).get("Job_Status", "")
    return result["accepted_count"] > 0 and job_status in ("SUCCESS", "SUCCESS_WITH_EXCEPTIONS")


# Usage
success = run_automated_ingestion("data/invoices.csv")
if not success:
    # Alert / retry / escalate
    raise RuntimeError("Automated ingestion failed — check profile or file format")
```

---

## 7. Field reference — run response

Complete guide to every field in the `/jobs/run` response.

### Job identification
| Field | Meaning |
|---|---|
| `Job_ID` | UUID unique to this run. Use it to find output files — all artefacts include the job ID in their filename. |
| `Job_Status` | `SUCCESS` / `SUCCESS_WITH_EXCEPTIONS` / `BLOCKED` / `FAILED` |
| `Job_Narrative` | One-line human-readable summary, e.g. `"Job SUCCESS_WITH_EXCEPTIONS: 3 RI failures, 1 mandatory null."` |
| `Domain` | Domain that was processed |
| `Source_Filename` | Comma-separated list of uploaded filenames |

### Row counts
| Field | Meaning |
|---|---|
| `Total_Source_Rows` | Total rows read across all uploaded files |
| `Total_Canonical_Rows_Written` | Total rows written to canonical tables. Zero if BLOCKED. |
| `Files_Processed` | Files successfully parsed |
| `Files_Failed` | Files that could not be parsed (encrypted archives, corrupt files, etc.) |

### Exceptions
| Field | Meaning |
|---|---|
| `Total_Exceptions` | Total exception count across all tiers |
| `Exceptions_Blocking` | Mandatory nulls, unmapped mandatory columns, file parse failures |
| `Exceptions_DataQuality` | Type mismatches, duplicates, referential integrity failures, business rule failures |
| `Exceptions_Informational` | Low-severity warnings |

### Mapping summary
| Field | Meaning |
|---|---|
| `Columns_Mapped_Exact` | Columns resolved via the lookup table (highest confidence) |
| `Columns_Mapped_Fuzzy` | Columns resolved via fuzzy string matching |
| `Columns_Mapped_LLM` | Columns resolved by the LLM (when enabled) |
| `Columns_Unmapped` | Columns that could not be mapped to any canonical column |
| `Mandatory_Columns_Blocked` | Count of mandatory columns that did not meet the confidence threshold |
| `Blocked_Mandatory_Details` | List of `TABLE.Column` strings that were blocked, e.g. `["TRD_INVOICE.Invoice_Number"]` |

### Versioning
| Field | Meaning |
|---|---|
| `Config_Version` | Version of the system config file used |
| `Canonical_Model_Version` | Version of the canonical model used |
| `Job_Start_Timestamp` / `Job_End_Timestamp` | UTC ISO 8601 timestamps |

### Output files
`output_files` is a dict of paths on the server:

| Key | Content |
|---|---|
| `canonical` | Dict of `{TABLE_NAME: path}` — one CSV per canonical table |
| `exceptions` | RECORD_EXCEPTIONS CSV (null if no exceptions) |
| `column_lineage` | COLUMN_LINEAGE CSV — one row per source column per file |
| `archive_lineage` | ARCHIVE_LINEAGE CSV — only present when input was a ZIP |
| `dq_report` | Data quality report JSON |
| `job_summary` | Full job summary JSON (same content as the API response) |

---

## 8. Supplying column overrides

Overrides force a specific source column to map to a specific canonical column, bypassing the automatic mapping engine. Use them when:
- The engine maps a column incorrectly
- A column name is highly domain-specific and not in the lookup table
- The `/jobs/analyze` response shows a column as `UNMAPPED` that you know the target for

Overrides are supplied as a JSON object in the `overrides` form field:

```json
{
  "Source column name exactly as in your file": "CANONICAL_TABLE.Canonical_Column"
}
```

**Example — two overrides:**
```bash
curl -X POST http://localhost:7861/api/v1/jobs/run \
  -F "files=@invoices.csv" \
  -F "domain=trade" \
  -F 'overrides={
    "Billing Ref":   "TRD_INVOICE.Invoice_Number",
    "Client Code":   "TRD_CUSTOMER.Account_Number"
  }'
```

**Finding valid canonical targets:**  
Valid targets are any `TABLE.Column` combination in the canonical model. You can see them all in the Gradio UI's "Canonical target" dropdown after clicking Analyze, or by looking at `trade_canonical_model.json`.

For the trade domain, common targets include:

| Canonical target | Meaning |
|---|---|
| `TRD_CUSTOMER.Account_Number` | Customer account identifier (shared key) |
| `TRD_CUSTOMER.Company_Name` | Customer legal name |
| `TRD_CUSTOMER.Credit_Limit` | Approved credit limit |
| `TRD_INVOICE.Invoice_Number` | Invoice reference number |
| `TRD_INVOICE.Invoice_Date` | Date invoice was raised |
| `TRD_INVOICE.Due_Date` | Payment due date |
| `TRD_INVOICE.Invoice_Amount` | Total invoice value |
| `TRD_INVOICE.Paid_Amount` | Amount paid to date |
| `TRD_INVOICE.Currency` | ISO 4217 currency code |

---

## 9. Multi-file batches

You can submit multiple files in a single request. They are processed under one `Job_ID` and their canonical rows are merged into shared output tables.

```bash
curl -X POST http://localhost:7861/api/v1/jobs/run \
  -F "files=@customers.csv" \
  -F "files=@invoices.xlsx" \
  -F "domain=trade" \
  -F "contributor_id=CONTRIB001"
```

**Batch behaviour:**
- All files share one `Job_ID`
- Each file is parsed and mapped independently
- Canonical tables are merged after all files complete
- `Job_Status` is `BLOCKED` only if **every** file was blocked — if some succeed, status is `SUCCESS_WITH_EXCEPTIONS`
- Data quality fill rates are recalculated as weighted averages across files
- A customer-only file will not write hollow rows into `TRD_INVOICE` even though `Account_Number` propagates to both tables

---

## 10. Using LLM-assisted mapping

When some columns cannot be resolved by the lookup table or fuzzy matching, you can enable an LLM to infer the mapping. This is especially useful for novel column names not yet in the lookup table.

**Supported providers:** Claude, OpenAI, Gemini

```bash
curl -X POST http://localhost:7861/api/v1/jobs/run \
  -F "files=@invoices.csv" \
  -F "domain=trade" \
  -F "llm_provider=Claude" \
  -F "llm_api_key=sk-ant-..." \
  -F "llm_accept_threshold=55" \
  -F "mandatory_threshold=80"
```

**How it works:**
1. Exact lookup runs first — if a column matches the lookup table exactly, LLM is never called
2. Fuzzy matching runs second — if a fuzzy match scores above `fuzzy_min_similarity`, LLM is only called if the score is still below `llm_disambiguation_required_below` (70 by default)
3. LLM runs for remaining unmatched columns — if the LLM response confidence ≥ `llm_accept_threshold`, the mapping is accepted
4. NO MATCH is returned if LLM also fails or scores below threshold

The `llm_reasoning` field in the scorecard (from `/jobs/analyze`) shows the LLM's one-sentence explanation for each column it mapped.

**Note:** If you have set your API key in a `.env` file in the project root, you do not need to pass `llm_api_key` — the server picks it up automatically at startup.

---

## 11. HTTP status codes

| Code | When it occurs |
|---|---|
| `200 OK` | Request succeeded. Check `Job_Status` in the body for pipeline outcome. |
| `404 Not Found` | Profile ID not found (DELETE endpoint) |
| `422 Unprocessable Entity` | Missing required field, invalid override format, no parseable files found |
| `500 Internal Server Error` | Unexpected pipeline error — check the server logs |

**Important:** A `200` response from `/jobs/run` does not mean the data was clean. It means the API call itself succeeded. Always check `Job_Status` in the response body — it can be `BLOCKED` or `FAILED` even when HTTP is `200`.

---

## 12. Interactive docs (Swagger UI)

When the server is running, open `http://localhost:7861/docs` in your browser.

You will see a full interactive API explorer:
- Every endpoint listed with its description
- All form fields with their types, defaults, and descriptions
- A **Try it out** button — you can upload a real file and call the API directly from the browser
- The exact JSON response shape shown after each call

This is the easiest way to explore the API without writing any code.

---

## 13. Testing without writing real code

### Option A — Swagger UI (browser)
Open `http://localhost:7861/docs`, click an endpoint → **Try it out** → fill in fields → **Execute**.

### Option B — curl (command line)

```bash
# 1. Check the server is up
curl http://localhost:7861/api/v1/health

# 2. Analyze a file — see how columns will be mapped
curl -X POST http://localhost:7861/api/v1/jobs/analyze \
  -F "files=@your_file.csv" \
  -F "domain=trade"

# 3. Run the pipeline (manual — any file, with overrides if needed)
curl -X POST http://localhost:7861/api/v1/jobs/run \
  -F "files=@your_file.csv" \
  -F "domain=trade" \
  -F "contributor_id=MY_SYSTEM"

# 4. Run the automated pipeline (profile-gated — no overrides needed)
curl -X POST http://localhost:7861/api/v1/jobs/auto \
  -F "files=@your_file.csv" \
  -F "domain=trade"

# 5. See saved profiles
curl http://localhost:7861/api/v1/profiles?domain=trade
```

### Option C — Python requests

```python
import requests

BASE = "http://localhost:7861/api/v1"

# Health check
r = requests.get(f"{BASE}/health")
print(r.json())

# Analyze
with open("invoices.csv", "rb") as f:
    r = requests.post(
        f"{BASE}/jobs/analyze",
        files={"files": ("invoices.csv", f, "text/csv")},
        data={"domain": "trade"},
    )
scorecard = r.json()["scorecard"]
blocked = [row for row in scorecard if row["was_mandatory"] and not row["met_threshold"]]
print(f"Blocked mandatory columns: {len(blocked)}")
for row in blocked:
    print(f"  {row['source_column']} → {row['suggested_target']}")

# Run (only if no blocked columns, or after adding overrides)
with open("invoices.csv", "rb") as f:
    r = requests.post(
        f"{BASE}/jobs/run",
        files={"files": ("invoices.csv", f, "text/csv")},
        data={
            "domain": "trade",
            "contributor_id": "MY_SYSTEM",
        },
    )
summary = r.json()
print(f"Status : {summary['Job_Status']}")
print(f"Summary: {summary['Job_Narrative']}")
print(f"Rows   : {summary['Total_Canonical_Rows_Written']} canonical rows written")
```

---

## 14. How it was built

This section explains the architecture for anyone who wants to extend or maintain the API.

### File structure

```
Data_Ingestion/
├── pipeline.py        ← Core engine — run_pipeline(), load_config(), validate_config()
├── auto_pipeline.py   ← Profile-gated automated pipeline — run_auto_pipeline() + CLI
├── engine/
│   ├── file_parser.py     ← Parses CSV, XLSX, JSON, XML, ZIP, PDF, DOCX, ...
│   ├── column_mapper.py   ← 4-tier mapping: Exact → Fuzzy → LLM → No Match
│   ├── dq_engine.py       ← Data quality checks
│   ├── output_writer.py   ← Writes canonical CSVs, DQ report, job summary
│   ├── lineage_writer.py  ← Builds COLUMN_LINEAGE and RECORD_EXCEPTIONS
│   └── profile_store.py   ← Saves/matches mapping profiles (atomic writes)
├── app.py             ← Gradio UI (unchanged — no API code here)
├── api.py             ← FastAPI router — HTTP transport only, no business logic
└── server.py          ← Mounts api.py + app.py on one port
```

### Design principle

`api.py` contains **no business logic** — it is purely HTTP transport. Every endpoint either:
- Calls `run_pipeline()` directly (for `/jobs/run`)
- Calls the same engine functions that `app.py` calls (for `/jobs/analyze`)
- Calls `run_auto_pipeline()` from `auto_pipeline.py` (for `/jobs/auto`)

This means the API and UI are always in sync — a bug fix in `pipeline.py` benefits all three paths without any additional work.

`auto_pipeline.py` is also independently callable as a CLI (`python auto_pipeline.py file.csv`) for shell scripts and CI/CD pipelines — it uses the same `run_auto_pipeline()` function as the API endpoint.

### How Gradio and FastAPI share one port

Gradio 4+ is built on top of FastAPI/Starlette internally. The `gr.mount_gradio_app()` function registers the Gradio ASGI sub-application as a mounted route on a FastAPI app:

```python
# server.py (simplified)
from fastapi import FastAPI
import gradio as gr
from api import router
from app import build_ui

app = FastAPI()
app.include_router(router)              # REST routes at /api/v1/...
app = gr.mount_gradio_app(app, build_ui(), path="/ui")   # Gradio at /ui
```

One process, one port, both fully operational.

### Adding a new endpoint

1. Open `api.py`
2. Add a new function decorated with `@router.get(...)`, `@router.post(...)`, etc.
3. Call the relevant engine function (from `pipeline.py` or `engine/`)
4. Return a `JSONResponse`

No changes needed to `server.py`, `app.py`, or any engine file.

### File uploads

Uploaded files are saved to a temporary directory (auto-deleted after each request) with the original filename preserved (prefixed with a short UUID to avoid collisions). The pipeline receives normal file paths — it has no knowledge that the files came from HTTP.

---

*Document covers API version 1.1 — server.py + api.py introduced April 2026; auto_pipeline.py + `/jobs/auto` endpoint added April 2026.*
