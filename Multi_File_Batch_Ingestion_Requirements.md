# Requirement Specification: Multi-File Batch Ingestion with Shared Job Identity

**Version:** 1.0  
**Date:** 2026-03-12  
**Status:** DRAFT  
**Target:** Claude Code Implementation  

> This document covers ONLY the multi-file batch ingestion capability. No other features (transformation, referential integrity, etc.) are in scope.

---

## 1. Summary

Enable users to upload multiple separate source files in a single interaction. All files are processed under one `Job_ID`, producing unified lineage, exceptions, and canonical outputs. The feature is entirely config-driven with zero hardcoded domain, table, or column references.

---

## 2. Problem Statement

Contributors often supply related data across multiple files (e.g., one for customer records, another for invoices) that share foreign keys and must be traceable to a single ingestion event. Currently, each file requires a separate upload and pipeline run, producing separate `Job_ID`s. This breaks cross-file traceability and makes auditing impossible.

---

## 3. Critical Constraint: Config-Driven Only

This feature **MUST NOT** introduce any hardcoded references to:

- Domain names (no `"trade"`, `"insurance"`, etc. in feature logic)
- Canonical table names (no `"TRD_CUSTOMER"`, `"TRD_INVOICE"` in feature logic)
- Canonical column names (no `"Account_Number"`, `"Invoice_Number"` in feature logic)

All domain-specific behaviour is derived at runtime from the loaded config files: `system_config.json`, `canonical_model.json`, `lookup_table.csv`. The multi-file code must be completely domain-agnostic. One domain is selected per batch and all files use that single domain config.

> **Pre-existing note (not part of this feature):** `app.py` line 43 has `DOMAINS = ["trade"]` hardcoded, and line 54 has a fallback `["TRD_CUSTOMER", "TRD_INVOICE"]`. These are pre-existing issues outside this feature's scope but worth addressing separately.

---

## 4. Functional Requirements

### 4.1 FR-01: Multi-File Upload (UI)

| Field | Detail |
|-------|--------|
| **ID** | FR-01 |
| **Title** | Multi-file upload in Gradio UI |
| **Description** | The Gradio file upload component accepts multiple files in a single interaction. Users can drag-and-drop or browse to select 1 to N files. Each file can be any supported format (CSV, XLSX, JSON, XML, TXT, DOCX, PDF). ZIP files within a multi-file upload are also supported and extracted as today. |
| **Current Code** | `app.py` line 633: `gr.File()` with default `file_count="single"`. Only one file or one ZIP per upload. |
| **Required Change** | Change to `gr.File(file_count="multiple")`. Update `analyze_mappings()` and `run_ingestion()` to accept and iterate over a list of file paths. **Important:** When `file_count="multiple"`, Gradio always sends a list — even for a single file. All downstream code must handle list input. |
| **Acceptance** | User selects 2+ files. All filenames display. Clicking Analyze Mapping processes all files. |

### 4.2 FR-02: Multi-File Upload (CLI)

| Field | Detail |
|-------|--------|
| **ID** | FR-02 |
| **Title** | Multi-file argument in CLI pipeline |
| **Description** | The CLI (`pipeline.py`) accepts multiple file paths as positional arguments. All files processed under a single job. Single-file usage remains backward-compatible. |
| **Current Code** | `pipeline.py` line 611: `parser.add_argument("file")` accepts one path. `run_pipeline()` takes `file_path: str`. |
| **Required Change** | Change to `parser.add_argument("files", nargs="+")`. `run_pipeline()` accepts `file_paths: list[str] | str` with backward-compatible wrapping. Also update the file existence check at lines 621-623 to loop over all paths. |
| **Acceptance** | `python pipeline.py file1.csv file2.xlsx --domain trade` produces one job with both files. |

### 4.3 FR-03: Single Job_ID Across All Files

| Field | Detail |
|-------|--------|
| **ID** | FR-03 |
| **Title** | Shared Job_ID for batch ingestion |
| **Description** | When N files are uploaded together, a single `Job_ID` (UUID4) is generated once at the start. All `COLUMN_LINEAGE`, `RECORD_EXCEPTIONS`, `ARCHIVE_LINEAGE`, and `JOB_SUMMARY` rows reference this single `Job_ID`. |
| **Current Code** | `pipeline.py` line 295: `job_id = str(uuid.uuid4())` generated once per `run_pipeline()` call. Works for ZIP multi-file but not separate uploads. |
| **Required Change** | One `run_pipeline()` call handles ALL uploaded files. Single `Job_ID` generated at start, passed through all per-file processing. Update log line at line 307 to show all file paths. |
| **Acceptance** | Upload 3 files. All `COLUMN_LINEAGE` rows have same `Job_ID`. `JOB_SUMMARY` shows `Files_Processed=3`. |

### 4.4 FR-04: Sequential Per-File Processing

| Field | Detail |
|-------|--------|
| **ID** | FR-04 |
| **Title** | Sequential file processing within a single job |
| **Description** | Each file is processed one at a time in upload order: parse via `parse_input_file()`, then for each parsed sub-file run `_process_parsed_file()` (mapping + DQ), collect results. After ALL files complete, merge and write outputs. |
| **Current Code** | `pipeline.py` lines 358-422 already does this for sub-files from a single ZIP. The existing per-file loop at lines 404-422 iterates `parsed_files` and accumulates results. |
| **Required Change** | Add an outer loop: for each uploaded `file_path`, call `parse_input_file()`, then iterate parsed sub-files through `_process_parsed_file()`. Accumulate into same collection lists (`all_mapping_results`, `all_exceptions`, `merged_canonical`, etc.). The inner loop (lines 404-422) stays the same — it just processes a larger `all_parsed_files` list. |
| **Acceptance** | Upload customer.csv + invoice.xlsx. Log shows sequential processing. Both share same `Job_ID`. |

### 4.5 FR-05: Unified Scorecard in Analysis Step

| Field | Detail |
|-------|--------|
| **ID** | FR-05 |
| **Title** | Combined mapping scorecard across all files |
| **Description** | When Analyze Mapping runs with multiple files, all are parsed and mapped. Scorecard has rows from all files with `Source_File` column identifying origin. Users can override any mapping. |
| **Current Code** | `app.py` `analyze_mappings()` (lines 310-435) already loops over `parsed_files` and builds a combined scorecard with `Source_File` column. This works for sub-files from a ZIP. |
| **Required Change** | `analyze_mappings()` must accept a list of file paths. For each path, call `parse_input_file()`, collect all parsed sub-files, then map them all. The scorecard building logic at lines 380-414 stays the same. |
| **Acceptance** | Upload 2 files. Scorecard shows columns from both, each row labelled with its `Source_File`. |

### 4.6 FR-06: Unified Outputs

After all files are processed, the pipeline produces a single set of output artefacts under one job folder:

- **Canonical CSVs:** one per canonical table, rows merged from all source files that mapped to that table
- **Exceptions CSV:** single file, all exceptions from all source files, each tagged with `Source_Filename`
- **Column Lineage CSV:** single file, all mapping decisions from all source files, all sharing same `Job_ID`
- **Archive Lineage CSV:** populated if any uploaded file was a ZIP
- **DQ Report JSON:** merged statistics across all files (existing merge logic at pipeline.py lines 440-492 handles this)
- **Job Summary JSON:** single row with aggregate counts across all files

### 4.7 FR-07: Backward Compatibility

Single-file upload **MUST** work exactly as today. If a user uploads one file, behaviour is identical to the current implementation. No existing tests, configs, or workflows should break. The multi-file feature is purely additive.

**Important Gradio note:** When `file_count="multiple"`, Gradio sends a list even for a single file upload. The code must normalise this: if a single-element list is received, it still works through the same multi-file path — just with one file. Do not branch into separate single-file vs multi-file code paths.

### 4.8 FR-08: Job Summary Aggregation

`JOB_SUMMARY` fields must aggregate across all files in the batch:

- **Source_Filename:** Comma-separated list of all uploaded filenames, or first filename with count suffix (e.g., `"customer.csv (+1 file)"`)
- **Files_Processed:** Total count of successfully parsed files across all uploads (including sub-files from ZIPs)
- **Files_Failed:** Total count of files that could not be parsed across all uploads
- **Total_Source_Rows:** Sum of all rows from all parsed files
- **Total_Canonical_Rows_Written:** Sum of all rows written to all canonical tables
- **Total_Exceptions:** Sum of all exceptions from all files
- **Columns_Mapped_Exact/Fuzzy/LLM/Unmapped:** Sum across all files per match method

---

## 5. Technical Changes Required

### 5.1 pipeline.py Changes

#### 5.1.1 `run_pipeline()` Signature

```python
# CURRENT (line 272):
def run_pipeline(
    file_path: str,
    domain: str = "trade",
    config_dir: str = ".",
    output_dir: str = "./output",
    user_overrides: dict | None = None,
    contributor_id: str = "UNKNOWN",
    preview: bool = False,
    llm_override: dict | None = None,
    log_fn=None,
) -> dict:

# NEW:
def run_pipeline(
    file_paths: list[str] | str,   # <-- renamed, accepts list or single string
    domain: str = "trade",
    config_dir: str = ".",
    output_dir: str = "./output",
    user_overrides: dict | None = None,
    contributor_id: str = "UNKNOWN",
    preview: bool = False,
    llm_override: dict | None = None,
    log_fn=None,
) -> dict:
    # Backward compat: wrap single string in list
    if isinstance(file_paths, str):
        file_paths = [file_paths]
```

#### 5.1.2 Outer File Loop

Replace the single `parse_input_file()` call at line 359 with a loop over all file paths:

```python
# NEW: Parse all uploaded files, collecting sub-files
all_parsed_files = []
all_failed_files = []
all_archive_rows = []

for fp in file_paths:
    _log(f"\nParsing input file: '{fp}'...")
    parsed, failed, arch_rows = parse_input_file(
        file_path=fp, cfg=system_cfg, job_id=job_id
    )
    all_parsed_files.extend(parsed)
    all_failed_files.extend(failed)
    all_archive_rows.extend(arch_rows)
    _log(f"  Parsed: {len(parsed)}, Failed: {len(failed)}")

# Then feed all_parsed_files into the existing per-file processing loop (lines 404-422)
# That loop stays exactly the same — it just processes a larger list now.
```

#### 5.1.3 Log and Summary Updates

```python
# Line 307 — update log to show all files:
_log(f"Files   : {', '.join(os.path.basename(fp) for fp in file_paths)}")

# Line 512 — update Source_Filename in job summary:
source_filename_str = ", ".join(os.path.basename(fp) for fp in file_paths)
# Use this in the build_job_summary() call instead of os.path.basename(file_path)
```

#### 5.1.4 CLI Argument Change

```python
# CURRENT (line 611):
parser.add_argument("file", help="Input file path (CSV, XLSX, JSON, XML, ZIP, etc.)")

# NEW:
parser.add_argument("files", nargs="+", help="One or more input file paths")

# CURRENT (lines 621-623) — file existence check:
if not os.path.exists(args.file):
    print(f"ERROR: File not found: {args.file}")
    sys.exit(1)

# NEW:
for f in args.files:
    if not os.path.exists(f):
        print(f"ERROR: File not found: {f}")
        sys.exit(1)

# CURRENT (line 628):
summary = run_pipeline(file_path=args.file, ...)

# NEW:
summary = run_pipeline(file_paths=args.files, ...)
```

### 5.2 app.py Changes

#### 5.2.1 File Upload Component

```python
# CURRENT (line 633):
file_input = gr.File(
    label="Upload file (CSV, XLSX, JSON, XML, TXT, DOCX, PDF, ZIP)",
    file_types=[".csv", ".tsv", ".txt", ".xlsx", ".xls",
                ".json", ".xml", ".html", ".docx", ".pdf", ".zip"],
)

# NEW:
file_input = gr.File(
    label="Upload file(s) (CSV, XLSX, JSON, XML, TXT, DOCX, PDF, ZIP)",
    file_count="multiple",
    file_types=[".csv", ".tsv", ".txt", ".xlsx", ".xls",
                ".json", ".xml", ".html", ".docx", ".pdf", ".zip"],
)
```

#### 5.2.2 `analyze_mappings()` (lines 310-435)

The `uploaded_file` parameter now receives a list from Gradio:

```python
# At the top of analyze_mappings(), replace lines 331-341:
if uploaded_file is None:
    return (error response)

# Normalise to list of paths (Gradio sends list even for 1 file)
if not isinstance(uploaded_file, list):
    uploaded_file = [uploaded_file]
file_paths = [f if isinstance(f, str) else f.name for f in uploaded_file]

# Parse ALL files, collecting all parsed sub-files
all_parsed = []
all_failed = []
for fp in file_paths:
    parsed, failed, _ = parse_input_file(
        file_path=fp, cfg=system_cfg, job_id=analyze_job_id,
    )
    all_parsed.extend(parsed)
    all_failed.extend(failed)

if not all_parsed:
    # return error — same as current "not parsed_files" branch

# Map all parsed files — existing loop at lines 383-414 stays the same
# Just use all_parsed instead of parsed_files
for pf in all_parsed:
    mapping_results, _ = map_columns(...)
```

#### 5.2.3 `run_ingestion()` (lines 482-591)

Same normalisation pattern:

```python
# Replace lines 511-524:
if uploaded_file is None:
    return (empty error response)

# Normalise to list of paths
if not isinstance(uploaded_file, list):
    uploaded_file = [uploaded_file]
file_paths = [f if isinstance(f, str) else f.name for f in uploaded_file]

# Call run_pipeline with list
summary = run_pipeline(
    file_paths=file_paths,      # <-- was file_path=file_path
    domain=domain,
    config_dir=CONFIG_DIR,
    output_dir=OUTPUT_DIR,
    user_overrides=user_overrides,
    contributor_id=contributor_id or "UNKNOWN",
    llm_override=llm_override,
    log_fn=log_fn,
)
```

#### 5.2.4 `file_input.change()` Reset Handler (line 814)

The lambda `fn=lambda _:` already ignores its input, so it works with a list. No logic change needed — just verify it still resets correctly when files are added/removed.

### 5.3 Files That Need NO Changes

- **`engine/file_parser.py`:** `parse_input_file()` processes one file/ZIP at a time. Called once per file in the new outer loop. No signature change.
- **`engine/column_mapper.py`:** `map_columns()` processes one set of source columns at a time. Called per parsed sub-file as today. No change.
- **`engine/dq_engine.py`:** `run_dq()` processes one set of canonical tables at a time. Called per file as today. No change.
- **`engine/lineage_writer.py`:** Build functions receive accumulated lists. No change.
- **`engine/output_writer.py`:** Write functions receive merged DataFrames. No change.
- **Domain config files:** `system_config.json`, `canonical_model.json`, `lookup_table.csv`, `prompt.txt` — no changes. Same config processes all files in a batch.
- **`global_system_tables.json`:** No schema changes. All system tables (`COLUMN_LINEAGE`, `RECORD_EXCEPTIONS`, `ARCHIVE_LINEAGE`, `JOB_SUMMARY`) already support multi-file via `Job_ID` + `Source_Filename` columns.

---

## 6. Data Flow

When a user uploads `customer.csv` and `invoice.xlsx` together:

1. **Upload:** User selects both files in the Gradio file picker. UI displays both filenames.
2. **Analyze Mapping:** For each file, `parse_input_file()` is called. All parsed DataFrames are collected. For each DataFrame, `map_columns()` runs using the selected domain config. A unified scorecard is built with `Source_File` column distinguishing rows.
3. **Review & Override:** User reviews the combined scorecard. They can override any mapping for any file. One override set applies to all files (overrides match on source column name, which may appear in multiple files).
4. **Run Pipeline:** Single `Job_ID` generated. For each uploaded file: parse → for each sub-file: map + DQ → collect results. After all files: merge canonical DataFrames, build lineage, write all outputs to one job folder.
5. **Results:** Unified results: canonical tables with rows from all files, one exception set, one lineage table, one DQ report, one job summary. Download tab offers all output files.

---

## 7. Edge Cases

### 7.1 Partial File Failure

If one file in the batch fails to parse:
- Log the failure with filename and reason
- Record it as a failed file exception under the shared `Job_ID`
- Continue processing remaining files
- Set `Job_Status = SUCCESS_WITH_EXCEPTIONS` if at least one file succeeds

### 7.2 All Files Fail

`Job_Status = FAILED`. Write failed file exceptions and archive lineage. `Files_Processed = 0`.

### 7.3 Mixed ZIP and Flat Files

If the user uploads a ZIP and a CSV together, both are processed. The ZIP is extracted as today (producing archive lineage rows), the CSV is parsed directly. All sub-files from the ZIP plus the CSV are processed under the same `Job_ID`.

### 7.4 Duplicate Column Names Across Files

Two files may have columns with the same name (e.g., both have `"Account_Number"`). Each file is mapped independently. Same column name in different files produces separate `COLUMN_LINEAGE` rows, distinguished by `Source_Filename`. The column mapper does not need to deduplicate across files.

### 7.5 Same Canonical Table from Multiple Files

Two files may map columns to the same canonical table (e.g., two invoice files both producing rows in the invoice table). DataFrames are merged via `pd.concat()` as today (pipeline.py lines 424-429). `Source_Filename` preserves row origin.

### 7.6 Mandatory Column Blocking

Blocking applies **per-file**. One file being blocked does not prevent other files from producing output. The overall `Job_Status = BLOCKED` only if ALL files are blocked.

---

## 8. Out of Scope

These items are **NOT** part of this feature and must **NOT** be implemented:

- Cross-file referential integrity validation (e.g., verifying foreign keys match between files)
- Per-file domain selection (all files in a batch use one domain)
- Per-file override sets (one override set applies to all files)
- Parallel / concurrent file processing (files are processed sequentially)
- Transformation layer changes (transformations remain a separate downstream concern)
- New canonical tables, columns, system tables, or config fields

---

## 9. Testing Guidance

| Test | Steps | Expected |
|------|-------|----------|
| **Basic multi-file** | Upload 2 CSVs mapping to different canonical tables | Single `Job_ID` in all lineage, distinct `Source_Filename`, correct table population, `Files_Processed=2` |
| **Mixed format** | Upload CSV + XLSX with overlapping column names | Independent mapping per file, correct `Source_Filename` tagging |
| **ZIP + flat file** | Upload a ZIP (2 files inside) + one standalone CSV | All 3 processed under one `Job_ID`, archive lineage for ZIP sub-files |
| **Single file compat** | Upload one file | Identical behaviour to current implementation |
| **Partial failure** | Upload one valid CSV + one corrupt file | Corrupt logged as failed, valid processes, `Job_Status=SUCCESS_WITH_EXCEPTIONS` |
| **All fail** | Upload 2 corrupt files | `Job_Status=FAILED`, `Files_Processed=0`, exceptions recorded |

---

## 10. Implementation Order

1. **Modify `run_pipeline()`** in `pipeline.py`: accept `file_paths: list[str] | str`, add outer parsing loop. Test via CLI with 2 files.
2. **Update CLI argparse**: accept multiple positional file arguments, update file existence check. Test backward compatibility with single file.
3. **Modify `app.py` upload component**: change `gr.File` to `file_count="multiple"`. Update `analyze_mappings()` to handle list of files. Test scorecard with 2 files.
4. **Update `run_ingestion()`**: pass list of paths to `run_pipeline()`. Test full pipeline run with 2 files via UI.
5. **Update Job Summary**: handle `Source_Filename` for multiple files, verify aggregation counts.
6. **Edge case testing**: partial failure, mixed ZIP + flat, duplicate columns, same canonical table from multiple files.
