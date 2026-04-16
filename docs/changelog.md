# Changelog

## Phase 2 Post-Demo — April 2026

Summary of all significant changes made after the Phase 2 demo.  
The README reflects the current state; this document captures *what changed and why*.

---

### 1. Multi-File Batch Ingestion (`pipeline.py`)

**What changed:**  
`run_pipeline` now accepts `file_paths: list[str]` instead of a single `file_path: str`.
A `file_path` kwarg is retained for backward compatibility but resolves to the new list internally.

**Key behaviour:**
- All files in a batch share a single `Job_ID` — every lineage row is traceable to the same job.
- Files are parsed and mapped sequentially; canonical outputs are merged after all files complete.
- Job status is `BLOCKED` only if **every** file in the batch was blocked; if some succeed → `SUCCESS_WITH_EXCEPTIONS`.
- DQ reports from multiple files are merged: fill rates are recalculated as weighted averages across files rather than last-file-wins.

**CLI updated:**  
`python pipeline.py file1.csv file2.xlsx` — accepts multiple positional arguments.

---

### 2. Hollow-Row Prevention: `direct_tables` Filter (`pipeline.py`, `_build_canonical_tables`)

**Problem before:** A customer-only file contained `Account_Number`. Because `Account_Number` propagates to both `TRD_CUSTOMER` and `TRD_INVOICE` (shared-key rule), the pipeline was writing hollow rows into `TRD_INVOICE` containing only an account number and null for all other columns.

**Fix:** Before building canonical rows, `_process_parsed_file` computes a `direct_tables` set — only tables with **≥ 2 non-propagated mapped columns** are considered "active" for that file. `_build_canonical_tables` now accepts `direct_tables` and skips any table not in the set. This mirrors the same threshold used by `get_blocked_mandatory_columns`.

---

### 3. Mapping Profile System (`engine/profile_store.py`, `app.py`, `pipeline.py`)

**What was added:**  
A new `engine/profile_store.py` module that saves and matches column-mapping profiles so repeated file shapes don't require re-analysis.

**How it works:**
- A *fingerprint* is a SHA-256 digest of the sorted, lowercased column headers. Column order and case are irrelevant.
- Profiles are stored under `profiles/<domain>/` — a tiny `index.json` for O(1) exact lookup plus one `<fingerprint8>.json` per profile.
- Each profile stores the human-approved overrides **and** the full scorecard rows from the Analyze step. On an EXACT match the mapping engine is skipped entirely and the cached scorecard is restored.

**Three match tiers:**

| Tier | Condition | Effect |
|------|-----------|--------|
| EXACT | 100% column overlap | Overrides auto-applied; engine skipped; green banner shown |
| PARTIAL | ≥ 70% Jaccard overlap | Amber banner + "Apply Suggested" button; human decides |
| NONE | Below threshold | Normal fresh analysis |

**Profile partial-threshold** is configurable via `system_config["matching"]["profile_partial_threshold"]`.

**UI additions in `app.py`:**
- `analyze_mappings` now runs the profile check *before* invoking the mapping engine, per file. For EXACT matches the mapping rows come from the profile cache; `files_needing_engine` is a subset.
- Profile banner (`profile_banner` HTML component) appears above the scorecard.
- "Apply Suggested Profile Overrides" button (hidden until a partial match exists).
- **Save as Profile** section: name field + Save button. One profile per file shape; multi-file batches produce one profile per distinct file.
- **Profiles tab**: table of saved profiles sortable by use count; refresh and delete-by-ID controls.
- `ui_save_profile`, `ui_apply_suggested_profile`, `ui_delete_profile`, `_profiles_table` functions added.
- `apply_scorecard_overrides`: reads inline edits from the scorecard dataframe and promotes them to the override textbox.
- `add_override_from_dropdowns`: typo-safe alternative to direct cell editing — source and target chosen from populated dropdowns.

**Pipeline logging (`pipeline.py`):**  
`run_pipeline` now does a profile check per flat file at startup (before parsing) and logs `[PROFILE] EXACT MATCH / PARTIAL MATCH / No profile match`. Archives (ZIP) are checked after extraction.

---

### 4. Dynamic Domain Discovery (`app.py`)

**What changed:**  
`DOMAINS` is no longer a hardcoded `["trade"]` constant. `_discover_domains()` scans the `domains/` directory at startup and returns any subdirectory containing both a `_system_config.json` and `_canonical_model.json`. The domain dropdown is populated dynamically. Adding a new domain no longer requires touching `app.py`.

---

### 5. Download Outputs Expanded (`app.py`, `run_ingestion`)

**What changed:**  
`run_ingestion` now returns two additional download file handles: `dl_archive` (Archive Lineage CSV) and `dl_summary` (Job Summary JSON). Both appear in the **Download Outputs** tab.

---

### 6. What Was Not Carried Forward

The following were prototyped in an earlier session but removed in the final revision:

| Feature | Reason not included |
|---------|---------------------|
| `validate_config()` in `pipeline.py` | Deferred — config schema is stable enough for POC phase; will add when config evolves |
| Pre-run blocking summary banner in `analyze_mappings` | Replaced by the profile-first workflow; blocking is visible in the mapping scorecard (`Met_Threshold` column) |
| `_override_risk_warnings` / safer override warnings | Replaced by `add_override_from_dropdowns` (typo-safe dropdowns are the safe-override mechanism) |
| Exception narrative + tier grouping in `job_summary` | `build_job_summary` / `output_writer.py` still contain `_classify_exceptions` and `_build_exception_narrative` but `pipeline.py` does not yet pass `all_exceptions` — carries forward as a ready-to-activate stub |

---

### Files Changed Summary

| File | Status | What changed |
|------|--------|-------------|
| `engine/profile_store.py` | **New** | Full profile save/match/apply system |
| `pipeline.py` | Modified | Multi-file batch, `direct_tables` filter, profile logging, backward-compat `file_path` alias |
| `app.py` | Modified | Profile integration in `analyze_mappings`, 5 new UI functions, dynamic domain discovery, expanded downloads |
| `engine/output_writer.py` | Modified | `_classify_exceptions`, `_build_exception_narrative`, `build_job_summary` new fields (stub, not yet wired) |
| `docs/decisions.md` | **New** | Cloud-breaking assumption documentation (see Phase 3 prep) |
| `docs/changelog.md` | **New** | This file |
