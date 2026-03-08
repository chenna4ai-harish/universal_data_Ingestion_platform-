# UI Documentation

This document explains the Column Mapper matching method shown in the UI:

`Exact -> Fuzzy -> LLM -> Override -> Propagation`

## 1) Where this runs in UI

In `Upload & Run` tab:
1. Click `1) Analyze Mapping`
2. Review scorecard (`Source_Column`, `Suggested_Target`, `Match_Method`, `Confidence_Score`)
3. Optionally apply override via dropdown/text
4. Click `2) Run Pipeline`

The same mapping engine is used in both Analyze and Run steps.

## 2) Execution order in code (important)

Inside `engine/column_mapper.py`, the per-column runtime priority is:
1. `USER OVERRIDE`
2. `EXACT LOOKUP`
3. `FUZZY MATCH`
4. `LLM` (disambiguation or fallback)
5. `NO MATCH`

After all columns are processed, shared-key `PROPAGATION` is applied.

Reason this differs from UI label:
- UI label describes review flow for users.
- Runtime gives highest priority to explicit user override.

## 3) Step-by-step matching method

### 3.1 Normalization

Each source column is normalized before matching:
- lowercase
- spaces/hyphens/dots to `_`
- remove special characters
- collapse repeated `_`
- trim leading/trailing `_`

Example:
- `Invoice Date` -> `invoice_date`
- `Invoice-Date` -> `invoice_date`

### 3.2 Exact Lookup

If normalized source name exists in `domains/trade/trade_lookup_table.csv`:
- map directly to configured `canonical_table.canonical_column`
- `Match_Method = EXACT LOOKUP`
- `Confidence_Score = exact_confidence` (default `100`)

### 3.3 Fuzzy Match

If exact lookup fails:
- compare normalized source column against all lookup aliases using:
  - Python `difflib.SequenceMatcher(...).ratio()`
- pick the best similarity score

Acceptance:
- similarity must be `>= fuzzy_min_similarity` (default `0.70`)

Confidence formula:
- `fuzzy_confidence = int(best_similarity * 100 * fuzzy_confidence_multiplier)`
- default multiplier is `0.91`

Meaning:
- fuzzy confidence is intentionally capped below exact confidence.
- near matches usually fall in ~`80-91`.

### 3.4 LLM stage

LLM can run in two places:

1. Fuzzy disambiguation:
- if fuzzy matched but confidence is below `llm_disambiguation_required_below` (default `70`)
- LLM can replace fuzzy result if LLM confidence passes threshold

2. LLM fallback:
- if no exact/fuzzy match
- runs when provider is enabled and `apply_to` allows it

LLM acceptance:
- require `LLM confidence >= confidence_accept_threshold` (default `55`)
- otherwise remains `NO MATCH`

### 3.5 User Override

Overrides are parsed from:
- text box lines in format:
  - `Source Column = TABLE.Column`
- dropdown apply/clear actions update the same override text

When override exists for a source column:
- it is used first
- `Match_Method = USER OVERRIDE`
- `Confidence_Score = 100`

### 3.6 Shared-key Propagation

After direct mappings are done:
- configured propagation rules add secondary mappings
- used for multi-target shared keys (for example `Account_Number` across related tables)
- propagated rows keep source column and confidence, and set `is_propagated = True`

## 4) Mandatory threshold and blocking

Mandatory canonical columns are checked against `mandatory_threshold` (default `80`):
- if mandatory mapping is missing or below threshold, it is treated as blocked
- pipeline may return `BLOCKED` when auto-block is enabled

Important behavior:
- mandatory checks are enforced only for tables actively targeted by the file
- propagation-only presence does not by itself make a table active

## 5) How to read scorecard fields

- `Suggested_Target`: engine suggestion (`TABLE.Column` or `UNMAPPED`)
- `Selected_Target`: suggestion or your override target
- `Match_Method`: `EXACT LOOKUP`, `FUZZY MATCH`, `LLM (...)`, `USER OVERRIDE`, `NO MATCH`
- `Confidence_Score`: numeric score used for threshold decisions
- `Was_Mandatory`: whether target canonical column is mandatory
- `Met_Threshold`: whether confidence satisfies mandatory threshold
- `Is_Propagated`: `True` if created by shared-key propagation
- `LLM_Reasoning`: one-line model explanation (only for LLM mappings)

## 6) Practical test guidance

To test fuzzy-heavy behavior:
- use near-match column names (typos, suffix/prefix variants)
- keep a few exact names as controls (expect confidence `100`)
- expect most near-match columns to map around `80-87` with current defaults

=======
# UI Documentation (POC)

This document explains every control in the Gradio UI and what it does in the pipeline.

## 1) Upload & Run Tab

### Upload file
- Accepts: `.csv`, `.tsv`, `.txt`, `.xlsx`, `.xls`, `.json`, `.xml`, `.html`, `.docx`, `.pdf`, `.zip`.
- ZIP files are unpacked (including nested ZIP, subject to config safety limits).

### Domain
- Current value: `trade` (POC currently exposes this single domain in the dropdown).
- Controls which files are loaded from `domains/<domain>/`.

### Contributor ID
- Example: `CONTRIB001`.
- If blank, pipeline uses `UNKNOWN`.
- Stored in canonical outputs as `Source_Contributor_ID` for lineage/audit.

### LLM Provider
- Options: `None`, `Claude`, `OpenAI`, `Gemini`.
- `None` disables LLM mapping and uses exact/fuzzy/override only.

### API Key
- Shown only when provider is not `None`.
- If entered, this key is used for the run.
- If left blank, environment key is used (from `.env`/process env).

### Apply LLM to
- `Unmatched only` (recommended): call LLM only after exact + fuzzy fail.
- `All columns`: allow wider LLM usage.

### Advanced Settings
- `Mandatory mapping threshold (%)` (default `80`)
  - Confidence threshold for mandatory mappings.
- `Fuzzy match minimum similarity (%)` (default `70`)
  - UI value is 0-100; pipeline converts to 0.0-1.0.
- `LLM accept threshold (%)` (default `55`)
  - Minimum LLM confidence score required to accept the LLM mapping.

### Column Overrides (optional)
- Purpose: force-map a source column to canonical target.
- Format: one per line, exact syntax:
  - `Source Column Name = CANONICAL_TABLE.Canonical_Column`
- Example:
  - `Billing Reference = TRD_INVOICE.Invoice_Number`
  - `Customer Code = TRD_CUSTOMER.Account_Number`
- Parsing behavior:
  - Lines missing `=` or `.` are ignored.
  - Valid overrides take highest priority over exact/fuzzy/LLM.

### Analyze Mapping button
- Step 1 action before execution.
- Parses input and runs mapping only (exact -> fuzzy -> LLM -> override -> propagation).
- Produces a scorecard with:
  - `Source_File`
  - `Source_Column`
  - `Suggested_Target`
  - `Selected_Target`
  - `Match_Method`
  - `Confidence_Score`
  - `Was_Mandatory`
  - `Met_Threshold`
  - `Is_Propagated`
  - `LLM_Reasoning`
- Enables the `Run Pipeline` button after a successful analysis.

### Dropdown mapping editor (optional)
- `Source column` dropdown: select source column from the analysis result.
- `Canonical target` dropdown: select target `TABLE.Column`.
- `Apply Selected Override`: writes/updates the override line in the textbox.
- `Clear Selected Override`: removes the override for selected source column.
- Re-run `Analyze Mapping` after changes to refresh scorecard.

### Run Pipeline button
- Step 2 action.
- Runs full pipeline using current override text.
- Updates Results, Lineage, Run Log, and Downloads tabs.
- Shows a completion banner near the run controls:
  - `Process Completed: <Status>`
  - `Job ID`
  - `Output folder`

## 2) Results Tab

- Status card:
  - Job status, row counts, mapping counts, exception counts, blocked mandatory details.
- DQ Report panel:
  - Exception summary and fill-rate status per canonical column.
- Data previews:
  - `TRD_CUSTOMER`
  - `TRD_INVOICE`
  - `RECORD_EXCEPTIONS`

## 3) Lineage Tab

- Column Lineage:
  - One row per source column mapping decision.
  - Includes method, confidence, LLM reasoning (if LLM was used).
- Archive Lineage:
  - Populated when uploaded input is ZIP.
  - Shows archive extraction details and statuses.

## 4) Run Log Tab

- Text log from pipeline execution:
  - config load
  - parse summary
  - per-file processing
  - output paths
  - final status

## 5) Download Outputs Tab

Downloads from the latest run:
- `TRD_CUSTOMER CSV`
- `TRD_INVOICE CSV`
- `Exceptions CSV`
- `Column Lineage CSV`
- `DQ Report JSON`

## Runtime Notes

- UI settings are runtime overrides for that run only.
- They do not modify files under `domains/trade/`.
- If no file is uploaded, UI returns a "No file uploaded" error state.

## Completion and Gating Notes

- `Run Pipeline` is disabled until `Analyze Mapping` succeeds.
- Selecting a new file resets this gate and requires analysis again.

