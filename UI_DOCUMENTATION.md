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
