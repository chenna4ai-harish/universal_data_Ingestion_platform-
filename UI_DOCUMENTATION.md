# UI Documentation

This document explains the Column Mapper matching method shown in the UI:

`Exact -> Fuzzy -> LLM -> Override -> Propagation`

## 1) Where this runs in UI

In `Upload & Run` tab:
1. Select **one or more files** in the file picker (multi-file supported)
2. Click `1) Analyze Mapping` — parses all files and builds a combined scorecard with a `Source_File` column identifying which file each row comes from
3. Review scorecard (`Source_File`, `Source_Column`, `Suggested_Target`, `Match_Method`, `Confidence_Score`)
4. Optionally apply overrides via dropdown or text box (overrides apply to all files in the batch by source column name)
5. Click `2) Run Pipeline` — processes all files sequentially under one shared `Job_ID`

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

Important behaviour (single file):
- mandatory checks are enforced only for tables actively targeted by the file
- propagation-only presence does not by itself make a table active

Important behaviour (multi-file batch):
- blocking is evaluated **per file** — one blocked file does not prevent other files from producing output
- `Job_Status = BLOCKED` only when **all** files in the batch are blocked
- if some files are blocked and others succeed → `Job_Status = SUCCESS_WITH_EXCEPTIONS`
- the Run Log shows `X/N files blocked` so you can identify which files need override attention

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

## 7) Multi-file scorecard behaviour

When multiple files are uploaded:
- `Source_File` column in the scorecard identifies which uploaded file each mapping row belongs to
- the same source column name appearing in two files produces **two separate scorecard rows** (one per file)
- overrides are matched on `Source_Column` name — if the same column name appears in multiple files, the override applies to all occurrences
- `Files: N` in the analysis message shows how many parsed sub-files were mapped (includes sub-files extracted from ZIPs)

