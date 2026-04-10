# UI Documentation

This document explains the Column Mapper matching method shown in the UI:

`Profile Check -> Exact -> Fuzzy -> LLM -> Override -> Propagation`

## 1) Where this runs in UI

In `Upload & Run` tab:
1. Select **one or more files** in the file picker (multi-file supported)
2. Click `1) Analyze Mapping` — parses all files, runs per-file profile check, and builds a combined scorecard with a `Source_File` column identifying which file each row comes from
3. A **profile banner** appears if a saved profile matched any uploaded file:
   - Green banner: EXACT MATCH — all columns matched; saved overrides are pre-applied automatically
   - Amber banner: PARTIAL MATCH — most columns overlapped; click `Apply Suggested` to adopt the saved overrides
4. Review and edit the scorecard directly in the **Column Mapper Scorecard** table (inline editing — click any `Selected_Target` cell to change it)
5. Use the **guardrail dropdowns** (`Source Column` + `Target Column`) and `Add Override` button to add a typo-safe override without typing free text
6. Click `Save Overrides from Scorecard` to commit all inline edits as active overrides, or `Clear All Overrides` to reset
7. Optionally enter a profile name and click `Save as Profile` to store the current column-set + overrides for automatic reuse on future similar files
8. Click `2) Run Pipeline` — processes all files sequentially under one shared `Job_ID`

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

Overrides can be applied in two ways:

**Inline scorecard editing:**
- The Column Mapper Scorecard table is directly editable
- Click any `Selected_Target` cell and type a new target (e.g. `TRD_INVOICE.Invoice_Number`)
- After editing, click `Save Overrides from Scorecard` — rows where `Selected_Target` differs from `Suggested_Target` become active overrides
- `Match_Method = USER OVERRIDE`, `Confidence_Score = 100` for those rows

**Guardrail dropdowns:**
- Use the `Source Column` dropdown (lists all source columns from uploaded files) and `Target Column` dropdown (lists all valid canonical targets) to build an override without free-text entry
- Click `Add Override` — the mapping is added directly to the active override set
- Prevents typos and ensures only valid canonical table/column combinations are accepted

When an override exists for a source column it is applied first regardless of the engine's suggestion.

### 3.6 Shared-key Propagation

After direct mappings are done:
- configured propagation rules add secondary mappings
- used for multi-target shared keys (for example `Account_Number` across related tables)
- propagated rows keep source column and confidence, and set `is_propagated = True`

**Important:** a table only appears in the canonical output if it has at least 2 columns mapped directly (non-propagated) from the source file. A single propagated shared-key column is not sufficient to trigger output for a table. This prevents hollow rows where a customer file inadvertently creates empty invoice rows (or vice versa) purely because a shared key like `Account_Number` propagates across table boundaries.

## 4) Mandatory threshold and blocking

Mandatory canonical columns are checked against `mandatory_threshold` (default `80`):
- if mandatory mapping is missing or below threshold, it is treated as blocked
- pipeline may return `BLOCKED` when auto-block is enabled

Important behaviour (single file):
- mandatory checks are enforced only for tables actively targeted by the file
- a table is considered actively targeted only when it has ≥2 non-propagated columns mapped to it
- propagation-only presence does not make a table active and does not trigger mandatory checks

Important behaviour (multi-file batch):
- blocking is evaluated **per file** — one blocked file does not prevent other files from producing output
- `Job_Status = BLOCKED` only when **all** files in the batch are blocked
- if some files are blocked and others succeed → `Job_Status = SUCCESS_WITH_EXCEPTIONS`
- the Run Log shows `X/N files blocked` so you can identify which files need override attention

## 5) How to read scorecard fields

- `Suggested_Target`: engine suggestion (`TABLE.Column` or `UNMAPPED`)
- `Selected_Target`: suggestion or your override target — **this cell is editable directly in the table**
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
- overrides matched on `Source_Column` name — if the same column name appears in multiple files, the override applies to all occurrences
- `Files: N` in the analysis message shows how many parsed sub-files were mapped (includes sub-files extracted from ZIPs)
- profile checks run **per file** — each file's column set is fingerprinted independently, so a profile saved from a 10-column customer file matches that file even when it is uploaded alongside a 12-column invoice file in the same batch

## 8) Mapping Profiles

Profiles store a file's column fingerprint and any overrides you applied, so the same decisions are automatically reused when a similar file arrives in the future.

### 8.1 How profiles are identified

Each file gets a **SHA-256 fingerprint** of its sorted, lowercased column names. Order and case are irrelevant — `Invoice_Date` in column 3 and `invoice_date` in column 7 produce the same fingerprint. File name is not part of the fingerprint.

### 8.2 Match tiers

| Tier | Condition | Behaviour |
|------|-----------|-----------|
| EXACT | 100% of columns match | Overrides pre-applied automatically; green banner shown |
| PARTIAL | ≥70% Jaccard overlap, same column count ±50% | Amber banner shown; click `Apply Suggested` to adopt overrides |
| NONE | Below threshold | No banner; fresh analysis only |

### 8.3 Saving a profile

1. After running Analyze Mapping, apply any overrides you want to save (inline edits + `Save Overrides from Scorecard`, or guardrail dropdowns + `Add Override`)
2. Type a name in the **Profile Name** field and click `Save as Profile`
3. For single-file uploads the profile is saved under the name you typed
4. For multi-file uploads one profile is saved per distinct file shape — each name is auto-suffixed with the source filename (e.g. `Trade Template — customers.csv`, `Trade Template — invoices.csv`)

### 8.4 Profiles tab (Tab 5)

- Lists all saved profiles: name, column count, times used, last used date
- Enter a profile name and click `Delete Profile` to remove it
- `Refresh` reloads the table from disk

### 8.5 Profile storage layout

```
profiles/
  trade/
    index.json          # lightweight index — fingerprint, name, col count, use stats
    3617d3b4.json       # full profile — column list + overrides dict
    ae8a1b04.json
    ...
```

Index is always loaded for exact lookup (O(1)). Partial scans pre-filter by column count (±50%) so at 1000 profiles only ~10-20 candidates are fully loaded.

### 8.6 CLI / Run Log output

Profile check results appear in the Run Log before mapping begins:

```
Checking mapping profiles...
  [PROFILE] EXACT MATCH for 'customers.csv': "Customer Master"
    Override active: tax_id -> TRD_CUSTOMER.Government_ID
  [PROFILE] PARTIAL MATCH for 'invoices_v2.csv': "Invoice Template" (12/15 cols matched)
  [PROFILE] No profile match for 'new_file.csv' — fresh analysis
```
