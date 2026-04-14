# Demo Guide — Universal Data Ingestion Platform
> Progression: Basic → Multi-file → Automation → Format Variability → Fuzzy → DQ → LLM → Real-world → Error Handling → Scale

---

## Stage 1 — Basic Single-File Ingestion

**Objective:** Show the platform ingesting a clean, well-formatted file end-to-end in seconds.

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_01_customers_clean.csv` | CSV | 10 | 10 global customers. Exact canonical headers. All mandatory + optional fields populated. |
| `demo_01_invoices_clean.csv` | CSV | 15 | 15 invoices across those customers. Exact headers. Mix of OPEN / PAID / CREDIT types and 8 currencies. |

**Run order:** customers first (loads TRD_CUSTOMER), then invoices (populates TRD_INVOICE with FK to customers).

**What to show:**
- Zero mapping errors — 100% exact match, confidence 100 on every column
- Full canonical output: `canonical_trade_trd_customer_*.csv`, `canonical_trade_trd_invoice_*.csv`
- Column lineage CSV shows every source→canonical mapping decision
- DQ report shows PASS on all fill-rate checks
- Job status: **SUCCESS**

**CLI:**
```
python pipeline.py test_data/demo/demo_01_customers_clean.csv test_data/demo/demo_01_invoices_clean.csv --domain trade
```

---

## Stage 2 — Multi-File Batch Processing

**Objective:** Show that a single job ID unifies multiple files from different contributors into one canonical output batch.

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_02_customers_batch.csv` | CSV | 8 | 8 new international customers (Japan, Germany, Brazil, Switzerland, Denmark, India, Mexico). |
| `demo_02_invoices_batch.csv` | CSV | 12 | 12 invoices in JPY, CHF, DKK, BRL, INR, MXN — multi-currency batch. |

**What to show:**
- Single Job ID across both files — one audit trail, one exceptions file
- `Files   : demo_02_customers_batch.csv, demo_02_invoices_batch.csv` in the job header
- Canonical rows across both tables merged under one job
- Referential integrity check (invoices → customers) passes because customer records are in the same batch
- Job status: **SUCCESS**

**CLI:**
```
python pipeline.py test_data/demo/demo_02_customers_batch.csv test_data/demo/demo_02_invoices_batch.csv --domain trade
```

---

## Stage 3 — Profile Reuse (Automation / Zero-Touch Ingestion)

**Objective:** Demonstrate that once a file layout is seen and saved, the platform recognises it automatically — no human re-mapping needed.

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_03_invoices_profile_reuse.csv` | CSV | 15 | Identical column structure to Stage 1 invoice file. Completely new data (Q2 invoices). |

**Pre-requisite:** Run Stage 1 first via the UI and click **Save Profile** after the analysis step.

**What to show:**
- Log line: `[PROFILE] EXACT MATCH for 'demo_03_invoices_profile_reuse.csv': "<profile name>" (ID: ..., N previous use(s))`
- Zero analyst intervention — overrides loaded automatically
- Mapping skips re-discovery entirely → faster processing
- Job status: **SUCCESS**

**CLI:**
```
python pipeline.py test_data/demo/demo_03_invoices_profile_reuse.csv --domain trade
```

---

## Stage 4 — Schema / Format Variability

**Objective:** Show the platform ingesting the same business data in four completely different file formats without any configuration change.

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_04a_invoices_pipe.txt` | Pipe-delimited TXT | 8 | Auto-detected `|` delimiter. Same canonical headers. |
| `demo_04b_customers_semicolon.csv` | Semicolon-delimited CSV | 5 | European-style `;` delimiter. Parser auto-detects. |
| `demo_04c_invoices.tsv` | Tab-separated TSV | 8 | Tab delimiter auto-detected from `.tsv` extension and content. |
| `demo_04d_invoices.json` | JSON array | 6 | Nested JSON with lookup-variant keys (`acct_no`, `inv_num`, `ccy`). |

**What to show:**
- `Source_File_Format` column in canonical output shows: `pipe_delimited`, `semicolon_delimited`, `tsv`, `json`
- No configuration change between runs — the platform adapts
- Lineage output records the original format for full audit

**CLI (run each separately to showcase each format):**
```
python pipeline.py test_data/demo/demo_04a_invoices_pipe.txt --domain trade
python pipeline.py test_data/demo/demo_04b_customers_semicolon.csv --domain trade
python pipeline.py test_data/demo/demo_04c_invoices.tsv --domain trade
python pipeline.py test_data/demo/demo_04d_invoices.json --domain trade
```

---

## Stage 5 — Fuzzy Header Matching

**Objective:** Show that contributors don't need to use exact column names — the engine finds the best match using similarity scoring.

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_05_invoices_fuzzy.csv` | CSV | 10 | Headers with abbreviations and typos that require fuzzy matching. |

**Column mapping the engine must solve (0 exact / 12 fuzzy):**

| Source Header | Normalised | Closest Lookup Entry | Maps To | ~Confidence |
|---|---|---|---|---|
| `Acct Numbr` | `acct_numbr` | `acct_number` | `TRD_INVOICE.Account_Number` | 86 |
| `Invoice Numbr` | `invoice_numbr` | `invoice_number` | `TRD_INVOICE.Invoice_Number` | 87 |
| `Invoce Date` | `invoce_date` | `invoice_date` | `TRD_INVOICE.Invoice_Date` | 87 |
| `Du Date` | `du_date` | `due_date` | `TRD_INVOICE.Due_Date` | 84 |
| `Invoice Amnt` | `invoice_amnt` | `invoice_amount` | `TRD_INVOICE.Invoice_Amount` | 84 |
| `Payd Date` | `payd_date` | `paid_date` | `TRD_INVOICE.Paid_Date` | 80 |
| `Paid Amnt` | `paid_amnt` | `paid_amount` | `TRD_INVOICE.Paid_Amount` | 80 |
| `Payment Trms` | `payment_trms` | `payment_terms` | `TRD_INVOICE.Payment_Terms` | 87 |
| `Inv Typ` | `inv_typ` | `inv_type` | `TRD_INVOICE.Invoice_Type` | 84 |
| `Currnecy` | `currnecy` | `currency` | `TRD_INVOICE.Currency` | 79 |
| `Reportng Date` | `reportng_date` | `reporting_date` | `TRD_INVOICE.DOE` | 87 |

**What to show:**
- Column lineage shows `Match_Method = FUZZY MATCH` and confidence scores (70–95)
- Zero unmapped columns despite none of the headers being exact matches
- Canonical output is identical in structure to a clean file

**CLI:**
```
python pipeline.py test_data/demo/demo_05_invoices_fuzzy.csv --domain trade
```

---

## Stage 6 — Rule-Based DQ Transformations & Violation Detection

**Objective:** Show the engine auto-coercing messy data where possible and flagging genuine quality violations with precise exception reasons.

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_06_invoices_dq_issues.csv` | CSV | 10 | 10 rows, each with a different data quality issue. |

**Issue inventory (one per row):**

| Row | Issue | Expected Outcome |
|-----|-------|-----------------|
| 1 | Clean record | PASS — baseline comparison |
| 2 | `invoice_amount = "$28,750.50"` (currency symbol + comma) | Auto-coerced to `28750.50` — PASS |
| 3 | `paid_amount (50,000) > invoice_amount (30,000)` | FAIL — `PAID_LE_INVOICE` business rule violation |
| 4 | `due_date (2025-02-10) < invoice_date (2025-02-20)` | FAIL — `INV_DUE_GE_INV_DATE` rule violation |
| 5 | Exact duplicate of row 1 (same Account + Invoice + Date) | FAIL — `DUPLICATE_RECORD` |
| 6 | `invoice_date = "15-Jan-2025"`, `invoice_amount = "£92000.00"` | Date parsed, amount coerced — PASS with coercion note |
| 7 | `account_number` is NULL (mandatory field) | FAIL — `MANDATORY_NULL` |
| 8 | Currency symbol `€` stripped from amount fields | Auto-coerced — PASS |
| 9 | `currency = "DOLLARS"` (not ISO 4217) | WARN — `CURRENCY_ISO4217` |
| 10 | Clean record | PASS |

**What to show:**
- Exceptions CSV details every violation with `Exception_Type`, `Exception_Reason`, `Source_Row_Index`
- DQ report JSON shows per-column fill rates and statuses
- Job status: **SUCCESS_WITH_EXCEPTIONS** (data still written for clean rows; exceptions isolated)

**CLI:**
```
python pipeline.py test_data/demo/demo_06_invoices_dq_issues.csv --domain trade
```

---

## Stage 7 — LLM Intelligence (Semantic Column Mapping)

**Objective:** Show that completely non-standard, domain-specific terminology is resolved through AI reasoning when lookup and fuzzy both fail.

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_07_invoices_llm.csv` | CSV | 6 | Headers use financial/legal terminology that cannot be matched by rules alone. |

**Column mapping requiring LLM:**

| Source Header | Cannot Match Via | LLM Infers |
|---|---|---|
| `Obligor Code` | Lookup or fuzzy | `TRD_CUSTOMER.Account_Number` (obligor = debtor = customer) |
| `Transaction Reference` | Close but ambiguous | `TRD_INVOICE.Invoice_Number` |
| `Transaction Date` | Ambiguous (invoice vs paid date) | `TRD_INVOICE.Invoice_Date` |
| `Contractual Settlement Deadline` | Too long for fuzzy threshold | `TRD_INVOICE.Due_Date` |
| `Net Trade Receivable` | No lexical overlap with canonical | `TRD_INVOICE.Invoice_Amount` |
| `Remittance Confirmation Date` | No match | `TRD_INVOICE.Paid_Date` |
| `Settlement Value` | Ambiguous | `TRD_INVOICE.Paid_Amount` |
| `Commercial Settlement Terms` | Below fuzzy threshold | `TRD_INVOICE.Payment_Terms` |
| `AR Document Classification` | No match | `TRD_INVOICE.Invoice_Type` |
| `Functional Currency` | Below fuzzy threshold | `TRD_INVOICE.Currency` |
| `Data Snapshot Date` | Below fuzzy threshold | `TRD_INVOICE.DOE` |

**Pre-requisite:** Enable an LLM provider in UI settings (Claude / OpenAI / Gemini) and provide API key.

**What to show:**
- Column lineage: `Match_Method = LLM` with `LLM_Reasoning` column showing the model's explanation
- Without LLM: most columns are UNMAPPED → job blocked. With LLM: all columns resolve.
- Demonstrates the three-tier fallback: Exact → Fuzzy → LLM

**CLI (with LLM enabled in system config):**
```
python pipeline.py test_data/demo/demo_07_invoices_llm.csv --domain trade
```

---

## Stage 8 — Complex Real-World Scenario (Multi-Region ZIP Pack)

**Objective:** Show a realistic enterprise submission: one ZIP archive containing files from three global regions in three different formats, all processed atomically under one job.

| File | Contents | Format | Rows |
|------|----------|--------|------|
| `demo_08_realworld_pack.zip` | → `north_america/customers_na.csv` | CSV | 6 customers |
| | → `europe/customers_emea.csv` | CSV | 6 customers |
| | → `europe/invoices_emea.json` | JSON | 8 invoices |
| | → `asia_pacific/customers_apac.csv` | CSV | 2 customers |
| | → `asia_pacific/invoices_apac.tsv` | TSV | 6 invoices |

**Currencies covered:** GBP, EUR, CHF, SEK, DKK (EMEA); JPY, SGD (APAC); USD, CAD, MXN (NA)

**What to show:**
- Archive lineage CSV shows every file extracted from the ZIP with its nested path
- All 5 constituent files processed under one Job ID
- `Source_File_Format` varies across rows in the same canonical table (csv / json / tsv)
- Customers and invoices from 10 countries unified into one canonical dataset
- Job status: **SUCCESS**

**CLI:**
```
python pipeline.py test_data/demo/demo_08_realworld_pack.zip --domain trade
```

---

## Stage 9 — Error Handling & Graceful Degradation

**Objective:** Show the platform's two error modes: hard blocking on schema violations and soft exception logging on data quality failures.

### 9a — BLOCKED (Missing Mandatory Schema Elements)

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_09a_invoices_blocked.csv` | CSV | 5 | Uses `Internal_PO_Number` and `Cost_Centre_Code` instead of `Account_Number` and `Invoice_Number`. |

**What to show:**
- `Internal_PO_Number` and `Cost_Centre_Code` cannot be matched (no lookup, fuzzy, or semantic match) to mandatory columns `Account_Number` and `Invoice_Number`
- TRD_INVOICE is active (6 other columns mapped) but mandatory columns fail
- Log: `BLOCKED: mandatory columns not met: TRD_INVOICE.Account_Number, TRD_INVOICE.Invoice_Number`
- No canonical output written — protects data integrity
- Exceptions file still written with `Exception_Type = MANDATORY_COLUMN_UNMAPPED`
- Job status: **BLOCKED**

### 9b — SUCCESS_WITH_EXCEPTIONS (Data Quality Failures)

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_09b_invoices_dq_failures.csv` | CSV | 10 | Correct schema but catastrophically bad data values. |

**Issue inventory:**

| Row | Issue |
|-----|-------|
| 1 | Clean — baseline |
| 2 | `invoice_amount = "TBD"` — unparseable |
| 3 | Dates are `"ASAP"` and `"End of Q1"` — invalid |
| 4 | `due_date < invoice_date` AND `paid_amount > invoice_amount` — two rule violations |
| 5 | `invoice_amount = "See Attachment"` — unparseable |
| 6 | Exact duplicate of row 1 |
| 7 | `currency = "DOLLARS"` (invalid ISO code), `invoice_amount = -99999` with CREDIT type |
| 8 | `due_date < invoice_date`, `currency = "XYZ"` (invalid) |
| 9 | `invoice_amount = "N/A"` (null treatment) — mandatory null |
| 10 | Clean — passes fully |

**What to show:**
- Platform still writes rows 1 and 10 to canonical; isolates failures into exceptions CSV
- Exceptions CSV has 8 rows of detailed, actionable error descriptions
- Job status: **SUCCESS_WITH_EXCEPTIONS**

**CLI:**
```
python pipeline.py test_data/demo/demo_09a_invoices_blocked.csv --domain trade
python pipeline.py test_data/demo/demo_09b_invoices_dq_failures.csv --domain trade
```

---

## Stage 10 — Performance & Scalability

**Objective:** Demonstrate throughput at volume — 500 invoices processed in a single job.

| File | Format | Rows | Description |
|------|--------|------|-------------|
| `demo_10_invoices_large.csv` | CSV | 500 | 500 invoices across 20 customers in 20 currencies. Randomised dates, amounts, and payment terms. |

**What to show:**
- Processing time (typically < 5 seconds for 500 rows without LLM)
- Final log: `Canonical: 500 rows across 1 tables`
- DQ report covers all 500 rows with per-column fill-rate statistics
- Column lineage has one row per mapped column (not per data row) — stays compact
- Job status: **SUCCESS**

**CLI:**
```
python pipeline.py test_data/demo/demo_10_invoices_large.csv --domain trade
```

---

## Full Demo Flow Summary

| Stage | File(s) | What it Proves | Expected Status |
|-------|---------|----------------|-----------------|
| 1 — Basic Ingestion | `demo_01_*.csv` (2 files) | End-to-end pipeline, exact mapping, canonical output | SUCCESS |
| 2 — Multi-File Batch | `demo_02_*.csv` (2 files) | One job ID, two files, multi-currency | SUCCESS |
| 3 — Profile Reuse | `demo_03_invoices_profile_reuse.csv` | Zero-touch re-ingestion via saved profile | SUCCESS |
| 4 — Format Variability | `demo_04a/b/c/d` (4 files) | Pipe, semicolon, TSV, JSON auto-detected | SUCCESS |
| 5 — Fuzzy Matching | `demo_05_invoices_fuzzy.csv` | Abbreviated / typo headers resolved automatically | SUCCESS |
| 6 — DQ Rules | `demo_06_invoices_dq_issues.csv` | Auto-coercion, business rule violations isolated | SUCCESS_WITH_EXCEPTIONS |
| 7 — LLM Intelligence | `demo_07_invoices_llm.csv` | Semantic mapping of opaque financial terminology | SUCCESS |
| 8 — Real-World ZIP | `demo_08_realworld_pack.zip` | Multi-region, multi-format, single-job archive | SUCCESS |
| 9a — BLOCKED | `demo_09a_invoices_blocked.csv` | Hard block protects canonical tables from bad schema | BLOCKED |
| 9b — DQ Failures | `demo_09b_invoices_dq_failures.csv` | Graceful exception isolation, good rows still written | SUCCESS_WITH_EXCEPTIONS |
| 10 — Scale | `demo_10_invoices_large.csv` | 500-row throughput, DQ statistics at volume | SUCCESS |
