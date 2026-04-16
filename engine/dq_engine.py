"""
dq_engine.py
------------
Data quality engine. Runs all configured checks against canonical DataFrames:
  - Mandatory null checks
  - Type validation (numeric, date)
  - Duplicate key detection
  - Referential integrity
  - Business rule evaluation

Returns (canonical_tables_dict, exceptions_list, dq_report_dict).
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_null_value(val: Any, null_values: list[str]) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return s in null_values or s == ""


def _try_parse_numeric(val: str, strip_chars: list[str]) -> tuple[bool, float | None]:
    s = str(val).strip()
    for ch in strip_chars:
        s = s.replace(ch, "")
    s = s.strip()
    try:
        return True, float(s)
    except ValueError:
        return False, None


_DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y",
    "%Y/%m/%d", "%d.%m.%Y", "%m.%d.%Y", "%Y%m%d",
    "%d %b %Y", "%d %B %Y", "%b %d %Y", "%B %d %Y",
    "%d-%b-%Y", "%d-%B-%Y",
]


def _try_parse_date(val: str) -> tuple[bool, pd.Timestamp | None]:
    s = str(val).strip()
    for fmt in _DATE_FORMATS:
        try:
            return True, pd.to_datetime(s, format=fmt)
        except (ValueError, TypeError):
            continue
    # pandas flexible parse as last resort
    try:
        return True, pd.to_datetime(s)
    except Exception:
        return False, None


def _make_exception(
    job_id: str,
    domain: str,
    source_filename: str,
    source_row_index: int | None,
    source_col: str | None,
    canonical_table: str | None,
    canonical_col: str | None,
    raw_value: str | None,
    exc_type: str,
    reason: str,
) -> dict:
    return {
        "Exception_ID": str(uuid.uuid4()),
        "Job_ID": job_id,
        "Domain": domain,
        "Source_Filename": source_filename,
        "Source_Row_Index": source_row_index,
        "Source_Column_Name": source_col,
        "Canonical_Table": canonical_table,
        "Canonical_Column": canonical_col,
        "Raw_Value": raw_value,
        "Exception_Type": exc_type,
        "Reason": reason,
        "Insert_Timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Per-check functions
# ---------------------------------------------------------------------------

def _check_mandatory_nulls(
    df: pd.DataFrame,
    canonical_table: str,
    canonical_model: dict,
    source_col_map: dict[str, str],   # canonical_col -> source_col
    cfg: dict,
    job_id: str,
    domain: str,
    source_filename: str,
) -> list[dict]:
    """Emit MANDATORY_NULL for every null in a mandatory column."""
    null_values = cfg.get("quality", {}).get("null_values", ["", "null", "NULL", "n/a", "N/A"])
    tbl_def = canonical_model.get(canonical_table, {})
    mandatory_cols = [
        col for col, cdef in tbl_def.get("business_columns", {}).items()
        if cdef.get("mandatory", False)
    ]

    exceptions = []
    for col in mandatory_cols:
        if col not in df.columns:
            continue
        src_col = source_col_map.get(col, col)
        for idx, val in df[col].items():
            if _is_null_value(val, null_values):
                exceptions.append(_make_exception(
                    job_id, domain, source_filename,
                    int(idx) + 1,   # 1-based
                    src_col, canonical_table, col,
                    str(val),
                    "MANDATORY_NULL",
                    f"{col} is mandatory and is null in this record",
                ))
    return exceptions


def _check_type_validation(
    df: pd.DataFrame,
    canonical_table: str,
    canonical_model: dict,
    source_col_map: dict[str, str],
    cfg: dict,
    job_id: str,
    domain: str,
    source_filename: str,
) -> tuple[list[dict], pd.DataFrame]:
    """
    Validate and coerce types. Returns (exceptions, df_with_coerced_types).
    Numeric/date columns that fail validation get nulled in the output df.
    """
    null_values = cfg.get("quality", {}).get("null_values", [])
    type_cfg = cfg.get("quality", {}).get("type_validation", {})
    numeric_cfg = type_cfg.get("numeric", {})
    date_cfg = type_cfg.get("date", {})
    strip_chars = numeric_cfg.get("strip_chars", ["$", "£", "€", "¥", ",", " "])

    tbl_def = canonical_model.get(canonical_table, {})
    exceptions = []
    df = df.copy()

    for col, cdef in tbl_def.get("business_columns", {}).items():
        if col not in df.columns:
            continue
        col_type = cdef.get("type", "string")
        src_col = source_col_map.get(col, col)

        if col_type == "numeric" and numeric_cfg.get("enabled", True):
            coerced = []
            for idx, val in df[col].items():
                if _is_null_value(val, null_values):
                    coerced.append(None)
                    continue
                ok, num = _try_parse_numeric(str(val), strip_chars)
                if ok:
                    coerced.append(num)
                else:
                    exceptions.append(_make_exception(
                        job_id, domain, source_filename,
                        int(idx) + 1, src_col, canonical_table, col,
                        str(val), "TYPE_MISMATCH",
                        f"{col} expects numeric but got '{val}'",
                    ))
                    coerced.append(None)
            df[col] = coerced

        elif col_type == "date" and date_cfg.get("enabled", True):
            coerced = []
            for idx, val in df[col].items():
                if _is_null_value(val, null_values):
                    coerced.append(None)
                    continue
                ok, dt = _try_parse_date(str(val))
                if ok:
                    coerced.append(dt)
                else:
                    exceptions.append(_make_exception(
                        job_id, domain, source_filename,
                        int(idx) + 1, src_col, canonical_table, col,
                        str(val), "TYPE_MISMATCH",
                        f"{col} expects date but could not parse '{val}'",
                    ))
                    coerced.append(None)
            df[col] = coerced

    return exceptions, df


def _check_duplicates(
    df: pd.DataFrame,
    canonical_table: str,
    cfg: dict,
    job_id: str,
    domain: str,
    source_filename: str,
) -> list[dict]:
    """Emit DUPLICATE_KEY for every duplicate based on configured key columns."""
    dup_cfg = cfg.get("quality", {}).get("duplicate_detection", {})
    if not dup_cfg.get("enabled", True):
        return []

    key_cols = dup_cfg.get("keys", {}).get(canonical_table, [])
    if not key_cols:
        return []

    # Only use key columns that are present in df
    available_keys = [c for c in key_cols if c in df.columns]
    if not available_keys:
        return []

    exceptions = []
    dupes = df[df.duplicated(subset=available_keys, keep="first")]
    for idx, row in dupes.iterrows():
        key_vals = {c: str(row[c]) for c in available_keys}
        exceptions.append(_make_exception(
            job_id, domain, source_filename,
            int(idx) + 1, None, canonical_table, None,
            str(key_vals),
            "DUPLICATE_KEY",
            f"Duplicate key {key_vals} in {canonical_table}",
        ))
    return exceptions


def _check_referential_integrity(
    canonical_tables: dict[str, pd.DataFrame],
    cfg: dict,
    job_id: str,
    domain: str,
    source_filename: str,
) -> list[dict]:
    """Check FK relationships between canonical tables."""
    ri_cfg = cfg.get("quality", {}).get("referential_integrity", {})
    if not ri_cfg.get("enabled", True):
        return []

    exceptions = []
    for rule in ri_cfg.get("rules", []):
        from_tbl = rule["from_table"]
        from_col = rule["from_column"]
        to_tbl = rule["to_table"]
        to_col = rule["to_column"]

        if from_tbl not in canonical_tables or to_tbl not in canonical_tables:
            continue
        from_df = canonical_tables[from_tbl]
        to_df = canonical_tables[to_tbl]

        if from_col not in from_df.columns or to_col not in to_df.columns:
            continue

        null_values = cfg.get("quality", {}).get("null_values", [])
        valid_keys = set(
            str(v) for v in to_df[to_col]
            if not _is_null_value(v, null_values)
        )

        for idx, val in from_df[from_col].items():
            if _is_null_value(val, null_values):
                continue
            if str(val) not in valid_keys:
                exceptions.append(_make_exception(
                    job_id, domain, source_filename,
                    int(idx) + 1, from_col, from_tbl, from_col,
                    str(val),
                    "REFERENTIAL_INTEGRITY_FAIL",
                    f"{from_tbl}.{from_col}='{val}' has no matching {to_tbl}.{to_col}",
                ))
    return exceptions


# Default table per built-in rule_id (can be overridden via rule config's "table" key)
_RULE_TABLE_DEFAULTS: dict[str, str] = {
    "INV_DUE_GE_INV_DATE": "TRD_INVOICE",
    "PAID_LE_INVOICE":      "TRD_INVOICE",
    "CURRENCY_ISO4217":     "TRD_INVOICE",
}

# Full ISO 4217 currency code set — defined once at module level, not per-call
_ISO4217: frozenset[str] = frozenset({
    "AED","AFN","ALL","AMD","ANG","AOA","ARS","AUD","AWG","AZN",
    "BAM","BBD","BDT","BGN","BHD","BIF","BMD","BND","BOB","BRL",
    "BSD","BTN","BWP","BYN","BZD","CAD","CDF","CHF","CLP","CNY",
    "COP","CRC","CUP","CVE","CZK","DJF","DKK","DOP","DZD","EGP",
    "ERN","ETB","EUR","FJD","FKP","GBP","GEL","GHS","GIP","GMD",
    "GNF","GTQ","GYD","HKD","HNL","HRK","HTG","HUF","IDR","ILS",
    "INR","IQD","IRR","ISK","JMD","JOD","JPY","KES","KGS","KHR",
    "KMF","KPW","KRW","KWD","KYD","KZT","LAK","LBP","LKR","LRD",
    "LSL","LYD","MAD","MDL","MGA","MKD","MMK","MNT","MOP","MRU",
    "MUR","MVR","MWK","MXN","MYR","MZN","NAD","NGN","NIO","NOK",
    "NPR","NZD","OMR","PAB","PEN","PGK","PHP","PKR","PLN","PYG",
    "QAR","RON","RSD","RUB","RWF","SAR","SBD","SCR","SDG","SEK",
    "SGD","SHP","SLL","SOS","SRD","STN","SVC","SYP","SZL","THB",
    "TJS","TMT","TND","TOP","TRY","TTD","TWD","TZS","UAH","UGX",
    "USD","UYU","UZS","VES","VND","VUV","WST","XAF","XCD","XOF",
    "XPF","YER","ZAR","ZMW","ZWL",
})


# ---------------------------------------------------------------------------
# Business rule handlers — each takes (tbl_df, tbl_name, null_values,
# job_id, domain, source_filename) and returns list[dict] of exceptions.
# Register new rules by adding a function + an entry in _BUSINESS_RULE_HANDLERS.
# ---------------------------------------------------------------------------

_BusinessRuleHandler = Callable[
    [pd.DataFrame, str, list[str], str, str, str], list[dict]
]


def _rule_inv_due_ge_inv_date(
    tbl_df: pd.DataFrame, tbl_name: str, null_values: list[str],
    job_id: str, domain: str, source_filename: str,
) -> list[dict]:
    """Due_Date must be >= Invoice_Date."""
    rule_id = "INV_DUE_GE_INV_DATE"
    if "Due_Date" not in tbl_df.columns or "Invoice_Date" not in tbl_df.columns:
        return []
    exceptions = []
    for idx, row in tbl_df.iterrows():
        due = row.get("Due_Date")
        inv = row.get("Invoice_Date")
        if due is None or inv is None:
            continue
        if _is_null_value(due, null_values) or _is_null_value(inv, null_values):
            continue
        try:
            if pd.to_datetime(str(due)) < pd.to_datetime(str(inv)):
                exceptions.append(_make_exception(
                    job_id, domain, source_filename,
                    int(idx) + 1, "Due_Date", tbl_name, "Due_Date",
                    str(due), "BUSINESS_RULE_FAIL",
                    f"[{rule_id}] Due_Date '{due}' is before Invoice_Date '{inv}'",
                ))
        except Exception:
            continue
    return exceptions


def _rule_paid_le_invoice(
    tbl_df: pd.DataFrame, tbl_name: str, null_values: list[str],
    job_id: str, domain: str, source_filename: str,
) -> list[dict]:
    """Paid_Amount must be <= Invoice_Amount."""
    rule_id = "PAID_LE_INVOICE"
    if "Paid_Amount" not in tbl_df.columns or "Invoice_Amount" not in tbl_df.columns:
        return []
    exceptions = []
    for idx, row in tbl_df.iterrows():
        paid = row.get("Paid_Amount")
        total = row.get("Invoice_Amount")
        if paid is None or total is None:
            continue
        if _is_null_value(paid, null_values) or _is_null_value(total, null_values):
            continue
        try:
            paid_f, total_f = float(str(paid)), float(str(total))
            if paid_f > total_f:
                exceptions.append(_make_exception(
                    job_id, domain, source_filename,
                    int(idx) + 1, "Paid_Amount", tbl_name, "Paid_Amount",
                    str(paid), "BUSINESS_RULE_FAIL",
                    f"[{rule_id}] Paid_Amount {paid_f} > Invoice_Amount {total_f}",
                ))
        except Exception:
            continue
    return exceptions


def _rule_currency_iso4217(
    tbl_df: pd.DataFrame, tbl_name: str, null_values: list[str],
    job_id: str, domain: str, source_filename: str,
) -> list[dict]:
    """Currency must be a valid ISO 4217 code."""
    rule_id = "CURRENCY_ISO4217"
    if "Currency" not in tbl_df.columns:
        return []
    exceptions = []
    for idx, val in tbl_df["Currency"].items():
        if _is_null_value(val, null_values):
            continue
        if str(val).upper() not in _ISO4217:
            exceptions.append(_make_exception(
                job_id, domain, source_filename,
                int(idx) + 1, "Currency", tbl_name, "Currency",
                str(val), "BUSINESS_RULE_FAIL",
                f"[{rule_id}] '{val}' is not a valid ISO 4217 currency code",
            ))
    return exceptions


# Registry: add new rule handlers here without touching _check_business_rules.
_BUSINESS_RULE_HANDLERS: dict[str, _BusinessRuleHandler] = {
    "INV_DUE_GE_INV_DATE": _rule_inv_due_ge_inv_date,
    "PAID_LE_INVOICE":      _rule_paid_le_invoice,
    "CURRENCY_ISO4217":     _rule_currency_iso4217,
}


def _check_business_rules(
    canonical_tables: dict[str, pd.DataFrame],
    cfg: dict,
    job_id: str,
    domain: str,
    source_filename: str,
) -> list[dict]:
    """Evaluate configured business rules via the handler registry.

    Each rule resolves its target table via:
      1. rule["table"] key (explicit override in config)
      2. _RULE_TABLE_DEFAULTS map (built-in fallback)
    Rules whose target table is not present in this job are silently skipped.
    Unknown rule_ids are logged as warnings and skipped (no crash).
    To add a new rule: write a handler function and register it in
    _BUSINESS_RULE_HANDLERS — no changes to this function needed.
    """
    rules = cfg.get("quality", {}).get("business_rules", [])
    null_values = cfg.get("quality", {}).get("null_values", [])
    exceptions = []

    for rule in rules:
        rule_id = rule["rule_id"]
        handler = _BUSINESS_RULE_HANDLERS.get(rule_id)
        if handler is None:
            logger.warning("Unknown business rule '%s' — skipped. "
                           "Register a handler in _BUSINESS_RULE_HANDLERS.", rule_id)
            continue

        # Resolve target table — explicit config key wins over built-in default
        tbl_name = rule.get("table") or _RULE_TABLE_DEFAULTS.get(rule_id, "TRD_INVOICE")
        tbl_df = canonical_tables.get(tbl_name)
        if tbl_df is None:
            continue

        exceptions.extend(handler(tbl_df, tbl_name, null_values, job_id, domain, source_filename))

    return exceptions


# ---------------------------------------------------------------------------
# DQ report builder
# ---------------------------------------------------------------------------

def _build_dq_report(
    canonical_tables: dict[str, pd.DataFrame],
    exceptions: list[dict],
    canonical_model: dict,
    cfg: dict,
    job_id: str,
    source_filename: str,
) -> dict:
    pass_fill = cfg.get("quality", {}).get("dq_pass_fill_rate", 95)
    warn_fill = cfg.get("quality", {}).get("dq_warn_fill_rate", 70)
    null_values = cfg.get("quality", {}).get("null_values", [])

    report = {
        "job_id": job_id,
        "source_filename": source_filename,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_exceptions": len(exceptions),
        "exception_summary": {},
        "tables": {},
    }

    # Exception breakdown by type
    for exc in exceptions:
        etype = exc["Exception_Type"]
        report["exception_summary"][etype] = report["exception_summary"].get(etype, 0) + 1

    # Per-table column fill rates
    for tbl, df in canonical_tables.items():
        if df.empty:
            continue
        total_rows = len(df)
        table_report = {"total_rows": total_rows, "columns": {}}
        tbl_def = canonical_model.get(tbl, {})

        for col, cdef in tbl_def.get("business_columns", {}).items():
            if col not in df.columns:
                table_report["columns"][col] = {
                    "present": False,
                    "fill_rate": 0.0,
                    "status": "MISSING",
                }
                continue
            null_count = sum(1 for v in df[col] if _is_null_value(v, null_values))
            filled = total_rows - null_count
            fill_rate = round(filled / total_rows * 100, 1) if total_rows > 0 else 0.0
            status = "PASS" if fill_rate >= pass_fill else ("WARN" if fill_rate >= warn_fill else "FAIL")
            table_report["columns"][col] = {
                "present": True,
                "total_rows": total_rows,
                "null_count": null_count,
                "fill_rate": fill_rate,
                "mandatory": cdef.get("mandatory", False),
                "status": status,
            }
        report["tables"][tbl] = table_report

    return report


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_dq(
    canonical_tables: dict[str, pd.DataFrame],
    canonical_model: dict,
    source_col_maps: dict[str, dict[str, str]],   # {table: {canonical_col -> source_col}}
    cfg: dict,
    job_id: str,
    domain: str,
    source_filename: str,
) -> tuple[dict[str, pd.DataFrame], list[dict], dict]:
    """
    Run all DQ checks.
    Returns (coerced_canonical_tables, exceptions_list, dq_report).
    """
    all_exceptions: list[dict] = []
    coerced: dict[str, pd.DataFrame] = {}

    for tbl, df in canonical_tables.items():
        src_map = source_col_maps.get(tbl, {})

        # Mandatory null check
        null_excs = _check_mandatory_nulls(
            df, tbl, canonical_model, src_map, cfg, job_id, domain, source_filename
        )
        all_exceptions.extend(null_excs)

        # Type validation + coercion
        type_excs, df_coerced = _check_type_validation(
            df, tbl, canonical_model, src_map, cfg, job_id, domain, source_filename
        )
        all_exceptions.extend(type_excs)
        coerced[tbl] = df_coerced

        # Duplicate detection
        dup_excs = _check_duplicates(
            df_coerced, tbl, cfg, job_id, domain, source_filename
        )
        all_exceptions.extend(dup_excs)

    # Cross-table checks (use original dfs for RI)
    ri_excs = _check_referential_integrity(
        coerced, cfg, job_id, domain, source_filename
    )
    all_exceptions.extend(ri_excs)

    br_excs = _check_business_rules(
        coerced, cfg, job_id, domain, source_filename
    )
    all_exceptions.extend(br_excs)

    dq_report = _build_dq_report(
        coerced, all_exceptions, canonical_model, cfg, job_id, source_filename
    )

    return coerced, all_exceptions, dq_report
