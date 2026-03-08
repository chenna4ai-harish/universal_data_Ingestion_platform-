"""
output_writer.py
----------------
Writes all job outputs to the configured output directory:
  - canonical_trade_<table>_<job_id>.csv
  - exceptions_trade_<job_id>.csv
  - column_lineage_<job_id>.csv
  - archive_lineage_<job_id>.csv   (only if archive was involved)
  - dq_trade_<job_id>.json
  - job_summary_<job_id>.json
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _ensure_output_dir(output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)


def write_canonical_tables(
    canonical_tables: dict[str, pd.DataFrame],
    output_dir: str,
    cfg: dict,
    job_id: str,
) -> dict[str, str]:
    """Write each canonical table to CSV. Returns {table: filepath}."""
    _ensure_output_dir(output_dir)
    prefix = cfg.get("output", {}).get("canonical_prefix", "canonical")
    written = {}
    for tbl, df in canonical_tables.items():
        if df.empty:
            continue
        filename = f"{prefix}_{tbl.lower()}_{job_id}.csv"
        path = os.path.join(output_dir, filename)
        df.to_csv(path, index=False)
        written[tbl] = path
    return written


def write_exceptions(
    exceptions_df: pd.DataFrame,
    output_dir: str,
    cfg: dict,
    job_id: str,
) -> str | None:
    """Write RECORD_EXCEPTIONS to CSV. Returns filepath or None if empty."""
    if exceptions_df.empty:
        return None
    _ensure_output_dir(output_dir)
    prefix = cfg.get("output", {}).get("exceptions_prefix", "exceptions")
    filename = f"{prefix}_{job_id}.csv"
    path = os.path.join(output_dir, filename)
    max_rows = cfg.get("output", {}).get("max_exceptions_display", 200)
    if len(exceptions_df) > max_rows:
        print(
            f"  WARNING: exceptions CSV truncated to {max_rows} rows "
            f"({len(exceptions_df)} total). Increase max_exceptions_display in config to capture all."
        )
    exceptions_df.head(max_rows).to_csv(path, index=False)
    return path


def write_column_lineage(
    column_lineage_df: pd.DataFrame,
    output_dir: str,
    job_id: str,
) -> str | None:
    if column_lineage_df.empty:
        return None
    _ensure_output_dir(output_dir)
    path = os.path.join(output_dir, f"column_lineage_{job_id}.csv")
    column_lineage_df.to_csv(path, index=False)
    return path


def write_archive_lineage(
    archive_lineage_df: pd.DataFrame,
    output_dir: str,
    job_id: str,
) -> str | None:
    if archive_lineage_df.empty:
        return None
    _ensure_output_dir(output_dir)
    path = os.path.join(output_dir, f"archive_lineage_{job_id}.csv")
    archive_lineage_df.to_csv(path, index=False)
    return path


def write_dq_report(
    dq_report: dict,
    output_dir: str,
    cfg: dict,
    job_id: str,
) -> str:
    _ensure_output_dir(output_dir)
    prefix = cfg.get("output", {}).get("dq_report_prefix", "dq_report")
    path = os.path.join(output_dir, f"{prefix}_{job_id}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(dq_report, fh, indent=2, default=str)
    return path


def write_job_summary(
    summary: dict,
    output_dir: str,
    job_id: str,
) -> str:
    _ensure_output_dir(output_dir)
    path = os.path.join(output_dir, f"job_summary_{job_id}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)
    return path


def build_job_summary(
    job_id: str,
    domain: str,
    source_filename: str,
    job_status: str,
    files_processed: int,
    files_failed: int,
    total_source_rows: int,
    total_canonical_rows: int,
    total_exceptions: int,
    mapping_results: list,
    blocked_mandatory: list,
    job_start: datetime,
    cfg: dict,
    canonical_model_version: str,
) -> dict:
    from engine.column_mapper import MappingResult
    counts = {"EXACT LOOKUP": 0, "FUZZY MATCH": 0, "LLM": 0, "NO MATCH": 0}
    for r in mapping_results:
        if r.match_method == "EXACT LOOKUP":
            counts["EXACT LOOKUP"] += 1
        elif r.match_method == "FUZZY MATCH":
            counts["FUZZY MATCH"] += 1
        elif r.match_method.startswith("LLM"):
            counts["LLM"] += 1
        elif r.match_method == "NO MATCH":
            counts["NO MATCH"] += 1

    return {
        "Job_ID": job_id,
        "Domain": domain,
        "Source_Filename": source_filename,
        "Job_Status": job_status,
        "Files_Processed": files_processed,
        "Files_Failed": files_failed,
        "Total_Source_Rows": total_source_rows,
        "Total_Canonical_Rows_Written": total_canonical_rows,
        "Total_Exceptions": total_exceptions,
        "Columns_Mapped_Exact": counts["EXACT LOOKUP"],
        "Columns_Mapped_Fuzzy": counts["FUZZY MATCH"],
        "Columns_Mapped_LLM": counts["LLM"],
        "Columns_Unmapped": counts["NO MATCH"],
        "Mandatory_Columns_Blocked": len(blocked_mandatory),
        "Blocked_Mandatory_Details": [
            f"{tbl}.{col}" for tbl, col in blocked_mandatory
        ],
        "Job_Start_Timestamp": job_start.isoformat(),
        "Job_End_Timestamp": datetime.now(timezone.utc).isoformat(),
        "Config_Version": cfg.get("_metadata", {}).get("config_version", "unknown"),
        "Canonical_Model_Version": canonical_model_version,
    }
