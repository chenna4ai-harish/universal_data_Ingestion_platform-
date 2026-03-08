"""
lineage_writer.py
-----------------
Converts internal result objects to COLUMN_LINEAGE and ARCHIVE_LINEAGE DataFrames,
and merges DQ exceptions into the RECORD_EXCEPTIONS DataFrame.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

import pandas as pd

from engine.column_mapper import MappingResult
from engine.file_parser import ArchiveLineageRow


# ---------------------------------------------------------------------------
# COLUMN_LINEAGE
# ---------------------------------------------------------------------------

COLUMN_LINEAGE_COLS = [
    "Lineage_ID", "Mapping_Reference_ID", "Job_ID", "Domain",
    "Source_Filename", "Source_Column_Name", "Source_Column_Normalised",
    "Canonical_Table", "Canonical_Column", "Match_Method",
    "Confidence_Score", "Was_Mandatory", "Met_Threshold",
    "LLM_Reasoning", "Lookup_Variant_Matched", "Archive_Lineage_ID",
    "Insert_Timestamp",
]


def build_column_lineage_df(mapping_results: list[MappingResult]) -> pd.DataFrame:
    rows = []
    for r in mapping_results:
        rows.append({
            "Lineage_ID": r.lineage_id,
            "Mapping_Reference_ID": r.mapping_reference_id,
            "Job_ID": r.job_id,
            "Domain": r.domain,
            "Source_Filename": r.source_filename,
            "Source_Column_Name": r.source_column_name,
            "Source_Column_Normalised": r.source_column_normalised,
            "Canonical_Table": r.canonical_table,
            "Canonical_Column": r.canonical_column,
            "Match_Method": r.match_method,
            "Confidence_Score": r.confidence_score,
            "Was_Mandatory": r.was_mandatory,
            "Met_Threshold": r.met_threshold,
            "LLM_Reasoning": r.llm_reasoning,
            "Lookup_Variant_Matched": r.lookup_variant_matched,
            "Archive_Lineage_ID": r.archive_lineage_id,
            "Insert_Timestamp": r.insert_timestamp,
        })
    return pd.DataFrame(rows, columns=COLUMN_LINEAGE_COLS) if rows else pd.DataFrame(columns=COLUMN_LINEAGE_COLS)


# ---------------------------------------------------------------------------
# ARCHIVE_LINEAGE
# ---------------------------------------------------------------------------

ARCHIVE_LINEAGE_COLS = [
    "Archive_Lineage_ID", "Job_ID", "Source_Filename", "Parent_Archive",
    "Root_Archive", "Archive_Entry_Name", "Extracted_Path", "Nested_Level",
    "File_Size_Bytes", "Extraction_Status", "Insert_Timestamp",
]


def build_archive_lineage_df(archive_rows: list[ArchiveLineageRow]) -> pd.DataFrame:
    rows = [asdict(r) for r in archive_rows]
    # Rename keys to match column schema (dataclass uses snake_case)
    renamed = []
    for r in rows:
        renamed.append({
            "Archive_Lineage_ID": r["archive_lineage_id"],
            "Job_ID": r["job_id"],
            "Source_Filename": r["source_filename"],
            "Parent_Archive": r["parent_archive"],
            "Root_Archive": r["root_archive"],
            "Archive_Entry_Name": r["archive_entry_name"],
            "Extracted_Path": r["extracted_path"],
            "Nested_Level": r["nested_level"],
            "File_Size_Bytes": r["file_size_bytes"],
            "Extraction_Status": r["extraction_status"],
            "Insert_Timestamp": r["insert_timestamp"],
        })
    return pd.DataFrame(renamed, columns=ARCHIVE_LINEAGE_COLS) if renamed else pd.DataFrame(columns=ARCHIVE_LINEAGE_COLS)


# ---------------------------------------------------------------------------
# RECORD_EXCEPTIONS
# ---------------------------------------------------------------------------

RECORD_EXCEPTIONS_COLS = [
    "Exception_ID", "Job_ID", "Domain", "Source_Filename",
    "Source_Row_Index", "Source_Column_Name", "Canonical_Table",
    "Canonical_Column", "Raw_Value", "Exception_Type", "Reason",
    "Insert_Timestamp",
]


def build_record_exceptions_df(
    dq_exceptions: list[dict],
    failed_files_exceptions: list[dict] | None = None,
    mapping_exceptions: list[dict] | None = None,
) -> pd.DataFrame:
    """
    Consolidates all exception sources:
    - dq_exceptions: from dq_engine (type/null/dup/RI/business)
    - failed_files_exceptions: file-level failures from file_parser
    - mapping_exceptions: unmapped mandatory / low confidence mapping
    """
    all_rows = list(dq_exceptions or [])
    all_rows.extend(failed_files_exceptions or [])
    all_rows.extend(mapping_exceptions or [])

    if not all_rows:
        return pd.DataFrame(columns=RECORD_EXCEPTIONS_COLS)

    df = pd.DataFrame(all_rows)
    for col in RECORD_EXCEPTIONS_COLS:
        if col not in df.columns:
            df[col] = None
    return df[RECORD_EXCEPTIONS_COLS]


def build_mapping_exceptions(
    mapping_results: list[MappingResult],
    cfg: dict,
    job_id: str,
    domain: str,
    source_filename: str,
) -> list[dict]:
    """
    Emit exceptions for:
    - UNMAPPED_MANDATORY: mandatory column with no mapping
    - LOW_CONFIDENCE_MAPPING: mapped but below threshold
    """
    mandatory_threshold = cfg.get("quality", {}).get("mandatory_threshold", 80)
    ts = datetime.now(timezone.utc).isoformat()
    exceptions = []

    for r in mapping_results:
        if r.canonical_column == "UNMAPPED" and r.was_mandatory:
            exceptions.append({
                "Exception_ID": __import__("uuid").uuid4().__str__(),
                "Job_ID": job_id,
                "Domain": domain,
                "Source_Filename": source_filename,
                "Source_Row_Index": None,
                "Source_Column_Name": r.source_column_name,
                "Canonical_Table": None,
                "Canonical_Column": None,
                "Raw_Value": None,
                "Exception_Type": "UNMAPPED_MANDATORY",
                "Reason": (
                    f"Source column '{r.source_column_name}' could not be mapped to any canonical "
                    f"column and is required for this domain"
                ),
                "Insert_Timestamp": ts,
            })
        elif (r.canonical_column != "UNMAPPED" and r.was_mandatory
              and r.confidence_score < mandatory_threshold):
            exceptions.append({
                "Exception_ID": __import__("uuid").uuid4().__str__(),
                "Job_ID": job_id,
                "Domain": domain,
                "Source_Filename": source_filename,
                "Source_Row_Index": None,
                "Source_Column_Name": r.source_column_name,
                "Canonical_Table": r.canonical_table,
                "Canonical_Column": r.canonical_column,
                "Raw_Value": None,
                "Exception_Type": "LOW_CONFIDENCE_MAPPING",
                "Reason": (
                    f"Source column '{r.source_column_name}' mapped to "
                    f"{r.canonical_table}.{r.canonical_column} with confidence "
                    f"{r.confidence_score} (threshold {mandatory_threshold})"
                ),
                "Insert_Timestamp": ts,
            })

    return exceptions


def build_failed_file_exceptions(
    failed_files: list,   # list of FailedFile
    job_id: str,
    domain: str,
) -> list[dict]:
    """Convert FailedFile objects to RECORD_EXCEPTIONS rows."""
    ts = datetime.now(timezone.utc).isoformat()
    rows = []
    for ff in failed_files:
        rows.append({
            "Exception_ID": __import__("uuid").uuid4().__str__(),
            "Job_ID": job_id,
            "Domain": domain,
            "Source_Filename": ff.source_filename,
            "Source_Row_Index": None,
            "Source_Column_Name": None,
            "Canonical_Table": None,
            "Canonical_Column": None,
            "Raw_Value": None,
            "Exception_Type": ff.exception_type,
            "Reason": ff.reason,
            "Insert_Timestamp": ts,
        })
    return rows
