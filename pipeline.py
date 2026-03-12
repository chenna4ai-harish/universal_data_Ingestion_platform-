"""
pipeline.py
-----------
Main orchestrator for the Universal Data Ingestion and Normalisation Platform.

Usage:
  python pipeline.py <file_path> [options]

Options:
  --domain DOMAIN         Domain key (default: trade)
  --config-dir DIR        Directory containing config files (default: current dir)
  --output-dir DIR        Output directory (default: ./output)
  --override KEY=TBL.COL  User column override (repeatable)
  --preview               Print a preview of outputs to console

Example:
  python pipeline.py data/invoices.csv --domain trade
  python pipeline.py data/batch.zip --domain trade --output-dir ./out
  python pipeline.py data/file.csv --override "Inv No=TRD_INVOICE.Invoice_Number"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Load .env file if present (API keys for LLM providers)
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)   # don't override keys already set in shell env
except ImportError:
    pass

from engine.file_parser import parse_input_file
from engine.column_mapper import (
    load_lookup_table,
    map_columns,
    get_blocked_mandatory_columns,
    build_column_map,
)
from engine.dq_engine import run_dq
from engine.lineage_writer import (
    build_column_lineage_df,
    build_archive_lineage_df,
    build_record_exceptions_df,
    build_mapping_exceptions,
    build_failed_file_exceptions,
)
from engine.output_writer import (
    write_canonical_tables,
    write_exceptions,
    write_column_lineage,
    write_archive_lineage,
    write_dq_report,
    write_job_summary,
    build_job_summary,
)


# ---------------------------------------------------------------------------
# Config loaders
# ---------------------------------------------------------------------------

def load_config(config_dir: str, domain: str) -> tuple[dict, dict, dict]:
    """Load and return (system_config, canonical_model, global_system_tables).

    Domain-specific files are expected under:  <config_dir>/domains/<domain>/
    The global system tables file stays at:    <config_dir>/global_system_tables.json
    """
    domain_dir = os.path.join(config_dir, "domains", domain)
    sc_path = os.path.join(domain_dir, f"{domain}_system_config.json")
    cm_path = os.path.join(domain_dir, f"{domain}_canonical_model.json")
    gs_path = os.path.join(config_dir, "global_system_tables.json")

    for p in [sc_path, cm_path, gs_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Config file not found: {p}")

    with open(sc_path, encoding="utf-8") as fh:
        system_cfg = json.load(fh)
    with open(cm_path, encoding="utf-8") as fh:
        canonical_model = json.load(fh)
    with open(gs_path, encoding="utf-8") as fh:
        global_tables = json.load(fh)

    return system_cfg, canonical_model, global_tables


def load_lookup(config_dir: str, domain: str) -> dict:
    path = os.path.join(config_dir, "domains", domain, f"{domain}_lookup_table.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Lookup table not found: {path}")
    return load_lookup_table(path)


# ---------------------------------------------------------------------------
# Canonical record builder
# ---------------------------------------------------------------------------

def _build_canonical_tables(
    df: pd.DataFrame,
    column_map: dict[str, list[tuple[str, str, int]]],
    canonical_model: dict,
    mapping_reference_id: str,
    source_filename: str,
    source_contributor_id: str,
    source_file_format: str,
    job_id: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, str]]]:
    """
    Project source DataFrame rows into canonical tables based on mapping results.

    Returns (canonical_tables_dict, source_col_maps_dict).
    source_col_maps: {canonical_table: {canonical_col -> source_col_name}}
    """
    ts = datetime.now(timezone.utc).isoformat()

    # Build reverse maps: canonical_table -> {canonical_col -> source_col}
    table_col_map: dict[str, dict[str, str]] = {}
    for src_col, targets in column_map.items():
        # Use highest-confidence target for each canonical column
        for (tbl, can_col, confidence) in sorted(targets, key=lambda x: -x[2]):
            table_col_map.setdefault(tbl, {})
            if can_col not in table_col_map[tbl]:
                table_col_map[tbl][can_col] = src_col

    canonical_tables: dict[str, pd.DataFrame] = {}
    source_col_maps: dict[str, dict[str, str]] = {}

    for tbl, tdef in canonical_model.items():
        if tbl.startswith("_") or not isinstance(tdef, dict):
            continue
        if tbl not in table_col_map:
            continue

        col_map = table_col_map[tbl]    # canonical_col -> source_col
        business_cols = list(tdef.get("business_columns", {}).keys())

        rows = []
        for row_idx, src_row in df.iterrows():
            record = {}
            for can_col in business_cols:
                src_col = col_map.get(can_col)
                record[can_col] = str(src_row[src_col]).strip() if (src_col and src_col in src_row) else None

            # Lineage columns
            record["Source_Filename"] = source_filename
            record["Source_Row_Index"] = row_idx + 1   # 1-based
            record["Source_Contributor_ID"] = source_contributor_id
            record["Source_File_Format"] = source_file_format
            record["Mapping_Reference_ID"] = mapping_reference_id

            # Audit columns
            record["Record_Status"] = "ACTIVE"
            record["Record_Version"] = 1
            record["Insert_Timestamp"] = ts
            record["Last_Modified_Timestamp"] = ts
            record["Inserted_By_Job"] = job_id
            record["Last_Modified_By_Job"] = job_id
            record["Processing_System"] = "DataIngestionPOC-1.0"

            rows.append(record)

        canonical_tables[tbl] = pd.DataFrame(rows)
        # DQ engine expects canonical_col -> source_col
        source_col_maps[tbl] = col_map

    return canonical_tables, source_col_maps


# ---------------------------------------------------------------------------
# Per-file processor
# ---------------------------------------------------------------------------

def _process_parsed_file(
    parsed_file,
    lookup_table: dict,
    canonical_model: dict,
    system_cfg: dict,
    job_id: str,
    domain: str,
    user_overrides: dict,
    contributor_id: str,
    config_dir: str = ".",
) -> tuple[
    dict[str, pd.DataFrame],   # canonical_tables
    list,                       # mapping_results
    str,                        # mapping_reference_id
    list[dict],                 # all exceptions
    dict,                       # dq_report
    list[tuple[str, str]],      # blocked_mandatory
]:
    df = parsed_file.dataframe
    source_filename = parsed_file.source_filename
    source_cols = list(df.columns)

    print(f"  Mapping {len(source_cols)} columns from '{source_filename}'...")  # also printed by pipeline loop

    mapping_results, mapping_reference_id = map_columns(
        source_columns=source_cols,
        lookup_table=lookup_table,
        canonical_model=canonical_model,
        cfg=system_cfg,
        job_id=job_id,
        domain=domain,
        source_filename=source_filename,
        user_overrides=user_overrides,
        archive_lineage_id=parsed_file.archive_lineage_id,
        config_dir=config_dir,
    )

    # Check blocked mandatory columns
    blocked_mandatory = get_blocked_mandatory_columns(
        mapping_results, canonical_model, system_cfg
    )

    # Emit mapping-level exceptions
    mapping_excs = build_mapping_exceptions(
        mapping_results, system_cfg, job_id, domain, source_filename
    )

    # Block if auto_block enabled and mandatory columns are missing
    auto_block = system_cfg.get("matching", {}).get("auto_block_unmapped_mandatory", True)
    if auto_block and blocked_mandatory:
        cols_str = ", ".join(f"{t}.{c}" for t, c in blocked_mandatory)
        print(f"  BLOCKED: mandatory columns not met: {cols_str}")
        return {}, mapping_results, mapping_reference_id, mapping_excs, {}, blocked_mandatory

    # Build canonical tables
    col_map = build_column_map(mapping_results)
    canonical_tables, source_col_maps = _build_canonical_tables(
        df, col_map, canonical_model,
        mapping_reference_id,
        source_filename,
        contributor_id,
        parsed_file.file_format,
        job_id,
    )

    print(f"  Running DQ on {sum(len(t) for t in canonical_tables.values())} canonical records...")

    # Run DQ
    coerced_tables, dq_exceptions, dq_report = run_dq(
        canonical_tables=canonical_tables,
        canonical_model=canonical_model,
        source_col_maps=source_col_maps,
        cfg=system_cfg,
        job_id=job_id,
        domain=domain,
        source_filename=source_filename,
    )

    all_exceptions = mapping_excs + dq_exceptions

    for tbl, cnt in {t: len(d) for t, d in coerced_tables.items()}.items():
        print(f"  {tbl}: {cnt} rows")

    return coerced_tables, mapping_results, mapping_reference_id, all_exceptions, dq_report, blocked_mandatory


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    file_paths: list[str] | str | None = None,
    domain: str = "trade",
    config_dir: str = ".",
    output_dir: str = "./output",
    user_overrides: dict | None = None,
    contributor_id: str = "UNKNOWN",
    preview: bool = False,
    llm_override: dict | None = None,
    log_fn=None,
    # backward-compat alias
    file_path: str | None = None,
) -> dict:
    """
    Full pipeline run. Returns job summary dict.

    file_paths: one or more input file paths (list or single string).
    file_path:  backward-compat alias for a single file path.
    llm_override: optional dict to override LLM config at runtime, e.g.
        {"provider": "Claude", "api_key": "sk-ant-..."}
    log_fn: optional callable(str) for streaming log lines to a UI.
    """
    # Backward compat: accept old file_path kwarg or single string
    if file_paths is None:
        file_paths = file_path
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    if not file_paths:
        raise ValueError("run_pipeline() requires at least one file path via file_paths or file_path")

    def _log(msg: str) -> None:
        print(msg)
        if log_fn:
            log_fn(msg)

    job_id = str(uuid.uuid4())
    job_start = datetime.now(timezone.utc)
    user_overrides = user_overrides or {}

    # Create a per-job output subfolder: <base_output_dir>/<YYYYMMDD>_<job_id>/
    job_date_str = job_start.strftime("%Y%m%d")
    output_dir = os.path.join(output_dir, f"{job_date_str}_{job_id}")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    _log(f"\n{'='*60}")
    _log(f"Job ID  : {job_id}")
    _log(f"Domain  : {domain}")
    _log(f"Files   : {', '.join(os.path.basename(fp) for fp in file_paths)}")
    _log(f"Started : {job_start.isoformat()}")
    _log(f"Output  : {output_dir}")
    _log(f"{'='*60}\n")

    # --- Load config ---
    _log("Loading configuration...")
    try:
        system_cfg, canonical_model, global_tables = load_config(config_dir, domain)
        lookup_table = load_lookup(config_dir, domain)
    except FileNotFoundError as e:
        _log(f"CONFIG ERROR: {e}")
        return {"Job_ID": job_id, "Job_Status": "FAILED", "Reason": str(e)}

    # --- Apply runtime LLM override (from UI) ---
    if llm_override:
        provider = llm_override.get("provider")
        api_key = llm_override.get("api_key", "").strip()
        if provider and provider != "None":
            system_cfg["llm"]["provider"] = provider
            _log(f"LLM provider overridden to: {provider}")
            # Set the API key into the environment so the SDK picks it up
            if api_key:
                key_env_map = {
                    "Claude": "ANTHROPIC_API_KEY",
                    "OpenAI": "OPENAI_API_KEY",
                    "Gemini": "GOOGLE_API_KEY",
                }
                env_var = key_env_map.get(provider)
                if env_var:
                    os.environ[env_var] = api_key
        else:
            system_cfg["llm"]["provider"] = "None"
            _log("LLM disabled (provider = None)")

        # Optional threshold overrides from UI
        for key in ("confidence_accept_threshold", "mandatory_threshold",
                    "fuzzy_min_similarity", "llm_disambiguation_required_below"):
            if key in llm_override:
                if key in ("mandatory_threshold", "fuzzy_min_similarity"):
                    system_cfg.setdefault("quality", {})[key] = llm_override[key]
                else:
                    system_cfg.setdefault("llm", {})[key] = llm_override[key]

    canonical_model_version = canonical_model.get("_metadata", {}).get("version", "unknown")
    _log(f"Config version: {system_cfg.get('_metadata', {}).get('config_version')}, "
         f"Model version: {canonical_model_version}, "
         f"Lookup entries: {len(lookup_table)}, "
         f"LLM: {system_cfg.get('llm', {}).get('provider', 'None')}")

    # --- Parse all uploaded files ---
    all_parsed_files = []
    all_failed_files_raw = []
    archive_lineage_rows = []

    for fp in file_paths:
        _log(f"\nParsing input file: '{fp}'...")
        parsed, failed, arch_rows = parse_input_file(
            file_path=fp,
            cfg=system_cfg,
            job_id=job_id,
        )
        all_parsed_files.extend(parsed)
        all_failed_files_raw.extend(failed)
        archive_lineage_rows.extend(arch_rows)
        _log(f"  Parsed: {len(parsed)}, Failed: {len(failed)}")

    parsed_files = all_parsed_files
    failed_files = all_failed_files_raw

    _log(f"\n  Total parsed files: {len(parsed_files)}")
    _log(f"  Total failed files: {len(failed_files)}")
    if failed_files:
        for ff in failed_files:
            _log(f"  FAIL [{ff.exception_type}] {ff.source_filename}: {ff.reason}")

    source_filename_str = ", ".join(os.path.basename(fp) for fp in file_paths)

    if not parsed_files:
        _log("\nNo files could be parsed. Job FAILED.")
        failed_excs = build_failed_file_exceptions(failed_files, job_id, domain)
        exc_df = build_record_exceptions_df([], failed_excs)
        arch_df = build_archive_lineage_df(archive_lineage_rows)
        exc_path = write_exceptions(exc_df, output_dir, system_cfg, job_id)
        arch_path = write_archive_lineage(arch_df, output_dir, job_id)
        summary = build_job_summary(
            job_id=job_id, domain=domain,
            source_filename=source_filename_str,
            job_status="FAILED",
            files_processed=0, files_failed=len(failed_files),
            total_source_rows=0, total_canonical_rows=0,
            total_exceptions=len(failed_excs),
            mapping_results=[], blocked_mandatory=[],
            job_start=job_start, cfg=system_cfg,
            canonical_model_version=canonical_model_version,
        )
        write_job_summary(summary, output_dir, job_id)
        return summary

    # --- Process each parsed file ---
    all_mapping_results = []
    all_exceptions: list[dict] = []
    all_dq_reports = []
    merged_canonical: dict[str, list[pd.DataFrame]] = {}
    total_source_rows = 0
    blocked_mandatory_all = []
    blocked_files_count = 0  # for per-file BLOCKED logic (spec §7.6)

    # File-level failures -> exceptions
    failed_excs = build_failed_file_exceptions(failed_files, job_id, domain)
    all_exceptions.extend(failed_excs)

    for pf in parsed_files:
        total_source_rows += len(pf.dataframe)
        _log(f"\nProcessing '{pf.source_filename}' ({len(pf.dataframe)} rows, format={pf.file_format})...")

        can_tables, mapping_results, mapping_ref_id, file_excs, dq_report, blocked = _process_parsed_file(
            pf, lookup_table, canonical_model, system_cfg,
            job_id, domain, user_overrides, contributor_id,
            config_dir=config_dir,
        )

        all_mapping_results.extend(mapping_results)
        all_exceptions.extend(file_excs)
        if blocked:
            blocked_mandatory_all.extend(blocked)
            blocked_files_count += 1

        if dq_report:
            all_dq_reports.append(dq_report)

        for tbl, df in can_tables.items():
            merged_canonical.setdefault(tbl, []).append(df)

    # Merge canonical tables across files
    final_canonical: dict[str, pd.DataFrame] = {
        tbl: pd.concat(dfs, ignore_index=True)
        for tbl, dfs in merged_canonical.items()
        if dfs
    }

    total_canonical_rows = sum(len(df) for df in final_canonical.values())

    # --- Build lineage DataFrames ---
    _log("\nBuilding lineage...")
    column_lineage_df = build_column_lineage_df(all_mapping_results)
    archive_lineage_df = build_archive_lineage_df(archive_lineage_rows)
    exceptions_df = build_record_exceptions_df(all_exceptions)

    # --- Merge DQ reports ---
    merged_dq = {
        "job_id": job_id,
        "source_filename": source_filename_str,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_exceptions": len(all_exceptions),
        "exception_summary": {},
        "tables": {},
        "per_file_reports": all_dq_reports,
    }
    pass_fill = system_cfg.get("quality", {}).get("dq_pass_fill_rate", 95)
    warn_fill = system_cfg.get("quality", {}).get("dq_warn_fill_rate", 70)

    for report in all_dq_reports:
        for k, v in report.get("exception_summary", {}).items():
            merged_dq["exception_summary"][k] = merged_dq["exception_summary"].get(k, 0) + v

        for tbl, tdata in report.get("tables", {}).items():
            if tbl not in merged_dq["tables"]:
                merged_dq["tables"][tbl] = tdata
                continue

            # Accumulate per-table stats across files instead of last-file-wins
            existing = merged_dq["tables"][tbl]
            combined_rows = existing["total_rows"] + tdata["total_rows"]
            merged_cols: dict = {}
            all_cols = set(existing.get("columns", {})) | set(tdata.get("columns", {}))

            for col in all_cols:
                e_col = existing.get("columns", {}).get(col)
                t_col = tdata.get("columns", {}).get(col)

                if e_col is None:
                    merged_cols[col] = t_col
                elif t_col is None:
                    merged_cols[col] = e_col
                elif not e_col.get("present") and not t_col.get("present"):
                    merged_cols[col] = {"present": False, "fill_rate": 0.0, "status": "MISSING"}
                else:
                    e_null = e_col.get("null_count", round((1 - e_col.get("fill_rate", 0) / 100) * existing["total_rows"]))
                    t_null = t_col.get("null_count", round((1 - t_col.get("fill_rate", 0) / 100) * tdata["total_rows"]))
                    combined_null = e_null + t_null
                    fill_rate = round((combined_rows - combined_null) / combined_rows * 100, 1) if combined_rows > 0 else 0.0
                    status = "PASS" if fill_rate >= pass_fill else ("WARN" if fill_rate >= warn_fill else "FAIL")
                    merged_cols[col] = {
                        "present": True,
                        "total_rows": combined_rows,
                        "null_count": combined_null,
                        "fill_rate": fill_rate,
                        "mandatory": e_col.get("mandatory", t_col.get("mandatory", False)),
                        "status": status,
                    }

            merged_dq["tables"][tbl] = {"total_rows": combined_rows, "columns": merged_cols}

    # --- Determine job status ---
    # Per spec §7.6: BLOCKED only if ALL parsed files were blocked
    all_files_blocked = blocked_files_count > 0 and blocked_files_count == len(parsed_files)
    has_exceptions = len(all_exceptions) > 0
    if all_files_blocked:
        job_status = "BLOCKED"
    elif has_exceptions or blocked_files_count > 0:
        # Some files blocked but not all, or there are DQ/mapping exceptions
        job_status = "SUCCESS_WITH_EXCEPTIONS"
    else:
        job_status = "SUCCESS"

    # --- Write outputs ---
    _log(f"\nWriting outputs to '{output_dir}'...")
    canonical_paths = write_canonical_tables(final_canonical, output_dir, system_cfg, job_id)
    exc_path = write_exceptions(exceptions_df, output_dir, system_cfg, job_id)
    lineage_path = write_column_lineage(column_lineage_df, output_dir, job_id)
    arch_path = write_archive_lineage(archive_lineage_df, output_dir, job_id)
    dq_path = write_dq_report(merged_dq, output_dir, system_cfg, job_id)

    summary = build_job_summary(
        job_id=job_id,
        domain=domain,
        source_filename=source_filename_str,
        job_status=job_status,
        files_processed=len(parsed_files),
        files_failed=len(failed_files),
        total_source_rows=total_source_rows,
        total_canonical_rows=total_canonical_rows,
        total_exceptions=len(all_exceptions),
        mapping_results=all_mapping_results,
        blocked_mandatory=blocked_mandatory_all,
        job_start=job_start,
        cfg=system_cfg,
        canonical_model_version=canonical_model_version,
    )
    summary_path = write_job_summary(summary, output_dir, job_id)

    # --- Console summary ---
    _log(f"\n{'='*60}")
    _log(f"JOB STATUS  : {job_status}")
    _log(f"Source rows : {total_source_rows}")
    _log(f"Canonical   : {total_canonical_rows} rows across {len(final_canonical)} tables")
    _log(f"Exceptions  : {len(all_exceptions)}")
    _log(f"Mapping     : {summary['Columns_Mapped_Exact']} exact, "
         f"{summary['Columns_Mapped_Fuzzy']} fuzzy, "
         f"{summary['Columns_Mapped_LLM']} LLM, "
         f"{summary['Columns_Unmapped']} unmapped")
    if blocked_mandatory_all:
        _log(f"BLOCKED     : {blocked_files_count}/{len(parsed_files)} files blocked, "
             f"{len(blocked_mandatory_all)} mandatory columns unresolved")
    print(f"\nOutputs:")
    for tbl, path in canonical_paths.items():
        print(f"  [{tbl}] {path}")
    if exc_path:
        print(f"  [EXCEPTIONS] {exc_path}")
    if lineage_path:
        _log(f"  [COLUMN_LINEAGE] {lineage_path}")
    if arch_path:
        _log(f"  [ARCHIVE_LINEAGE] {arch_path}")
    _log(f"  [DQ REPORT] {dq_path}")
    _log(f"  [JOB SUMMARY] {summary_path}")
    _log(f"{'='*60}\n")

    # --- Preview ---
    if preview:
        preview_rows = system_cfg.get("output", {}).get("preview_rows", 20)
        for tbl, df in final_canonical.items():
            _log(f"\n--- Preview: {tbl} (first {preview_rows} rows) ---")
            _log(df.head(preview_rows).to_string(index=False))

        if not exceptions_df.empty:
            max_disp = system_cfg.get("output", {}).get("max_exceptions_display", 200)
            _log(f"\n--- Exceptions (first {min(max_disp, len(exceptions_df))}) ---")
            _log(exceptions_df.head(max_disp).to_string(index=False))

    # Attach output artefacts to summary for programmatic consumers (e.g. Gradio UI)
    summary["_output_files"] = {
        "canonical": canonical_paths,          # {table: path}
        "exceptions": exc_path,
        "column_lineage": lineage_path,
        "archive_lineage": arch_path,
        "dq_report": dq_path,
        "job_summary": summary_path,
    }
    summary["_dataframes"] = {
        "canonical": final_canonical,          # {table: DataFrame}
        "exceptions": exceptions_df,
        "column_lineage": column_lineage_df,
        "archive_lineage": archive_lineage_df,
        "dq_report": merged_dq,
    }

    return summary


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_overrides(override_args: list[str]) -> dict[str, tuple[str, str]]:
    """Parse --override "Source Col=TBL.canonical_col" into dict."""
    result = {}
    for arg in (override_args or []):
        if "=" not in arg:
            print(f"WARNING: Ignoring invalid override (no '='): {arg}")
            continue
        src, target = arg.split("=", 1)
        if "." not in target:
            print(f"WARNING: Ignoring invalid override target (no '.'): {target}")
            continue
        tbl, col = target.split(".", 1)
        result[src.strip()] = (tbl.strip(), col.strip())
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Universal Data Ingestion and Normalisation Pipeline"
    )
    parser.add_argument("files", nargs="+", help="One or more input file paths (CSV, XLSX, JSON, XML, ZIP, etc.)")
    parser.add_argument("--domain", default="trade", help="Domain key (default: trade)")
    parser.add_argument("--config-dir", default=".", help="Directory with config files (default: .)")
    parser.add_argument("--output-dir", default="./output", help="Output directory (default: ./output)")
    parser.add_argument("--override", action="append", metavar="SRC=TBL.COL",
                        help="User column override, e.g. 'Inv No=TRD_INVOICE.Invoice_Number'")
    parser.add_argument("--contributor-id", default="UNKNOWN", help="Contributor identifier")
    parser.add_argument("--preview", action="store_true", help="Print output preview to console")
    args = parser.parse_args()

    for f in args.files:
        if not os.path.exists(f):
            print(f"ERROR: File not found: {f}")
            sys.exit(1)

    overrides = _parse_overrides(args.override or [])

    summary = run_pipeline(
        file_paths=args.files,
        domain=args.domain,
        config_dir=args.config_dir,
        output_dir=args.output_dir,
        user_overrides=overrides,
        contributor_id=args.contributor_id,
        preview=args.preview,
    )

    status = summary.get("Job_Status", "UNKNOWN")
    sys.exit(0 if status in ("SUCCESS", "SUCCESS_WITH_EXCEPTIONS") else 1)


if __name__ == "__main__":
    main()
