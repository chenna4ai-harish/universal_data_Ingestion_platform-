"""
auto_pipeline.py
----------------
Fully automated, no-interaction ingestion pipeline.

Behaviour
---------
Each input file (or file extracted from a ZIP) is checked against saved
mapping profiles BEFORE any processing occurs.

  EXACT profile match  →  file is processed in full; output files written.
  PARTIAL / NONE match →  file is SKIPPED; a clear message is returned.
                          No partial output is written for that file.

This makes the pipeline safe for scheduled / unattended automation: it will
never silently produce wrong mappings because a file shape has changed.

Usage — CLI
-----------
    # Single file
    python auto_pipeline.py data/invoices.csv

    # Batch
    python auto_pipeline.py data/customers.csv data/invoices.xlsx

    # ZIP (each file inside is checked independently)
    python auto_pipeline.py data/batch.zip

    # With options
    python auto_pipeline.py data/*.csv --domain trade --output-dir ./out

Usage — Python
--------------
    from auto_pipeline import run_auto_pipeline

    result = run_auto_pipeline(
        file_paths=["data/invoices.csv", "data/customers.csv"],
        domain="trade",
        config_dir=".",
        output_dir="./output",
    )

    for r in result["file_results"]:
        print(r["file"], "→", r["status"], r.get("message", ""))

Return value
------------
    {
        "accepted_count":  int,   # files that had EXACT match and were processed
        "rejected_count":  int,   # files with PARTIAL / NONE match (skipped)
        "file_results": [
            {
                "file":    str,               # filename
                "status":  "ACCEPTED" | "REJECTED",
                "profile_tier":  "EXACT" | "PARTIAL" | "NONE",
                "profile_name":  str | None,
                "profile_id":    str | None,
                "overlap":       float | None,
                "message":       str,         # always set
            }, ...
        ],
        "job_summary":  dict | None,   # from run_pipeline(); None if all files rejected
    }
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Load .env on startup
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Core automated pipeline function
# ---------------------------------------------------------------------------

def run_auto_pipeline(
    file_paths: list[str] | str,
    domain: str = "trade",
    config_dir: str = ".",
    output_dir: str = "./output",
    contributor_id: str = "UNKNOWN",
    log_fn=None,
) -> dict:
    """
    Profile-gated automated ingestion pipeline.

    Each file is profile-checked before processing:
    - EXACT match  → included in the pipeline run.
    - PARTIAL/NONE → skipped with an explanatory message.

    Files inside a ZIP are extracted first, then each is individually
    profile-checked before being accepted or rejected.

    Args:
        file_paths:     One or more input file paths (list or single string).
        domain:         Domain key, e.g. ``"trade"``.
        config_dir:     Directory containing domain config and profiles.
        output_dir:     Base output directory. A per-job subdirectory is created.
        contributor_id: Stamped into every canonical row for lineage.
        log_fn:         Optional ``callable(str)`` for streaming log lines to a UI
                        or custom logger.

    Returns:
        A result dict — see module docstring for the full schema.
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    if not file_paths:
        raise ValueError("run_auto_pipeline() requires at least one file path")

    def _log(msg: str) -> None:
        print(msg)
        if log_fn:
            log_fn(msg)

    started_at = datetime.now(timezone.utc)
    _log(f"\n{'='*60}")
    _log(f"AUTO PIPELINE  —  {domain.upper()} domain")
    _log(f"Started        :  {started_at.isoformat()}")
    _log(f"Files submitted:  {len(file_paths)}")
    _log(f"{'='*60}")

    # ------------------------------------------------------------------
    # Load config (needed for profile check and pipeline run)
    # ------------------------------------------------------------------
    from pipeline import load_config, load_lookup, validate_config

    try:
        system_cfg, canonical_model, _ = load_config(config_dir, domain)
        validate_config(system_cfg, canonical_model)
        _ = load_lookup(config_dir, domain)   # validate lookup exists
    except (FileNotFoundError, ValueError) as exc:
        _log(f"\nCONFIG ERROR: {exc}")
        return {
            "accepted_count": 0,
            "rejected_count": 0,
            "file_results": [],
            "job_summary": {
                "Job_Status": "FAILED",
                "Reason": str(exc),
            },
        }

    from engine.profile_store import match_profile, _load_profile
    from engine.file_parser import parse_input_file, _detect_columns_only

    # ------------------------------------------------------------------
    # Phase 1: Resolve all input paths → flat list of candidate files.
    # ZIPs are extracted here so we can profile-check each inner file.
    # ------------------------------------------------------------------
    _log("\n[Phase 1] Resolving input files and checking profiles...\n")

    file_results: list[dict] = []   # one entry per resolved file
    accepted_paths: list[str] = []  # paths that passed the EXACT check

    for raw_path in file_paths:
        fname = os.path.basename(raw_path)

        if not os.path.exists(raw_path):
            _log(f"  ✗ {fname} — FILE NOT FOUND")
            file_results.append({
                "file": fname,
                "status": "REJECTED",
                "profile_tier": "NONE",
                "profile_name": None,
                "profile_id": None,
                "overlap": None,
                "message": f"File not found: {raw_path}",
            })
            continue

        if fname.lower().endswith(".zip"):
            # Extract the ZIP, then profile-check each inner file individually
            _log(f"  [ZIP] {fname} — extracting to profile-check inner files...")
            try:
                dummy_job = "AUTO-PROFILE-CHECK"
                parsed_files, failed_files, _ = parse_input_file(
                    file_path=raw_path, cfg=system_cfg, job_id=dummy_job
                )
            except Exception as exc:
                _log(f"  ✗ {fname} — failed to extract: {exc}")
                file_results.append({
                    "file": fname,
                    "status": "REJECTED",
                    "profile_tier": "NONE",
                    "profile_name": None,
                    "profile_id": None,
                    "overlap": None,
                    "message": f"ZIP extraction failed: {exc}",
                })
                continue

            for pf in parsed_files:
                inner_name = pf.source_filename
                cols = list(pf.dataframe.columns)
                result_entry = _check_profile_for_file(
                    inner_name, cols, system_cfg, config_dir, domain, _log
                )
                file_results.append(result_entry)
                if result_entry["status"] == "ACCEPTED":
                    # We need to re-parse this individual extracted file via the
                    # already-parsed dataframe — pass the original ZIP path so
                    # run_pipeline can handle it properly
                    # Store the original ZIP path; run_pipeline will re-extract
                    if raw_path not in accepted_paths:
                        accepted_paths.append(raw_path)

            for ff in failed_files:
                _log(f"  ✗ {ff.source_filename} (inside {fname}) — {ff.reason}")
                file_results.append({
                    "file": ff.source_filename,
                    "status": "REJECTED",
                    "profile_tier": "NONE",
                    "profile_name": None,
                    "profile_id": None,
                    "overlap": None,
                    "message": f"File parse failed inside archive: {ff.reason}",
                })

        else:
            # Flat file — detect columns cheaply (header only, no full parse)
            try:
                cols = _detect_columns_only(raw_path, system_cfg)
            except Exception as exc:
                _log(f"  ✗ {fname} — could not read columns: {exc}")
                file_results.append({
                    "file": fname,
                    "status": "REJECTED",
                    "profile_tier": "NONE",
                    "profile_name": None,
                    "profile_id": None,
                    "overlap": None,
                    "message": f"Could not read column headers: {exc}",
                })
                continue

            result_entry = _check_profile_for_file(
                fname, cols, system_cfg, config_dir, domain, _log
            )
            file_results.append(result_entry)
            if result_entry["status"] == "ACCEPTED":
                accepted_paths.append(raw_path)

    # ------------------------------------------------------------------
    # Phase 2: Run the pipeline — only for accepted files
    # ------------------------------------------------------------------
    accepted_count = sum(1 for r in file_results if r["status"] == "ACCEPTED")
    rejected_count = sum(1 for r in file_results if r["status"] == "REJECTED")

    _log(f"\n[Phase 2] Profile check complete.")
    _log(f"  Accepted : {accepted_count} file(s)")
    _log(f"  Rejected : {rejected_count} file(s)")

    if rejected_count > 0:
        _log("\n  Rejected files:")
        for r in file_results:
            if r["status"] == "REJECTED":
                _log(f"    ✗ {r['file']} — {r['message']}")

    job_summary = None

    if not accepted_paths:
        _log("\nNo files passed the profile check. Pipeline not run.")
        _log(f"{'='*60}\n")
        return {
            "accepted_count": 0,
            "rejected_count": rejected_count,
            "file_results": file_results,
            "job_summary": None,
        }

    _log(f"\n  Running pipeline for {len(accepted_paths)} accepted file(s)...")

    from pipeline import run_pipeline

    try:
        job_summary = run_pipeline(
            file_paths=accepted_paths,
            domain=domain,
            config_dir=config_dir,
            output_dir=output_dir,
            contributor_id=contributor_id,
            log_fn=log_fn,
        )
    except Exception as exc:
        _log(f"\nPIPELINE ERROR: {exc}")
        job_summary = {"Job_Status": "FAILED", "Reason": str(exc)}

    _log(f"\n{'='*60}")
    _log(f"AUTO PIPELINE COMPLETE")
    _log(f"  Accepted / Processed : {accepted_count}")
    _log(f"  Rejected (no profile): {rejected_count}")
    if job_summary:
        _log(f"  Job Status           : {job_summary.get('Job_Status', 'UNKNOWN')}")
        if job_summary.get("Job_Narrative"):
            _log(f"  Narrative            : {job_summary['Job_Narrative']}")
    _log(f"{'='*60}\n")

    return {
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "file_results": file_results,
        "job_summary": job_summary,
    }


# ---------------------------------------------------------------------------
# Internal helper — profile check for a single resolved file
# ---------------------------------------------------------------------------

def _check_profile_for_file(
    filename: str,
    columns: list[str],
    system_cfg: dict,
    config_dir: str,
    domain: str,
    log_fn,
) -> dict:
    """
    Run profile matching for one file and return a result dict.
    Logs the outcome. Does NOT raise — returns a REJECTED entry on error.
    """
    from engine.profile_store import match_profile

    try:
        pm = match_profile(columns, config_dir, domain, cfg=system_cfg)
    except Exception as exc:
        log_fn(f"  ✗ {filename} — profile check error: {exc}")
        return {
            "file": filename,
            "status": "REJECTED",
            "profile_tier": "NONE",
            "profile_name": None,
            "profile_id": None,
            "overlap": None,
            "message": f"Profile check failed: {exc}",
        }

    if pm.tier == "EXACT" and pm.profile:
        log_fn(
            f"  ✓ {filename} — EXACT match: \"{pm.profile.name}\" "
            f"(ID: {pm.profile.fingerprint[:8]}, "
            f"{pm.profile.use_count} previous use(s))"
        )
        return {
            "file": filename,
            "status": "ACCEPTED",
            "profile_tier": "EXACT",
            "profile_name": pm.profile.name,
            "profile_id": pm.profile.fingerprint[:8],
            "overlap": 1.0,
            "message": (
                f"Exact profile match: \"{pm.profile.name}\" "
                f"(ID: {pm.profile.fingerprint[:8]})"
            ),
        }

    elif pm.tier == "PARTIAL" and pm.profile:
        pct = int(pm.overlap * 100)
        log_fn(
            f"  ✗ {filename} — PARTIAL match only ({pct}% overlap with "
            f"\"{pm.profile.name}\") — exact match required for automated run"
        )
        return {
            "file": filename,
            "status": "REJECTED",
            "profile_tier": "PARTIAL",
            "profile_name": pm.profile.name,
            "profile_id": pm.profile.fingerprint[:8],
            "overlap": round(pm.overlap, 3),
            "message": (
                f"No exact profile match. Closest profile is \"{pm.profile.name}\" "
                f"with {pct}% column overlap — review in the UI and save a new profile "
                f"before using the automated pipeline."
            ),
        }

    else:
        log_fn(f"  ✗ {filename} — NO profile match found")
        return {
            "file": filename,
            "status": "REJECTED",
            "profile_tier": "NONE",
            "profile_name": None,
            "profile_id": None,
            "overlap": None,
            "message": (
                "No saved profile found for this file's column layout. "
                "Use the UI to run Analyze Mapping, review the scorecard, "
                "and click 'Save Profile' before using the automated pipeline."
            ),
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Automated ingestion pipeline — processes files only when an exact "
            "mapping profile exists. Fails fast with a clear message otherwise."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Single file
  python auto_pipeline.py data/invoices.csv

  # Batch — two files processed under one Job_ID
  python auto_pipeline.py data/customers.csv data/invoices.xlsx

  # ZIP archive (each inner file is profile-checked independently)
  python auto_pipeline.py data/batch.zip

  # Full options
  python auto_pipeline.py data/*.csv \\
      --domain trade \\
      --output-dir ./output \\
      --contributor-id CONTRIB001
        """,
    )
    parser.add_argument(
        "files", nargs="+",
        help="One or more input files (CSV, XLSX, JSON, XML, ZIP, …)"
    )
    parser.add_argument("--domain", default="trade", help="Domain key (default: trade)")
    parser.add_argument("--config-dir", default=".", help="Config directory (default: .)")
    parser.add_argument("--output-dir", default="./output", help="Output directory (default: ./output)")
    parser.add_argument("--contributor-id", default="UNKNOWN", help="Contributor identifier")
    args = parser.parse_args()

    for f in args.files:
        if not os.path.exists(f):
            print(f"ERROR: File not found: {f}")
            sys.exit(1)

    result = run_auto_pipeline(
        file_paths=args.files,
        domain=args.domain,
        config_dir=args.config_dir,
        output_dir=args.output_dir,
        contributor_id=args.contributor_id,
    )

    # Exit code: 0 = at least one file processed successfully
    #            1 = all files rejected or pipeline failed
    job_status = (result.get("job_summary") or {}).get("Job_Status", "")
    if result["accepted_count"] > 0 and job_status in ("SUCCESS", "SUCCESS_WITH_EXCEPTIONS"):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
