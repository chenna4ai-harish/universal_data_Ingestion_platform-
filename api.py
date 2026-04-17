"""
api.py
------
FastAPI REST router for the Universal Data Ingestion & Normalisation Platform.

All endpoints call the same pipeline/engine functions that the Gradio UI uses —
no business logic lives here. This file is purely HTTP transport.

Endpoints
---------
POST   /api/v1/jobs/run         Run the full ingestion pipeline
POST   /api/v1/jobs/analyze     Column-mapping analysis only (no output written)
GET    /api/v1/profiles         List saved mapping profiles
DELETE /api/v1/profiles/{id}    Delete a profile by its 8-char short ID
GET    /api/v1/health           Liveness / version check

Running
-------
  # Gradio UI at /ui  +  REST API at /api/v1/...  on the same port:
  uvicorn server:app --port 7861 --reload

  # API only (no Gradio):
  uvicorn api:standalone_app --port 7861 --reload
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi import FastAPI
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Paths — same as app.py so both share one config / output dir
# ---------------------------------------------------------------------------

CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(CONFIG_DIR, "output")

# ---------------------------------------------------------------------------
# Router — included into either server.py (shared) or standalone_app below
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/v1", tags=["ingestion"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _save_upload(upload: UploadFile, tmp_dir: str) -> str:
    """Write an UploadFile to a temp file, preserving the original filename."""
    original_name = Path(upload.filename or "upload").name
    # Prefix with a short UUID to avoid collisions when two files share the same name
    dest = os.path.join(tmp_dir, f"{uuid.uuid4().hex[:8]}_{original_name}")
    content = upload.file.read()
    with open(dest, "wb") as fh:
        fh.write(content)
    return dest


def _parse_overrides_json(overrides_json: str) -> dict[str, tuple[str, str]]:
    """
    Parse overrides from JSON string.

    Expected format:
        {"Invoice_Number": "TRD_INVOICE.Invoice_Number",
         "Account_Number": "TRD_CUSTOMER.Account_Number"}

    Returns internal format:
        {source_col: (canonical_table, canonical_column)}
    """
    if not overrides_json or overrides_json.strip() in ("", "{}", "null"):
        return {}
    try:
        raw = json.loads(overrides_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"overrides is not valid JSON: {exc}")
    result: dict[str, tuple[str, str]] = {}
    for src, tgt in raw.items():
        if "." not in str(tgt):
            raise HTTPException(
                status_code=422,
                detail=f"Override target must be 'TABLE.Column', got '{tgt}' for source '{src}'",
            )
        tbl, col = str(tgt).split(".", 1)
        result[src] = (tbl.strip(), col.strip())
    return result


def _clean_summary(summary: dict) -> dict:
    """Remove non-serialisable internal keys (_dataframes, _output_files DataFrames)."""
    clean = {k: v for k, v in summary.items() if not k.startswith("_")}
    # Attach output file paths (strings only — no DataFrames)
    out_files = summary.get("_output_files", {})
    if isinstance(out_files, dict):
        clean["output_files"] = out_files
    return clean


# ---------------------------------------------------------------------------
# POST /api/v1/jobs/run
# ---------------------------------------------------------------------------

@router.post(
    "/jobs/run",
    summary="Run the full ingestion pipeline",
    response_class=JSONResponse,
)
async def run_job(
    files: Annotated[list[UploadFile], File(description="One or more source files (CSV, XLSX, JSON, XML, ZIP, …)")],
    domain: Annotated[str, Form(description="Domain key, e.g. 'trade'")] = "trade",
    contributor_id: Annotated[str, Form()] = "UNKNOWN",
    overrides: Annotated[
        str,
        Form(description='JSON object mapping source columns to canonical targets, e.g. {"Inv No": "TRD_INVOICE.Invoice_Number"}'),
    ] = "{}",
    llm_provider: Annotated[str, Form(description="LLM provider: None / Claude / OpenAI / Gemini")] = "None",
    llm_api_key: Annotated[str, Form()] = "",
    llm_accept_threshold: Annotated[int, Form(ge=0, le=100)] = 55,
    mandatory_threshold: Annotated[int, Form(ge=0, le=100)] = 80,
    fuzzy_min_similarity: Annotated[int, Form(ge=0, le=100, description="0-100 (UI scale)")] = 70,
):
    """
    Run the full ingestion pipeline against one or more uploaded files.

    All files in a batch share one **Job_ID** and produce merged canonical output.

    **Overrides** are supplied as a JSON object:
    ```json
    {"Billing Ref": "TRD_INVOICE.Invoice_Number", "Cust Code": "TRD_CUSTOMER.Account_Number"}
    ```

    **Returns** the job summary JSON.  The `output_files` key contains paths to
    all artefacts written to the server's output directory.
    """
    from pipeline import run_pipeline

    if not files:
        raise HTTPException(status_code=422, detail="At least one file is required")

    user_overrides = _parse_overrides_json(overrides)

    llm_override = {
        "provider": llm_provider,
        "api_key": llm_api_key.strip(),
        "confidence_accept_threshold": llm_accept_threshold,
        "mandatory_threshold": mandatory_threshold,
        "fuzzy_min_similarity": fuzzy_min_similarity / 100.0,
        "llm_disambiguation_required_below": 70,
        "apply_to": "unmatched_only",
    }

    # Save uploads to a temp directory for this request
    with tempfile.TemporaryDirectory() as tmp_dir:
        file_paths = [_save_upload(f, tmp_dir) for f in files]

        try:
            summary = run_pipeline(
                file_paths=file_paths,
                domain=domain,
                config_dir=CONFIG_DIR,
                output_dir=OUTPUT_DIR,
                user_overrides=user_overrides,
                contributor_id=contributor_id or "UNKNOWN",
                llm_override=llm_override,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    return JSONResponse(content=_clean_summary(summary))


# ---------------------------------------------------------------------------
# POST /api/v1/jobs/analyze
# ---------------------------------------------------------------------------

@router.post(
    "/jobs/analyze",
    summary="Analyze column mappings (no output written)",
    response_class=JSONResponse,
)
async def analyze_job(
    files: Annotated[list[UploadFile], File(description="One or more source files")],
    domain: Annotated[str, Form()] = "trade",
    overrides: Annotated[str, Form()] = "{}",
    llm_provider: Annotated[str, Form()] = "None",
    llm_api_key: Annotated[str, Form()] = "",
    llm_accept_threshold: Annotated[int, Form(ge=0, le=100)] = 55,
    mandatory_threshold: Annotated[int, Form(ge=0, le=100)] = 80,
    fuzzy_min_similarity: Annotated[int, Form(ge=0, le=100)] = 70,
    check_profiles: Annotated[bool, Form(description="Run profile matching before analysis")] = True,
):
    """
    Parse uploaded file(s), check saved profiles, and run the column mapping engine.

    **No canonical rows or output files are written.**  Use this to preview the
    mapping scorecard and decide on overrides before calling `/jobs/run`.

    **Returns** a list of scorecard rows (one per source column per file) plus
    a `profile_match` section describing any profile hit.
    """
    from pipeline import load_config, load_lookup
    from app import _apply_llm_override_to_config
    from engine.file_parser import parse_input_file
    from engine.column_mapper import map_columns

    if not files:
        raise HTTPException(status_code=422, detail="At least one file is required")

    user_overrides = _parse_overrides_json(overrides)

    llm_override_dict = {
        "provider": llm_provider,
        "api_key": llm_api_key.strip(),
        "confidence_accept_threshold": llm_accept_threshold,
        "mandatory_threshold": mandatory_threshold,
        "fuzzy_min_similarity": fuzzy_min_similarity / 100.0,
        "llm_disambiguation_required_below": 70,
        "apply_to": "unmatched_only",
    }

    try:
        system_cfg, canonical_model, _ = load_config(CONFIG_DIR, domain)
        lookup_table = load_lookup(CONFIG_DIR, domain)
        _apply_llm_override_to_config(system_cfg, llm_override_dict)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    analyze_job_id = f"ANALYZE-{uuid.uuid4()}"
    scorecard_rows: list[dict] = []
    profile_match_info: dict = {}

    with tempfile.TemporaryDirectory() as tmp_dir:
        file_paths = [_save_upload(f, tmp_dir) for f in files]

        # Parse all files
        all_parsed = []
        all_failed = []
        for fp in file_paths:
            try:
                parsed, failed, _ = parse_input_file(
                    file_path=fp, cfg=system_cfg, job_id=analyze_job_id
                )
                all_parsed.extend(parsed)
                all_failed.extend(failed)
            except Exception as exc:
                raise HTTPException(status_code=500, detail=f"Parse error: {exc}")

        if not all_parsed:
            reasons = "; ".join(f"{ff.source_filename}: {ff.reason}" for ff in all_failed[:3])
            raise HTTPException(status_code=422, detail=f"No parseable files. {reasons}")

        # Profile check
        if check_profiles:
            try:
                from engine.profile_store import match_profile, increment_use_count
                best_tier = "NONE"
                for pf in all_parsed:
                    cols = list(pf.dataframe.columns)
                    pm = match_profile(cols, CONFIG_DIR, domain, cfg=system_cfg)
                    if pm.tier == "EXACT" and pm.profile:
                        increment_use_count(pm.profile.fingerprint, CONFIG_DIR, domain)
                        # Merge profile overrides under user overrides
                        for src, tgt in pm.profile.overrides.items():
                            if "." in str(tgt):
                                t, c = str(tgt).split(".", 1)
                                user_overrides.setdefault(src, (t, c))
                        profile_match_info = {
                            "tier": "EXACT",
                            "profile_name": pm.profile.name,
                            "profile_id": pm.profile.fingerprint[:8],
                            "overlap": 1.0,
                        }
                        best_tier = "EXACT"
                    elif pm.tier == "PARTIAL" and pm.profile and best_tier == "NONE":
                        profile_match_info = {
                            "tier": "PARTIAL",
                            "profile_name": pm.profile.name,
                            "profile_id": pm.profile.fingerprint[:8],
                            "overlap": round(pm.overlap, 3),
                        }
                        best_tier = "PARTIAL"
            except Exception:
                pass  # profile errors are non-fatal

        # Run column mapper for each parsed file
        for pf in all_parsed:
            mapping_results, _ = map_columns(
                source_columns=list(pf.dataframe.columns),
                lookup_table=lookup_table,
                canonical_model=canonical_model,
                cfg=system_cfg,
                job_id=analyze_job_id,
                domain=domain,
                source_filename=pf.source_filename,
                user_overrides=user_overrides,
                archive_lineage_id=pf.archive_lineage_id,
                config_dir=CONFIG_DIR,
            )
            for r in mapping_results:
                suggested = (
                    f"{r.canonical_table}.{r.canonical_column}"
                    if r.canonical_table and r.canonical_column != "UNMAPPED"
                    else "UNMAPPED"
                )
                scorecard_rows.append({
                    "source_file": r.source_filename,
                    "source_column": r.source_column_name,
                    "source_column_normalised": r.source_column_normalised,
                    "suggested_target": suggested,
                    "match_method": r.match_method,
                    "confidence_score": r.confidence_score,
                    "was_mandatory": r.was_mandatory,
                    "met_threshold": r.met_threshold,
                    "is_propagated": r.is_propagated,
                    "llm_reasoning": r.llm_reasoning,
                })

    blocked = [r for r in scorecard_rows if r["was_mandatory"] and not r["met_threshold"]]

    return JSONResponse(content={
        "analyze_job_id": analyze_job_id,
        "domain": domain,
        "files_analyzed": len(all_parsed),
        "total_columns": len(scorecard_rows),
        "blocked_mandatory_count": len(blocked),
        "profile_match": profile_match_info or {"tier": "NONE"},
        "scorecard": scorecard_rows,
    })


# ---------------------------------------------------------------------------
# POST /api/v1/jobs/auto
# ---------------------------------------------------------------------------

@router.post(
    "/jobs/auto",
    summary="Automated profile-gated ingestion pipeline",
    response_class=JSONResponse,
)
async def run_auto_job(
    files: Annotated[list[UploadFile], File(description="One or more source files (CSV, XLSX, JSON, XML, ZIP, …)")],
    domain: Annotated[str, Form(description="Domain key, e.g. 'trade'")] = "trade",
    contributor_id: Annotated[str, Form()] = "UNKNOWN",
):
    """
    Profile-gated automated ingestion pipeline.

    Each uploaded file is checked against saved mapping profiles **before**
    any processing occurs:

    - **EXACT match** → file is processed in full; canonical output is written.
    - **PARTIAL / NONE match** → file is skipped; a clear message is returned.
      No partial output is written for that file.

    **Returns** a summary of accepted/rejected files plus the job summary for
    all accepted files.

    ```json
    {
      "accepted_count": 2,
      "rejected_count": 1,
      "file_results": [
        {"file": "invoices.csv", "status": "ACCEPTED", "profile_tier": "EXACT", ...},
        {"file": "unknown.csv",  "status": "REJECTED", "profile_tier": "NONE",  ...}
      ],
      "job_summary": { "Job_Status": "SUCCESS", ... }
    }
    ```
    """
    from auto_pipeline import run_auto_pipeline

    if not files:
        raise HTTPException(status_code=422, detail="At least one file is required")

    with tempfile.TemporaryDirectory() as tmp_dir:
        file_paths = [_save_upload(f, tmp_dir) for f in files]

        try:
            result = run_auto_pipeline(
                file_paths=file_paths,
                domain=domain,
                config_dir=CONFIG_DIR,
                output_dir=OUTPUT_DIR,
                contributor_id=contributor_id or "UNKNOWN",
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    # Clean up non-serialisable internals from job_summary
    if result.get("job_summary"):
        result["job_summary"] = _clean_summary(result["job_summary"])

    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# GET /api/v1/profiles
# ---------------------------------------------------------------------------

@router.get(
    "/profiles",
    summary="List saved mapping profiles",
    response_class=JSONResponse,
)
async def list_saved_profiles(
    domain: Annotated[str, Query(description="Domain key")] = "trade",
):
    """
    Return all saved mapping profiles for the domain, sorted by use count.

    Each entry includes the 8-char short ID, name, column count, override count,
    use count, and last-used date.
    """
    from engine.profile_store import list_profiles, _load_profile

    profiles_meta = list_profiles(CONFIG_DIR, domain)
    result = []
    for meta in profiles_meta:
        fp = meta.get("fingerprint", "")
        profile = _load_profile(CONFIG_DIR, domain, fp)
        result.append({
            "id": fp[:8],
            "fingerprint": fp,
            "name": meta.get("name", "-"),
            "column_count": meta.get("column_count", 0),
            "override_count": len(profile.overrides) if profile else 0,
            "use_count": meta.get("use_count", 0),
            "last_used": meta.get("last_used", "-"),
        })
    return JSONResponse(content={"domain": domain, "profiles": result})


# ---------------------------------------------------------------------------
# DELETE /api/v1/profiles/{profile_id}
# ---------------------------------------------------------------------------

@router.delete(
    "/profiles/{profile_id}",
    summary="Delete a profile by its 8-char short ID",
    response_class=JSONResponse,
)
async def delete_saved_profile(
    profile_id: str,
    domain: Annotated[str, Query()] = "trade",
):
    """
    Delete a mapping profile.  `profile_id` is the 8-character short ID shown
    in `GET /api/v1/profiles` and in the Gradio Profiles tab.
    """
    from engine.profile_store import _load_index, delete_profile

    index = _load_index(CONFIG_DIR, domain)
    full_fp = next((k for k in index if k.startswith(profile_id)), None)
    if not full_fp:
        raise HTTPException(status_code=404, detail=f"Profile '{profile_id}' not found")
    name = index[full_fp].get("name", profile_id)
    delete_profile(full_fp, CONFIG_DIR, domain)
    return JSONResponse(content={"deleted": True, "profile_id": profile_id, "name": name})


# ---------------------------------------------------------------------------
# GET /api/v1/health
# ---------------------------------------------------------------------------

@router.get("/health", summary="Liveness check", response_class=JSONResponse)
async def health():
    """Returns platform version info and confirms the config is readable."""
    from pipeline import load_config

    try:
        cfg, _, _ = load_config(CONFIG_DIR, "trade")
        config_version = cfg.get("_metadata", {}).get("config_version", "unknown")
        status = "ok"
    except Exception as exc:
        config_version = "error"
        status = f"config_error: {exc}"

    return JSONResponse(content={
        "status": status,
        "platform": "Universal Data Ingestion & Normalisation Platform",
        "config_version": config_version,
        "config_dir": CONFIG_DIR,
    })


# ---------------------------------------------------------------------------
# Standalone app (no Gradio) — for API-only deployments
# ---------------------------------------------------------------------------

standalone_app = FastAPI(
    title="Data Ingestion Platform API",
    description="REST API for the Universal Data Ingestion & Normalisation Platform. "
                "For the full UI, run server.py instead.",
    version="1.0.0",
)
standalone_app.include_router(router)
