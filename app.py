"""
app.py
------
Gradio UI for the Universal Data Ingestion and Normalisation Platform.

Run:
  python app.py
  python app.py --port 7861 --share
"""


from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

import gradio as gr
import pandas as pd

# Load .env on startup
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

sys.path.insert(0, os.path.dirname(__file__))
from pipeline import run_pipeline, load_config, load_lookup
from engine.file_parser import parse_input_file
from engine.column_mapper import map_columns

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONFIG_DIR = os.path.dirname(__file__)
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
DOMAINS = ["trade"]
LLM_PROVIDERS = ["None", "Claude", "OpenAI", "Gemini"]

# Load canonical table names at startup so the UI can create the right number of tabs/downloads.
try:
    _, _startup_canonical_model, _ = load_config(CONFIG_DIR, DOMAINS[0])
    CANONICAL_TABLE_NAMES: list[str] = [
        t for t in _startup_canonical_model
        if not t.startswith("_") and isinstance(_startup_canonical_model[t], dict)
    ]
except Exception:
    CANONICAL_TABLE_NAMES = ["TRD_CUSTOMER", "TRD_INVOICE"]

STATUS_COLORS = {
    "SUCCESS": "SUCCESS",
    "SUCCESS_WITH_EXCEPTIONS": "SUCCESS WITH EXCEPTIONS",
    "BLOCKED": "BLOCKED",
    "FAILED": "FAILED",
}

UI_THEME = gr.themes.Soft(primary_hue="blue", neutral_hue="slate")
UI_CSS = """
.tab-nav button { font-size: 14px !important; }
.gr-button-primary { background: #1a5fb4 !important; }
footer { display: none !important; }
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api_key_from_env(provider: str) -> str:
    """Return the API key already in env for the selected provider."""
    mapping = {
        "Claude": "ANTHROPIC_API_KEY",
        "OpenAI": "OPENAI_API_KEY",
        "Gemini": "GOOGLE_API_KEY",
    }
    env_var = mapping.get(provider, "")
    val = os.environ.get(env_var, "")
    # Mask for display: show first 8 chars + ***
    if val and len(val) > 8:
        return val[:8] + "***"
    return val


def _format_status_badge(status: str) -> str:
    return STATUS_COLORS.get(status, status)


def _summary_html(summary: dict) -> str:
    status = summary.get("Job_Status", "UNKNOWN")
    badge = STATUS_COLORS.get(status, status)
    color = {
        "SUCCESS": "#1a7a4a",
        "SUCCESS_WITH_EXCEPTIONS": "#b07d00",
        "BLOCKED": "#c0392b",
        "FAILED": "#7f1e1e",
    }.get(status, "#333")

    rows = [
        ("Job ID", summary.get("Job_ID", "-")),
        ("Domain", summary.get("Domain", "-")),
        ("Source File", summary.get("Source_Filename", "-")),
        ("Source Rows", summary.get("Total_Source_Rows", 0)),
        ("Canonical Rows Written", summary.get("Total_Canonical_Rows_Written", 0)),
        ("Files Processed", summary.get("Files_Processed", 0)),
        ("Files Failed", summary.get("Files_Failed", 0)),
        ("Total Exceptions", summary.get("Total_Exceptions", 0)),
        ("Mapped - Exact", summary.get("Columns_Mapped_Exact", 0)),
        ("Mapped - Fuzzy", summary.get("Columns_Mapped_Fuzzy", 0)),
        ("Mapped - LLM", summary.get("Columns_Mapped_LLM", 0)),
        ("Unmapped", summary.get("Columns_Unmapped", 0)),
        ("Mandatory Blocked", summary.get("Mandatory_Columns_Blocked", 0)),
        ("Config Version", summary.get("Config_Version", "-")),
        ("Model Version", summary.get("Canonical_Model_Version", "-")),
    ]

    table_rows = "".join(
        f"<tr><td style='padding:4px 12px 4px 0;color:#888;font-size:13px'>{k}</td>"
        f"<td style='padding:4px 0;font-size:13px;font-weight:500'>{v}</td></tr>"
        for k, v in rows
    )

    blocked_detail = ""
    if summary.get("Blocked_Mandatory_Details"):
        cols = ", ".join(summary["Blocked_Mandatory_Details"])
        blocked_detail = f"<p style='color:#c0392b;font-size:12px;margin-top:6px'>Blocked: {cols}</p>"

    return f"""
    <div style='border-left:4px solid {color};padding:12px 16px;background:#fafafa;border-radius:4px'>
      <div style='font-size:18px;font-weight:700;color:{color};margin-bottom:10px'>{badge}</div>
      <table>{table_rows}</table>
      {blocked_detail}
    </div>
    """


def _dq_html(dq: dict) -> str:
    if not dq:
        return "<p style='color:#888'>No DQ report available.</p>"

    exc_summary = dq.get("exception_summary", {})
    tables = dq.get("tables", {})

    exc_rows = "".join(
        f"<tr><td style='padding:3px 12px 3px 0;font-size:13px'>{k}</td>"
        f"<td style='font-size:13px;font-weight:600'>{v}</td></tr>"
        for k, v in exc_summary.items()
    ) if exc_summary else "<tr><td colspan=2 style='color:#888;font-size:13px'>No exceptions</td></tr>"

    tbl_sections = ""
    for tbl, tdata in tables.items():
        col_rows = ""
        for col, cdata in tdata.get("columns", {}).items():
            if not isinstance(cdata, dict):
                continue
            fill = cdata.get("fill_rate", 0)
            status = cdata.get("status", "-")
            mandatory = "*" if cdata.get("mandatory") else ""
            color = {"PASS": "#1a7a4a", "WARN": "#b07d00", "FAIL": "#c0392b", "MISSING": "#888"}.get(status, "#333")
            col_rows += (
                f"<tr><td style='padding:3px 8px 3px 0;font-size:12px'>{mandatory}{col}</td>"
                f"<td style='font-size:12px'>{fill}%</td>"
                f"<td style='font-size:12px;color:{color};font-weight:600'>{status}</td></tr>"
            )
        tbl_sections += (
            f"<h4 style='margin:12px 0 4px;font-size:13px'>{tbl} ({tdata.get('total_rows',0)} rows)</h4>"
            f"<table><tr><th style='text-align:left;font-size:11px;color:#888'>Column</th>"
            f"<th style='text-align:left;font-size:11px;color:#888'>Fill%</th>"
            f"<th style='text-align:left;font-size:11px;color:#888'>Status</th></tr>{col_rows}</table>"
        )

    return f"""
    <div style='font-size:13px'>
      <h4 style='margin:0 0 6px'>Exception Summary</h4>
      <table>{exc_rows}</table>
      <h4 style='margin:12px 0 6px'>Column Fill Rates  <span style='font-size:11px;color:#888'>(* = mandatory)</span></h4>
      {tbl_sections}
    </div>
    """


# ---------------------------------------------------------------------------
# UI workflow helpers
# ---------------------------------------------------------------------------

def _parse_override_text(override_text: str | None) -> dict[str, tuple[str, str]]:
    """Parse multiline override text into {source_col: (table, column)}."""
    user_overrides: dict[str, tuple[str, str]] = {}
    for line in (override_text or "").splitlines():
        line = line.strip()
        if not line or "=" not in line or "." not in line:
            continue
        src, target = line.split("=", 1)
        tbl, col = target.strip().split(".", 1)
        user_overrides[src.strip()] = (tbl.strip(), col.strip())
    return user_overrides


def _format_override_text(overrides: dict[str, tuple[str, str]]) -> str:
    """Serialize override dict to the textbox multiline format."""
    lines = [
        f"{src} = {tbl}.{col}"
        for src, (tbl, col) in sorted(overrides.items(), key=lambda x: x[0].lower())
    ]
    return "\n".join(lines)


def _build_llm_override(
    llm_provider: str,
    api_key_input: str | None,
    mandatory_threshold: int,
    fuzzy_min_similarity: float,
    llm_accept_threshold: int,
    apply_to: str,
) -> dict:
    """Build runtime LLM override payload shared by analyze + run steps."""
    resolved_key = (api_key_input or "").strip()
    return {
        "provider": llm_provider,
        "api_key": resolved_key,
        "confidence_accept_threshold": llm_accept_threshold,
        "mandatory_threshold": mandatory_threshold,
        "fuzzy_min_similarity": fuzzy_min_similarity / 100.0,   # UI shows 0-100, config uses 0.0-1.0
        "llm_disambiguation_required_below": 70,
        "apply_to": "unmatched_only" if apply_to == "Unmatched only" else "all",
    }


def _apply_llm_override_to_config(system_cfg: dict, llm_override: dict | None) -> None:
    """Apply UI LLM/runtime overrides into loaded system config (in-place)."""
    if not llm_override:
        return

    provider = llm_override.get("provider")
    api_key = llm_override.get("api_key", "").strip()

    if provider and provider != "None":
        system_cfg["llm"]["provider"] = provider
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

    for key in (
        "confidence_accept_threshold",
        "mandatory_threshold",
        "fuzzy_min_similarity",
        "llm_disambiguation_required_below",
    ):
        if key in llm_override:
            if key in ("mandatory_threshold", "fuzzy_min_similarity"):
                system_cfg.setdefault("quality", {})[key] = llm_override[key]
            else:
                system_cfg.setdefault("llm", {})[key] = llm_override[key]


def _canonical_target_options(canonical_model: dict) -> list[str]:
    """Flatten canonical model business columns into 'TABLE.Column' options."""
    options = []
    for tbl, tdef in canonical_model.items():
        if tbl.startswith("_") or not isinstance(tdef, dict):
            continue
        for col in tdef.get("business_columns", {}):
            options.append(f"{tbl}.{col}")
    return sorted(options)


def _mapping_target_from_result(mapping_result) -> str:
    if mapping_result.canonical_table and mapping_result.canonical_column != "UNMAPPED":
        return f"{mapping_result.canonical_table}.{mapping_result.canonical_column}"
    return "UNMAPPED"


def _completion_html(summary: dict) -> str:
    """Build top-level completion banner shown near run controls."""
    status = summary.get("Job_Status", "UNKNOWN")
    color = {
        "SUCCESS": "#1a7a4a",
        "SUCCESS_WITH_EXCEPTIONS": "#b07d00",
        "BLOCKED": "#c0392b",
        "FAILED": "#7f1e1e",
    }.get(status, "#333")
    badge = STATUS_COLORS.get(status, status)

    output_dir = "-"
    out_files = summary.get("_output_files", {})
    summary_path = out_files.get("job_summary") if isinstance(out_files, dict) else None
    if summary_path:
        output_dir = str(Path(summary_path).parent)

    return (
        f"<div style='border-left:4px solid {color};background:#fafafa;padding:10px 12px;border-radius:4px'>"
        f"<b style='color:{color}'>Process Completed: {badge}</b><br>"
        f"<span style='font-size:12px'>Job ID: {summary.get('Job_ID', '-')} | Output: {output_dir}</span>"
        f"</div>"
    )


def analyze_mappings(
    uploaded_file,
    domain: str,
    llm_provider: str,
    api_key_input: str,
    mandatory_threshold: int,
    fuzzy_min_similarity: float,
    llm_accept_threshold: int,
    apply_to: str,
    override_text: str,
):
    """
    Step 1: parse input + run mapping only, then expose mapping suggestions
    with confidence scores before full pipeline execution.
    """
    empty_df = pd.DataFrame(columns=[
        "Source_File", "Source_Column", "Suggested_Target", "Selected_Target",
        "Match_Method", "Confidence_Score", "Was_Mandatory",
        "Met_Threshold", "Is_Propagated", "LLM_Reasoning",
    ])

    if uploaded_file is None:
        return (
            "<p style='color:#c0392b'>Upload a file first, then click Analyze Mapping.</p>",
            empty_df,
            gr.update(choices=[], value=None),
            gr.update(choices=[], value=None),
            gr.update(interactive=False),
            "<p style='color:#888'>Waiting for mapping analysis...</p>",
        )

    # Normalise to list of paths (Gradio sends list even for 1 file with file_count="multiple")
    if not isinstance(uploaded_file, list):
        uploaded_file = [uploaded_file]
    file_paths = [f if isinstance(f, str) else f.name for f in uploaded_file]

    if not file_paths:
        return (
            "<p style='color:#c0392b'>Upload a file first, then click Analyze Mapping.</p>",
            empty_df,
            gr.update(choices=[], value=None),
            gr.update(choices=[], value=None),
            gr.update(interactive=False),
            "<p style='color:#888'>Waiting for mapping analysis...</p>",
        )

    user_overrides = _parse_override_text(override_text)
    llm_override = _build_llm_override(
        llm_provider, api_key_input,
        mandatory_threshold, fuzzy_min_similarity,
        llm_accept_threshold, apply_to,
    )

    try:
        system_cfg, canonical_model, _ = load_config(CONFIG_DIR, domain)
        lookup_table = load_lookup(CONFIG_DIR, domain)
        _apply_llm_override_to_config(system_cfg, llm_override)

        analyze_job_id = f"ANALYZE-{uuid.uuid4()}"
        all_parsed = []
        all_failed = []
        for fp in file_paths:
            parsed, failed, _ = parse_input_file(
                file_path=fp,
                cfg=system_cfg,
                job_id=analyze_job_id,
            )
            all_parsed.extend(parsed)
            all_failed.extend(failed)
        parsed_files = all_parsed
        failed_files = all_failed
    except Exception as e:
        return (
            f"<p style='color:#c0392b'>Mapping analysis failed: {e}</p>",
            empty_df,
            gr.update(choices=[], value=None),
            gr.update(choices=[], value=None),
            gr.update(interactive=False),
            "<p style='color:#c0392b'>Analyze step failed. Fix input/config and retry.</p>",
        )

    if not parsed_files:
        fail_notes = "; ".join(f"{ff.source_filename}: {ff.reason}" for ff in failed_files[:3]) or "No parseable files"
        return (
            f"<p style='color:#c0392b'>No parseable files found. {fail_notes}</p>",
            empty_df,
            gr.update(choices=[], value=None),
            gr.update(choices=[], value=None),
            gr.update(interactive=False),
            "<p style='color:#c0392b'>Analyze step failed. No file available for mapping.</p>",
        )

    rows = []
    source_columns = set()
    for pf in parsed_files:
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
            suggested = _mapping_target_from_result(r)
            selected = suggested
            if r.source_column_name in user_overrides:
                tbl, col = user_overrides[r.source_column_name]
                selected = f"{tbl}.{col}"
            source_columns.add(r.source_column_name)
            rows.append({
                "Source_File": r.source_filename,
                "Source_Column": r.source_column_name,
                "Suggested_Target": suggested,
                "Selected_Target": selected,
                "Match_Method": r.match_method,
                "Confidence_Score": r.confidence_score,
                "Was_Mandatory": r.was_mandatory,
                "Met_Threshold": r.met_threshold,
                "Is_Propagated": r.is_propagated,
                "LLM_Reasoning": r.llm_reasoning or "",
            })

    mapping_df = pd.DataFrame(rows) if rows else empty_df
    source_choices = sorted(source_columns)
    target_choices = _canonical_target_options(canonical_model)

    blocked_count = int((~mapping_df["Met_Threshold"]).sum()) if not mapping_df.empty else 0
    analysis_msg = (
        f"<p style='color:#1a7a4a'><b>Mapping Analysis Completed.</b> "
        f"Files: {len(parsed_files)}, Columns analyzed: {len(mapping_df)}, "
        f"Columns below threshold: {blocked_count}."
        f"</p>"
    )

    return (
        analysis_msg,
        mapping_df,
        gr.update(choices=source_choices, value=source_choices[0] if source_choices else None),
        gr.update(choices=target_choices, value=target_choices[0] if target_choices else None),
        gr.update(interactive=True),
        "<p style='color:#1a7a4a'><b>Step 1 complete.</b> Review mappings and run pipeline.</p>",
    )


def apply_mapping_override(
    selected_source: str,
    selected_target: str,
    override_text: str,
):
    """Add/update one override mapping from dropdown editor."""
    if not selected_source:
        return override_text, "<p style='color:#c0392b'>Select a source column first.</p>"
    if not selected_target or "." not in selected_target:
        return override_text, "<p style='color:#c0392b'>Select a valid canonical target.</p>"

    tbl, col = selected_target.split(".", 1)
    overrides = _parse_override_text(override_text)
    overrides[selected_source] = (tbl, col)
    updated_text = _format_override_text(overrides)
    return updated_text, (
        f"<p style='color:#1a7a4a'>Override set: <b>{selected_source}</b> -> "
        f"<b>{tbl}.{col}</b>. Re-run Analyze Mapping to refresh scorecard.</p>"
    )


def clear_mapping_override(
    selected_source: str,
    override_text: str,
):
    """Clear override for selected source column."""
    if not selected_source:
        return override_text, "<p style='color:#c0392b'>Select a source column first.</p>"

    overrides = _parse_override_text(override_text)
    if selected_source in overrides:
        overrides.pop(selected_source, None)
        updated_text = _format_override_text(overrides)
        return updated_text, (
            f"<p style='color:#b07d00'>Override cleared for <b>{selected_source}</b>. "
            f"Re-run Analyze Mapping to refresh scorecard.</p>"
        )
    return override_text, "<p style='color:#888'>No override exists for selected source column.</p>"


# ---------------------------------------------------------------------------
# Core run function (called from Gradio)
# ---------------------------------------------------------------------------

def run_ingestion(
    uploaded_file,
    domain: str,
    llm_provider: str,
    api_key_input: str,
    contributor_id: str,
    mandatory_threshold: int,
    fuzzy_min_similarity: float,
    llm_accept_threshold: int,
    apply_to: str,
    override_text: str,
) -> tuple:
    """
    Called by the Gradio Run button. Returns all UI component updates.
    Yields intermediate log lines for streaming, then final outputs.
    """
    log_lines: list[str] = []

    def log_fn(msg: str) -> None:
        log_lines.append(msg)

    user_overrides = _parse_override_text(override_text)
    llm_override = _build_llm_override(
        llm_provider, api_key_input,
        mandatory_threshold, fuzzy_min_similarity,
        llm_accept_threshold, apply_to,
    )

    # Handle Gradio file object
    if uploaded_file is None:
        _empty = [pd.DataFrame() for _ in CANONICAL_TABLE_NAMES]
        _no_files = [None for _ in CANONICAL_TABLE_NAMES]
        return (
            "<p style='color:#c0392b'>Process not started. Upload a file first.</p>",
            "<p style='color:red'>No file uploaded.</p>",
            "No file uploaded.", "",
            *_empty,
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            *_no_files,
            None, None, None, None, None,
        )

    # Normalise to list of paths (Gradio sends list even for 1 file with file_count="multiple")
    if not isinstance(uploaded_file, list):
        uploaded_file = [uploaded_file]
    file_paths = [f if isinstance(f, str) else f.name for f in uploaded_file]

    if not file_paths:
        _empty = [pd.DataFrame() for _ in CANONICAL_TABLE_NAMES]
        _no_files = [None for _ in CANONICAL_TABLE_NAMES]
        return (
            "<p style='color:#c0392b'>Process not started. Upload a file first.</p>",
            "<p style='color:red'>No file uploaded.</p>",
            "No file uploaded.", "",
            *_empty,
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            *_no_files,
            None, None, None, None, None,
        )

    try:
        summary = run_pipeline(
            file_paths=file_paths,
            domain=domain,
            config_dir=CONFIG_DIR,
            output_dir=OUTPUT_DIR,
            user_overrides=user_overrides,
            contributor_id=contributor_id or "UNKNOWN",
            llm_override=llm_override,
            log_fn=log_fn,
        )
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        _empty = [pd.DataFrame() for _ in CANONICAL_TABLE_NAMES]
        _no_files = [None for _ in CANONICAL_TABLE_NAMES]
        return (
            "<p style='color:#c0392b'>Process failed before completion.</p>",
            f"<p style='color:red'><b>Pipeline error:</b><br><pre>{err}</pre></p>",
            "\n".join(log_lines) + f"\n\nERROR: {err}",
            "",
            *_empty,
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            *_no_files,
            None, None, None, None, None,
        )

    dfs = summary.get("_dataframes", {})
    files = summary.get("_output_files", {})

    # Canonical DataFrames — one per table in CANONICAL_TABLE_NAMES order
    canonical = dfs.get("canonical", {})
    canonical_dfs_out = [canonical.get(tbl, pd.DataFrame()) for tbl in CANONICAL_TABLE_NAMES]
    exceptions_df = dfs.get("exceptions", pd.DataFrame())
    lineage_df = dfs.get("column_lineage", pd.DataFrame())
    archive_df = dfs.get("archive_lineage", pd.DataFrame())
    dq_report = dfs.get("dq_report", {})

    # Clean up internal keys from summary before display
    display_summary = {k: v for k, v in summary.items() if not k.startswith("_")}
    status_html = _summary_html(display_summary)
    dq_html = _dq_html(dq_report)
    log_text = "\n".join(log_lines)
    completion_html = _completion_html(summary)

    # Downloadable files — one per table in CANONICAL_TABLE_NAMES order
    canonical_files_out = [files.get("canonical", {}).get(tbl) for tbl in CANONICAL_TABLE_NAMES]
    dl_exc = files.get("exceptions")
    dl_lineage = files.get("column_lineage")
    dl_dq = files.get("dq_report")
    dl_archive = files.get("archive_lineage")
    dl_summary = files.get("job_summary")

    return (
        completion_html,
        status_html,
        log_text,
        dq_html,
        *canonical_dfs_out,
        exceptions_df if not exceptions_df.empty else pd.DataFrame(columns=["No exceptions"]),
        lineage_df,
        archive_df if not archive_df.empty else pd.DataFrame(columns=["No archive (direct upload)"]),
        *canonical_files_out,
        dl_exc,
        dl_lineage,
        dl_dq,
        dl_archive,
        dl_summary,
    )


# ---------------------------------------------------------------------------
# UI provider change helpers
# ---------------------------------------------------------------------------

def on_provider_change(provider: str):
    """Show/hide API key input and populate placeholder from env."""
    if provider == "None":
        return gr.update(visible=False, value="")
    env_val = _api_key_from_env(provider)
    placeholder = f"Loaded from .env ({env_val})" if env_val else f"Enter {provider} API key"
    return gr.update(visible=True, placeholder=placeholder, value="")


# ---------------------------------------------------------------------------
# Gradio UI
# ---------------------------------------------------------------------------

def build_ui() -> gr.Blocks:
    with gr.Blocks(
        title="Data Ingestion Platform"
    ) as demo:

        gr.Markdown(
            """
            # Universal Data Ingestion & Normalisation Platform
            **POC - Trade Domain**  |  Config-driven | Column mapping | DQ | Full lineage
            """,
            elem_id="header"
        )

        with gr.Tabs():

            # ----------------------------------------------------------------
            # TAB 1 - Upload & Configure
            # ----------------------------------------------------------------
            with gr.Tab("Upload & Run"):
                with gr.Row():
                    with gr.Column(scale=2):
                        gr.Markdown("### Input File")
                        file_input = gr.File(
                            label="Upload file(s) (CSV, XLSX, JSON, XML, TXT, DOCX, PDF, ZIP)",
                            file_count="multiple",
                            file_types=[
                                ".csv", ".tsv", ".txt", ".xlsx", ".xls",
                                ".json", ".xml", ".html", ".docx", ".pdf", ".zip"
                            ],
                        )
                        domain_dd = gr.Dropdown(
                            choices=DOMAINS,
                            value="trade",
                            label="Domain",
                        )
                        contributor_id = gr.Textbox(
                            label="Contributor ID",
                            placeholder="e.g. CONTRIB001",
                            value="",
                        )

                    with gr.Column(scale=2):
                        gr.Markdown("### LLM Configuration")
                        llm_provider_dd = gr.Dropdown(
                            choices=LLM_PROVIDERS,
                            value="None",
                            label="LLM Provider",
                            info="Select a provider to enable AI-assisted column mapping for unresolved columns.",
                        )
                        api_key_input = gr.Textbox(
                            label="API Key",
                            placeholder="Leave blank to use key from .env file",
                            type="password",
                            visible=False,
                            interactive=True,
                        )
                        apply_to_dd = gr.Dropdown(
                            choices=["Unmatched only", "All columns"],
                            value="Unmatched only",
                            label="Apply LLM to",
                            info="'Unmatched only' calls LLM only when exact + fuzzy both fail.",
                        )

                with gr.Accordion("Advanced Settings", open=False):
                    with gr.Row():
                        mandatory_threshold = gr.Slider(
                            minimum=50, maximum=100, value=80, step=5,
                            label="Mandatory mapping threshold (%)",
                            info="Minimum confidence for a mandatory column mapping to be accepted.",
                        )
                        fuzzy_min = gr.Slider(
                            minimum=50, maximum=100, value=70, step=5,
                            label="Fuzzy match minimum similarity (%)",
                        )
                        llm_accept_threshold = gr.Slider(
                            minimum=30, maximum=100, value=55, step=5,
                            label="LLM accept threshold (%)",
                            info="Minimum confidence from LLM response to accept the mapping.",
                        )

                with gr.Accordion("Column Overrides (optional)", open=False):
                    gr.Markdown(
                        "Force specific source columns to a canonical target. "
                        "One per line: `Source Column Name = CANONICAL_TABLE.Canonical_Column`"
                    )
                    override_text = gr.Textbox(
                        lines=4,
                        placeholder="Billing Reference = TRD_INVOICE.Invoice_Number\nCustomer Code = TRD_CUSTOMER.Account_Number",
                        label="Manual overrides text",
                    )

                with gr.Row():
                    analyze_btn = gr.Button("1) Analyze Mapping", variant="secondary", size="lg")
                    run_btn = gr.Button("2) Run Pipeline", variant="primary", size="lg", interactive=False)

                completion_banner = gr.HTML("<p style='color:#888'>Analyze mapping, review/adjust, then run pipeline.</p>")

                gr.Markdown("### Step 1 Output - Column Mapper Scorecard")
                mapping_analysis_html = gr.HTML("<p style='color:#888'>No mapping analysis yet.</p>")
                mapping_scorecard = gr.Dataframe(
                    label="Column Mapper (Exact -> Fuzzy -> LLM -> Override -> Propagation)",
                    interactive=False,
                    wrap=False,
                )

                gr.Markdown("### Adjust Mapping via Dropdown (optional)")
                with gr.Row():
                    source_col_selector = gr.Dropdown(
                        choices=[],
                        label="Source column",
                        info="Pick a source column from analysis results. In multi-file batches, overrides apply by column name across all uploaded files.",
                    )
                    target_col_selector = gr.Dropdown(
                        choices=[],
                        label="Canonical target",
                        info="Pick canonical mapping as TABLE.Column.",
                    )
                with gr.Row():
                    apply_override_btn = gr.Button("Apply Selected Override", variant="secondary")
                    clear_override_btn = gr.Button("Clear Selected Override", variant="secondary")
                override_feedback_html = gr.HTML("<p style='color:#888'>No override action yet.</p>")

            # ----------------------------------------------------------------
            # TAB 2 - Results
            # ----------------------------------------------------------------
            with gr.Tab("Results"):
                status_html = gr.HTML("<p style='color:#888'>Run the pipeline to see results.</p>")

                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### DQ Report")
                        dq_html = gr.HTML("<p style='color:#888'>-</p>")

                with gr.Tabs():
                    canonical_df_components: dict[str, gr.Dataframe] = {}
                    for _tbl in CANONICAL_TABLE_NAMES:
                        with gr.Tab(_tbl):
                            canonical_df_components[_tbl] = gr.Dataframe(
                                label=_tbl,
                                interactive=False,
                                wrap=False,
                            )
                    with gr.Tab("Exceptions"):
                        df_exc = gr.Dataframe(
                            label="RECORD_EXCEPTIONS",
                            interactive=False,
                            wrap=False,
                        )

            # ----------------------------------------------------------------
            # TAB 3 - Lineage
            # ----------------------------------------------------------------
            with gr.Tab("Lineage"):
                with gr.Tabs():
                    with gr.Tab("Column Lineage"):
                        df_lineage = gr.Dataframe(
                            label="COLUMN_LINEAGE - one row per source column per file",
                            interactive=False,
                            wrap=False,
                        )
                    with gr.Tab("Archive Lineage"):
                        df_archive = gr.Dataframe(
                            label="ARCHIVE_LINEAGE - populated when input is a ZIP",
                            interactive=False,
                            wrap=False,
                        )

            # ----------------------------------------------------------------
            # TAB 4 - Log
            # ----------------------------------------------------------------
            with gr.Tab("Run Log"):
                log_output = gr.Textbox(
                    label="Pipeline log",
                    lines=30,
                    max_lines=60,
                    interactive=False,
                )

            # ----------------------------------------------------------------
            # TAB 5 - Downloads
            # ----------------------------------------------------------------
            with gr.Tab("Download Outputs"):
                gr.Markdown("Output files from the last run. Click to download.")
                canonical_file_components: dict[str, gr.File] = {}
                with gr.Row():
                    for _tbl in CANONICAL_TABLE_NAMES:
                        canonical_file_components[_tbl] = gr.File(label=f"{_tbl} CSV")
                with gr.Row():
                    dl_exc = gr.File(label="Exceptions CSV")
                    dl_lineage = gr.File(label="Column Lineage CSV")
                    dl_dq = gr.File(label="DQ Report JSON")
                with gr.Row():
                    dl_archive = gr.File(label="Archive Lineage CSV")
                    dl_summary = gr.File(label="Job Summary JSON")

        # ----------------------------------------------------------------
        # Event wiring
        # ----------------------------------------------------------------

        # Show/hide API key box when provider changes
        llm_provider_dd.change(
            fn=on_provider_change,
            inputs=[llm_provider_dd],
            outputs=[api_key_input],
        )

        # Reset step gating when a new file is selected
        file_input.change(
            fn=lambda _: (
                gr.update(interactive=False),
                "<p style='color:#888'>Analyze mapping, review/adjust, then run pipeline.</p>",
                "<p style='color:#888'>No mapping analysis yet.</p>",
                pd.DataFrame(),
                gr.update(choices=[], value=None),
                gr.update(choices=[], value=None),
                "<p style='color:#888'>No override action yet.</p>",
            ),
            inputs=[file_input],
            outputs=[
                run_btn,
                completion_banner,
                mapping_analysis_html,
                mapping_scorecard,
                source_col_selector,
                target_col_selector,
                override_feedback_html,
            ],
        )

        # Step 1: analyze mapping before full pipeline execution
        analyze_btn.click(
            fn=analyze_mappings,
            inputs=[
                file_input,
                domain_dd,
                llm_provider_dd,
                api_key_input,
                mandatory_threshold,
                fuzzy_min,
                llm_accept_threshold,
                apply_to_dd,
                override_text,
            ],
            outputs=[
                mapping_analysis_html,
                mapping_scorecard,
                source_col_selector,
                target_col_selector,
                run_btn,
                completion_banner,
            ],
        )

        # Dropdown-driven single override editor
        apply_override_btn.click(
            fn=apply_mapping_override,
            inputs=[source_col_selector, target_col_selector, override_text],
            outputs=[override_text, override_feedback_html],
        )

        clear_override_btn.click(
            fn=clear_mapping_override,
            inputs=[source_col_selector, override_text],
            outputs=[override_text, override_feedback_html],
        )

        # Step 2: run full pipeline
        run_btn.click(
            fn=run_ingestion,
            inputs=[
                file_input,
                domain_dd,
                llm_provider_dd,
                api_key_input,
                contributor_id,
                mandatory_threshold,
                fuzzy_min,
                llm_accept_threshold,
                apply_to_dd,
                override_text,
            ],
            outputs=[
                completion_banner,
                status_html,
                log_output,
                dq_html,
                *list(canonical_df_components.values()),
                df_exc,
                df_lineage,
                df_archive,
                *list(canonical_file_components.values()),
                dl_exc,
                dl_lineage,
                dl_dq,
                dl_archive,
                dl_summary,
            ],
        )

    return demo


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=7861)
    parser.add_argument("--share", action="store_true", help="Create a public Gradio share link")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    demo = build_ui()
    demo.launch(
        server_port=args.port,
        share=args.share,
        inbrowser=not args.no_browser,
        theme=UI_THEME,
        css=UI_CSS,
    )
