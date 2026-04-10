"""
column_mapper.py
----------------
4-tier column mapping pipeline:
  1. Exact lookup   (lookup_table.csv)
  2. Fuzzy match    (difflib SequenceMatcher)
  3. LLM inference  (Claude / OpenAI / Gemini — if enabled)
  4. NO MATCH

Also handles shared-key multi-target propagation.

Returns a list of MappingResult per source column.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class MappingResult:
    lineage_id: str
    mapping_reference_id: str
    job_id: str
    domain: str
    source_filename: str
    source_column_name: str          # original header
    source_column_normalised: str    # after normalisation
    canonical_table: str | None      # None if UNMAPPED
    canonical_column: str            # 'UNMAPPED' if no match
    match_method: str                # EXACT LOOKUP / FUZZY MATCH / LLM (...) / USER OVERRIDE / NO MATCH
    confidence_score: int            # 0-100
    was_mandatory: bool
    met_threshold: bool
    llm_reasoning: str | None
    lookup_variant_matched: str | None
    archive_lineage_id: str | None
    insert_timestamp: str
    is_propagated: bool = False      # True if created by shared-key propagation (not a direct source mapping)


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def normalise_column(name: str, cfg: dict) -> str:
    """Apply normalisation rules from config to a column name."""
    n_cfg = cfg.get("matching", {}).get("normalisation", {})
    s = name.strip()
    if n_cfg.get("lowercase", True):
        s = s.lower()
    if n_cfg.get("spaces_to_underscore", True):
        s = s.replace(" ", "_")
    if n_cfg.get("hyphens_to_underscore", True):
        s = s.replace("-", "_")
    if n_cfg.get("dots_to_underscore", True):
        s = s.replace(".", "_")
    if n_cfg.get("strip_special_chars", True):
        s = re.sub(r"[^a-z0-9_]", "", s)
    if n_cfg.get("collapse_underscores", True):
        s = re.sub(r"_+", "_", s)
    if n_cfg.get("strip_leading_trailing_underscores", True):
        s = s.strip("_")
    return s


# ---------------------------------------------------------------------------
# Lookup table loader
# ---------------------------------------------------------------------------

def load_lookup_table(lookup_csv_path: str) -> dict[str, tuple[str, str]]:
    """
    Returns {normalised_variation: (canonical_table, canonical_column)}
    Keys are already lower-case (source_variation column is already normalised).
    """
    table = {}
    with open(lookup_csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            variant = row["source_variation"].strip().lower()
            table[variant] = (
                row["canonical_table"].strip(),
                row["canonical_column"].strip(),
            )
    return table


# ---------------------------------------------------------------------------
# Canonical model helper
# ---------------------------------------------------------------------------

def _get_mandatory_columns(canonical_model: dict) -> dict[str, set[str]]:
    """Returns {table_name: {mandatory_column, ...}}"""
    result = {}
    for tbl, tdef in canonical_model.items():
        if tbl.startswith("_"):
            continue
        if not isinstance(tdef, dict):
            continue
        mandatory = set()
        for col, cdef in tdef.get("business_columns", {}).items():
            if cdef.get("mandatory", False):
                mandatory.add(col)
        result[tbl] = mandatory
    return result


def _all_canonical_columns(canonical_model: dict) -> list[tuple[str, str]]:
    """Returns [(table, column), ...] for all business columns."""
    result = []
    for tbl, tdef in canonical_model.items():
        if tbl.startswith("_"):
            continue
        if not isinstance(tdef, dict):
            continue
        for col in tdef.get("business_columns", {}):
            result.append((tbl, col))
    return result


# ---------------------------------------------------------------------------
# Prompt template loader + lookup context builder
# ---------------------------------------------------------------------------

def _build_lookup_context(lookup_table: dict[str, tuple[str, str]]) -> str:
    """
    Group lookup table entries by canonical column and return a compact
    'known aliases' block for injection into the LLM prompt.

    Example output:
        TRD_CUSTOMER.Account_Number : account_number, acc_no, debtor_ref, buyer_id ...
        TRD_INVOICE.Invoice_Number  : invoice_number, inv_no, bill_number, invoice_ref ...
    """
    groups: dict[str, list[str]] = {}
    for variant, (tbl, col) in lookup_table.items():
        groups.setdefault(f"{tbl}.{col}", []).append(variant)

    lines = []
    for key in sorted(groups):
        aliases = ", ".join(sorted(groups[key]))
        lines.append(f"  {key} : {aliases}")
    return "\n".join(lines)


def load_prompt_template(config_dir: str, domain: str) -> str:
    """
    Return the LLM prompt template for the given domain.

    Resolution order:
      1. <config_dir>/domains/<domain>/<domain>_prompt.txt   (domain-specific)
      2. <config_dir>/global_prompt.txt                       (global fallback)
      3. Built-in default (no external file required)

    Template placeholders: {domain}, {source_col}, {options_text}
    Note: the JSON braces in the response instruction must be escaped as {{ }}.
    """
    domain_prompt = Path(config_dir) / "domains" / domain / f"{domain}_prompt.txt"
    global_prompt = Path(config_dir) / "global_prompt.txt"

    for p in [domain_prompt, global_prompt]:
        if p.exists():
            return p.read_text(encoding="utf-8")

    # Built-in fallback (no file needed)
    return (
        "You are a data mapping assistant for the {domain} domain.\n"
        "Source column: \"{source_col}\"\n\n"
        "Known source column aliases per canonical column (use these as semantic clues):\n{lookup_context}\n\n"
        "Available canonical columns to map to:\n{options_text}\n\n"
        "Task: identify which canonical column this source column most likely maps to.\n"
        "Respond ONLY with valid JSON: "
        '{{\"canonical_table\": \"...\", \"canonical_column\": \"...\", \"confidence\": 0-100, \"reasoning\": \"one sentence\"}}\n'
        'If no match, use {{\"canonical_table\": null, \"canonical_column\": \"UNMAPPED\", \"confidence\": 0, \"reasoning\": \"...\"}}'
    )


# ---------------------------------------------------------------------------
# LLM adapters
# ---------------------------------------------------------------------------

def _llm_map_claude(
    source_col: str,
    canonical_options: list[tuple[str, str]],
    cfg: dict,
    domain: str,
    prompt_template: str,
    lookup_context: str,
) -> tuple[str | None, str | None, int, str]:
    """Returns (canonical_table, canonical_column, confidence, reasoning)."""
    model = cfg["llm"]["model"]["Claude"]
    max_tokens = cfg["llm"].get("max_tokens", 1000)
    temperature = cfg["llm"].get("temperature", 0)
    timeout = cfg["llm"].get("timeout_seconds", 30)

    try:
        import anthropic
    except ImportError:
        raise ImportError("anthropic SDK not installed; run: pip install anthropic")

    options_text = "\n".join(
        f"  - {tbl}.{col}" for tbl, col in canonical_options
    )
    prompt = prompt_template.format(
        domain=domain, source_col=source_col,
        options_text=options_text, lookup_context=lookup_context,
    )

    client = anthropic.Anthropic()
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        messages=[{"role": "user", "content": prompt}],
        timeout=timeout,
    )
    raw = message.content[0].text.strip()
    # Strip markdown fences if present
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM (Claude) returned invalid JSON for '{source_col}': {exc}. Raw: {raw[:200]!r}")
    return (
        parsed.get("canonical_table"),
        parsed.get("canonical_column", "UNMAPPED"),
        int(parsed.get("confidence", 0)),
        parsed.get("reasoning", ""),
    )


def _llm_map_openai(
    source_col: str,
    canonical_options: list[tuple[str, str]],
    cfg: dict,
    domain: str,
    prompt_template: str,
    lookup_context: str,
) -> tuple[str | None, str | None, int, str]:
    model = cfg["llm"]["model"]["OpenAI"]
    timeout = cfg["llm"].get("timeout_seconds", 30)
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("openai SDK not installed; run: pip install openai")

    options_text = "\n".join(f"  - {tbl}.{col}" for tbl, col in canonical_options)
    prompt = prompt_template.format(
        domain=domain, source_col=source_col,
        options_text=options_text, lookup_context=lookup_context,
    )
    client = OpenAI()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        timeout=timeout,
    )
    raw = resp.choices[0].message.content or ""
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM (OpenAI) returned invalid JSON for '{source_col}': {exc}. Raw: {raw[:200]!r}")
    return (
        parsed.get("canonical_table"),
        parsed.get("canonical_column", "UNMAPPED"),
        int(parsed.get("confidence", 0)),
        parsed.get("reasoning", ""),
    )


def _llm_map_gemini(
    source_col: str,
    canonical_options: list[tuple[str, str]],
    cfg: dict,
    domain: str,
    prompt_template: str,
    lookup_context: str,
) -> tuple[str | None, str | None, int, str]:
    model_name = cfg["llm"]["model"]["Gemini"]
    timeout = cfg["llm"].get("timeout_seconds", 30)
    try:
        from google import genai
        from google.genai import types as genai_types
    except ImportError:
        raise ImportError("google-genai not installed; run: pip install google-genai")

    options_text = "\n".join(f"  - {tbl}.{col}" for tbl, col in canonical_options)
    prompt = prompt_template.format(
        domain=domain, source_col=source_col,
        options_text=options_text, lookup_context=lookup_context,
    )

    # google-genai supports either GEMINI_API_KEY or GOOGLE_API_KEY.
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    client = genai.Client(api_key=api_key) if api_key else genai.Client()
    gen_cfg = genai_types.GenerateContentConfig(
        temperature=cfg["llm"].get("temperature", 0),
        max_output_tokens=cfg["llm"].get("max_tokens", 1000),
        http_options={"timeout": timeout},
    )
    resp = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=gen_cfg,
    )
    raw = resp.text.strip()
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM (Gemini) returned invalid JSON for '{source_col}': {exc}. Raw: {raw[:200]!r}")
    return (
        parsed.get("canonical_table"),
        parsed.get("canonical_column", "UNMAPPED"),
        int(parsed.get("confidence", 0)),
        parsed.get("reasoning", ""),
    )


def _llm_map(
    source_col: str,
    canonical_options: list[tuple[str, str]],
    cfg: dict,
    domain: str,
    prompt_template: str,
    lookup_context: str,
) -> tuple[str | None, str | None, int, str, str]:
    """Dispatch to configured LLM. Returns (table, col, confidence, reasoning, method_label)."""
    provider = cfg["llm"].get("provider", "None")
    if provider == "Claude":
        t, c, s, r = _llm_map_claude(source_col, canonical_options, cfg, domain, prompt_template, lookup_context)
        return t, c, s, r, "LLM (Claude)"
    if provider == "OpenAI":
        t, c, s, r = _llm_map_openai(source_col, canonical_options, cfg, domain, prompt_template, lookup_context)
        return t, c, s, r, "LLM (OpenAI)"
    if provider == "Gemini":
        t, c, s, r = _llm_map_gemini(source_col, canonical_options, cfg, domain, prompt_template, lookup_context)
        return t, c, s, r, "LLM (Gemini)"
    raise ValueError(f"Unknown LLM provider: {provider}")


# ---------------------------------------------------------------------------
# Shared-key propagation
# ---------------------------------------------------------------------------

def _apply_propagation(
    mapping_results: list[MappingResult],
    cfg: dict,
    canonical_model: dict,
    mapping_reference_id: str,
    job_id: str,
    domain: str,
    source_filename: str,
    archive_lineage_id: str | None,
) -> list[MappingResult]:
    """
    For each shared-key propagation rule: if ANY source column has resolved
    (via any method) to one of the target canonical columns, emit additional
    MappingResult rows for the other targets — inheriting the same source column.
    """
    if not cfg.get("matching", {}).get("allow_shared_key_multi_target_propagation", False):
        return mapping_results

    mandatory_cols = _get_mandatory_columns(canonical_model)
    rules = cfg.get("matching", {}).get("shared_key_propagation_rules", [])
    ts = datetime.now(timezone.utc).isoformat()

    extra: list[MappingResult] = []

    for rule in rules:
        targets = rule.get("targets", [])
        if len(targets) < 2:
            continue

        # Find any existing result that already mapped to one of the targets
        for existing in list(mapping_results) + extra:
            matched_target = None
            for t in targets:
                if (existing.canonical_table == t["canonical_table"]
                        and existing.canonical_column == t["canonical_column"]
                        and existing.canonical_column != "UNMAPPED"):
                    matched_target = t
                    break

            if matched_target is None:
                continue

            # Propagate to all other targets
            for t in targets:
                if t == matched_target:
                    continue
                # Don't duplicate if already mapped
                already = any(
                    r.canonical_table == t["canonical_table"]
                    and r.canonical_column == t["canonical_column"]
                    for r in mapping_results + extra
                )
                if already:
                    continue

                is_mandatory = t["canonical_column"] in mandatory_cols.get(t["canonical_table"], set())
                extra.append(MappingResult(
                    lineage_id=str(uuid.uuid4()),
                    mapping_reference_id=mapping_reference_id,
                    job_id=job_id,
                    domain=domain,
                    source_filename=source_filename,
                    source_column_name=existing.source_column_name,
                    source_column_normalised=existing.source_column_normalised,
                    canonical_table=t["canonical_table"],
                    canonical_column=t["canonical_column"],
                    match_method=existing.match_method,
                    confidence_score=existing.confidence_score,
                    was_mandatory=is_mandatory,
                    met_threshold=True,
                    llm_reasoning=None,
                    lookup_variant_matched=None,
                    archive_lineage_id=archive_lineage_id,
                    insert_timestamp=ts,
                    is_propagated=True,
                ))
            break  # only propagate once per rule per pass

    return mapping_results + extra


# ---------------------------------------------------------------------------
# Main mapper
# ---------------------------------------------------------------------------

def map_columns(
    source_columns: list[str],
    lookup_table: dict[str, tuple[str, str]],
    canonical_model: dict,
    cfg: dict,
    job_id: str,
    domain: str,
    source_filename: str,
    user_overrides: dict[str, tuple[str, str]] | None = None,
    archive_lineage_id: str | None = None,
    config_dir: str = ".",
) -> tuple[list[MappingResult], str]:
    """
    Map source columns to canonical columns.

    user_overrides: {source_column_name: (canonical_table, canonical_column)}
    config_dir: root config directory used to locate prompt template files.
    Returns (mapping_results, mapping_reference_id).
    """
    matching = cfg.get("matching", {})
    fuzzy_min = matching.get("fuzzy_min_similarity", 0.7)
    fuzzy_mult = matching.get("fuzzy_confidence_multiplier", 0.91)
    exact_confidence = matching.get("exact_confidence", 100)
    llm_disambig_threshold = matching.get("llm_disambiguation_required_below", 70)
    llm_accept_threshold = cfg.get("llm", {}).get("confidence_accept_threshold", 55)
    mandatory_threshold = cfg.get("quality", {}).get("mandatory_threshold", 80)
    llm_apply_to = cfg.get("llm", {}).get("apply_to", "unmatched_only")
    llm_provider = cfg.get("llm", {}).get("provider", "None")

    # Load prompt template once (domain-specific → global fallback → built-in)
    # Also pre-build the lookup_context string (grouped aliases) for injection into the prompt
    if llm_provider != "None":
        prompt_template = load_prompt_template(config_dir, domain)
        lookup_context = _build_lookup_context(lookup_table)
    else:
        prompt_template = ""
        lookup_context = ""

    mandatory_cols = _get_mandatory_columns(canonical_model)
    all_canonical = _all_canonical_columns(canonical_model)
    mapping_reference_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    results: list[MappingResult] = []
    user_overrides = user_overrides or {}

    for src_col in source_columns:
        norm = normalise_column(src_col, cfg)
        lineage_id = str(uuid.uuid4())

        # --- USER OVERRIDE ---
        if src_col in user_overrides:
            tbl, col = user_overrides[src_col]
            is_mandatory = col in mandatory_cols.get(tbl, set())
            results.append(MappingResult(
                lineage_id=lineage_id,
                mapping_reference_id=mapping_reference_id,
                job_id=job_id,
                domain=domain,
                source_filename=source_filename,
                source_column_name=src_col,
                source_column_normalised=norm,
                canonical_table=tbl,
                canonical_column=col,
                match_method="USER OVERRIDE",
                confidence_score=100,
                was_mandatory=is_mandatory,
                met_threshold=True,
                llm_reasoning=None,
                lookup_variant_matched=None,
                archive_lineage_id=archive_lineage_id,
                insert_timestamp=ts,
            ))
            continue

        # --- EXACT LOOKUP ---
        if norm in lookup_table:
            tbl, col = lookup_table[norm]
            is_mandatory = col in mandatory_cols.get(tbl, set())

            results.append(MappingResult(
                lineage_id=lineage_id,
                mapping_reference_id=mapping_reference_id,
                job_id=job_id,
                domain=domain,
                source_filename=source_filename,
                source_column_name=src_col,
                source_column_normalised=norm,
                canonical_table=tbl,
                canonical_column=col,
                match_method="EXACT LOOKUP",
                confidence_score=exact_confidence,
                was_mandatory=is_mandatory,
                met_threshold=True,
                llm_reasoning=None,
                lookup_variant_matched=norm,
                archive_lineage_id=archive_lineage_id,
                insert_timestamp=ts,
            ))
            continue

        # --- FUZZY MATCH ---
        best_score = 0.0
        best_variant = None
        best_tbl = None
        best_col = None
        for variant, (tbl, col) in lookup_table.items():
            score = SequenceMatcher(None, norm, variant).ratio()
            if score > best_score:
                best_score = score
                best_variant = variant
                best_tbl = tbl
                best_col = col

        fuzzy_confidence = int(best_score * 100 * fuzzy_mult) if best_score >= fuzzy_min else 0

        if best_score >= fuzzy_min and fuzzy_confidence > 0:
            is_mandatory = best_col in mandatory_cols.get(best_tbl, set())
            met = fuzzy_confidence >= mandatory_threshold if is_mandatory else True

            # LLM disambiguation if below threshold
            if llm_provider != "None" and fuzzy_confidence < llm_disambig_threshold:
                try:
                    lt, lc, lscore, lreason, lmethod = _llm_map(src_col, all_canonical, cfg, domain, prompt_template, lookup_context)
                    if lscore >= llm_accept_threshold and lc != "UNMAPPED":
                        is_mandatory_l = lc in mandatory_cols.get(lt or "", set())
                        results.append(MappingResult(
                            lineage_id=lineage_id,
                            mapping_reference_id=mapping_reference_id,
                            job_id=job_id,
                            domain=domain,
                            source_filename=source_filename,
                            source_column_name=src_col,
                            source_column_normalised=norm,
                            canonical_table=lt,
                            canonical_column=lc,
                            match_method=lmethod,
                            confidence_score=lscore,
                            was_mandatory=is_mandatory_l,
                            met_threshold=lscore >= mandatory_threshold if is_mandatory_l else True,
                            llm_reasoning=lreason,
                            lookup_variant_matched=None,
                            archive_lineage_id=archive_lineage_id,
                            insert_timestamp=ts,
                        ))
                        continue
                except Exception as llm_err:
                    logger.warning("LLM disambiguation failed for %r: %s", src_col, llm_err)

            results.append(MappingResult(
                lineage_id=lineage_id,
                mapping_reference_id=mapping_reference_id,
                job_id=job_id,
                domain=domain,
                source_filename=source_filename,
                source_column_name=src_col,
                source_column_normalised=norm,
                canonical_table=best_tbl,
                canonical_column=best_col,
                match_method="FUZZY MATCH",
                confidence_score=fuzzy_confidence,
                was_mandatory=is_mandatory,
                met_threshold=met,
                llm_reasoning=None,
                lookup_variant_matched=best_variant,
                archive_lineage_id=archive_lineage_id,
                insert_timestamp=ts,
            ))
            continue

        # --- LLM FALLBACK ---
        if llm_provider != "None" and (llm_apply_to == "unmatched_only" or llm_apply_to == "all"):
            try:
                lt, lc, lscore, lreason, lmethod = _llm_map(src_col, all_canonical, cfg, domain, prompt_template, lookup_context)
                if lscore >= llm_accept_threshold and lc != "UNMAPPED":
                    is_mandatory_l = lc in mandatory_cols.get(lt or "", set())
                    results.append(MappingResult(
                        lineage_id=lineage_id,
                        mapping_reference_id=mapping_reference_id,
                        job_id=job_id,
                        domain=domain,
                        source_filename=source_filename,
                        source_column_name=src_col,
                        source_column_normalised=norm,
                        canonical_table=lt,
                        canonical_column=lc,
                        match_method=lmethod,
                        confidence_score=lscore,
                        was_mandatory=is_mandatory_l,
                        met_threshold=lscore >= mandatory_threshold if is_mandatory_l else True,
                        llm_reasoning=lreason,
                        lookup_variant_matched=None,
                        archive_lineage_id=archive_lineage_id,
                        insert_timestamp=ts,
                    ))
                    continue
            except Exception as llm_err:
                logger.warning("LLM fallback failed for %r: %s", src_col, llm_err)

        # --- NO MATCH ---
        is_mandatory = False
        results.append(MappingResult(
            lineage_id=lineage_id,
            mapping_reference_id=mapping_reference_id,
            job_id=job_id,
            domain=domain,
            source_filename=source_filename,
            source_column_name=src_col,
            source_column_normalised=norm,
            canonical_table=None,
            canonical_column="UNMAPPED",
            match_method="NO MATCH",
            confidence_score=0,
            was_mandatory=is_mandatory,
            met_threshold=False,
            llm_reasoning=None,
            lookup_variant_matched=None,
            archive_lineage_id=archive_lineage_id,
            insert_timestamp=ts,
        ))

    # Apply shared-key propagation
    results = _apply_propagation(
        results, cfg, canonical_model,
        mapping_reference_id, job_id, domain, source_filename, archive_lineage_id,
    )

    return results, mapping_reference_id


# ---------------------------------------------------------------------------
# Mapping result analysis helpers
# ---------------------------------------------------------------------------

def get_blocked_mandatory_columns(
    mapping_results: list[MappingResult],
    canonical_model: dict,
    cfg: dict,
) -> list[tuple[str, str]]:
    """
    Returns [(canonical_table, canonical_column)] for mandatory columns
    that are either unmapped or below threshold.

    Only checks tables that have at least one successfully mapped column.
    A file covering only customers should not be blocked for missing invoice columns.
    """
    mandatory_cols = _get_mandatory_columns(canonical_model)
    mandatory_threshold = cfg.get("quality", {}).get("mandatory_threshold", 80)

    mapped = {}  # (table, col) -> best confidence
    tables_with_any_mapping: set[str] = set()

    direct_count: dict[str, int] = {}   # table -> count of non-propagated mapped columns

    for r in mapping_results:
        if r.canonical_table and r.canonical_column != "UNMAPPED":
            key = (r.canonical_table, r.canonical_column)
            mapped[key] = max(mapped.get(key, 0), r.confidence_score)
            if not r.is_propagated:
                direct_count[r.canonical_table] = direct_count.get(r.canonical_table, 0) + 1

    # A table is "active" (i.e., this file is expected to populate it) only if it has
    # >= 2 directly-mapped columns. A single shared-key FK column alone does not make a
    # table the target of this file.
    tables_with_any_mapping = {tbl for tbl, cnt in direct_count.items() if cnt >= 2}

    blocked = []
    for tbl, cols in mandatory_cols.items():
        # Only enforce mandatory columns for tables that are actively targeted by this file
        if tbl not in tables_with_any_mapping:
            continue
        for col in cols:
            score = mapped.get((tbl, col), 0)
            if score < mandatory_threshold:
                blocked.append((tbl, col))
    return blocked


def build_column_map(
    mapping_results: list[MappingResult],
) -> dict[str, list[tuple[str, str, int]]]:
    """
    Returns {source_col: [(canonical_table, canonical_column, confidence), ...]}
    Only includes successful (non-UNMAPPED) mappings.
    """
    result: dict[str, list] = {}
    for r in mapping_results:
        if r.canonical_column != "UNMAPPED" and r.canonical_table:
            result.setdefault(r.source_column_name, []).append(
                (r.canonical_table, r.canonical_column, r.confidence_score)
            )
    return result
