"""
profile_store.py
----------------
Mapping Profile system for the Universal Data Ingestion Platform.

A profile captures the human-approved column override decisions for a specific
file shape (identified by a SHA-256 fingerprint of its sorted column names).

Directory layout:
    profiles/
      <domain>/
        index.json          <- lightweight index, always loaded, never large
        <fingerprint8>.json <- full profile, loaded only on a match

Matching tiers:
    EXACT   — column fingerprint matches 100%  -> auto-apply, no human needed
    PARTIAL — >= PARTIAL_THRESHOLD overlap     -> suggest to user, human confirms
    NONE    — below threshold                  -> run fresh analysis
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal


# Minimum column overlap ratio (0-1) to surface a partial suggestion
PARTIAL_THRESHOLD = 0.70


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ProfileMeta:
    """Lightweight entry stored in index.json."""
    fingerprint: str        # full SHA-256 hex
    name: str               # user-defined display name
    column_count: int
    use_count: int
    last_used: str          # ISO date string


@dataclass
class Profile:
    """Full profile, stored as <fingerprint8>.json."""
    fingerprint: str
    name: str
    domain: str
    columns: list[str]      # sorted source column names
    overrides: dict[str, str]   # {source_col: "TABLE.Column"}
    use_count: int
    created_at: str
    last_used: str
    # Full scorecard rows — list of dicts with the same keys as the UI scorecard.
    # When present, EXACT matches skip the mapping engine entirely.
    mappings: list[dict] = field(default_factory=list)


@dataclass
class MatchResult:
    tier: Literal["EXACT", "PARTIAL", "NONE"]
    profile: Profile | None = None
    overlap: float = 0.0    # 0-1, only meaningful for PARTIAL


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------

def fingerprint(columns: list[str]) -> str:
    """Return a SHA-256 hex digest that uniquely identifies a set of column headers.

    Normalisation applied before hashing: strip whitespace, lowercase, sort.
    This means column order and case are irrelevant — two files with the same
    columns in any order will produce the same fingerprint.

    Args:
        columns: Raw column header strings from the source file.

    Returns:
        64-character lowercase hex SHA-256 digest.
    """
    normalised = sorted(c.strip().lower() for c in columns)
    return hashlib.sha256("|".join(normalised).encode()).hexdigest()


def _short(fp: str) -> str:
    return fp[:8]


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _profile_dir(config_dir: str, domain: str) -> str:
    return os.path.join(config_dir, "profiles", domain)


def _index_path(config_dir: str, domain: str) -> str:
    return os.path.join(_profile_dir(config_dir, domain), "index.json")


def _profile_path(config_dir: str, domain: str, fp: str) -> str:
    return os.path.join(_profile_dir(config_dir, domain), f"{_short(fp)}.json")


# ---------------------------------------------------------------------------
# Index I/O
# ---------------------------------------------------------------------------

def _load_index(config_dir: str, domain: str) -> dict[str, dict]:
    """Load index.json -> {fingerprint: meta_dict}. Returns {} if not found."""
    path = _index_path(config_dir, domain)
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _save_index(config_dir: str, domain: str, index: dict[str, dict]) -> None:
    path = _index_path(config_dir, domain)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Write to a temp file first, then atomically replace — prevents corruption
    # if the process is killed mid-write.
    dir_ = os.path.dirname(path)
    with tempfile.NamedTemporaryFile("w", dir=dir_, suffix=".tmp",
                                     delete=False, encoding="utf-8") as tmp:
        json.dump(index, tmp, indent=2)
        tmp_path = tmp.name
    os.replace(tmp_path, path)


# ---------------------------------------------------------------------------
# Profile I/O
# ---------------------------------------------------------------------------

def _load_profile(config_dir: str, domain: str, fp: str) -> Profile | None:
    path = _profile_path(config_dir, domain, fp)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        d = json.load(fh)
    d.setdefault("mappings", [])   # backwards-compat: old profiles had no mappings field
    return Profile(**d)


def _save_profile(config_dir: str, domain: str, profile: Profile) -> None:
    path = _profile_path(config_dir, domain, profile.fingerprint)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Write to a temp file first, then atomically replace.
    dir_ = os.path.dirname(path)
    with tempfile.NamedTemporaryFile("w", dir=dir_, suffix=".tmp",
                                     delete=False, encoding="utf-8") as tmp:
        json.dump(profile.__dict__, tmp, indent=2)
        tmp_path = tmp.name
    os.replace(tmp_path, path)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def match_profile(
    columns: list[str],
    config_dir: str,
    domain: str,
    cfg: dict | None = None,
) -> MatchResult:
    """
    Check incoming columns against saved profiles.

    Returns MatchResult with tier EXACT / PARTIAL / NONE.
    For EXACT: profile is fully loaded, overrides ready to apply.
    For PARTIAL: best-match profile loaded, overlap ratio provided.
    For NONE: profile is None.

    cfg: system config dict — if provided, reads profile_partial_threshold
         from cfg["matching"]. Falls back to PARTIAL_THRESHOLD constant.
    """
    threshold = PARTIAL_THRESHOLD
    if cfg:
        threshold = float(cfg.get("matching", {}).get("profile_partial_threshold", PARTIAL_THRESHOLD))

    fp = fingerprint(columns)
    index = _load_index(config_dir, domain)

    # --- Tier 1: exact fingerprint hit ---
    if fp in index:
        profile = _load_profile(config_dir, domain, fp)
        if profile:
            return MatchResult(tier="EXACT", profile=profile, overlap=1.0)
        # Profile file missing on disk despite index entry — remove stale entry
        index.pop(fp, None)
        _save_index(config_dir, domain, index)

    # --- Tier 2: partial overlap ---
    incoming_set = {c.strip().lower() for c in columns}
    incoming_count = len(incoming_set)

    best_overlap = 0.0
    best_fp = None

    for saved_fp, meta in index.items():
        saved_count = meta.get("column_count", 0)
        if saved_count == 0:
            continue
        # Quick filter: skip if column counts are too far apart
        if abs(saved_count - incoming_count) > max(incoming_count * 0.5, 3):
            continue
        # Use column list from index if available (avoids loading full profile file)
        saved_cols = meta.get("columns")
        if saved_cols is not None:
            saved_set = set(saved_cols)   # already lowercased at save time
        else:
            # Fall back to loading the full profile (old profiles without "columns" in index)
            profile = _load_profile(config_dir, domain, saved_fp)
            if not profile:
                continue
            saved_set = {c.strip().lower() for c in profile.columns}
        intersection = len(incoming_set & saved_set)
        union = len(incoming_set | saved_set)
        overlap = intersection / union if union else 0.0
        if overlap > best_overlap:
            best_overlap = overlap
            best_fp = saved_fp

    if best_overlap >= threshold and best_fp:
        profile = _load_profile(config_dir, domain, best_fp)
        tier = "EXACT" if best_overlap == 1.0 else "PARTIAL"
        return MatchResult(tier=tier, profile=profile, overlap=round(best_overlap, 3))

    return MatchResult(tier="NONE")


def save_profile(
    columns: list[str],
    overrides: dict[str, str],
    name: str,
    domain: str,
    config_dir: str,
    mappings: list[dict] | None = None,
) -> Profile:
    """Save or update a named mapping profile for a specific file shape.

    Args:
        columns: Source file column headers (used to compute the fingerprint).
        overrides: Human-approved column mappings in ``{source_col: "TABLE.Column"}`` form.
        name: Display name shown in the Profiles tab.
        domain: Domain key (e.g. ``"trade"``). Controls the storage subdirectory.
        config_dir: Root config directory; profiles are written to
            ``<config_dir>/profiles/<domain>/``.
        mappings: Full scorecard rows (list of dicts with the same keys as the
            UI scorecard dataframe). When provided, a future EXACT match can
            restore the scorecard and skip the mapping engine entirely.

    Returns:
        The saved :class:`Profile` object.

    Notes:
        If a profile with the same fingerprint already exists, its overrides are
        merged (new overrides take precedence) and its ``use_count`` is preserved.
        The ``mappings`` list is always replaced with the latest value when provided.
    """
    fp = fingerprint(columns)
    now = datetime.now(timezone.utc).date().isoformat()
    existing = _load_profile(config_dir, domain, fp)

    if existing:
        # Merge overrides: new take precedence over saved
        merged = {**existing.overrides, **overrides}
        profile = Profile(
            fingerprint=fp,
            name=name or existing.name,
            domain=domain,
            columns=sorted(c.strip().lower() for c in columns),
            overrides=merged,
            use_count=existing.use_count,
            created_at=existing.created_at,
            last_used=now,
            mappings=mappings if mappings is not None else existing.mappings,
        )
    else:
        profile = Profile(
            fingerprint=fp,
            name=name,
            domain=domain,
            columns=sorted(c.strip().lower() for c in columns),
            overrides=overrides,
            use_count=0,
            created_at=now,
            last_used=now,
            mappings=mappings or [],
        )

    _save_profile(config_dir, domain, profile)

    # Update index — store column list so partial-scan can compare without
    # loading the full profile file (O(1) I/O per candidate instead of O(n)).
    index = _load_index(config_dir, domain)
    index[fp] = {
        "fingerprint": fp,
        "name": profile.name,
        "column_count": len(profile.columns),
        "columns": profile.columns,   # already sorted + lowercased
        "use_count": profile.use_count,
        "last_used": now,
    }
    _save_index(config_dir, domain, index)
    return profile


def increment_use_count(fp: str, config_dir: str, domain: str) -> None:
    """Increment the use counter and update ``last_used`` for a saved profile.

    Should be called immediately after a profile is auto-applied (EXACT match)
    so the Profiles tab reflects real usage frequency. Updates both the
    lightweight index and the full profile file.

    Args:
        fp: Full SHA-256 fingerprint of the profile.
        config_dir: Root config directory.
        domain: Domain key.
    """
    index = _load_index(config_dir, domain)
    if fp not in index:
        return
    now = datetime.now(timezone.utc).date().isoformat()
    index[fp]["use_count"] = index[fp].get("use_count", 0) + 1
    index[fp]["last_used"] = now
    _save_index(config_dir, domain, index)

    profile = _load_profile(config_dir, domain, fp)
    if profile:
        profile.use_count += 1
        profile.last_used = now
        _save_profile(config_dir, domain, profile)


def list_profiles(config_dir: str, domain: str) -> list[dict]:
    """Return metadata for all saved profiles, sorted by use count descending.

    Reads only ``index.json`` — does not load individual profile files.

    Args:
        config_dir: Root config directory.
        domain: Domain key.

    Returns:
        List of metadata dicts (fingerprint, name, column_count, use_count, last_used).
        Empty list if no profiles exist yet.
    """
    index = _load_index(config_dir, domain)
    return sorted(index.values(), key=lambda x: x.get("use_count", 0), reverse=True)


def delete_profile(fp: str, config_dir: str, domain: str) -> bool:
    """Delete a profile and remove it from the index.

    Args:
        fp: Full SHA-256 fingerprint of the profile to delete.
        config_dir: Root config directory.
        domain: Domain key.

    Returns:
        ``True`` if the profile was found and deleted; ``False`` if not found.
    """
    index = _load_index(config_dir, domain)
    if fp not in index:
        return False
    index.pop(fp)
    _save_index(config_dir, domain, index)
    path = _profile_path(config_dir, domain, fp)
    if os.path.exists(path):
        os.remove(path)
    return True
