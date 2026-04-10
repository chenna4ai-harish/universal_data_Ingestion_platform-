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
    """SHA-256 of lowercase-sorted column names joined by '|'."""
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
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(index, fh, indent=2)


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
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(profile.__dict__, fh, indent=2)


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
        # Load profile to get actual column list
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
    """
    Save or update a mapping profile.

    overrides format: {source_col: "TABLE.Column"}
    mappings: full scorecard rows (list of dicts). When provided, EXACT matches
              can skip the mapping engine entirely on future runs.
    If a profile with the same fingerprint already exists it is updated
    (overrides merged, mappings replaced, use_count preserved, name updated if changed).
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

    # Update index
    index = _load_index(config_dir, domain)
    index[fp] = {
        "fingerprint": fp,
        "name": profile.name,
        "column_count": len(profile.columns),
        "use_count": profile.use_count,
        "last_used": now,
    }
    _save_index(config_dir, domain, index)
    return profile


def increment_use_count(fp: str, config_dir: str, domain: str) -> None:
    """Call after auto-applying a profile to keep use_count accurate."""
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
    """Return all profile metadata entries sorted by use_count desc."""
    index = _load_index(config_dir, domain)
    return sorted(index.values(), key=lambda x: x.get("use_count", 0), reverse=True)


def delete_profile(fp: str, config_dir: str, domain: str) -> bool:
    """Delete a profile by fingerprint. Returns True if deleted."""
    index = _load_index(config_dir, domain)
    if fp not in index:
        return False
    index.pop(fp)
    _save_index(config_dir, domain, index)
    path = _profile_path(config_dir, domain, fp)
    if os.path.exists(path):
        os.remove(path)
    return True
