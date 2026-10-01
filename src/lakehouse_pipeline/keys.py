"""Deterministic identifiers and stable content hashing."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


def deterministic_record_id(*, source_system: str, facility_id: str) -> str:
    """Create a stable key for one CMS hospital across dataset snapshots."""
    canonical = "||".join(normalize_text(v) for v in (source_system, facility_id))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def content_hash(payload: Mapping[str, Any], *, ignored_fields: set[str] | None = None) -> str:
    """Hash business content while ignoring snapshot/ingestion-only metadata."""
    ignored = ignored_fields or set()
    filtered = {key: value for key, value in payload.items() if key not in ignored}
    canonical_json = json.dumps(
        _canonicalize(filtered),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
