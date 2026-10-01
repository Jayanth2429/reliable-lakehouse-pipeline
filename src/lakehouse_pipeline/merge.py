"""Pure-Python reference implementation of idempotent lakehouse merge semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .keys import content_hash, deterministic_record_id

BUSINESS_FIELDS = {
    "source_system",
    "patient_id",
    "referral_type",
    "referral_date",
    "priority",
    "status",
    "facility_region",
}

HASH_IGNORED_FIELDS = {
    "record_id",
    "row_hash",
    "ingested_at",
    "pipeline_run_id",
    "last_modified_at",
    "source_updated_at",
}


@dataclass(frozen=True)
class MergeStats:
    inserted: int
    updated: int
    unchanged: int
    source_duplicates: int


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def prepare_record(record: dict[str, Any]) -> dict[str, Any]:
    """Add a deterministic business key and stable content hash."""
    prepared = dict(record)
    missing = BUSINESS_FIELDS - prepared.keys()
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(sorted(missing))}")

    prepared["record_id"] = deterministic_record_id(
        source_system=prepared["source_system"],
        patient_id=prepared["patient_id"],
        referral_type=prepared["referral_type"],
        referral_date=prepared["referral_date"],
    )
    prepared["row_hash"] = content_hash(prepared, ignored_fields=HASH_IGNORED_FIELDS)
    return prepared


def dedupe_source(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Keep the latest source record for each deterministic key within a batch."""
    latest: dict[str, dict[str, Any]] = {}
    duplicates = 0

    for raw in records:
        prepared = prepare_record(raw)
        key = prepared["record_id"]
        existing = latest.get(key)
        if existing is None:
            latest[key] = prepared
            continue

        duplicates += 1
        if _parse_ts(prepared["source_updated_at"]) >= _parse_ts(existing["source_updated_at"]):
            latest[key] = prepared

    return list(latest.values()), duplicates


def merge_batch(
    target: dict[str, dict[str, Any]],
    source_records: list[dict[str, Any]],
    *,
    pipeline_run_id: str,
    processed_at: str | None = None,
) -> MergeStats:
    """Apply insert/update/no-op semantics to a mutable target mapping.

    Re-running the same source batch is idempotent: unchanged rows are not rewritten.
    """
    processed_at = processed_at or datetime.now(timezone.utc).isoformat()
    deduped, duplicate_count = dedupe_source(source_records)

    inserted = updated = unchanged = 0

    for source in deduped:
        key = source["record_id"]
        existing = target.get(key)

        if existing is None:
            row = dict(source)
            row["pipeline_run_id"] = pipeline_run_id
            row["ingested_at"] = processed_at
            row["last_modified_at"] = processed_at
            target[key] = row
            inserted += 1
            continue

        if existing["row_hash"] == source["row_hash"]:
            unchanged += 1
            continue

        row = dict(source)
        row["pipeline_run_id"] = pipeline_run_id
        row["ingested_at"] = existing.get("ingested_at", processed_at)
        row["last_modified_at"] = processed_at
        target[key] = row
        updated += 1

    return MergeStats(
        inserted=inserted,
        updated=updated,
        unchanged=unchanged,
        source_duplicates=duplicate_count,
    )
