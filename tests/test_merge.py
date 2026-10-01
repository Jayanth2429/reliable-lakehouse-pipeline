from lakehouse_pipeline.merge import merge_batch


BASE = {
    "source_system": "ehr-a",
    "patient_id": "P-100",
    "referral_type": "cardiology",
    "referral_date": "2026-09-30",
    "priority": "routine",
    "status": "new",
    "facility_region": "south",
    "source_updated_at": "2026-09-30T14:00:00Z",
}


def test_first_batch_inserts_record():
    target = {}
    stats = merge_batch(target, [BASE], pipeline_run_id="run-1", processed_at="2026-10-01T10:00:00+00:00")
    assert stats.inserted == 1
    assert stats.updated == 0
    assert len(target) == 1


def test_rerun_is_idempotent():
    target = {}
    merge_batch(target, [BASE], pipeline_run_id="run-1", processed_at="2026-10-01T10:00:00+00:00")
    before = dict(next(iter(target.values())))

    stats = merge_batch(target, [BASE], pipeline_run_id="run-2", processed_at="2026-10-01T11:00:00+00:00")
    after = next(iter(target.values()))

    assert stats.unchanged == 1
    assert stats.inserted == 0
    assert stats.updated == 0
    assert after == before


def test_changed_business_content_updates_existing_record():
    target = {}
    merge_batch(target, [BASE], pipeline_run_id="run-1", processed_at="2026-10-01T10:00:00+00:00")

    changed = dict(BASE)
    changed["priority"] = "urgent"
    changed["source_updated_at"] = "2026-10-01T09:00:00Z"
    stats = merge_batch(target, [changed], pipeline_run_id="run-2", processed_at="2026-10-01T11:00:00+00:00")

    row = next(iter(target.values()))
    assert stats.updated == 1
    assert row["priority"] == "urgent"
    assert row["pipeline_run_id"] == "run-2"
    assert row["ingested_at"] == "2026-10-01T10:00:00+00:00"
    assert row["last_modified_at"] == "2026-10-01T11:00:00+00:00"


def test_duplicate_source_rows_keep_latest_version():
    target = {}
    older = dict(BASE)
    newer = dict(BASE)
    older["priority"] = "routine"
    older["source_updated_at"] = "2026-09-30T12:00:00Z"
    newer["priority"] = "urgent"
    newer["source_updated_at"] = "2026-09-30T15:00:00Z"

    stats = merge_batch(target, [older, newer], pipeline_run_id="run-1")
    row = next(iter(target.values()))

    assert stats.source_duplicates == 1
    assert stats.inserted == 1
    assert row["priority"] == "urgent"


def test_source_timestamp_only_change_is_noop():
    target = {}
    merge_batch(target, [BASE], pipeline_run_id="run-1", processed_at="2026-10-01T10:00:00+00:00")

    replay_with_new_source_timestamp = dict(BASE)
    replay_with_new_source_timestamp["source_updated_at"] = "2026-10-01T09:30:00Z"
    stats = merge_batch(
        target,
        [replay_with_new_source_timestamp],
        pipeline_run_id="run-2",
        processed_at="2026-10-01T11:00:00+00:00",
    )

    assert stats.unchanged == 1
    assert stats.updated == 0
