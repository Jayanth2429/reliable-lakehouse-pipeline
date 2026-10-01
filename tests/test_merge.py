from lakehouse_pipeline.merge import merge_batch

BASE = {
    "source_system": "cms_provider_data_catalog",
    "facility_id": "010001",
    "facility_name": "SOUTHEAST HEALTH MEDICAL CENTER",
    "address": "1108 ROSS CLARK CIRCLE",
    "citytown": "DOTHAN",
    "state": "AL",
    "zip_code": "36301",
    "countyparish": "HOUSTON",
    "hospital_type": "Acute Care Hospitals",
    "hospital_ownership": "Government - Hospital District or Authority",
    "emergency_services": "Yes",
    "hospital_overall_rating": "4",
    "snapshot_date": "2026-02-25",
    "source_updated_at": "2026-02-25T00:00:00+00:00",
}


def test_first_snapshot_inserts_record():
    target = {}
    stats = merge_batch(target, [BASE], pipeline_run_id="run-1", processed_at="2026-02-25T12:00:00+00:00")
    assert stats.inserted == 1
    assert stats.updated == 0
    assert len(target) == 1


def test_replay_is_idempotent():
    target = {}
    merge_batch(target, [BASE], pipeline_run_id="run-1", processed_at="2026-02-25T12:00:00+00:00")
    before = dict(next(iter(target.values())))
    stats = merge_batch(target, [BASE], pipeline_run_id="run-2", processed_at="2026-02-25T13:00:00+00:00")
    assert stats.unchanged == 1
    assert next(iter(target.values())) == before


def test_changed_public_business_content_updates_existing_record():
    target = {}
    merge_batch(target, [BASE], pipeline_run_id="run-1", processed_at="2026-02-25T12:00:00+00:00")
    changed = dict(BASE)
    changed["hospital_overall_rating"] = "3"
    changed["snapshot_date"] = "2026-05-13"
    changed["source_updated_at"] = "2026-05-13T00:00:00+00:00"
    stats = merge_batch(target, [changed], pipeline_run_id="run-2", processed_at="2026-05-13T12:00:00+00:00")
    row = next(iter(target.values()))
    assert stats.updated == 1
    assert row["hospital_overall_rating"] == "3"
    assert row["ingested_at"] == "2026-02-25T12:00:00+00:00"


def test_snapshot_metadata_only_change_is_noop():
    target = {}
    merge_batch(target, [BASE], pipeline_run_id="run-1")
    later = dict(BASE)
    later["snapshot_date"] = "2026-05-13"
    later["source_updated_at"] = "2026-05-13T00:00:00+00:00"
    stats = merge_batch(target, [later], pipeline_run_id="run-2")
    assert stats.unchanged == 1
    assert stats.updated == 0


def test_duplicate_rows_keep_latest_snapshot():
    target = {}
    older = dict(BASE)
    newer = dict(BASE)
    newer["hospital_overall_rating"] = "3"
    newer["snapshot_date"] = "2026-05-13"
    newer["source_updated_at"] = "2026-05-13T00:00:00+00:00"
    stats = merge_batch(target, [older, newer], pipeline_run_id="run-1")
    row = next(iter(target.values()))
    assert stats.source_duplicates == 1
    assert row["hospital_overall_rating"] == "3"
