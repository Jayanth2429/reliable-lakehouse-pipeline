from lakehouse_pipeline.keys import content_hash, deterministic_record_id


def test_deterministic_key_normalizes_case_and_whitespace():
    left = deterministic_record_id(
        source_system=" EHR-A ", patient_id="P-001", referral_type=" Oncology ", referral_date="2026-09-30"
    )
    right = deterministic_record_id(
        source_system="ehr-a", patient_id=" p-001 ", referral_type="oncology", referral_date="2026-09-30"
    )
    assert left == right


def test_content_hash_ignores_field_order():
    assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})
