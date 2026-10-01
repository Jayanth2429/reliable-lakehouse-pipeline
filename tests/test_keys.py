from lakehouse_pipeline.keys import content_hash, deterministic_record_id


def test_deterministic_key_normalizes_case_and_whitespace():
    left = deterministic_record_id(source_system=" CMS_PROVIDER_DATA_CATALOG ", facility_id="010001")
    right = deterministic_record_id(source_system="cms_provider_data_catalog", facility_id=" 010001 ")
    assert left == right


def test_content_hash_ignores_field_order():
    assert content_hash({"a": 1, "b": 2}) == content_hash({"b": 2, "a": 1})
