# Architecture

The project compares chronological snapshots of the public CMS Hospital General Information dataset.

1. Download two hospital Snapshot ZIPs from the official CMS Provider Data Catalog archive.
2. `scripts/extract_cms_snapshot.py` locates `HOSPITAL_GENERAL_INFORMATION.CSV`, normalizes headers, selects stable analytical fields, and writes JSONL.
3. A deterministic key identifies the same facility across snapshots using `source_system + facility_id`.
4. A SHA-256 hash represents business content while ignoring snapshot/ingestion metadata.
5. The MERGE layer classifies each facility as insert, update, or no-op.
6. Run-level metrics make changes observable and provide a reproducible audit trail.

The optional Spark path maps the same logic to Delta Lake MERGE semantics.
