# Design decisions

## Why CMS snapshot archives?

CMS publishes real public hospital data plus historical snapshots. That lets the project demonstrate actual change detection between source releases instead of manufacturing updates in a synthetic dataset.

## Why `facility_id`?

CMS Facility ID is the natural source identifier for a hospital in this dataset. The project wraps `source_system + facility_id` in a deterministic SHA-256 key so the same pattern can generalize across sources.

## Why exclude snapshot metadata from the row hash?

A later snapshot date should not create an update when the hospital's business attributes are unchanged. `snapshot_date`, `source_updated_at`, and pipeline metadata are therefore excluded from the business-content hash.

## Why keep tiny fixtures in tests?

The production-style demo uses public CMS files. CI tests use compact deterministic fixtures because edge cases need to remain stable, fast, and intentionally reproducible.
