# Design decisions

## 1. Deterministic business keys

Random UUIDs are useful for uniqueness, but they do not help a rerun identify the same logical source record. This project derives a SHA-256 identifier from normalized immutable attributes instead.

## 2. Content hashing before writes

A stable hash of business fields lets the merge distinguish a real change from a replay. If the key exists and the hash is unchanged, the pipeline deliberately performs no write.

## 3. Deduplicate before MERGE

Multiple versions of the same record can arrive in a single batch. The pipeline keeps the latest version by `source_updated_at` before reaching the target table, avoiding ambiguous merge behavior.

## 4. Preserve original ingestion time

Updates retain the original `ingested_at` timestamp and change only `last_modified_at`. This separates "when did this record first enter the platform?" from "when did its business content last change?"

## 5. Audit at the pipeline-run level

Operational metrics are first-class data. A production workflow should be able to answer how many rows were read, inserted, updated, unchanged, or deduplicated for every run.

## 6. Lightweight tests, optional Spark runtime

CI validates the reliability semantics without requiring a Spark cluster. The repository also contains a PySpark + Delta implementation for Databricks or local Spark environments.
