# Architecture

```mermaid
flowchart LR
    A[Source batch files] --> B[Bronze landing]
    B --> C[Schema + required field validation]
    C --> D[Deterministic business key]
    D --> E[Within-batch latest-record deduplication]
    E --> F[Stable row hash]
    F --> G{Target row exists?}
    G -- No --> H[Insert]
    G -- Yes, hash changed --> I[Update]
    G -- Yes, same hash --> J[No-op]
    H --> K[Curated Delta table]
    I --> K
    J --> K
    K --> L[Audit metrics]
```

## Layers

- **Bronze:** immutable landed source files with ingestion metadata.
- **Silver / curated:** one current row per deterministic business key.
- **Audit:** run-level counts for source rows, inserts, updates, unchanged rows, duplicates, and processing timestamps.

The local reference implementation demonstrates the merge semantics without requiring Spark. The optional PySpark + Delta module maps the same design to a lakehouse runtime.
