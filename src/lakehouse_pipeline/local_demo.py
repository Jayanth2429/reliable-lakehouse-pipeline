"""Run the merge design locally without Spark or Databricks."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from .io import read_jsonl, write_jsonl
from .merge import merge_batch


def run(batch_paths: list[Path], output_dir: Path) -> None:
    target: dict[str, dict] = {}
    audit_rows: list[dict] = []

    for index, batch_path in enumerate(batch_paths, start=1):
        run_id = f"local-demo-{index:03d}"
        records = read_jsonl(batch_path)
        stats = merge_batch(target, records, pipeline_run_id=run_id)
        audit_rows.append(
            {
                "pipeline_run_id": run_id,
                "source_file": batch_path.name,
                **asdict(stats),
            }
        )
        print(f"{batch_path.name}: {json.dumps(asdict(stats), sort_keys=True)}")

    output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(output_dir / "curated_records.jsonl", list(target.values()))
    write_jsonl(output_dir / "audit_log.jsonl", audit_rows)
    print(f"Wrote {len(target)} curated rows to {output_dir}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "batches",
        nargs="*",
        default=["sample_data/batch_001.jsonl", "sample_data/batch_002.jsonl"],
    )
    parser.add_argument("--output-dir", default="demo_output")
    args = parser.parse_args()
    run([Path(path) for path in args.batches], Path(args.output_dir))


if __name__ == "__main__":
    main()
