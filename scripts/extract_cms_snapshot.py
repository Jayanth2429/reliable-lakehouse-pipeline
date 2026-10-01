"""Normalize CMS Hospital General Information from an archive ZIP or CSV.

Download hospital snapshots from the CMS Provider Data Catalog archives page,
then run this script for each snapshot. Raw public files remain outside Git.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import zipfile
from pathlib import Path

SELECTED_FIELDS = [
    "facility_id",
    "facility_name",
    "address",
    "citytown",
    "state",
    "zip_code",
    "countyparish",
    "hospital_type",
    "hospital_ownership",
    "emergency_services",
    "hospital_overall_rating",
]


def snake(value: str) -> str:
    value = value.strip().lower().replace("/", " ")
    value = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
    aliases = {
        "city_town": "citytown",
        "county_parish": "countyparish",
    }
    return aliases.get(value, value)


def _open_csv(path: Path):
    if path.suffix.lower() == ".zip":
        archive = zipfile.ZipFile(path)
        matches = [
            name
            for name in archive.namelist()
            if "hospital_general_information" in name.lower() and name.lower().endswith(".csv")
        ]
        if not matches:
            raise FileNotFoundError("Could not find HOSPITAL_GENERAL_INFORMATION.CSV inside archive")
        raw = archive.open(matches[0])
        return io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""), archive
    return path.open("r", encoding="utf-8-sig", newline=""), None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="CMS hospital snapshot ZIP or Hospital General Information CSV")
    parser.add_argument("--snapshot-date", required=True, help="Snapshot date in YYYY-MM-DD format")
    parser.add_argument("--output", required=True, help="Normalized JSONL output path")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    handle, archive = _open_csv(input_path)
    count = 0
    try:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV is missing headers")
        normalized_headers = {name: snake(name) for name in reader.fieldnames}

        with output_path.open("w", encoding="utf-8") as out:
            for row in reader:
                normalized = {normalized_headers[k]: (v or "").strip() for k, v in row.items()}
                record = {field: normalized.get(field, "") for field in SELECTED_FIELDS}
                if not record["facility_id"]:
                    continue
                record.update(
                    {
                        "source_system": "cms_provider_data_catalog",
                        "snapshot_date": args.snapshot_date,
                        "source_updated_at": f"{args.snapshot_date}T00:00:00+00:00",
                    }
                )
                out.write(json.dumps(record, sort_keys=True) + "\n")
                count += 1
    finally:
        handle.close()
        if archive is not None:
            archive.close()

    print(f"Wrote {count} normalized hospital rows to {output_path}")


if __name__ == "__main__":
    main()
