"""
Validation report helpers.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path


REPORT_DIR = Path("quality_reports")


def ensure_report_dir() -> None:
    REPORT_DIR.mkdir(exist_ok=True)


def export_csv(results, filename: str = "validation_report.csv") -> None:
    ensure_report_dir()

    path = REPORT_DIR / filename

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["check_name", "passed", "record_count", "severity"],
        )

        writer.writeheader()

        for result in results:
            writer.writerow(asdict(result))


def export_json(results, filename: str = "validation_report.json") -> None:
    ensure_report_dir()

    path = REPORT_DIR / filename

    with path.open("w", encoding="utf-8") as file:
        json.dump(
            [asdict(result) for result in results],
            file,
            indent=4,
            default=str,
        )