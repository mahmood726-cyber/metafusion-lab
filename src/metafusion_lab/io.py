from __future__ import annotations

import csv
from pathlib import Path

from .models import TrialRecord


def _optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return float(stripped)


def _optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return int(stripped)


def load_trial_records(csv_path: str | Path) -> list[TrialRecord]:
    path = Path(csv_path)
    records: list[TrialRecord] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            records.append(
                TrialRecord(
                    study_id=row["study_id"],
                    year=int(row["year"]),
                    comparison=row["comparison"],
                    outcome=row["outcome"],
                    effect_log_rr=float(row["effect_log_rr"]),
                    standard_error=float(row["standard_error"]),
                    sample_size=_optional_int(row.get("sample_size")),
                    extraction_confidence=_optional_float(row.get("extraction_confidence")),
                    risk_of_bias=_optional_float(row.get("risk_of_bias")),
                    registry_match=_optional_float(row.get("registry_match")),
                    notes=row.get("notes", "").strip(),
                )
            )
    return records
