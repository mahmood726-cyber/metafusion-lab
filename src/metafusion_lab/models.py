from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TrialRecord:
    study_id: str
    year: int
    comparison: str
    outcome: str
    effect_log_rr: float
    standard_error: float
    sample_size: int | None = None
    extraction_confidence: float | None = None
    risk_of_bias: float | None = None
    registry_match: float | None = None
    notes: str = ""


@dataclass(frozen=True)
class LedgerEntry:
    record: TrialRecord
    certainty_score: float
    components: dict[str, float]
    flags: tuple[str, ...]


@dataclass(frozen=True)
class MetaSummary:
    label: str
    pooled_log_rr: float
    pooled_rr: float
    ci_low_rr: float
    ci_high_rr: float
    tau2: float
    study_count: int


@dataclass(frozen=True)
class LocalRepositorySignal:
    name: str
    path: Path
    role: str


@dataclass(frozen=True)
class EvidenceReport:
    classical: MetaSummary
    certainty_weighted: MetaSummary
    ledger: tuple[LedgerEntry, ...]
    opportunity_areas: tuple[str, ...]
    local_signals: tuple[LocalRepositorySignal, ...] = ()
