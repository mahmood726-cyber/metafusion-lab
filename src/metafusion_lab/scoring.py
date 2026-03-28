from __future__ import annotations

from math import log1p

from .models import LedgerEntry, TrialRecord


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def score_trial(record: TrialRecord) -> LedgerEntry:
    extraction = clamp(record.extraction_confidence or 0.7)
    bias_resistance = 1.0 - clamp(record.risk_of_bias if record.risk_of_bias is not None else 0.25)
    registry_alignment = clamp(record.registry_match if record.registry_match is not None else 0.75)
    size_signal = (
        min(log1p(record.sample_size) / log1p(2000), 1.0)
        if record.sample_size is not None
        else 0.5
    )

    components = {
        "extraction": round(extraction, 3),
        "bias_resistance": round(bias_resistance, 3),
        "registry_alignment": round(registry_alignment, 3),
        "size_signal": round(size_signal, 3),
    }

    certainty_score = (
        0.35 * extraction
        + 0.25 * bias_resistance
        + 0.25 * registry_alignment
        + 0.15 * size_signal
    )

    flags: list[str] = []
    if record.extraction_confidence is None:
        flags.append("Extraction confidence unavailable")
    if extraction < 0.7:
        flags.append("Low extraction confidence")
    if record.risk_of_bias is None:
        flags.append("Risk of bias unavailable")
    elif record.risk_of_bias > 0.5:
        flags.append("High risk of bias")
    if record.registry_match is None:
        flags.append("Cross-source agreement unavailable")
    elif registry_alignment < 0.6:
        flags.append("Weak cross-source agreement")
    if record.sample_size is None:
        flags.append("Sample size unavailable")
    elif record.sample_size < 150:
        flags.append("Small sample size")

    return LedgerEntry(
        record=record,
        certainty_score=round(certainty_score, 3),
        components=components,
        flags=tuple(flags),
    )
