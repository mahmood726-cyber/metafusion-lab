"""Regression tests for statistical edge/failure paths (audit findings MF-1..MF-5).

Each test below fails against the pre-fix code and passes after the fix:
- MF-1: uniform certainty scaling must not change the certainty-weighted CI.
- MF-2: a genuine extraction_confidence of 0.0 must score as 0.0 and be flagged.
- MF-4: a non-positive standard error must raise a clear ValueError, not crash.
- MF-5: the empty-records guard must raise ValueError.
"""

from __future__ import annotations

from math import isclose

import pytest

from metafusion_lab.analysis import estimate_tau2, summarize_meta_analysis
from metafusion_lab.models import LedgerEntry, TrialRecord
from metafusion_lab.scoring import score_trial


def _records() -> list[TrialRecord]:
    return [
        TrialRecord("A", 2020, "c", "o", 0.00, 0.10),
        TrialRecord("B", 2021, "c", "o", -0.20, 0.15),
        TrialRecord("C", 2022, "c", "o", 0.10, 0.12),
    ]


def _uniform_ledger(records: list[TrialRecord], certainty: float) -> tuple[LedgerEntry, ...]:
    return tuple(
        LedgerEntry(record=record, certainty_score=certainty, components={}, flags=())
        for record in records
    )


# --- MF-5(a): empty-records guard ---------------------------------------------
def test_summarize_meta_analysis_rejects_empty_records() -> None:
    with pytest.raises(ValueError):
        summarize_meta_analysis([], "empty")


# --- MF-2 / MF-5(b): extraction_confidence == 0.0 is honoured and flagged -----
def test_score_trial_preserves_zero_extraction_confidence() -> None:
    entry = score_trial(TrialRecord("Z", 2020, "c", "o", 0.0, 0.1, extraction_confidence=0.0))
    assert entry.components["extraction"] == 0.0
    assert "Low extraction confidence" in entry.flags
    # A true 0.0 is not "unavailable": the None-only flag must not fire.
    assert "Extraction confidence unavailable" not in entry.flags


# --- MF-1 / MF-5(c): uniform certainty scaling leaves the CI invariant --------
def test_uniform_certainty_scaling_preserves_confidence_interval() -> None:
    records = _records()
    classical = summarize_meta_analysis(records, "classical")
    classical_width = classical.ci_high_rr - classical.ci_low_rr

    for certainty in (1.0, 0.5, 0.25):
        weighted = summarize_meta_analysis(
            records,
            "certainty",
            ledger=_uniform_ledger(records, certainty),
            certainty_weighted=True,
        )
        width = weighted.ci_high_rr - weighted.ci_low_rr
        assert isclose(width, classical_width, rel_tol=0, abs_tol=1e-9)
        assert isclose(
            weighted.pooled_log_rr, classical.pooled_log_rr, rel_tol=0, abs_tol=1e-9
        )


# --- MF-4 / MF-5(d): non-positive standard error raises a clear ValueError -----
def test_estimate_tau2_rejects_nonpositive_standard_error() -> None:
    with pytest.raises(ValueError):
        estimate_tau2(
            [
                TrialRecord("A", 2020, "c", "o", 0.0, 0.0),
                TrialRecord("B", 2021, "c", "o", 0.1, 0.1),
            ]
        )


def test_summarize_meta_analysis_rejects_nonpositive_standard_error() -> None:
    with pytest.raises(ValueError):
        summarize_meta_analysis([TrialRecord("A", 2020, "c", "o", 0.0, 0.0)], "degenerate")
