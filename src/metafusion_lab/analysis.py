from __future__ import annotations

from math import exp, sqrt

from .models import LedgerEntry, MetaSummary, TrialRecord


def _require_positive_standard_errors(records: list[TrialRecord]) -> None:
    for record in records:
        if record.standard_error <= 0:
            raise ValueError(
                "standard_error must be > 0 "
                f"(study {record.study_id!r} has {record.standard_error})."
            )


def estimate_tau2(records: list[TrialRecord]) -> float:
    _require_positive_standard_errors(records)
    if len(records) < 2:
        return 0.0

    variances = [record.standard_error**2 for record in records]
    base_weights = [1.0 / variance for variance in variances]
    fixed_effect = sum(
        weight * record.effect_log_rr
        for weight, record in zip(base_weights, records, strict=True)
    ) / sum(base_weights)

    q_statistic = sum(
        weight * (record.effect_log_rr - fixed_effect) ** 2
        for weight, record in zip(base_weights, records, strict=True)
    )
    degrees_freedom = len(records) - 1
    weight_sum = sum(base_weights)
    correction = weight_sum - sum(weight**2 for weight in base_weights) / weight_sum
    if correction <= 0:
        return 0.0
    return max((q_statistic - degrees_freedom) / correction, 0.0)


def summarize_meta_analysis(
    records: list[TrialRecord],
    label: str,
    ledger: tuple[LedgerEntry, ...] = (),
    certainty_weighted: bool = False,
) -> MetaSummary:
    if not records:
        raise ValueError("At least one trial record is required.")
    _require_positive_standard_errors(records)

    tau2 = estimate_tau2(records)
    certainty_by_study = {
        entry.record.study_id: max(entry.certainty_score, 0.05) for entry in ledger
    }

    weights: list[float] = []
    for record in records:
        weight = 1.0 / (record.standard_error**2 + tau2)
        if certainty_weighted:
            weight *= certainty_by_study.get(record.study_id, 1.0)
        weights.append(weight)

    weight_total = sum(weights)
    pooled_log_rr = sum(
        weight * record.effect_log_rr
        for weight, record in zip(weights, records, strict=True)
    ) / weight_total
    # Sandwich (robust) variance for a weighted inverse-variance mean. This
    # reduces to 1/sum(weights) when weights are the unmodified inverse
    # variances, but stays correct when weights are rescaled by certainty
    # factors -- a uniform rescale then leaves the CI unchanged, as it must.
    pooled_variance = sum(
        weight**2 * (record.standard_error**2 + tau2)
        for weight, record in zip(weights, records, strict=True)
    ) / weight_total**2
    pooled_se = sqrt(pooled_variance)
    z_value = 1.96

    return MetaSummary(
        label=label,
        pooled_log_rr=pooled_log_rr,
        pooled_rr=exp(pooled_log_rr),
        ci_low_rr=exp(pooled_log_rr - z_value * pooled_se),
        ci_high_rr=exp(pooled_log_rr + z_value * pooled_se),
        tau2=tau2,
        study_count=len(records),
    )
