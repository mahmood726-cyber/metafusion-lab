from __future__ import annotations

from pathlib import Path

from .adapters import discover_local_repositories
from .analysis import summarize_meta_analysis
from .models import EvidenceReport, LedgerEntry, TrialRecord
from .scoring import score_trial


def build_evidence_report(
    records: list[TrialRecord],
    base_dir: str | Path | None = None,
) -> EvidenceReport:
    ledger: tuple[LedgerEntry, ...] = tuple(score_trial(record) for record in records)
    classical = summarize_meta_analysis(
        records=records,
        label="Classical random-effects",
    )
    certainty_weighted = summarize_meta_analysis(
        records=records,
        label="Certainty-adjusted random-effects",
        ledger=ledger,
        certainty_weighted=True,
    )

    opportunities: list[str] = []
    if any(entry.flags for entry in ledger):
        opportunities.append(
            "Route flagged studies into manual adjudication before they enter the next living update."
        )
    if abs(certainty_weighted.pooled_log_rr - classical.pooled_log_rr) > 0.05:
        opportunities.append(
            "The pooled effect shifts materially after certainty weighting, so provenance should be reviewed alongside the headline estimate."
        )
    if not opportunities:
        opportunities.append(
            "The current evidence ledger is stable enough to extend toward real-time living re-analysis."
        )

    local_signals = discover_local_repositories(base_dir) if base_dir else ()
    return EvidenceReport(
        classical=classical,
        certainty_weighted=certainty_weighted,
        ledger=ledger,
        opportunity_areas=tuple(opportunities),
        local_signals=local_signals,
    )


def render_markdown_report(report: EvidenceReport) -> str:
    lines = [
        "# MetaFusion Lab Report",
        "",
        "## Synthesis Snapshot",
        "",
        "| Model | RR | 95% CI | tau^2 | Studies |",
        "| --- | --- | --- | --- | --- |",
        (
            f"| {report.classical.label} | "
            f"{report.classical.pooled_rr:.3f} | "
            f"{report.classical.ci_low_rr:.3f} to {report.classical.ci_high_rr:.3f} | "
            f"{report.classical.tau2:.4f} | {report.classical.study_count} |"
        ),
        (
            f"| {report.certainty_weighted.label} | "
            f"{report.certainty_weighted.pooled_rr:.3f} | "
            f"{report.certainty_weighted.ci_low_rr:.3f} to {report.certainty_weighted.ci_high_rr:.3f} | "
            f"{report.certainty_weighted.tau2:.4f} | {report.certainty_weighted.study_count} |"
        ),
        "",
        "## Evidence Ledger",
        "",
        "| Study | Certainty | Flags | Notes |",
        "| --- | --- | --- | --- |",
    ]

    ordered_entries = sorted(report.ledger, key=lambda entry: entry.certainty_score)
    for entry in ordered_entries:
        flags = "; ".join(entry.flags) if entry.flags else "None"
        notes = entry.record.notes or "None"
        lines.append(
            f"| {entry.record.study_id} | {entry.certainty_score:.3f} | {flags} | {notes} |"
        )

    if report.opportunity_areas:
        lines.extend(["", "## Next Actions", ""])
        for item in report.opportunity_areas:
            lines.append(f"- {item}")

    if report.local_signals:
        lines.extend(["", "## Local Connectors", ""])
        for signal in report.local_signals:
            lines.append(f"- `{signal.name}` at `{signal.path}`: {signal.role}")

    return "\n".join(lines)
