from pathlib import Path

from metafusion_lab.io import load_trial_records
from metafusion_lab.pipeline import build_evidence_report, render_markdown_report


def demo_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "demo_trials.csv"


def test_certainty_weighting_downweights_the_low_trust_outlier(tmp_path: Path) -> None:
    (tmp_path / "truthcert").mkdir()
    (tmp_path / "rct-extractor-v2").mkdir()

    records = load_trial_records(demo_path())
    report = build_evidence_report(records, base_dir=tmp_path)

    assert report.classical.study_count == 5
    assert report.certainty_weighted.pooled_rr < report.classical.pooled_rr
    assert any("Low extraction confidence" in entry.flags for entry in report.ledger)
    assert {signal.name for signal in report.local_signals} == {
        "truthcert",
        "rct-extractor-v2",
    }


def test_rendered_report_contains_actionable_sections(tmp_path: Path) -> None:
    (tmp_path / "living-meta").mkdir()

    report = build_evidence_report(load_trial_records(demo_path()), base_dir=tmp_path)
    markdown = render_markdown_report(report)

    assert "# MetaFusion Lab Report" in markdown
    assert "## Evidence Ledger" in markdown
    assert "HF-Bridge-04" in markdown
    assert "## Local Connectors" in markdown
