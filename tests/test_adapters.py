import copy
import json
from math import isclose, log
from pathlib import Path

import pytest

from metafusion_lab.adapters import (
    build_fused_import_result,
    load_fused_records,
    load_rct_extractor_records,
    load_truthcert_records,
    write_reconciliation_report,
)
from metafusion_lab.cli import main


def fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures"


def build_rct_extractor_batch_dir(tmp_path: Path) -> Path:
    source_rows = [
        json.loads(line)
        for line in (fixtures_dir() / "rct_extractor_ma_records.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    batch_dir = tmp_path / "rct_batch"
    batch_dir.mkdir()

    nested_dir = batch_dir / "nested"
    nested_dir.mkdir()

    (batch_dir / "actt.jsonl").write_text(
        json.dumps(source_rows[0]) + "\n",
        encoding="utf-8",
    )
    (nested_dir / "checkmate.json").write_text(
        json.dumps([source_rows[1]], indent=2),
        encoding="utf-8",
    )
    (batch_dir / "metadata.json").write_text(
        json.dumps({"note": "skip me", "generated_by": "batch harness"}, indent=2),
        encoding="utf-8",
    )
    (nested_dir / "manifest.json").write_text(
        json.dumps({"files": ["actt.jsonl", "checkmate.json"]}, indent=2),
        encoding="utf-8",
    )
    return batch_dir


def build_truthcert_batch_dir(tmp_path: Path) -> Path:
    source_payload = json.loads(
        (fixtures_dir() / "truthcert_verify_response.json").read_text(encoding="utf-8")
    )
    batch_dir = tmp_path / "truthcert_batch"
    batch_dir.mkdir()

    (batch_dir / "checkmate.json").write_text(
        json.dumps(source_payload, indent=2),
        encoding="utf-8",
    )

    actt_payload = copy.deepcopy(source_payload)
    actt_payload["bundle"]["bundleId"] = "bundle-actt-1"
    actt_payload["bundle"]["values"]["study_id"]["value"] = "actt_1_2020"
    actt_payload["bundle"]["values"]["publication_year"]["value"] = 2020
    actt_payload["bundle"]["values"]["primary_outcome_name"]["value"] = "Recovery rate"
    actt_payload["bundle"]["values"]["effect_estimate_type"]["value"] = "RR"
    actt_payload["bundle"]["values"]["effect_estimate_value"]["value"] = 1.3263157894736843
    actt_payload["bundle"]["values"]["effect_estimate_value"]["sourceSnippet"] = (
        "Recovery rate 1.33 (95% CI 0.70 to 2.51)"
    )
    actt_payload["bundle"]["values"]["primary_outcome_ci_lower"]["value"] = 0.7001308497460719
    actt_payload["bundle"]["values"]["primary_outcome_ci_upper"]["value"] = 2.5125497241625756
    actt_payload["bundle"]["values"]["primary_outcome_treatment_n"]["value"] = 531
    actt_payload["bundle"]["values"]["primary_outcome_control_n"]["value"] = 530
    actt_payload["fieldAgreements"][0]["consensusValue"] = "actt_1_2020"
    actt_payload["fieldAgreements"][1]["consensusValue"] = "Recovery rate"
    actt_payload["fieldAgreements"][2]["consensusValue"] = "RR"
    actt_payload["fieldAgreements"][3]["consensusValue"] = 1.3263157894736843
    actt_payload["fieldAgreements"][4]["consensusValue"] = 0.7001308497460719
    actt_payload["fieldAgreements"][5]["consensusValue"] = 2.5125497241625756

    (batch_dir / "actt.json").write_text(
        json.dumps(actt_payload, indent=2),
        encoding="utf-8",
    )
    (batch_dir / "metadata.json").write_text(
        json.dumps({"note": "skip me"}, indent=2),
        encoding="utf-8",
    )
    return batch_dir


def build_truthcert_conflict_payload(
    *,
    study_id: str = "checkmate_067_2015",
    outcome_name: str = "PFS",
    point_estimate: float = 1.48,
    ci_lower: float = 1.14,
    ci_upper: float = 1.92,
) -> dict:
    payload = json.loads(
        (fixtures_dir() / "truthcert_verify_response.json").read_text(encoding="utf-8")
    )
    payload["bundle"]["bundleId"] = f"bundle-{study_id}"
    payload["bundle"]["values"]["study_id"]["value"] = study_id
    payload["bundle"]["values"]["primary_outcome_name"]["value"] = outcome_name
    payload["bundle"]["values"]["effect_estimate_value"]["value"] = point_estimate
    payload["bundle"]["values"]["effect_estimate_value"]["sourceSnippet"] = (
        f"Estimated effect {point_estimate:.2f} (95% CI {ci_lower:.2f} to {ci_upper:.2f})"
    )
    payload["bundle"]["values"]["primary_outcome_ci_lower"]["value"] = ci_lower
    payload["bundle"]["values"]["primary_outcome_ci_upper"]["value"] = ci_upper
    payload["fieldAgreements"][0]["consensusValue"] = study_id
    payload["fieldAgreements"][1]["consensusValue"] = outcome_name
    payload["fieldAgreements"][3]["consensusValue"] = point_estimate
    payload["fieldAgreements"][4]["consensusValue"] = ci_lower
    payload["fieldAgreements"][5]["consensusValue"] = ci_upper
    return payload


def test_load_rct_extractor_records_imports_validated_ma_jsonl() -> None:
    records = load_rct_extractor_records(fixtures_dir() / "rct_extractor_ma_records.jsonl")

    assert len(records) == 2
    assert records[0].study_id == "actt_1_2020"
    assert isclose(records[0].effect_log_rr, log(1.3263157894736843))
    assert records[0].sample_size is None
    assert "rct-extractor-v2" in records[0].notes


def test_load_rct_extractor_records_supports_batch_directory_and_skips_metadata(
    tmp_path: Path,
) -> None:
    records = load_rct_extractor_records(build_rct_extractor_batch_dir(tmp_path))

    assert len(records) == 2
    assert [record.study_id for record in records] == [
        "actt_1_2020",
        "checkmate_067_2015",
    ]


def test_load_truthcert_records_imports_verify_response_bundle() -> None:
    records = load_truthcert_records(fixtures_dir() / "truthcert_verify_response.json")

    assert len(records) == 1
    record = records[0]
    assert record.study_id == "checkmate_067_2015"
    assert record.year == 2015
    assert record.sample_size == 942
    assert record.extraction_confidence is not None and record.extraction_confidence > 0.9
    assert isclose(record.registry_match or 0.0, 0.927, rel_tol=0, abs_tol=1e-6)
    assert record.risk_of_bias == 0.45
    assert "TruthCert SHIPPED bundle" in record.notes


def test_load_truthcert_records_supports_batch_directory(tmp_path: Path) -> None:
    records = load_truthcert_records(build_truthcert_batch_dir(tmp_path))

    assert len(records) == 2
    assert {record.study_id for record in records} == {
        "actt_1_2020",
        "checkmate_067_2015",
    }


def test_load_fused_records_enriches_matching_rows_and_flags_unmatched() -> None:
    records = load_fused_records(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        fixtures_dir() / "truthcert_verify_response.json",
    )

    assert len(records) == 2
    by_study = {record.study_id: record for record in records}

    matched = by_study["checkmate_067_2015"]
    assert matched.sample_size == 942
    assert matched.extraction_confidence is not None and matched.extraction_confidence > 0.9
    assert isclose(matched.registry_match or 0.0, 0.927, rel_tol=0, abs_tol=1e-6)
    assert "Matched TruthCert SHIPPED bundle" in matched.notes
    assert "agrees with the imported MA effect estimate" in matched.notes

    unmatched = by_study["actt_1_2020"]
    assert unmatched.sample_size is None
    assert "No matching TruthCert bundle found" in unmatched.notes


def test_load_fused_records_supports_truthcert_batch_directory(tmp_path: Path) -> None:
    records = load_fused_records(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        build_truthcert_batch_dir(tmp_path),
    )

    by_study = {record.study_id: record for record in records}
    assert by_study["actt_1_2020"].sample_size == 1061
    assert "Matched TruthCert SHIPPED bundle" in by_study["actt_1_2020"].notes
    assert by_study["checkmate_067_2015"].sample_size == 942


def test_load_fused_records_supports_rct_and_truthcert_batch_directories(
    tmp_path: Path,
) -> None:
    records = load_fused_records(
        build_rct_extractor_batch_dir(tmp_path),
        build_truthcert_batch_dir(tmp_path),
    )

    by_study = {record.study_id: record for record in records}
    assert by_study["actt_1_2020"].sample_size == 1061
    assert "Matched TruthCert SHIPPED bundle" in by_study["actt_1_2020"].notes
    assert by_study["checkmate_067_2015"].sample_size == 942


def test_fused_records_quarantine_critical_exact_match_conflict(tmp_path: Path) -> None:
    conflict_path = tmp_path / "conflict.json"
    conflict_path.write_text(
        json.dumps(build_truthcert_conflict_payload(), indent=2),
        encoding="utf-8",
    )

    records = load_fused_records(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        conflict_path,
    )
    by_study = {record.study_id: record for record in records}

    conflicted = by_study["checkmate_067_2015"]
    assert conflicted.sample_size is None
    assert conflicted.extraction_confidence is not None and conflicted.extraction_confidence <= 0.42
    assert conflicted.registry_match is not None and conflicted.registry_match <= 0.3
    assert conflicted.risk_of_bias is not None and conflicted.risk_of_bias >= 0.8
    assert "Critical extractor-verifier conflict detected" in conflicted.notes
    assert "sample size was not imported" in conflicted.notes


def test_fused_records_reject_conflicting_study_only_fallback(tmp_path: Path) -> None:
    conflict_path = tmp_path / "study_only_conflict.json"
    conflict_payload = build_truthcert_conflict_payload(outcome_name="Progression free survival")
    conflict_path.write_text(json.dumps(conflict_payload, indent=2), encoding="utf-8")

    records = load_fused_records(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        conflict_path,
    )
    by_study = {record.study_id: record for record in records}

    rejected = by_study["checkmate_067_2015"]
    assert rejected.sample_size is None
    assert rejected.extraction_confidence is None
    assert rejected.registry_match is None
    assert "Rejected TruthCert SHIPPED bundle matched via study-only shipped key" in rejected.notes
    assert "Manual adjudication required" in rejected.notes


def test_build_fused_import_result_rejects_incompatible_study_only_outcomes(tmp_path: Path) -> None:
    incompatible_path = tmp_path / "wrong_outcome.json"
    incompatible_payload = build_truthcert_conflict_payload(
        outcome_name="Overall survival",
        point_estimate=0.57,
        ci_lower=0.43,
        ci_upper=0.76,
    )
    incompatible_path.write_text(
        json.dumps(incompatible_payload, indent=2),
        encoding="utf-8",
    )

    result = build_fused_import_result(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        incompatible_path,
    )

    checkmate_entry = next(
        entry for entry in result.reconciliation_entries if entry.study_id == "checkmate_067_2015"
    )
    unused_entry = next(
        entry
        for entry in result.reconciliation_entries
        if entry.match_status == "unused_truthcert_overlay"
    )

    assert checkmate_entry.match_status == "no_match"
    assert checkmate_entry.truthcert_source_ref is None
    assert unused_entry.truthcert_outcome == "Overall survival"
    assert unused_entry.truthcert_study_id == "checkmate_067_2015"


def test_build_fused_import_result_tracks_aligned_and_unused_overlays(tmp_path: Path) -> None:
    batch_dir = build_truthcert_batch_dir(tmp_path)
    unused_payload = build_truthcert_conflict_payload(study_id="unused_trial_2024", outcome_name="OS")
    (batch_dir / "unused.json").write_text(
        json.dumps(unused_payload, indent=2),
        encoding="utf-8",
    )

    result = build_fused_import_result(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        batch_dir,
    )

    assert len(result.records) == 2

    aligned_entries = [entry for entry in result.reconciliation_entries if entry.match_status == "aligned"]
    assert len(aligned_entries) == 2
    assert {entry.study_id for entry in aligned_entries} == {
        "actt_1_2020",
        "checkmate_067_2015",
    }

    unused_entries = [
        entry for entry in result.reconciliation_entries if entry.match_status == "unused_truthcert_overlay"
    ]
    assert len(unused_entries) == 1
    assert unused_entries[0].truthcert_study_id == "unused_trial_2024"
    assert unused_entries[0].action == "review"


def test_build_fused_import_result_tracks_rejected_and_quarantined_conflicts(tmp_path: Path) -> None:
    exact_conflict_path = tmp_path / "exact_conflict.json"
    exact_conflict_path.write_text(
        json.dumps(build_truthcert_conflict_payload(), indent=2),
        encoding="utf-8",
    )
    exact_result = build_fused_import_result(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        exact_conflict_path,
    )
    exact_entry = next(
        entry
        for entry in exact_result.reconciliation_entries
        if entry.study_id == "checkmate_067_2015"
    )
    assert exact_entry.match_status == "critical_conflict"
    assert exact_entry.action == "quarantine"
    assert exact_entry.match_reason == "study+outcome"
    assert exact_entry.effect_gap is not None and exact_entry.effect_gap > 0.0

    fallback_conflict_path = tmp_path / "fallback_conflict.json"
    fallback_conflict_path.write_text(
        json.dumps(
            build_truthcert_conflict_payload(outcome_name="Progression free survival"),
            indent=2,
        ),
        encoding="utf-8",
    )
    fallback_result = build_fused_import_result(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        fallback_conflict_path,
    )
    fallback_entry = next(
        entry
        for entry in fallback_result.reconciliation_entries
        if entry.study_id == "checkmate_067_2015"
    )
    assert fallback_entry.match_status == "rejected_fallback"
    assert fallback_entry.action == "reject"
    assert fallback_entry.match_reason == "study-only shipped"
    assert "rejected" in fallback_entry.notes.lower()


def test_build_fused_import_result_consumes_truthcert_overlay_once(tmp_path: Path) -> None:
    source_rows = (
        fixtures_dir() / "rct_extractor_ma_records.jsonl"
    ).read_text(encoding="utf-8").strip().splitlines()
    duplicate_rct_path = tmp_path / "duplicate_rct.jsonl"
    duplicate_rct_path.write_text(
        "\n".join([*source_rows, source_rows[1]]) + "\n",
        encoding="utf-8",
    )

    result = build_fused_import_result(
        duplicate_rct_path,
        fixtures_dir() / "truthcert_verify_response.json",
    )

    checkmate_entries = [
        entry for entry in result.reconciliation_entries if entry.study_id == "checkmate_067_2015"
    ]

    assert len(checkmate_entries) == 2
    assert {entry.match_status for entry in checkmate_entries} == {"aligned", "no_match"}
    assert sum(1 for entry in checkmate_entries if entry.truthcert_source_ref is not None) == 1


def test_write_reconciliation_report_supports_csv_jsonl_and_ndjson(tmp_path: Path) -> None:
    result = build_fused_import_result(
        fixtures_dir() / "rct_extractor_ma_records.jsonl",
        fixtures_dir() / "truthcert_verify_response.json",
    )

    csv_path = tmp_path / "reconciliation.csv"
    jsonl_path = tmp_path / "reconciliation.jsonl"
    ndjson_path = tmp_path / "reconciliation.ndjson"
    write_reconciliation_report(result.reconciliation_entries, csv_path)
    write_reconciliation_report(result.reconciliation_entries, jsonl_path)
    write_reconciliation_report(result.reconciliation_entries, ndjson_path)

    csv_text = csv_path.read_text(encoding="utf-8")
    jsonl_lines = jsonl_path.read_text(encoding="utf-8").strip().splitlines()
    ndjson_lines = ndjson_path.read_text(encoding="utf-8").strip().splitlines()

    assert "match_status" in csv_text
    assert "checkmate_067_2015" in csv_text
    assert len(jsonl_lines) == len(result.reconciliation_entries)
    assert len(ndjson_lines) == len(result.reconciliation_entries)
    assert json.loads(jsonl_lines[0])["match_status"] in {"aligned", "no_match"}
    assert json.loads(ndjson_lines[0])["match_status"] in {"aligned", "no_match"}


def test_cli_report_supports_rct_extractor_format(capsys, tmp_path: Path) -> None:
    exit_code = main(
        [
            "report",
            "--format",
            "rct-extractor",
            "--input",
            str(fixtures_dir() / "rct_extractor_ma_records.jsonl"),
            "--base-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "# MetaFusion Lab Report" in captured.out
    assert "actt_1_2020" in captured.out


def test_cli_report_supports_rct_extractor_directory_input(capsys, tmp_path: Path) -> None:
    exit_code = main(
        [
            "report",
            "--format",
            "rct-extractor",
            "--input",
            str(build_rct_extractor_batch_dir(tmp_path)),
            "--base-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "# MetaFusion Lab Report" in captured.out
    assert "actt_1_2020" in captured.out
    assert "checkmate_067_2015" in captured.out


def test_cli_report_supports_fused_format(capsys, tmp_path: Path) -> None:
    exit_code = main(
        [
            "report",
            "--format",
            "fused",
            "--input",
            str(fixtures_dir() / "rct_extractor_ma_records.jsonl"),
            "--truthcert-input",
            str(fixtures_dir() / "truthcert_verify_response.json"),
            "--base-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "# MetaFusion Lab Report" in captured.out
    assert "Matched TruthCert SHIPPED bundle" in captured.out


def test_cli_report_supports_truthcert_directory_input(capsys, tmp_path: Path) -> None:
    exit_code = main(
        [
            "report",
            "--format",
            "fused",
            "--input",
            str(fixtures_dir() / "rct_extractor_ma_records.jsonl"),
            "--truthcert-input",
            str(build_truthcert_batch_dir(tmp_path)),
            "--base-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "# MetaFusion Lab Report" in captured.out
    assert "Filled sample size from TruthCert: 1061." in captured.out


def test_cli_report_supports_fused_rct_and_truthcert_directory_inputs(
    capsys,
    tmp_path: Path,
) -> None:
    exit_code = main(
        [
            "report",
            "--format",
            "fused",
            "--input",
            str(build_rct_extractor_batch_dir(tmp_path)),
            "--truthcert-input",
            str(build_truthcert_batch_dir(tmp_path)),
            "--base-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "# MetaFusion Lab Report" in captured.out
    assert "Filled sample size from TruthCert: 1061." in captured.out
    assert "checkmate_067_2015" in captured.out


def test_cli_report_writes_reconciliation_output(capsys, tmp_path: Path) -> None:
    reconciliation_path = tmp_path / "reconciliation.json"

    exit_code = main(
        [
            "report",
            "--format",
            "fused",
            "--input",
            str(fixtures_dir() / "rct_extractor_ma_records.jsonl"),
            "--truthcert-input",
            str(fixtures_dir() / "truthcert_verify_response.json"),
            "--reconciliation-output",
            str(reconciliation_path),
            "--base-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    exported_rows = json.loads(reconciliation_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "# MetaFusion Lab Report" in captured.out
    assert reconciliation_path.exists()
    assert len(exported_rows) == 2
    assert {row["match_status"] for row in exported_rows} == {"aligned", "no_match"}


def test_cli_report_creates_output_parent_directories(capsys, tmp_path: Path) -> None:
    markdown_path = tmp_path / "reports" / "meta" / "report.md"
    reconciliation_path = tmp_path / "reports" / "audit" / "reconciliation.ndjson"

    exit_code = main(
        [
            "report",
            "--format",
            "fused",
            "--input",
            str(fixtures_dir() / "rct_extractor_ma_records.jsonl"),
            "--truthcert-input",
            str(fixtures_dir() / "truthcert_verify_response.json"),
            "--output",
            str(markdown_path),
            "--reconciliation-output",
            str(reconciliation_path),
            "--base-dir",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == ""
    assert markdown_path.exists()
    assert reconciliation_path.exists()


def test_cli_report_rejects_output_collisions(tmp_path: Path) -> None:
    truthcert_copy = tmp_path / "truthcert.json"
    truthcert_copy.write_text(
        (fixtures_dir() / "truthcert_verify_response.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "report",
                "--format",
                "fused",
                "--input",
                str(fixtures_dir() / "rct_extractor_ma_records.jsonl"),
                "--truthcert-input",
                str(truthcert_copy),
                "--reconciliation-output",
                str(truthcert_copy),
                "--base-dir",
                str(tmp_path),
            ]
        )

    assert exc_info.value.code == 2


def test_cli_report_rejects_invalid_reconciliation_suffix(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "report",
                "--format",
                "fused",
                "--input",
                str(fixtures_dir() / "rct_extractor_ma_records.jsonl"),
                "--truthcert-input",
                str(fixtures_dir() / "truthcert_verify_response.json"),
                "--reconciliation-output",
                str(tmp_path / "reconciliation.csvv"),
                "--base-dir",
                str(tmp_path),
            ]
        )

    assert exc_info.value.code == 2
