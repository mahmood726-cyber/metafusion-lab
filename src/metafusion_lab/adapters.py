from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import csv
import json
import re
from math import log
from pathlib import Path
from statistics import mean
from typing import Any

from .io import load_trial_records
from .models import LocalRepositorySignal, TrialRecord


KNOWN_LOCAL_REPOSITORIES = {
    "truthcert": "certainty calibration and trust-scored evidence synthesis",
    "rct-extractor-v2": "trial extraction and structured evidence capture",
    "living-meta": "living evidence update and presentation workflows",
    "ipd_qma_project": "advanced quantitative meta-analysis and benchmarking",
    "llm-meta-analysis": "model-assisted synthesis and interface ideas",
}

LOG_SCALE_EFFECT_TYPES = {"HR", "OR", "RR", "IRR", "GMR"}
EFFECT_TYPE_ALIASES = {
    "RISK RATIO": "RR",
    "ODDS RATIO": "OR",
    "HAZARD RATIO": "HR",
    "INCIDENCE RATE RATIO": "IRR",
    "GEOMETRIC MEAN RATIO": "GMR",
}
TRUTHCERT_STATE_MULTIPLIER = {
    "SHIPPED": 1.0,
    "DRAFT": 0.85,
    "REJECTED": 0.55,
}
OUTCOME_NORMALIZATION_ALIASES = {
    "pfs": "progression free survival",
    "progression free survival": "progression free survival",
    "os": "overall survival",
    "overall survival": "overall survival",
    "dfs": "disease free survival",
    "disease free survival": "disease free survival",
    "efs": "event free survival",
    "event free survival": "event free survival",
    "rfs": "recurrence free survival",
    "recurrence free survival": "recurrence free survival",
    "orr": "objective response rate",
    "objective response rate": "objective response rate",
    "dcr": "disease control rate",
    "disease control rate": "disease control rate",
}
FUSION_LOG_DIFF_TOLERANCE = 0.05
FUSION_SE_DIFF_TOLERANCE = 0.05
STRUCTURED_IMPORT_SUFFIXES = {".json", ".jsonl", ".ndjson"}


@dataclass(frozen=True)
class TruthCertOverlay:
    record: TrialRecord
    terminal_state: str
    effect_type: str
    source_ref: str
    source_path: str


@dataclass(frozen=True)
class ReconciliationAssessment:
    severity: str
    effect_gap: float
    se_gap: float
    ci_overlap: bool
    direction_conflict: bool


@dataclass(frozen=True)
class ReconciliationReportEntry:
    study_id: str
    outcome: str
    match_status: str
    action: str
    match_reason: str | None
    truthcert_terminal_state: str | None
    truthcert_study_id: str | None
    truthcert_outcome: str | None
    truthcert_source_ref: str | None
    truthcert_source_path: str | None
    effect_gap: float | None
    se_gap: float | None
    ci_overlap: bool | None
    direction_conflict: bool | None
    notes: str


@dataclass(frozen=True)
class FusedImportResult:
    records: tuple[TrialRecord, ...]
    reconciliation_entries: tuple[ReconciliationReportEntry, ...]


def discover_local_repositories(base_dir: str | Path) -> tuple[LocalRepositorySignal, ...]:
    root = Path(base_dir)
    signals: list[LocalRepositorySignal] = []
    for name, role in KNOWN_LOCAL_REPOSITORIES.items():
        path = root / name
        if path.exists():
            signals.append(LocalRepositorySignal(name=name, path=path, role=role))
    return tuple(signals)


def load_records_for_format(
    source_format: str,
    input_path: str | Path,
    truthcert_input_path: str | Path | None = None,
) -> list[TrialRecord]:
    normalized = source_format.strip().lower()
    if normalized == "csv":
        return load_trial_records(input_path)
    if normalized == "rct-extractor":
        return load_rct_extractor_records(input_path)
    if normalized == "truthcert":
        return load_truthcert_records(input_path)
    if normalized == "fused":
        if truthcert_input_path is None:
            raise ValueError("Fused import requires a TruthCert input path.")
        return load_fused_records(input_path, truthcert_input_path)
    raise ValueError(f"Unsupported source format: {source_format}")


def load_rct_extractor_records(input_path: str | Path) -> list[TrialRecord]:
    path = Path(input_path)
    records: list[TrialRecord] = []
    if path.is_dir():
        payloads = _iter_rct_extractor_payloads(path)
        for source_ref, payload in payloads:
            try:
                records.append(_rct_payload_to_trial_record(payload))
            except Exception as exc:
                raise ValueError(
                    f"Failed to import rct-extractor-v2 record {source_ref}: {exc}"
                ) from exc
        if not records:
            raise ValueError("No rct-extractor-v2 validated MA records were found.")
        return records

    for index, payload in enumerate(_load_structured_objects(path), start=1):
        try:
            records.append(_rct_payload_to_trial_record(payload))
        except Exception as exc:
            raise ValueError(
                f"Failed to import rct-extractor-v2 record {index}: {exc}"
            ) from exc
    return records


def load_truthcert_records(input_path: str | Path) -> list[TrialRecord]:
    return [overlay.record for overlay in _load_truthcert_overlays(input_path)]


def load_fused_records(
    rct_input_path: str | Path,
    truthcert_input_path: str | Path,
) -> list[TrialRecord]:
    return list(build_fused_import_result(rct_input_path, truthcert_input_path).records)


def build_fused_import_result(
    rct_input_path: str | Path,
    truthcert_input_path: str | Path,
) -> FusedImportResult:
    rct_records = load_rct_extractor_records(rct_input_path)
    overlays = _load_truthcert_overlays(truthcert_input_path)

    overlays_by_pair: dict[tuple[str, str], list[TruthCertOverlay]] = {}
    overlays_by_study: dict[str, list[TruthCertOverlay]] = {}
    for overlay in overlays:
        pair_key = (_normalized_key(overlay.record.study_id), _normalized_key(overlay.record.outcome))
        study_key = _normalized_key(overlay.record.study_id)
        overlays_by_pair.setdefault(pair_key, []).append(overlay)
        overlays_by_study.setdefault(study_key, []).append(overlay)

    fused_records: list[TrialRecord] = []
    reconciliation_entries: list[ReconciliationReportEntry] = []
    used_overlay_refs: set[str] = set()
    for record in rct_records:
        overlay, match_reason = _match_truthcert_overlay(
            record,
            overlays_by_pair=overlays_by_pair,
            overlays_by_study=overlays_by_study,
        )
        if overlay is None:
            unmatched_record = replace(
                record,
                notes=_join_notes(
                    record.notes,
                    "No matching TruthCert bundle found for this MA record.",
                ),
            )
            fused_records.append(unmatched_record)
            reconciliation_entries.append(
                ReconciliationReportEntry(
                    study_id=unmatched_record.study_id,
                    outcome=unmatched_record.outcome,
                    match_status="no_match",
                    action="none",
                    match_reason=None,
                    truthcert_terminal_state=None,
                    truthcert_study_id=None,
                    truthcert_outcome=None,
                    truthcert_source_ref=None,
                    truthcert_source_path=None,
                    effect_gap=None,
                    se_gap=None,
                    ci_overlap=None,
                    direction_conflict=None,
                    notes="No matching TruthCert bundle found for this MA record.",
                )
            )
            continue
        fused_record, reconciliation_entry = _merge_rct_and_truthcert(
            record,
            overlay,
            match_reason,
        )
        _consume_overlay(
            overlay,
            overlays_by_pair=overlays_by_pair,
            overlays_by_study=overlays_by_study,
        )
        used_overlay_refs.add(overlay.source_ref)
        fused_records.append(fused_record)
        reconciliation_entries.append(reconciliation_entry)

    for overlay in overlays:
        if overlay.source_ref in used_overlay_refs:
            continue
        reconciliation_entries.append(
            ReconciliationReportEntry(
                study_id="",
                outcome="",
                match_status="unused_truthcert_overlay",
                action="review",
                match_reason=None,
                truthcert_terminal_state=overlay.terminal_state,
                truthcert_study_id=overlay.record.study_id,
                truthcert_outcome=overlay.record.outcome,
                truthcert_source_ref=overlay.source_ref,
                truthcert_source_path=overlay.source_path,
                effect_gap=None,
                se_gap=None,
                ci_overlap=None,
                direction_conflict=None,
                notes="TruthCert overlay was not matched to any imported MA record.",
            )
        )

    return FusedImportResult(
        records=tuple(fused_records),
        reconciliation_entries=tuple(reconciliation_entries),
    )


def write_reconciliation_report(
    entries: tuple[ReconciliationReportEntry, ...] | list[ReconciliationReportEntry],
    output_path: str | Path,
) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [_reconciliation_entry_to_dict(entry) for entry in entries]
    suffix = path.suffix.lower()
    if suffix == ".csv":
        fieldnames = list(rows[0].keys()) if rows else list(_reconciliation_entry_to_dict(
            ReconciliationReportEntry(
                study_id="",
                outcome="",
                match_status="",
                action="",
                match_reason=None,
                truthcert_terminal_state=None,
                truthcert_study_id=None,
                truthcert_outcome=None,
                truthcert_source_ref=None,
                truthcert_source_path=None,
                effect_gap=None,
                se_gap=None,
                ci_overlap=None,
                direction_conflict=None,
                notes="",
            )
        ).keys())
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return
    if suffix in {".jsonl", ".ndjson"}:
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=True) + "\n")
        return
    if suffix == ".json":
        with path.open("w", encoding="utf-8") as handle:
            json.dump(rows, handle, indent=2, ensure_ascii=True)
        return
    raise ValueError(
        "Unsupported reconciliation output format. Use .json, .jsonl, .ndjson, or .csv."
    )


def _load_truthcert_overlays(input_path: str | Path) -> list[TruthCertOverlay]:
    overlays: list[TruthCertOverlay] = []
    for index, (source_ref, source_path, payload) in enumerate(
        _iter_truthcert_payloads(input_path),
        start=1,
    ):
        try:
            overlays.append(_truthcert_payload_to_overlay(payload, source_ref, source_path))
        except Exception as exc:
            raise ValueError(f"Failed to import TruthCert bundle {index}: {exc}") from exc
    if not overlays:
        raise ValueError("No TruthCert verify responses or bundles were found.")
    return overlays


def _iter_rct_extractor_payloads(
    input_path: str | Path,
) -> list[tuple[str, dict[str, Any]]]:
    path = Path(input_path)
    payloads: list[tuple[str, dict[str, Any]]] = []
    candidate_files = sorted(
        file_path
        for file_path in path.rglob("*")
        if file_path.is_file() and file_path.suffix.lower() in STRUCTURED_IMPORT_SUFFIXES
    )
    for file_path in candidate_files:
        for row_index, payload in enumerate(_load_structured_objects(file_path), start=1):
            if _looks_like_rct_extractor_payload(payload):
                payloads.append((f"{file_path}:{row_index}", payload))
    return payloads


def _load_structured_objects(input_path: str | Path) -> list[dict[str, Any]]:
    path = Path(input_path)
    suffix = path.suffix.lower()
    if suffix in {".jsonl", ".ndjson"}:
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if stripped:
                    rows.append(json.loads(stripped))
        return rows
    if suffix == ".json":
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            return [payload]
        raise ValueError("JSON input must contain an object or a list of objects.")
    raise ValueError("Supported import files are .json, .jsonl, and .ndjson.")


def _looks_like_rct_extractor_payload(payload: dict[str, Any]) -> bool:
    study_id = str(payload.get("study_id") or "").strip()
    outcome_name = str(payload.get("outcome_name") or "").strip()
    effect_type = str(payload.get("effect_type") or "").strip()
    if not study_id or not outcome_name or not effect_type:
        return False
    if _to_float(payload.get("point_estimate")) is None:
        return False
    if _to_float(payload.get("standard_error")) is not None:
        return True
    return (
        _to_float(payload.get("ci_lower")) is not None
        and _to_float(payload.get("ci_upper")) is not None
    )


def _iter_truthcert_payloads(
    input_path: str | Path,
) -> list[tuple[str, str, dict[str, Any]]]:
    path = Path(input_path)
    if path.is_dir():
        payloads: list[tuple[str, str, dict[str, Any]]] = []
        candidate_files = sorted(
            file_path
            for file_path in path.rglob("*")
            if file_path.is_file() and file_path.suffix.lower() in STRUCTURED_IMPORT_SUFFIXES
        )
        for file_path in candidate_files:
            for row_index, payload in enumerate(_load_structured_objects(file_path), start=1):
                if _unwrap_truthcert_bundle(payload) is not None:
                    payloads.append(
                        (f"{file_path}:{row_index}", str(file_path), payload)
                    )
        return payloads

    return [
        (f"{Path(input_path)}:{row_index}", str(Path(input_path)), payload)
        for row_index, payload in enumerate(_load_structured_objects(input_path), start=1)
        if _unwrap_truthcert_bundle(payload) is not None
    ]


def _rct_payload_to_trial_record(payload: dict[str, Any]) -> TrialRecord:
    effect_type = _normalize_effect_type(payload.get("effect_type"))
    point_estimate = _require_float(payload.get("point_estimate"), "point_estimate")
    standard_error = _derive_log_standard_error(
        effect_type=effect_type,
        point_estimate=point_estimate,
        ci_lower=_to_float(payload.get("ci_lower")),
        ci_upper=_to_float(payload.get("ci_upper")),
        standard_error=_to_float(payload.get("standard_error")),
    )
    effect_log_rr = log(point_estimate)

    provenance = payload.get("provenance") or {}
    notes: list[str] = ["Imported from rct-extractor-v2 validated MA record."]
    computation_origin = str(payload.get("computation_origin") or "").strip()
    if computation_origin:
        notes.append(f"Computation origin: {computation_origin}.")
    source_type = str(provenance.get("source_type") or "").strip()
    page_number = _to_int(provenance.get("page_number"))
    if source_type or page_number is not None:
        page_label = f" page {page_number + 1}" if page_number is not None else ""
        notes.append(f"Provenance: {source_type or 'unknown'}{page_label}.")
    timepoint = str(payload.get("timepoint") or "").strip()
    if timepoint:
        notes.append(f"Timepoint: {timepoint}.")
    if not _infer_sample_size(payload):
        notes.append("Sample size missing in upstream MA record.")

    return TrialRecord(
        study_id=str(payload.get("study_id") or "").strip(),
        year=_infer_year(payload),
        comparison=str(payload.get("comparison") or f"{effect_type} imported estimate"),
        outcome=str(payload.get("outcome_name") or "").strip(),
        effect_log_rr=effect_log_rr,
        standard_error=standard_error,
        sample_size=_infer_sample_size(payload),
        extraction_confidence=_to_float(
            payload.get("extraction_confidence") or payload.get("confidence")
        ),
        risk_of_bias=_to_float(payload.get("risk_of_bias")),
        registry_match=_to_float(payload.get("registry_match")),
        notes=" ".join(notes),
    )


def _truthcert_payload_to_overlay(
    payload: dict[str, Any],
    source_ref: str,
    source_path: str,
) -> TruthCertOverlay:
    bundle = _unwrap_truthcert_bundle(payload)
    if bundle is None:
        raise ValueError(
            "TruthCert input must be a VerificationBundle, a VerifyResponse with "
            "'bundle', or an ExploreResponse with 'draft'."
        )

    values = bundle.get("values")
    if not isinstance(values, dict) or not values:
        raise ValueError("TruthCert bundle has no extracted values.")

    effect_type = _normalize_effect_type(_extracted_value(values, "effect_estimate_type", "effect_type"))
    point_estimate = _require_float(
        _extracted_value(values, "effect_estimate_value", "point_estimate"),
        "effect estimate",
    )
    standard_error = _derive_log_standard_error(
        effect_type=effect_type,
        point_estimate=point_estimate,
        ci_lower=_to_float(_extracted_value(values, "primary_outcome_ci_lower", "ci_lower")),
        ci_upper=_to_float(_extracted_value(values, "primary_outcome_ci_upper", "ci_upper")),
        standard_error=_to_float(_extracted_value(values, "standard_error")),
    )
    effect_log_rr = log(point_estimate)

    study_id = _string_value(
        values,
        "study_id",
        "study_title",
        default=str(bundle.get("bundleId") or "truthcert-bundle"),
    )
    terminal_state = str(bundle.get("terminalState") or "DRAFT").upper()
    field_confidences = _truthcert_field_confidences(
        values,
        "study_id",
        "publication_year",
        "primary_outcome_name",
        "outcome_name",
        "effect_estimate_type",
        "effect_type",
        "effect_estimate_value",
        "point_estimate",
        "primary_outcome_ci_lower",
        "ci_lower",
        "primary_outcome_ci_upper",
        "ci_upper",
    )
    base_confidence = mean(field_confidences) if field_confidences else None
    extraction_confidence = None
    if base_confidence is not None:
        multiplier = TRUTHCERT_STATE_MULTIPLIER.get(terminal_state, 0.75)
        extraction_confidence = round(base_confidence * multiplier, 3)

    gate_outcomes = bundle.get("gateOutcomes") or {}
    field_agreements = payload.get("fieldAgreements") if isinstance(payload, dict) else None
    registry_match = _truthcert_agreement_rate(
        field_agreements,
        "study_id",
        "primary_outcome_name",
        "outcome_name",
        "effect_estimate_type",
        "effect_type",
        "effect_estimate_value",
        "point_estimate",
        "primary_outcome_ci_lower",
        "ci_lower",
        "primary_outcome_ci_upper",
        "ci_upper",
    )
    if registry_match is None:
        registry_match = _truthcert_pass_rate(gate_outcomes)
    risk_of_bias = _infer_risk_of_bias(_extracted_value(values, "allocation_concealment", "risk_of_bias"))
    notes = [f"Imported from TruthCert {terminal_state} bundle."]
    if terminal_state != "SHIPPED":
        notes.append("Bundle is not SHIPPED, so treat this as exploratory rather than verified.")
    if registry_match is not None:
        notes.append(f"Agreement signal: {registry_match:.3f}.")
    if gate_outcomes:
        passed = sum(1 for outcome in gate_outcomes.values() if outcome.get("passed"))
        notes.append(f"Gates passed: {passed}/{len(gate_outcomes)}.")
    effect_field = _first_extracted_field(values, "effect_estimate_value", "point_estimate")
    if effect_field and isinstance(effect_field.get("sourceSnippet"), str):
        notes.append(f"Source snippet: {_truncate(effect_field['sourceSnippet'])}")
    if _infer_sample_size_from_truthcert(values) is None:
        notes.append("Sample size missing in TruthCert bundle.")

    return TruthCertOverlay(
        record=TrialRecord(
            study_id=study_id,
            year=_infer_year(
                {
                    "publication_year": _extracted_value(values, "publication_year", "year"),
                    "study_id": study_id,
                }
            ),
            comparison="Treatment vs Control",
            outcome=_string_value(
                values,
                "primary_outcome_name",
                "outcome_name",
                default=study_id,
            ),
            effect_log_rr=effect_log_rr,
            standard_error=standard_error,
            sample_size=_infer_sample_size_from_truthcert(values),
            extraction_confidence=extraction_confidence,
            risk_of_bias=risk_of_bias,
            registry_match=registry_match,
            notes=" ".join(notes),
        ),
        terminal_state=terminal_state,
        effect_type=effect_type,
        source_ref=source_ref,
        source_path=source_path,
    )


def _match_truthcert_overlay(
    record: TrialRecord,
    *,
    overlays_by_pair: dict[tuple[str, str], list[TruthCertOverlay]],
    overlays_by_study: dict[str, list[TruthCertOverlay]],
) -> tuple[TruthCertOverlay | None, str]:
    study_key = _normalized_key(record.study_id)
    outcome_key = _normalized_key(record.outcome)

    exact_matches = overlays_by_pair.get((study_key, outcome_key), [])
    if exact_matches:
        return _choose_best_overlay(record, exact_matches), "study+outcome"

    study_matches = [
        overlay
        for overlay in overlays_by_study.get(study_key, [])
        if _study_only_outcome_compatible(record.outcome, overlay.record.outcome)
    ]
    shipped_matches = [overlay for overlay in study_matches if overlay.terminal_state == "SHIPPED"]
    if len(shipped_matches) == 1:
        return shipped_matches[0], "study-only shipped"
    if shipped_matches:
        return _choose_best_overlay(record, shipped_matches), "study-only shipped"
    if len(study_matches) == 1:
        return study_matches[0], "study-only"
    if study_matches:
        return _choose_best_overlay(record, study_matches), "study-only"
    return None, ""


def _choose_best_overlay(record: TrialRecord, overlays: list[TruthCertOverlay]) -> TruthCertOverlay:
    return sorted(
        overlays,
        key=lambda overlay: (
            0 if overlay.terminal_state == "SHIPPED" else 1,
            abs(overlay.record.effect_log_rr - record.effect_log_rr),
            abs(overlay.record.standard_error - record.standard_error),
        ),
    )[0]


def _consume_overlay(
    overlay: TruthCertOverlay,
    *,
    overlays_by_pair: dict[tuple[str, str], list[TruthCertOverlay]],
    overlays_by_study: dict[str, list[TruthCertOverlay]],
) -> None:
    pair_key = (_normalized_key(overlay.record.study_id), _normalized_key(overlay.record.outcome))
    study_key = _normalized_key(overlay.record.study_id)
    _remove_overlay_from_bucket(overlays_by_pair, pair_key, overlay.source_ref)
    _remove_overlay_from_bucket(overlays_by_study, study_key, overlay.source_ref)


def _remove_overlay_from_bucket(
    buckets: dict[Any, list[TruthCertOverlay]],
    key: Any,
    source_ref: str,
) -> None:
    matches = buckets.get(key)
    if not matches:
        return
    remaining = [overlay for overlay in matches if overlay.source_ref != source_ref]
    if remaining:
        buckets[key] = remaining
        return
    buckets.pop(key, None)


def _merge_rct_and_truthcert(
    rct_record: TrialRecord,
    overlay: TruthCertOverlay,
    match_reason: str,
) -> tuple[TrialRecord, ReconciliationReportEntry]:
    truth_record = overlay.record
    assessment = _assess_reconciliation(rct_record, truth_record)

    if match_reason.startswith("study-only") and assessment.severity in {"material", "critical"}:
        rejected_record = replace(
            rct_record,
            notes=_join_notes(
                rct_record.notes,
                (
                    f"Rejected TruthCert {overlay.terminal_state} bundle matched via {match_reason} key "
                    f"because {_describe_assessment(assessment)}. Manual adjudication required before "
                    "using this verification overlay."
                ),
            ),
        )
        return rejected_record, _build_reconciliation_entry(
            rejected_record,
            overlay,
            assessment,
            match_reason=match_reason,
            match_status="rejected_fallback",
            action="reject",
            notes="Study-only fallback match was rejected because the reconciliation conflict was too large.",
        )

    notes = [rct_record.notes]
    notes.append(
        f"Matched TruthCert {overlay.terminal_state} bundle via {match_reason} key."
    )

    sample_size = rct_record.sample_size
    if (
        sample_size is None
        and truth_record.sample_size is not None
        and assessment.severity in {"aligned", "moderate"}
    ):
        sample_size = truth_record.sample_size
        notes.append(f"Filled sample size from TruthCert: {truth_record.sample_size}.")
    elif (
        sample_size is None
        and truth_record.sample_size is not None
        and assessment.severity in {"material", "critical"}
    ):
        notes.append(
            "TruthCert sample size was not imported because the effect estimates conflict materially."
        )
    elif (
        rct_record.sample_size is not None
        and truth_record.sample_size is not None
        and rct_record.sample_size != truth_record.sample_size
    ):
        notes.append(
            "TruthCert sample size differs from the MA record; retained the MA-record value."
        )

    extraction_confidence = _blend_optional_scores(
        rct_record.extraction_confidence,
        truth_record.extraction_confidence,
        truth_weight=0.7,
    )
    registry_match = (
        truth_record.registry_match
        if truth_record.registry_match is not None
        else rct_record.registry_match
    )
    risk_of_bias = (
        truth_record.risk_of_bias
        if truth_record.risk_of_bias is not None
        else rct_record.risk_of_bias
    )

    if assessment.severity == "aligned":
        notes.append("TruthCert agrees with the imported MA effect estimate.")
    elif assessment.severity == "moderate":
        notes.append(
            f"Moderate extractor-verifier drift detected: {_describe_assessment(assessment)}. "
            "The verification overlay was down-weighted."
        )
        extraction_confidence = _cap_score(_penalize_score(extraction_confidence, 0.12), 0.78)
        registry_match = _cap_score(_penalize_score(registry_match, 0.10), 0.74)
    elif assessment.severity == "material":
        notes.append(
            f"Material extractor-verifier conflict detected: {_describe_assessment(assessment)}. "
            "Manual adjudication is recommended before trusting the fused certainty score."
        )
        extraction_confidence = _cap_score(_penalize_score(extraction_confidence, 0.28), 0.58)
        registry_match = _cap_score(_penalize_score(registry_match, 0.30), 0.5)
        risk_of_bias = _raise_floor(risk_of_bias, 0.65)
    elif assessment.severity == "critical":
        notes.append(
            f"Critical extractor-verifier conflict detected: {_describe_assessment(assessment)}. "
            "The verification overlay was quarantined to conservative confidence levels and needs manual adjudication."
        )
        extraction_confidence = _cap_score(_penalize_score(extraction_confidence, 0.45), 0.42)
        registry_match = _cap_score(_penalize_score(registry_match, 0.50), 0.3)
        risk_of_bias = _raise_floor(risk_of_bias, 0.8)

    if overlay.terminal_state != "SHIPPED":
        notes.append(
            "TruthCert bundle is not SHIPPED, so the verification overlay remains exploratory."
        )

    fused_record = replace(
        rct_record,
        sample_size=sample_size,
        extraction_confidence=extraction_confidence,
        risk_of_bias=risk_of_bias,
        registry_match=registry_match,
        notes=_join_notes(*notes),
    )
    if assessment.severity == "aligned":
        match_status = "aligned"
        action = "overlay"
        entry_notes = "TruthCert overlay aligned with the imported MA record."
    elif assessment.severity == "moderate":
        match_status = "moderate_conflict"
        action = "downweight"
        entry_notes = "TruthCert overlay was retained but down-weighted because of moderate drift."
    elif assessment.severity == "material":
        match_status = "material_conflict"
        action = "quarantine"
        entry_notes = "TruthCert overlay was quarantined to conservative confidence levels."
    else:
        match_status = "critical_conflict"
        action = "quarantine"
        entry_notes = "TruthCert overlay was quarantined because of a critical conflict."
    return fused_record, _build_reconciliation_entry(
        fused_record,
        overlay,
        assessment,
        match_reason=match_reason,
        match_status=match_status,
        action=action,
        notes=entry_notes,
    )


def _build_reconciliation_entry(
    record: TrialRecord,
    overlay: TruthCertOverlay,
    assessment: ReconciliationAssessment,
    *,
    match_reason: str,
    match_status: str,
    action: str,
    notes: str,
) -> ReconciliationReportEntry:
    return ReconciliationReportEntry(
        study_id=record.study_id,
        outcome=record.outcome,
        match_status=match_status,
        action=action,
        match_reason=match_reason,
        truthcert_terminal_state=overlay.terminal_state,
        truthcert_study_id=overlay.record.study_id,
        truthcert_outcome=overlay.record.outcome,
        truthcert_source_ref=overlay.source_ref,
        truthcert_source_path=overlay.source_path,
        effect_gap=round(assessment.effect_gap, 6),
        se_gap=round(assessment.se_gap, 6),
        ci_overlap=assessment.ci_overlap,
        direction_conflict=assessment.direction_conflict,
        notes=notes,
    )


def _reconciliation_entry_to_dict(entry: ReconciliationReportEntry) -> dict[str, Any]:
    return asdict(entry)


def _assess_reconciliation(
    rct_record: TrialRecord,
    truth_record: TrialRecord,
) -> ReconciliationAssessment:
    effect_gap = abs(rct_record.effect_log_rr - truth_record.effect_log_rr)
    se_gap = abs(rct_record.standard_error - truth_record.standard_error)
    direction_conflict = rct_record.effect_log_rr * truth_record.effect_log_rr < 0
    rct_ci = _log_interval(rct_record)
    truth_ci = _log_interval(truth_record)
    ci_overlap = _intervals_overlap(rct_ci, truth_ci)

    if direction_conflict or not ci_overlap:
        severity = "critical"
    elif effect_gap > 0.18 or se_gap > 0.12:
        severity = "material"
    elif effect_gap > FUSION_LOG_DIFF_TOLERANCE or se_gap > FUSION_SE_DIFF_TOLERANCE:
        severity = "moderate"
    else:
        severity = "aligned"

    return ReconciliationAssessment(
        severity=severity,
        effect_gap=effect_gap,
        se_gap=se_gap,
        ci_overlap=ci_overlap,
        direction_conflict=direction_conflict,
    )


def _describe_assessment(assessment: ReconciliationAssessment) -> str:
    fragments = [
        f"log-effect gap {assessment.effect_gap:.3f}",
        f"SE gap {assessment.se_gap:.3f}",
    ]
    if assessment.direction_conflict:
        fragments.append("effect direction differs")
    if not assessment.ci_overlap:
        fragments.append("95% intervals do not overlap")
    return ", ".join(fragments)


def _unwrap_truthcert_bundle(payload: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(payload.get("bundle"), dict):
        return payload["bundle"]
    if isinstance(payload.get("draft"), dict):
        return payload["draft"]
    if "terminalState" in payload and "values" in payload:
        return payload
    return None


def _normalize_effect_type(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        raise ValueError("Missing effect type.")
    normalized = EFFECT_TYPE_ALIASES.get(text, text)
    if normalized not in LOG_SCALE_EFFECT_TYPES:
        supported = ", ".join(sorted(LOG_SCALE_EFFECT_TYPES))
        raise ValueError(
            f"Effect type '{normalized}' is not supported yet. "
            f"Current adapter supports {supported}."
        )
    return normalized


def _normalized_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _canonical_outcome_text(value: str) -> str:
    raw = " ".join(re.findall(r"[a-z0-9]+", value.lower()))
    if not raw:
        return ""
    return OUTCOME_NORMALIZATION_ALIASES.get(raw, raw)


def _study_only_outcome_compatible(left: str, right: str) -> bool:
    canonical_left = _canonical_outcome_text(left)
    canonical_right = _canonical_outcome_text(right)
    if not canonical_left or not canonical_right:
        return False
    if canonical_left == canonical_right:
        return True
    if canonical_left in canonical_right or canonical_right in canonical_left:
        return True

    left_tokens = set(canonical_left.split())
    right_tokens = set(canonical_right.split())
    if not left_tokens or not right_tokens:
        return False
    overlap = len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))
    if overlap >= 0.75:
        return True
    return _outcome_acronym(canonical_left) == _outcome_acronym(canonical_right)


def _outcome_acronym(value: str) -> str:
    tokens = value.split()
    if len(tokens) == 1:
        return tokens[0]
    return "".join(token[0] for token in tokens if token)


def _derive_log_standard_error(
    *,
    effect_type: str,
    point_estimate: float,
    ci_lower: float | None,
    ci_upper: float | None,
    standard_error: float | None,
) -> float:
    if effect_type not in LOG_SCALE_EFFECT_TYPES:
        raise ValueError(f"Unsupported effect type for log-scale synthesis: {effect_type}")
    if point_estimate <= 0:
        raise ValueError("Point estimate must be positive for log-scale synthesis.")
    if standard_error is not None:
        return standard_error
    if ci_lower is None or ci_upper is None or ci_lower <= 0 or ci_upper <= 0:
        raise ValueError("A log-scale effect needs either standard_error or positive CI bounds.")
    return (log(ci_upper) - log(ci_lower)) / (2 * 1.96)


def _log_interval(record: TrialRecord) -> tuple[float, float]:
    half_width = 1.96 * record.standard_error
    return (record.effect_log_rr - half_width, record.effect_log_rr + half_width)


def _intervals_overlap(
    interval_a: tuple[float, float],
    interval_b: tuple[float, float],
) -> bool:
    return interval_a[0] <= interval_b[1] and interval_b[0] <= interval_a[1]


def _infer_sample_size(payload: dict[str, Any]) -> int | None:
    direct = _to_int(payload.get("sample_size") or payload.get("n_total"))
    if direct is not None:
        return direct
    treatment = _to_int(
        payload.get("treatment_n") or payload.get("primary_outcome_treatment_n")
    )
    control = _to_int(
        payload.get("control_n") or payload.get("primary_outcome_control_n")
    )
    if treatment is None and control is None:
        return None
    return (treatment or 0) + (control or 0)


def _infer_sample_size_from_truthcert(values: dict[str, Any]) -> int | None:
    direct = _to_int(_extracted_value(values, "sample_size", "n_total"))
    if direct is not None:
        return direct
    treatment = _to_int(
        _extracted_value(
            values,
            "primary_outcome_treatment_n",
            "treatment_n",
        )
    )
    control = _to_int(
        _extracted_value(
            values,
            "primary_outcome_control_n",
            "control_n",
        )
    )
    if treatment is None and control is None:
        return None
    return (treatment or 0) + (control or 0)


def _truthcert_field_confidences(values: dict[str, Any], *names: str) -> list[float]:
    confidences: list[float] = []
    for name in names:
        field = values.get(name)
        if isinstance(field, dict):
            confidence = _to_float(field.get("confidence"))
            if confidence is not None:
                confidences.append(confidence)
    return confidences


def _truthcert_pass_rate(gate_outcomes: Any) -> float | None:
    if not isinstance(gate_outcomes, dict) or not gate_outcomes:
        return None
    outcomes = [outcome for outcome in gate_outcomes.values() if isinstance(outcome, dict)]
    if not outcomes:
        return None
    passed = sum(1 for outcome in outcomes if outcome.get("passed") is True)
    return round(passed / len(outcomes), 3)


def _truthcert_agreement_rate(field_agreements: Any, *names: str) -> float | None:
    if not isinstance(field_agreements, list) or not field_agreements:
        return None
    wanted = set(names)
    scores: list[float] = []
    for field_agreement in field_agreements:
        if not isinstance(field_agreement, dict):
            continue
        if field_agreement.get("field") not in wanted:
            continue
        agreement = _to_float(field_agreement.get("agreement"))
        if agreement is not None:
            scores.append(agreement)
    if not scores:
        return None
    return round(mean(scores), 3)


def _infer_risk_of_bias(value: Any) -> float | None:
    numeric = _to_float(value)
    if numeric is not None:
        return numeric
    text = str(value or "").strip().lower()
    if not text:
        return None
    if any(token in text for token in ("low", "adequate", "good")):
        return 0.15
    if any(token in text for token in ("unclear", "some concern")):
        return 0.45
    if any(token in text for token in ("high", "poor", "inadequate")):
        return 0.7
    return None


def _extracted_value(values: dict[str, Any], *names: str) -> Any:
    field = _first_extracted_field(values, *names)
    if not field:
        return None
    return field.get("value")


def _first_extracted_field(values: dict[str, Any], *names: str) -> dict[str, Any] | None:
    for name in names:
        field = values.get(name)
        if isinstance(field, dict):
            return field
    return None


def _string_value(values: dict[str, Any], *names: str, default: str) -> str:
    extracted = _extracted_value(values, *names)
    text = str(extracted or "").strip()
    return text or default


def _require_float(value: Any, label: str) -> float:
    parsed = _to_float(value)
    if parsed is None:
        raise ValueError(f"Missing {label}.")
    return parsed


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _infer_year(payload: dict[str, Any]) -> int:
    for key in ("publication_year", "year"):
        parsed = _to_int(payload.get(key))
        if parsed is not None and 1900 <= parsed <= 2100:
            return parsed

    for key in ("study_id", "outcome_name", "notes"):
        text = str(payload.get(key) or "")
        match = re.search(r"(19|20)\d{2}", text)
        if match:
            return int(match.group(0))

    return 1900


def _truncate(text: str, max_length: int = 140) -> str:
    stripped = text.strip()
    if len(stripped) <= max_length:
        return stripped
    return stripped[: max_length - 3].rstrip() + "..."


def _blend_optional_scores(
    base_score: float | None,
    truth_score: float | None,
    *,
    truth_weight: float,
) -> float | None:
    if base_score is None:
        return truth_score
    if truth_score is None:
        return base_score
    combined = (1.0 - truth_weight) * base_score + truth_weight * truth_score
    return round(combined, 3)


def _penalize_score(score: float | None, amount: float) -> float | None:
    if score is None:
        return None
    return round(max(score - amount, 0.0), 3)


def _cap_score(score: float | None, ceiling: float) -> float | None:
    if score is None:
        return None
    return round(min(score, ceiling), 3)


def _raise_floor(score: float | None, floor: float) -> float:
    if score is None:
        return round(floor, 3)
    return round(max(score, floor), 3)


def _join_notes(*parts: str) -> str:
    return " ".join(part.strip() for part in parts if part and part.strip())
