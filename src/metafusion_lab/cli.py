from __future__ import annotations

import argparse
from pathlib import Path

from .adapters import (
    build_fused_import_result,
    discover_local_repositories,
    load_records_for_format,
    write_reconciliation_report,
)
from .io import load_trial_records
from .pipeline import build_evidence_report, render_markdown_report


SUPPORTED_RECONCILIATION_SUFFIXES = {".json", ".jsonl", ".ndjson", ".csv"}


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_demo_path() -> Path:
    return project_root() / "data" / "demo_trials.csv"


def default_base_dir() -> Path:
    return project_root().parent


def _normalized_path_identity(path: Path) -> str:
    return str(path.expanduser().resolve()).lower()


def _validate_report_paths(
    parser: argparse.ArgumentParser,
    *,
    input_path: Path,
    truthcert_input: Path | None,
    output_path: Path | None,
    reconciliation_output: Path | None,
) -> None:
    if reconciliation_output and reconciliation_output.suffix.lower() not in SUPPORTED_RECONCILIATION_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_RECONCILIATION_SUFFIXES))
        parser.error(
            f"--reconciliation-output must end in one of: {supported}."
        )

    input_paths = [("input", input_path)]
    if truthcert_input is not None:
        input_paths.append(("truthcert input", truthcert_input))

    output_paths: list[tuple[str, Path]] = []
    if output_path is not None:
        output_paths.append(("markdown output", output_path))
    if reconciliation_output is not None:
        output_paths.append(("reconciliation output", reconciliation_output))

    input_identities = {
        _normalized_path_identity(path): f"{label} '{path}'"
        for label, path in input_paths
    }
    seen_outputs: dict[str, str] = {}
    for label, path in output_paths:
        identity = _normalized_path_identity(path)
        if identity in input_identities:
            parser.error(
                f"{label} '{path}' collides with {input_identities[identity]}."
            )
        if identity in seen_outputs:
            parser.error(
                f"{label} '{path}' collides with {seen_outputs[identity]}."
            )
        seen_outputs[identity] = f"{label} '{path}'"


def _prepare_output_parent(path: Path | None) -> None:
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="MetaFusion Lab: certainty-aware living meta-analysis."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo_parser = subparsers.add_parser("demo", help="Run the bundled demo dataset.")
    demo_parser.add_argument(
        "--base-dir",
        default=str(default_base_dir()),
        help="Directory used to discover local evidence-synthesis repositories.",
    )

    report_parser = subparsers.add_parser(
        "report", help="Generate a markdown report from a supported input file."
    )
    report_parser.add_argument("--input", required=True, help="Path to an input file.")
    report_parser.add_argument(
        "--format",
        default="csv",
        choices=["csv", "rct-extractor", "truthcert", "fused"],
        help="Input format to import before synthesis.",
    )
    report_parser.add_argument(
        "--truthcert-input",
        help="TruthCert verify-response or bundle file, or a directory of them, used when --format fused.",
    )
    report_parser.add_argument(
        "--reconciliation-output",
        help=(
            "Optional path to write structured fused-reconciliation rows as JSON, "
            "JSONL/NDJSON, or CSV. Only valid when --format fused."
        ),
    )
    report_parser.add_argument(
        "--output",
        help="Optional markdown output path. If omitted, the report is printed to stdout.",
    )
    report_parser.add_argument(
        "--base-dir",
        default=str(default_base_dir()),
        help="Directory used to discover local evidence-synthesis repositories.",
    )

    inspect_parser = subparsers.add_parser(
        "inspect-local",
        help="List local repositories that can inform adapters.",
    )
    inspect_parser.add_argument(
        "--base-dir",
        default=str(default_base_dir()),
        help="Directory to scan for known repositories.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    output_path = Path(getattr(args, "output")) if getattr(args, "output", None) else None
    reconciliation_output = (
        Path(getattr(args, "reconciliation_output"))
        if getattr(args, "reconciliation_output", None)
        else None
    )
    fused_result = None

    if args.command == "inspect-local":
        signals = discover_local_repositories(args.base_dir)
        for signal in signals:
            print(f"{signal.name}\t{signal.path}\t{signal.role}")
        return 0

    if args.command == "demo":
        records = load_trial_records(default_demo_path())
    else:
        if args.format == "fused" and not args.truthcert_input:
            parser.error("--truthcert-input is required when --format fused is used.")
        if args.reconciliation_output and args.format != "fused":
            parser.error("--reconciliation-output is only supported when --format fused is used.")
        truthcert_input = Path(args.truthcert_input) if args.truthcert_input else None
        _validate_report_paths(
            parser,
            input_path=Path(args.input),
            truthcert_input=truthcert_input,
            output_path=output_path,
            reconciliation_output=reconciliation_output,
        )
        if args.format == "fused":
            fused_result = build_fused_import_result(
                Path(args.input),
                truthcert_input,
            )
            records = list(fused_result.records)
        else:
            records = load_records_for_format(
                args.format,
                Path(args.input),
                truthcert_input_path=truthcert_input,
            )
    report = build_evidence_report(records, base_dir=args.base_dir)
    markdown = render_markdown_report(report)

    _prepare_output_parent(output_path)
    _prepare_output_parent(reconciliation_output)

    if args.command == "report" and output_path:
        output_path.write_text(markdown, encoding="utf-8")
    else:
        print(markdown)
    if args.command == "report" and reconciliation_output and fused_result is not None:
        write_reconciliation_report(
            fused_result.reconciliation_entries,
            reconciliation_output,
        )
    return 0
