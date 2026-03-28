# MetaFusion Lab

MetaFusion Lab is a new project at `C:\Users\user\MetaFusion-Lab` designed to combine the strongest ideas already present in this workspace into one certainty-aware living meta-analysis engine.

The gap across most meta-analysis tooling is structural: one system extracts studies, another pools them, another publishes dashboards, and another evaluates trust. MetaFusion Lab starts from a different assumption. Every estimate should carry its provenance, extraction confidence, cross-source agreement, and bias signal all the way into the pooled result.

## Why this could matter

If this project grows beyond the MVP, it could shift meta-analysis from static spreadsheets and one-off scripts to an evidence operating system:

- `Extraction-aware`: ingest structured trial data from automated or manual extractors.
- `Certainty-aware`: score each study before it influences a pooled estimate.
- `Living`: rerun synthesis every time evidence changes.
- `Traceable`: keep a ledger for why a study was trusted, down-weighted, or flagged.

## Local inspiration

This scaffold is intentionally shaped by projects already present in `C:\Users\user`:

- `truthcert`: verified field extraction and agreement-aware confidence.
- `rct-extractor-v2`: validated MA-record ingestion from extracted trials.
- `living-meta`: inspiration for living evidence updates and presentation.
- `ipd_qma_project`: inspiration for advanced quantitative meta-analysis workflows.
- `llm-meta-analysis`: inspiration for model-assisted synthesis and narrative generation.

The current code now imports the stable file outputs from those repositories instead of copying their internals into this project.

## MVP shipped here

This repository includes a working Python MVP with:

- a trial record schema
- a certainty ledger
- classical random-effects pooling
- certainty-adjusted random-effects pooling
- local repository discovery for the key meta-analysis projects already on this machine
- a demo dataset that shows how a low-trust outlier changes the pooled result
- direct import of `rct-extractor-v2` MA-contract `JSONL`
- direct import of TruthCert verification responses or saved bundles
- direct fusion of `rct-extractor-v2` MA records with matching TruthCert verification

## Project structure

```text
MetaFusion-Lab/
  data/demo_trials.csv
  src/metafusion_lab/
    adapters.py
    analysis.py
    cli.py
    io.py
    models.py
    pipeline.py
    scoring.py
  tests/test_adapters.py
  tests/test_pipeline.py
```

## Quick start

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab demo
```

Generate a report file:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab report --format csv --input data/demo_trials.csv --output report.md
```

Inspect which local repositories can be connected next:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab inspect-local --base-dir /mnt/c/Users/user
```

## Upstream adapters

Import validated `rct-extractor-v2` MA records:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab report \
  --format rct-extractor \
  --input /mnt/c/Users/user/rct-extractor-v2/output/external_all_validated_augmented_v3_deep_pdf_only_advfix_ma_records_validated.jsonl
```

You can also point `--input` at a directory of validated `rct-extractor-v2` exports:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab report \
  --format rct-extractor \
  --input /mnt/c/Users/user/rct-extractor-v2/output/batches/
```

When a directory is provided for `rct-extractor`, MetaFusion Lab scans it recursively for `.json`, `.jsonl`, and `.ndjson` MA-record exports and skips unrelated JSON metadata or manifest files that do not conform to the MA-record schema.

Import a saved TruthCert verify response or bundle:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab report \
  --format truthcert \
  --input /mnt/c/Users/user/truthcert/saved_verify_response.json
```

For TruthCert, the strongest downstream contract is the saved verify response with `bundle` and `fieldAgreements`. A `SHIPPED` bundle should be treated as verified; non-`SHIPPED` bundles are imported as exploratory evidence.

Fuse validated `rct-extractor-v2` rows with TruthCert verification:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab report \
  --format fused \
  --input /mnt/c/Users/user/rct-extractor-v2/output/external_all_validated_augmented_v3_deep_pdf_only_advfix_ma_records_validated.jsonl \
  --truthcert-input /mnt/c/Users/user/truthcert/saved_verify_response.json
```

You can also point `--truthcert-input` at a directory of exported TruthCert verify responses:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab report \
  --format fused \
  --input /mnt/c/Users/user/rct-extractor-v2/output/external_all_validated_augmented_v3_deep_pdf_only_advfix_ma_records_validated.jsonl \
  --truthcert-input /mnt/c/Users/user/truthcert/exports/
```

You can run fused imports from batch directories on both sides:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab report \
  --format fused \
  --input /mnt/c/Users/user/rct-extractor-v2/output/batches/ \
  --truthcert-input /mnt/c/Users/user/truthcert/exports/
```

The fused mode keeps the `rct-extractor-v2` MA record as the effect-estimate base layer, then overlays matching TruthCert confidence, agreement, and sample-size evidence using study and outcome keys. When directories are provided, MetaFusion Lab scans them recursively for supported record files and ignores unrelated JSON metadata or manifest files that are not valid MA rows or TruthCert bundles.

Reconciliation is intentionally strict:

- exact study+outcome matches with major estimate disagreement are heavily downgraded and marked for manual adjudication
- study-only fallback matches are only allowed when the outcome labels are still plausibly compatible
- study-only fallback matches with major disagreement are rejected instead of silently merged

You can also export a structured reconciliation ledger for review, audit, or downstream adjudication:

```bash
cd /mnt/c/Users/user/MetaFusion-Lab
PYTHONPATH=src python3 -m metafusion_lab report \
  --format fused \
  --input /mnt/c/Users/user/rct-extractor-v2/output/external_all_validated_augmented_v3_deep_pdf_only_advfix_ma_records_validated.jsonl \
  --truthcert-input /mnt/c/Users/user/truthcert/exports/ \
  --reconciliation-output outputs/reconciliation.jsonl
```

The reconciliation export supports `.json`, `.jsonl` or `.ndjson`, and `.csv`. Each row records the MA study/outcome when one exists, the `match_status`, action taken, numeric gap fields, and the originating TruthCert file reference. Unused TruthCert overlays are exported as review rows with blank MA-study fields instead of being silently dropped.

## Next steps

The next meaningful milestones are:

1. Support arm-level data, time-to-event outcomes, and network meta-analysis.
2. Add a web dashboard for living evidence snapshots.
3. Persist the evidence ledger as versioned JSON or SQLite.
4. Compare classical, certainty-weighted, and human-adjudicated pooled results across real review datasets.
