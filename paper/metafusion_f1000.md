# MetaFusion Lab: A Certainty-Aware Living Meta-Analysis Engine with Provenance Tracking

## Authors

Mahmood Ahmad^1

^1 Royal Free Hospital, London, United Kingdom

Correspondence: mahmood.ahmad2@nhs.net | ORCID: 0009-0003-7781-4478

---

## Abstract

Meta-analysis tooling is fragmented: extraction, pooling, bias assessment, and reporting exist as disconnected systems. MetaFusion Lab is a Python framework that integrates these into a single certainty-aware pipeline where every pooled estimate carries its provenance, extraction confidence, cross-source agreement score, and bias signal. The framework ingests structured trial data from multiple extractors (automated or manual), scores each study's certainty before it influences the pooled estimate, and maintains a ledger tracking why each study was trusted, down-weighted, or flagged. The current MVP (1,675 lines) implements adapter-based data ingestion from ClinicalTrials.gov and TruthCert-verified extractions, certainty-weighted random-effects pooling, and reproducibility manifests. Source code is available at https://github.com/mahmood726-cyber/metafusion-lab.

## 1. Introduction

Standard meta-analysis workflows treat extraction and synthesis as separate stages. A study's effect size enters the pooling model with equal standing regardless of whether it was extracted from a well-structured results table or inferred from a figure caption. MetaFusion Lab challenges this by propagating extraction uncertainty into the statistical model.

## 2. Implementation

The framework consists of 8 Python modules: adapters (1,144 lines, ingesting from ClinicalTrials.gov, TruthCert bundles, and CSV), analysis (pooling with certainty weighting), scoring (study-level quality assessment), pipeline (orchestration), models (data classes), IO (export), and CLI (command-line interface). Each study carries a CertaintyScore object that influences its weight in the random-effects model.

## 3. Availability

Python 3.11+, pip-installable. Source code: https://github.com/mahmood726-cyber/metafusion-lab. License: MIT.

## Funding
None.
