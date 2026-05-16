# E156 Protocol — `MetaFusion-Lab`

This repository is the source code and dashboard backing an E156 micro-paper on the [E156 Student Board](https://mahmood726-cyber.github.io/e156/students.html).

---

## `[94]` MetaFusion Lab: Certainty-Weighted Random-Effects Meta-Analysis

**Type:** methods  |  ESTIMAND: Risk ratio (certainty-weighted, 95% CI)  
**Data:** Six-trial demonstration dataset with extraction confidence, bias, and registry alignment metadata

### 156-word body

Can certainty-aware weighting improve random-effects meta-analysis when study-level trust varies? We built MetaFusion Lab, a Python engine ingesting trial records from automated extractors and verification bundles, assigning each study a composite certainty score from extraction confidence, risk of bias, registry alignment, and sample size. The engine computes both classical DerSimonian-Laird and certainty-weighted pooled log risk ratios using inverse-variance models with automated heterogeneity estimation. In a six-trial demonstration, certainty-weighted pooling shifted the pooled risk ratio from 0.82 (95% CI 0.68 to 0.98) to 0.79, shrinking the confidence interval width by 11 percent compared with standard weighting. Sensitivity analysis confirmed that removing the low-trust study produced a pooled estimate within 2 percent of the certainty-weighted result, supporting the down-weighting mechanism. These findings suggest that embedding provenance-derived certainty into meta-analytic weights can reduce outlier influence without excluding studies. A limitation is that the certainty scoring formula requires externally validated extraction and bias metrics not available for all review contexts.

### Submission metadata

```
Corresponding author: Mahmood Ahmad <mahmood.ahmad2@nhs.net>
ORCID: 0000-0001-9107-3704
Affiliation: Tahir Heart Institute, Rabwah, Pakistan

Links:
  Code:      https://github.com/mahmood726-cyber/MetaFusion-Lab
  Protocol:  https://github.com/mahmood726-cyber/MetaFusion-Lab/blob/main/E156-PROTOCOL.md
  Dashboard: https://mahmood726-cyber.github.io/metafusion-lab/

References (topic pack: risk of bias):
  1. Sterne JAC, Savović J, Page MJ, et al. 2019. RoB 2: a revised tool for assessing risk of bias in randomised trials. BMJ. 366:l4898. doi:10.1136/bmj.l4898
  2. Sterne JA, Hernán MA, Reeves BC, et al. 2016. ROBINS-I: a tool for assessing risk of bias in non-randomised studies of interventions. BMJ. 355:i4919. doi:10.1136/bmj.i4919

Data availability: No patient-level data used. Analysis derived exclusively
  from publicly available aggregate records. All source identifiers are in
  the protocol document linked above.

Ethics: Not required. Study uses only publicly available aggregate data; no
  human participants; no patient-identifiable information; no individual-
  participant data. No institutional review board approval sought or required
  under standard research-ethics guidelines for secondary methodological
  research on published literature.

Funding: None.

Competing interests: MA serves on the editorial board of Synthēsis (the
  target journal); MA had no role in editorial decisions on this
  manuscript, which was handled by an independent editor of the journal.

Author contributions (CRediT):
  [STUDENT REWRITER, first author] — Writing – original draft, Writing –
    review & editing, Validation.
  [SUPERVISING FACULTY, last/senior author] — Supervision, Validation,
    Writing – review & editing.
  Mahmood Ahmad (middle author, NOT first or last) — Conceptualization,
    Methodology, Software, Data curation, Formal analysis, Resources.

AI disclosure: Computational tooling (including AI-assisted coding via
  Claude Code [Anthropic]) was used to develop analysis scripts and assist
  with data extraction. The final manuscript was human-written, reviewed,
  and approved by the author; the submitted text is not AI-generated. All
  quantitative claims were verified against source data; cross-validation
  was performed where applicable. The author retains full responsibility for
  the final content.

Preprint: Not preprinted.

Reporting checklist: PRISMA 2020 (methods-paper variant — reports on review corpus).

Target journal: ◆ Synthēsis (https://www.synthesis-medicine.org/index.php/journal)
  Section: Methods Note — submit the 156-word E156 body verbatim as the main text.
  The journal caps main text at ≤400 words; E156's 156-word, 7-sentence
  contract sits well inside that ceiling. Do NOT pad to 400 — the
  micro-paper length is the point of the format.

Manuscript license: CC-BY-4.0.
Code license: MIT.

SUBMITTED: [ ]
```


---

_Auto-generated from the workbook by `C:/E156/scripts/create_missing_protocols.py`. If something is wrong, edit `rewrite-workbook.txt` and re-run the script — it will overwrite this file via the GitHub API._