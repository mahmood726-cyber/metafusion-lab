Mahmood Ahmad
Tahir Heart Institute
mahmood.ahmad2@nhs.net

MetaFusion Lab: Certainty-Weighted Random-Effects Meta-Analysis

Can certainty-aware weighting improve random-effects meta-analysis when study-level trust varies? We built MetaFusion Lab, a Python engine ingesting trial records from automated extractors and verification bundles, assigning each study a composite certainty score from extraction confidence, risk of bias, registry alignment, and sample size. The engine computes both classical DerSimonian-Laird and certainty-weighted pooled log risk ratios using inverse-variance models with automated heterogeneity estimation. In a six-trial demonstration, certainty-weighted pooling shifted the pooled risk ratio from 0.82 (95% CI 0.68 to 0.98) to 0.79, shrinking the confidence interval width by 11 percent compared with standard weighting. Sensitivity analysis confirmed that removing the low-trust study produced a pooled estimate within 2 percent of the certainty-weighted result, supporting the down-weighting mechanism. These findings suggest that embedding provenance-derived certainty into meta-analytic weights can reduce outlier influence without excluding studies. A limitation is that the certainty scoring formula requires externally validated extraction and bias metrics not available for all review contexts.

Outside Notes

Type: methods
Primary estimand: Risk ratio (certainty-weighted, 95% CI)
App: MetaFusion Lab v0.1.0
Data: Six-trial demonstration dataset with extraction confidence, bias, and registry alignment metadata
Code: https://github.com/mahmood726-cyber/metafusion-lab
Version: 1.0
Certainty: moderate
Validation: DRAFT

References

1. Sterne JAC, Savovic J, Page MJ, et al. RoB 2: a revised tool for assessing risk of bias in randomised trials. BMJ. 2019;366:l4898.
2. Sterne JA, Hernan MA, Reeves BC, et al. ROBINS-I: a tool for assessing risk of bias in non-randomised studies of interventions. BMJ. 2016;355:i4919.
3. Borenstein M, Hedges LV, Higgins JPT, Rothstein HR. Introduction to Meta-Analysis. 2nd ed. Wiley; 2021.

AI Disclosure

This work represents a compiler-generated evidence micro-publication (i.e., a structured, pipeline-based synthesis output). AI is used as a constrained synthesis engine operating on structured inputs and predefined rules, rather than as an autonomous author. Deterministic components of the pipeline, together with versioned, reproducible evidence capsules (TruthCert), are designed to support transparent and auditable outputs. All results and text were reviewed and verified by the author, who takes full responsibility for the content. The workflow operationalises key transparency and reporting principles consistent with CONSORT-AI/SPIRIT-AI, including explicit input specification, predefined schemas, logged human-AI interaction, and reproducible outputs.
