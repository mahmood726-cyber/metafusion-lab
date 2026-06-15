"""Coverage harness: metafusion-lab's pooling (DL + z=1.96 Wald) vs REML + HKSJ.

We feed both estimators the SAME known-truth datasets and measure the empirical
coverage of the true mu by each method's 95% CI. The repo engine works on the
log-RR scale and reports CIs on the RR scale; we convert back to the log scale
(symmetric on log scale: log(ci_low_rr), log(ci_high_rr)) to compare to true mu.
"""
from __future__ import annotations

import numpy as np
from scipy.stats import t

from dgp import draw_dataset
from engine import TrialRecord, summarize_meta_analysis


# ---------------------------------------------------------------------------
# Reference estimators.
# ---------------------------------------------------------------------------
def dl_tau2(y, v):
    """DerSimonian-Laird tau2 (same method the repo uses)."""
    k = len(y)
    if k < 2:
        return 0.0
    w0 = 1.0 / v
    mubar = np.sum(w0 * y) / np.sum(w0)
    q = np.sum(w0 * (y - mubar) ** 2)
    c = np.sum(w0) - np.sum(w0**2) / np.sum(w0)
    if c <= 0:
        return 0.0
    return max((q - (k - 1)) / c, 0.0)


def reml_tau2(y, v, iters=200):
    """REML tau2 via fixed-point iteration (Viechtbauer 2005), clamped >=0."""
    k = len(y)
    if k < 2:
        return 0.0
    tau2 = max(dl_tau2(y, v), 1e-6)  # warm start
    for _ in range(iters):
        w = 1.0 / (v + tau2)
        sw = np.sum(w)
        mu = np.sum(w * y) / sw
        num = np.sum(w**2 * ((y - mu) ** 2 - v)) + np.sum(w) / sw  # = sum w^2(y-mu)^2 - sum w^2 v + 1
        # standard REML update: tau2_new = [sum w^2{(y-mu)^2 - v} + 1/sum w * ... ] / sum w^2
        new = (np.sum(w**2 * ((y - mu) ** 2 - v)) + (np.sum(w**2) / sw)) / np.sum(w**2)
        new = max(new, 0.0)
        if abs(new - tau2) < 1e-9:
            tau2 = new
            break
        tau2 = new
    return tau2


def pool_hksj(y, v, tau2):
    """Random-effects point estimate with Knapp-Hartung (HKSJ) CI, t_{k-1}."""
    k = len(y)
    w = 1.0 / (v + tau2)
    mu = np.sum(w * y) / np.sum(w)
    q = np.sum(w * (y - mu) ** 2) / (k - 1)
    q = max(q, 1.0)  # HKSJ floor (advanced-stats rule): never narrow below Wald
    se_hksj = np.sqrt(q / np.sum(w))
    tcrit = t.ppf(0.975, k - 1)
    return mu, mu - tcrit * se_hksj, mu + tcrit * se_hksj, tau2


def pool_dl_hksj(y, v):
    return pool_hksj(y, v, dl_tau2(y, v))


def pool_reml_hksj(y, v):
    return pool_hksj(y, v, reml_tau2(y, v))


# ---------------------------------------------------------------------------
# Repo estimator wrapper: build TrialRecords, call the repo, read log-scale CI.
# ---------------------------------------------------------------------------
def pool_repo(y, v):
    se = np.sqrt(v)
    records = [
        TrialRecord(
            study_id=f"S{i}",
            year=2000 + i,
            comparison="A vs B",
            outcome="mortality",
            effect_log_rr=float(y[i]),
            standard_error=float(se[i]),
        )
        for i in range(len(y))
    ]
    summ = summarize_meta_analysis(records, label="mc")
    mu_hat = summ.pooled_log_rr
    lo = np.log(summ.ci_low_rr)
    hi = np.log(summ.ci_high_rr)
    return mu_hat, lo, hi, summ.tau2


# ---------------------------------------------------------------------------
# Monte Carlo coverage.
# ---------------------------------------------------------------------------
def run_coverage(mu=0.0, tau2=0.10, k=6, n_sims=3000, selection=False, seed=12345):
    rng = np.random.default_rng(seed)
    cov_repo = cov_dlh = cov_remlh = 0
    bias_repo = 0.0
    width_repo = width_dlh = width_remlh = 0.0
    n = 0
    for _ in range(n_sims):
        y, v = draw_dataset(rng, mu, tau2, k, selection=selection)
        if len(y) < 2:
            continue
        n += 1
        m_r, lo_r, hi_r, _ = pool_repo(y, v)
        _, lo_d, hi_d, _ = pool_dl_hksj(y, v)
        _, lo_e, hi_e, _ = pool_reml_hksj(y, v)
        cov_repo += int(lo_r <= mu <= hi_r)
        cov_dlh += int(lo_d <= mu <= hi_d)
        cov_remlh += int(lo_e <= mu <= hi_e)
        bias_repo += (m_r - mu)
        width_repo += (hi_r - lo_r)
        width_dlh += (hi_d - lo_d)
        width_remlh += (hi_e - lo_e)
    return {
        "n": n,
        "mu": mu, "tau2": tau2, "k": k, "selection": selection,
        "cov_repo": cov_repo / n,
        "cov_dlh": cov_dlh / n,
        "cov_remlh": cov_remlh / n,
        "bias_repo": bias_repo / n,
        "width_repo": width_repo / n,
        "width_dlh": width_dlh / n,
        "width_remlh": width_remlh / n,
    }


def mc_se(p, n):
    return np.sqrt(p * (1 - p) / n)


if __name__ == "__main__":
    print("metafusion-lab pooling coverage vs known truth (target 0.95)")
    print("repo = DL tau2 + z=1.96 Wald (the engine) | DL+HKSJ | REML+HKSJ\n")
    print(f"{'scenario':<26}{'k':>3}{'cov_repo':>10}{'cov_DLH':>9}{'cov_REMLH':>11}"
          f"{'w_repo':>8}{'w_DLH':>8}")
    scenarios = [
        dict(mu=0.0, tau2=0.10, k=4),
        dict(mu=-0.3, tau2=0.10, k=5),
        dict(mu=0.0, tau2=0.20, k=6),
        dict(mu=-0.3, tau2=0.15, k=8),
        dict(mu=0.0, tau2=0.10, k=20),
        dict(mu=-0.3, tau2=0.15, k=6, selection=True),
    ]
    for s in scenarios:
        r = run_coverage(**s, n_sims=4000)
        tag = f"mu={s['mu']},t2={s['tau2']}" + (",sel" if s.get("selection") else "")
        print(f"{tag:<26}{r['k']:>3}{r['cov_repo']:>10.3f}{r['cov_dlh']:>9.3f}"
              f"{r['cov_remlh']:>11.3f}{r['width_repo']:>8.3f}{r['width_dlh']:>8.3f}")
