"""Known-truth data-generating process for random-effects meta-analysis.

Generates k studies from a normal-normal RE model:
    theta_i ~ Normal(mu, tau2)            # true study effect (log-RR scale)
    yhat_i  ~ Normal(theta_i, v_i)        # observed estimate with known SE
where v_i = se_i^2 and se_i are drawn from a realistic spread.

Optional selection: one-sided significance-based selection that drops a fraction
of non-significant negative studies (a crude publication-bias mechanism), used to
probe whether the certainty machinery / pooling is robust to it.
"""
from __future__ import annotations

import numpy as np


def draw_dataset(rng, mu, tau2, k, se_low=0.10, se_high=0.45, selection=False):
    """Return (yhat, se) arrays for k studies under the RE model.

    se drawn uniform on [se_low, se_high] (typical log-RR SE range).
    If selection=True, applies one-sided favoring of significant results.
    """
    se = rng.uniform(se_low, se_high, size=k)
    theta = rng.normal(mu, np.sqrt(tau2), size=k)
    yhat = rng.normal(theta, se)

    if selection:
        # Favor studies whose estimate is "significant" (|z|>1.0) OR keep with p=0.4.
        z = yhat / se
        keep_prob = np.where(np.abs(z) > 1.0, 1.0, 0.4)
        keep = rng.uniform(size=k) < keep_prob
        if keep.sum() < 2:  # always keep at least 2
            keep[np.argsort(-np.abs(z))[:2]] = True
        yhat, se = yhat[keep], se[keep]

    return yhat, se
