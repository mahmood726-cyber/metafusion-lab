"""Thin re-export of metafusion-lab's pooling engine for truth-recovery testing.

The pure pooling functions live in src/metafusion_lab/analysis.py. We import them
VERBATIM (no copy) and only add a sys.path shim so the harness can run from the
truth-recovery/ directory without installing the package.
"""
from __future__ import annotations

import os
import sys

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from metafusion_lab.analysis import (  # noqa: E402
    estimate_tau2,
    summarize_meta_analysis,
)
from metafusion_lab.models import MetaSummary, TrialRecord  # noqa: E402

__all__ = [
    "estimate_tau2",
    "summarize_meta_analysis",
    "MetaSummary",
    "TrialRecord",
]
