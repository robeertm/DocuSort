"""Layout registry + detection.

Each `*.py` here exposes a Layout instance. `detect_layout(text)`
asks every layout to score itself against the OCR text and returns
the highest-scoring one, falling back to `generic` when nothing
beats it confidently.
"""

from __future__ import annotations

import logging

from ..base import Layout
from .commerzbank import CommerzbankLayout
from .dkb import DKBLayout
from .generic import GenericLayout
from .ing import INGLayout
from .paypal import PayPalLayout
from .postbank import PostbankLayout
from .sparkasse import SparkasseLayout
from .volksbank import VolksbankLayout

logger = logging.getLogger("docusort.finance.parser")


_GENERIC = GenericLayout()
# Order matters mildly: when two layouts return identical scores
# (rare), the first wins. We put the most-specific / least-likely-
# to-collide layouts first so they can outrank the catch-alls.
_LAYOUTS: list[Layout] = [
    SparkasseLayout(),
    DKBLayout(),
    PayPalLayout(),
    INGLayout(),
    CommerzbankLayout(),
    PostbankLayout(),
    VolksbankLayout(),
]


def detect_layout(text: str) -> Layout:
    """Pick the best-fitting layout for `text`. Returns Generic when
    no layout matches > 0.5 confidence."""
    best_score = 0.0
    best_layout: Layout | None = None
    for L in _LAYOUTS:
        score = L.matches(text)
        if score > best_score:
            best_score = score
            best_layout = L
    if best_layout is not None and best_score >= 0.5:
        logger.debug("Layout detection: %s @ %.2f", best_layout.name, best_score)
        return best_layout
    return _GENERIC


def get_layout(hint: str) -> Layout | None:
    """Look up a layout by its canonical `name` (case-insensitive
    substring match against the hint)."""
    h = (hint or "").strip().lower()
    if not h:
        return None
    for L in _LAYOUTS:
        if L.name in h or h in L.name:
            return L
    return None


__all__ = ["detect_layout", "get_layout"]
