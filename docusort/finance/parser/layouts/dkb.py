"""Deutsche Kreditbank (DKB) statement layout.

Two flavours appear in real data:
  - DKB Cash (Girokonto)         — IBAN-based with table layout
  - DKB Visa Card                — credit card statement

Both share a "DKB AG" or "Deutsche Kreditbank" header and use the
"Saldo Vormonat" / "Neuer Saldo" balance synonyms.
"""

from __future__ import annotations

import re

from .generic import GenericLayout


_DKB_HEADER_RE = re.compile(
    r"\b(DKB[-\s]?AG|Deutsche\s+Kreditbank|DKB[-\s]?Cash|DKB\s+Visa)\b",
    re.IGNORECASE,
)
_DKB_VISA_RE = re.compile(
    r"\b(Kreditkartenabrechnung|Visa[-\s]?Card|Saldo\s+Vormonat|"
    r"Neuer\s+Saldo)\b",
    re.IGNORECASE,
)


class DKBLayout(GenericLayout):
    name = "dkb"

    def matches(self, text: str) -> float:
        if not text:
            return 0.0
        score = 0.0
        if _DKB_HEADER_RE.search(text):
            score += 0.6
        if _DKB_VISA_RE.search(text):
            score += 0.2
        # DKB rows are usually "DD.MM.YY Buchungstext  Betrag"
        # without the H/S Sparkasse markers.
        if re.search(r"\b(SEPA|Lastschrift|Gutschrift)\b", text):
            score += 0.05
        return min(score, 0.97)
