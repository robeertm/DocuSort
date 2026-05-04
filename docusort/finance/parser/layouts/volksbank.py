"""Volksbank / Raiffeisenbank layout.

Generic Volksbanken-Raiffeisenbanken-Verbund statements share a
similar shape to Sparkasse but use different balance synonyms
("Alter Kontostand" / "Neuer Kontostand" instead of "Anfangs-/
Endsaldo") and tend to print the IBAN in a less spaced format.
"""

from __future__ import annotations

import re

from ..base import ParseResult
from .generic import GenericLayout


_VOLKSBANK_HEADER_RE = re.compile(
    r"\b(Volksbank|Raiffeisenbank|Volks-\s*und\s+Raiffeisenbank|"
    r"VR-Bank|VR\s+Bank|Genossenschaftsbank|GENO\s*BANK)\b",
    re.IGNORECASE,
)


class VolksbankLayout(GenericLayout):
    name = "volksbank"

    def matches(self, text: str) -> float:
        if not text:
            return 0.0
        score = 0.0
        if _VOLKSBANK_HEADER_RE.search(text):
            score += 0.6
        if re.search(r"\b(alter|neuer)\s+Kontostand\b", text, re.IGNORECASE):
            score += 0.15
        if re.search(r"\b(SEPA|Lastschrift|Gutschrift)\b", text):
            score += 0.05
        return min(score, 0.96)

    def parse(self, text: str) -> ParseResult:
        result = super().parse(text)
        result.layout = self.name
        result.statement.bank_name = result.statement.bank_name or "Volksbank"
        return result
