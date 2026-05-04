"""ING (formerly ING-DiBa) layout."""

from __future__ import annotations

import re

from ..base import ParseResult
from .generic import GenericLayout


_ING_HEADER_RE = re.compile(
    r"\b(ING(?:[-\s]?DiBa)?(?:\s+AG)?|ING\s+Bank)\b",
    re.IGNORECASE,
)


class INGLayout(GenericLayout):
    name = "ing"

    def matches(self, text: str) -> float:
        if not text:
            return 0.0
        score = 0.0
        if _ING_HEADER_RE.search(text):
            score += 0.6
        if re.search(r"\b(Girokonto|Tagesgeld|Extra-Konto)\b", text, re.IGNORECASE):
            score += 0.10
        if re.search(r"\bIBAN\s*:\s*DE\d{2}\s*5001\s*0517", text):  # ING-DiBa BLZ
            score += 0.20
        return min(score, 0.95)

    def parse(self, text: str) -> ParseResult:
        result = super().parse(text)
        result.layout = self.name
        result.statement.bank_name = result.statement.bank_name or "ING"
        return result
