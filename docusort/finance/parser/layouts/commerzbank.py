"""Commerzbank / comdirect layout."""

from __future__ import annotations

import re

from ..base import ParseResult
from .generic import GenericLayout


_COMMERZBANK_HEADER_RE = re.compile(
    r"\b(Commerzbank(?:\s+AG)?|comdirect(?:\s+bank)?)\b",
    re.IGNORECASE,
)


class CommerzbankLayout(GenericLayout):
    name = "commerzbank"

    def matches(self, text: str) -> float:
        if not text:
            return 0.0
        score = 0.0
        if _COMMERZBANK_HEADER_RE.search(text):
            score += 0.6
        if re.search(r"\bAlter\s+Kontostand\b", text, re.IGNORECASE):
            score += 0.10
        if re.search(r"\bNeuer\s+Kontostand\b", text, re.IGNORECASE):
            score += 0.10
        return min(score, 0.95)

    def parse(self, text: str) -> ParseResult:
        result = super().parse(text)
        result.layout = self.name
        result.statement.bank_name = result.statement.bank_name or "Commerzbank"
        return result
