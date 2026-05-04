"""Postbank / Deutsche Bank layout — both share parent-bank format."""

from __future__ import annotations

import re

from ..base import ParseResult
from .generic import GenericLayout


_POSTBANK_HEADER_RE = re.compile(
    r"\b(Postbank(?:\s+(?:AG|Filialdirektion))?|"
    r"Deutsche\s+Bank(?:\s+AG)?)\b",
    re.IGNORECASE,
)


class PostbankLayout(GenericLayout):
    name = "postbank"

    def matches(self, text: str) -> float:
        if not text:
            return 0.0
        score = 0.0
        if _POSTBANK_HEADER_RE.search(text):
            score += 0.6
        return min(score, 0.92)

    def parse(self, text: str) -> ParseResult:
        result = super().parse(text)
        result.layout = self.name
        if not result.statement.bank_name:
            # Pick the right one based on the header.
            if re.search(r"\bDeutsche\s+Bank\b", text, re.IGNORECASE):
                result.statement.bank_name = "Deutsche Bank"
            else:
                result.statement.bank_name = "Postbank"
        return result
