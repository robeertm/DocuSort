"""Ostsächsische / generic Sparkasse layout.

Real Sparkasse OCR samples from the live data show:

    Ostsächsische Sparkasse Dresden
    Kontoauszug Nr. 03/2026 vom 31.03.2026
    IBAN: DE89 3704 0044 0532 0130 00 BIC OSDDDE81XXX
    Kontoinhaber: Max Mustermann
    Buchungstag Wertstellung Verwendungszweck Betrag
    01.01.2026 01.01.2026 ANFANGSSALDO     +1.234,56
    03.01.2026 03.01.2026 SEPA-Lastschrift Stadtwerke ...     -89,00 S
    ...
    31.03.2026 ENDSALDO     +2.101,69 H

Sparkasse uses the H/S Haben/Soll markers a lot, has the second
date column ("Wertstellung"), and typically prints "Übertrag a.n.S."
between pages — a phrase we want to skip rather than treat as a
booking. Subclassing Generic with extra rules.
"""

from __future__ import annotations

import re

from ..base import ParseResult
from .generic import GenericLayout


_SPARKASSE_HEADER_RE = re.compile(
    r"\b(Ostsächsische\s+Sparkasse|Stadtsparkasse|Sparkasse\s+\w+|"
    r"Kreissparkasse|Sparkasse\b|Sparkassen-Finanzgruppe)\b",
    re.IGNORECASE,
)


class SparkasseLayout(GenericLayout):
    name = "sparkasse"

    def matches(self, text: str) -> float:
        if not text:
            return 0.0
        score = 0.0
        if _SPARKASSE_HEADER_RE.search(text):
            score += 0.6
        # Sparkasse-typical phrases that boost confidence.
        if re.search(r"\bAuszug[-\s]?Nr\.\s*\d", text, re.IGNORECASE):
            score += 0.15
        if re.search(r"\bÜbertrag\s+a\.n\.S\b", text):
            score += 0.10
        if re.search(r"\b\d+,\d{2}\s*[HS]\b", text):
            # H/S Haben/Soll markers are extremely Sparkasse-specific
            score += 0.20
        return min(score, 0.99)

    def parse(self, text: str) -> ParseResult:
        # Drop "Übertrag a.n.S." / "Übertrag von vorh. Seite" /
        # "ENDE DES AUSZUGES" before the generic parser sees them —
        # these are page-boundary chrome lines that the generic
        # heuristic doesn't recognise.
        cleaned_lines = []
        for line in text.split("\n"):
            L = line.strip()
            if (re.search(r"Übertrag\s+a\.\s*n\.\s*S", L, re.IGNORECASE)
                    or re.search(r"Übertrag\s+von\s+vorh", L, re.IGNORECASE)
                    or re.search(r"Übertrag\s+auf\s+nächste\s+Seite", L, re.IGNORECASE)
                    or re.search(r"Bitte\s+beachten\s+Sie", L, re.IGNORECASE)
                    or re.search(r"^\s*BIC\s+[A-Z0-9]+\s*$", L)
                    or re.search(r"^\s*Datum\s+der\s+Erstellung", L, re.IGNORECASE)):
                continue
            cleaned_lines.append(line)
        cleaned = "\n".join(cleaned_lines)

        result = super().parse(cleaned)
        # Tag the result with our layout name (super sets "generic").
        result.layout = self.name
        result.statement.bank_name = result.statement.bank_name or "Sparkasse"
        # Sparkasse-specific bonus: a successful saldo reconciliation
        # on this layout is worth more than on generic.
        if (result.statement.opening_balance is not None
                and result.statement.closing_balance is not None
                and result.statement.transactions):
            delta = (result.statement.closing_balance
                     - result.statement.opening_balance)
            tx_sum = sum(t.amount for t in result.statement.transactions)
            if abs(tx_sum - delta) < 1.0:
                result.confidence = min(0.99, result.confidence + 0.05)
        return result
