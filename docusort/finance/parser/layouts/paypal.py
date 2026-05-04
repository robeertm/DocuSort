"""PayPal "Kontoauszug" layout.

PayPal statements look quite different from a bank's:

    PayPal-Kontoauszug 02.2026
    Inhaber: Max Mustermann
    05.02.2026  An REWE digital GmbH (lebensmittel-online)   -67,89  EUR
    10.02.2026  Von Klaus Schmidt (Privat) Rückzahlung       +50,00  EUR

Often there's no IBAN, no opening/closing balance — just a date
column, a counterparty + memo, and a signed amount with EUR suffix.
"""

from __future__ import annotations

import re

from .generic import GenericLayout


_PAYPAL_HEADER_RE = re.compile(
    r"\b(PayPal[-\s]?Kontoauszug|PayPal\s+Statement|PayPal\s+\(Europe\))\b",
    re.IGNORECASE,
)
_PAYPAL_ROW_PREFIX_RE = re.compile(
    r"^\d{1,2}\.\d{1,2}\.\d{2,4}\s+(?:An|Von|To|From)\s",
    re.IGNORECASE,
)


class PayPalLayout(GenericLayout):
    name = "paypal"

    def matches(self, text: str) -> float:
        if not text:
            return 0.0
        score = 0.0
        if _PAYPAL_HEADER_RE.search(text):
            score += 0.7
        if _PAYPAL_ROW_PREFIX_RE.search(text):
            score += 0.15
        if re.search(r"\bEUR\b", text) and re.search(r"\d+,\d{2}\s*EUR", text):
            score += 0.05
        return min(score, 0.95)
