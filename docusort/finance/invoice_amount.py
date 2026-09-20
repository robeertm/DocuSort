"""Pull the amount an invoice asks for out of its text.

Documents carry a `due_date` since v0.36, but never an amount — so a
deadline could not be matched against an actual payment. Re-running the
KI over the archive to get one would cost money for something the text
already states, so this reads it instead.

The rule is deliberately conservative: a wrong amount would mark a bill
"paid" that was never paid, and Robert would miss it. Where the text
does not state a total plainly, we return None and the deadline keeps
nagging — a missing match is harmless, a wrong one is not.
"""

from __future__ import annotations

import re

# German invoice wording, strongest first. The label must be followed by
# the amount within a short distance — "Rechnungsbetrag 9,95 €" but also
# "Betrag: 123,24 EUR" and "Rechnungsbetrag (Guthaben) -113,05 €".
_LABELS = (
    (100, r"rechnungsbetrag"),
    (95,  r"gesamtbetrag"),
    (95,  r"zahlbetrag"),
    (90,  r"zu\s+zahlender\s+betrag"),
    (90,  r"endbetrag"),
    (85,  r"gesamtsumme"),
    (80,  r"zu\s+zahlen"),
    (75,  r"forderung"),
    (70,  r"beitrag"),
    (60,  r"\bbetrag\b"),
    (55,  r"gesamt"),
    (50,  r"\bsumme\b"),
)

# 1.234,56 or 1234,56 or 99,00 — German grouping, comma decimals.
_NUM = r"(-?\d{1,3}(?:\.\d{3})*,\d{2}|-?\d+,\d{2})"
# The currency may sit before or after the number.
_AMOUNT = rf"(?:€|EUR)?\s*{_NUM}\s*(?:€|EUR)?"

_CURRENCY_NEAR = re.compile(r"€|EUR", re.I)


def _to_float(raw: str) -> float:
    return float(raw.replace(".", "").replace(",", "."))


def find_invoice_amount(text: str) -> tuple[float, str] | None:
    """Return `(amount, evidence)` for the total this document asks for.

    `amount` is positive for "you owe this" and negative for a credit
    note; `evidence` is the snippet it was read from so the UI can show
    *why* a booking was matched instead of asserting it.
    """
    if not text:
        return None
    flat = re.sub(r"[ \t]+", " ", text)
    best: tuple[int, float, str] | None = None
    for weight, label in _LABELS:
        for m in re.finditer(rf"{label}[^\n\d\-]{{0,40}}?{_AMOUNT}", flat, re.I):
            window = m.group(0)
            # Require a currency marker somewhere in the hit: bare numbers
            # after "Summe" are just as often item counts or clause numbers.
            if not _CURRENCY_NEAR.search(window):
                continue
            try:
                value = _to_float(m.group(1))
            except ValueError:
                continue
            if value == 0:
                continue
            # A plausible household invoice. Six-figure "totals" in this
            # position are almost always insured sums, not what is owed.
            if abs(value) > 50_000:
                continue
            snippet = " ".join(window.split())[:90]
            cand = (weight, value, snippet)
            if best is None or weight > best[0]:
                best = cand
    if best is None:
        return None
    return best[1], best[2]
