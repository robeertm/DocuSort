"""Shared types + helpers for the deterministic statement parser.

Kept lightweight on purpose — the integration code wants plain dicts
to feed straight into `db.upsert_statement`, and the dataclasses
mirror the LLM-extractor's output so downstream code doesn't have to
know whether a statement came from regex or from the model.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Protocol


# ---------- Common amount / date helpers ----------------------------

# German amount tail: "-1.234,56" / "+89,00" / "1.234,56-" / "89,00 H".
# Captures: sign | digits | decimals | trailing_marker
# Trailing markers we accept: H = Haben (positive), S = Soll (negative),
# stand-alone +/- after the number, and "EUR" / "€" we just consume.
_AMOUNT_RE_STR = (
    r"(?P<sign>[-+]?)\s*"                             # leading sign
    r"(?P<int>\d{1,3}(?:[.  ]\d{3})+|\d+)"  # 1.234 or 1234
    r"(?:[,.](?P<dec>\d{1,2}))?"                       # ,56 or .56
    r"\s*(?:(?P<euro>EUR|€))?"                         # optional EUR/€
    r"\s*(?P<tail>[-+HS])?"                            # trailing marker
)
AMOUNT_RE = re.compile(_AMOUNT_RE_STR)
# Anchored to end-of-line for booking rows — most layouts put the
# amount last on the row.
AMOUNT_TAIL_RE = re.compile(r"\s" + _AMOUNT_RE_STR + r"\s*$")

# STRICT amount tail for booking rows. Always requires explicit
# cents (",dd" or ".dd"). Optional leading sign, optional trailing
# sign / H/S Haben-Soll marker, optional EUR/€ in either position.
# Reference numbers like "Kunden-Nr. 4711" don't have cents, so they
# never match.
_STRICT_AMOUNT_TAIL_RE = re.compile(
    r"(?:^|[\s\t])"
    r"(?P<core>"
        r"(?:[-+]\s*)?"                            # optional leading sign
        r"\d{1,3}(?:[.\s]\d{3})*[,.]\d{2}"  # digits with required cents
    r")"
    r"\s*(?:EUR|€)?"                                # optional EUR/€
    r"(?:\s*(?P<trail>[-+HS]))?"                    # optional trailing marker
    r"\s*(?:EUR|€)?"                                # EUR/€ either side
    r"\s*$"
)

# Date forms we see in German bank statements. ISO is the target.
DATE_DE_FULL_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b")
DATE_DE_SHORT_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})(?!\d)\b")
DATE_DE_NOYEAR_RE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(?!\d)")
DATE_ISO_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")

# IBAN — German format only for now (DE + 20 digits, possibly spaced).
IBAN_DE_RE = re.compile(r"\bDE\s*\d{2}(?:[\s ]?\d{2,4}){4,5}\b")


def parse_amount(s: str) -> float | None:
    """Parse a German-shaped amount string to float.

    Handles: thousands sep ".", decimal "," (or ".") , trailing
    sign, H/S Haben/Soll markers, optional currency suffix.
    Returns None when the string doesn't match the shape.
    """
    if s is None:
        return None
    m = AMOUNT_RE.search(s.strip())
    if not m:
        return None
    int_part = (m.group("int") or "").replace(".", "").replace(" ", "").replace(" ", "")
    dec_part = m.group("dec") or "0"
    try:
        value = float(int_part + "." + dec_part)
    except ValueError:
        return None
    sign = m.group("sign") or ""
    tail = (m.group("tail") or "").upper()
    if sign == "-" or tail == "-":
        value = -value
    elif tail == "S":  # Soll = outgoing
        value = -value
    # H or + leaves it positive; no marker → positive.
    return round(value, 2)


def parse_amount_at_end(line: str) -> tuple[float, str] | None:
    """Pull the amount off the END of `line`, return (amount, prefix)
    where prefix is everything left of the matched amount.

    Uses the STRICT amount pattern (cents required, OR sign / H/S
    marker) so reference numbers like "4711" or "002" embedded at
    the end of a continuation line don't get misread as amounts.
    Returns None when no plausible money-shaped amount lives at the
    end.
    """
    m = _STRICT_AMOUNT_TAIL_RE.search(line)
    if not m:
        return None
    matched = line[m.start():].lstrip()
    amt = parse_amount(matched)
    if amt is None:
        return None
    return amt, line[:m.start()].rstrip()


def parse_de_date(s: str, *, hint_year: int | None = None) -> str | None:
    """German date string → ISO YYYY-MM-DD.

    `hint_year` lets us resolve "DD.MM." rows (no year, common on
    multi-page statements where the year is only printed in the
    header). Returns None when nothing parseable.
    """
    s = (s or "").strip()
    m = DATE_DE_FULL_RE.search(s)
    if m:
        d, mo, y = m.groups()
        return _validate(int(y), int(mo), int(d))
    m = DATE_DE_SHORT_RE.search(s)
    if m:
        d, mo, yy = m.groups()
        y = 2000 + int(yy) if int(yy) < 70 else 1900 + int(yy)
        return _validate(y, int(mo), int(d))
    m = DATE_ISO_RE.search(s)
    if m:
        y, mo, d = m.groups()
        return _validate(int(y), int(mo), int(d))
    if hint_year is not None:
        m = DATE_DE_NOYEAR_RE.search(s)
        if m:
            d, mo = m.groups()
            return _validate(int(hint_year), int(mo), int(d))
    return None


def _validate(y: int, mo: int, d: int) -> str | None:
    if not (1900 <= y <= 2100):
        return None
    if not (1 <= mo <= 12):
        return None
    if not (1 <= d <= 31):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"


# ---------- Result dataclasses -------------------------------------


@dataclass
class ParsedTransaction:
    booking_date: str = ""
    value_date:   str = ""
    amount:       float = 0.0
    currency:     str = "EUR"
    counterparty: str = ""
    counterparty_iban: str = ""
    purpose:      str = ""
    tx_type:      str = ""
    category:     str = ""

    def as_dict(self) -> dict:
        return {
            "booking_date": self.booking_date,
            "value_date":   self.value_date,
            "amount":       self.amount,
            "currency":     self.currency,
            "counterparty": self.counterparty,
            "counterparty_iban": self.counterparty_iban,
            "purpose":      self.purpose,
            "tx_type":      self.tx_type,
            "category":     self.category,
        }


@dataclass
class ParsedStatement:
    bank_name:       str = ""
    iban:            str = ""
    iban_last4:      str = ""
    account_holder:  str = ""
    period_start:    str = ""
    period_end:      str = ""
    statement_no:    str = ""
    opening_balance: float | None = None
    closing_balance: float | None = None
    currency:        str = "EUR"
    transactions:    list[ParsedTransaction] = field(default_factory=list)


@dataclass
class ParseResult:
    statement:   ParsedStatement
    confidence:  float            # 0..1
    layout:      str              # "sparkasse" | "dkb" | "generic" | "empty"
    warnings:    list[str] = field(default_factory=list)
    notes:       list[str] = field(default_factory=list)

    @property
    def saldo_consistent(self) -> bool:
        """True if opening + Σtx ≈ closing — a key trust signal."""
        s = self.statement
        if s.opening_balance is None or s.closing_balance is None:
            return False
        if not s.transactions:
            return abs(s.opening_balance - s.closing_balance) < 0.01
        delta = s.closing_balance - s.opening_balance
        tx_sum = sum(t.amount for t in s.transactions)
        return abs(tx_sum - delta) < 1.0


# ---------- Page boundary handling ---------------------------------

# Lines that signal the start of a new page in OCR output. We use
# them to skip page-header repetitions (bank name, IBAN, "Seite X
# von Y", column labels). Each layout adds its own markers; this
# base set covers the universally-printed ones.
_PAGE_FOOTER_RE = re.compile(
    r"(?:^|\b)(?:Seite\s+\d+\s+von\s+\d+|Page\s+\d+\s+of\s+\d+)\b",
    re.IGNORECASE,
)


def normalize_lines(text: str) -> list[str]:
    """Split OCR text into clean lines: strip whitespace, drop empty,
    join obvious continuation lines (lines without enough structure
    to be a booking row often belong to the previous one's purpose).

    We DON'T reflow paragraphs — booking rows are line-anchored on
    purpose."""
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: list[str] = []
    for line in raw:
        # Replace NBSPs / thin-spaces with normal space so amount
        # regex doesn't have to worry about them.
        line = (line.replace(" ", " ").replace(" ", " ")
                    .replace(" ", " "))
        line = re.sub(r"[ \t]+", " ", line).strip()
        if not line:
            continue
        out.append(line)
    return out


def is_page_chrome(line: str) -> bool:
    """Lines that should be ignored regardless of layout — page
    numbers, "ENDE DES AUSZUGS"-style markers, etc."""
    if _PAGE_FOOTER_RE.search(line):
        return True
    L = line.strip()
    if not L:
        return True
    if L.lower() in {"ende des auszugs", "ende auszug", "fortsetzung folgt"}:
        return True
    return False


# ---------- Layout protocol -----------------------------------------


class Layout(Protocol):
    name: str

    def matches(self, text: str) -> float:
        """Confidence (0..1) that this layout fits the OCR text.
        Used by `detect_layout` to pick the best parser."""
        ...

    def parse(self, text: str) -> ParseResult:
        """Parse the OCR text into a `ParseResult`."""
        ...
