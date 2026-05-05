"""Date and IBAN helpers shared by the CSV importer.

Self-contained, no imports from the (now-removed) statement
extractor / parser modules. Kept tiny on purpose so the import
graph stays clean.
"""

from __future__ import annotations

import hashlib
import re


_ISO_RE   = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DE_RE    = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
_DE2_RE   = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{2})$")
_SLASH_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})$")


def normalise_date(raw: str) -> str:
    """Coerce a date string into ISO YYYY-MM-DD.

    Accepts ISO, German DD.MM.YYYY (or DD.MM.YY with a 70-cutoff for
    century rollover), and YYYY/MM/DD. Returns "" when the input
    can't be parsed — callers decide whether to skip the row or
    flag it.
    """
    if not raw:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    s = s.replace(" ", "")[:12]

    def _validate(y: int, mo: int, d: int) -> str:
        if not (1900 <= y <= 2100):
            return ""
        if not (1 <= mo <= 12):
            return ""
        if not (1 <= d <= 31):
            return ""
        return f"{y:04d}-{mo:02d}-{d:02d}"

    m = _ISO_RE.match(s)
    if m:
        y, mo, d = m.groups()
        return _validate(int(y), int(mo), int(d))
    m = _SLASH_RE.match(s)
    if m:
        y, mo, d = m.groups()
        return _validate(int(y), int(mo), int(d))
    m = _DE_RE.match(s)
    if m:
        d, mo, y = m.groups()
        return _validate(int(y), int(mo), int(d))
    m = _DE2_RE.match(s)
    if m:
        d, mo, yy = m.groups()
        y = 2000 + int(yy) if int(yy) < 70 else 1900 + int(yy)
        return _validate(y, int(mo), int(d))
    return ""


_IBAN_STRIP_RE = re.compile(r"\s+")


def normalise_iban(iban: str) -> str:
    """Strip whitespace, upper-case. Returns '' when input is empty."""
    if not iban:
        return ""
    return _IBAN_STRIP_RE.sub("", str(iban).strip()).upper()


def iban_hash(iban: str) -> str:
    """SHA256 of the normalised IBAN — used as the dedup key for the
    accounts table. Returns '' for empty input so callers can skip
    account creation cleanly."""
    n = normalise_iban(iban)
    if not n:
        return ""
    return hashlib.sha256(n.encode("utf-8")).hexdigest()
