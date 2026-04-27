"""Token-level pseudonymisation for bank-statement OCR text.

The goal is to send a cloud LLM enough context to classify transactions
("Lidl", "Stadtwerke München", "REWE Markt 4711") while never revealing
the user's own IBAN, address, or full account holder name. We replace
each unique identifier with a stable token (`IBAN_001`, `EMAIL_002`)
and keep a reverse map locally so we can restore real values into the
extracted JSON before it lands in SQLite.

Design constraints:

  * **Stable tokens** within one extraction run so the LLM can refer to
    the same IBAN consistently ("the user's IBAN_001 is debited and
    credited to IBAN_002"). Counter restarts on each new Pseudonymizer
    instance — never share state across statements.
  * **Conservative replacement.** Counterparty names ("Lidl GmbH",
    "Stadtwerke München") are NOT masked because they're business names
    the LLM needs to categorise the transaction. The user's own account
    holder name might appear in the address block; we mask that via the
    "Inhaber:" / "Kontoinhaber:" pattern, but we don't mask names
    elsewhere — too risky.
  * **No false-positive masking** of amounts: phone-number patterns
    overlap with EUR figures, so phone masking is intentionally OFF.
  * **Idempotent restore**: run restore on a string or a nested
    dict/list returned by the LLM; tokens get substituted back in place.

`finalize_iban_hash(token)` exposes the SHA256 of the real IBAN so the
DB layer can dedup accounts across statements without ever holding the
plaintext IBAN if the user opts to redact it before storage.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

# IBAN pattern: country code + 2 check digits + up to 30 alphanumerics,
# typically printed in groups of four. Whitespace inside the IBAN is
# limited to horizontal characters so the match never crosses a line
# break (otherwise "DE89... \nBIC: COKSDE33" would be captured as one
# token and the BIC would leak into the IBAN value).
_IBAN_RE = re.compile(r"\b([A-Z]{2}\d{2}(?:[ \t \.]*[A-Z\d]){10,32})\b")

# Email — straightforward.
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b")

# Address blocks: a German Strasse + house number on its own line, used
# alongside zip-code + city. We only mask when a "Straße" / "Str." token
# appears so we don't munge transaction purposes that mention street
# names ("Auftrag Hauptstraße 12"). Whitespace before the house number
# is horizontal-only so the regex doesn't span line breaks.
_STREET_RE = re.compile(r"\b([A-ZÄÖÜ][A-Za-zäöüß\-]+(?:weg|straße|strasse|str\.?|allee|platz|gasse))[ \t ]+\d+[a-zA-Z]?\b")

# 5-digit German zip + city name. Matches the most common shape; not
# essential for classification so masking is safe. Spaces between zip
# and city are horizontal-only — newlines must not be consumed,
# otherwise the next line ("IBAN: …") gets eaten as a "second city
# word" (= the false positive that flagged this fix).
_ZIP_CITY_RE = re.compile(r"\b(\d{5})[ \t ]+([A-ZÄÖÜ][A-Za-zäöüß\-]+(?:[ \t ]+[A-ZÄÖÜ][A-Za-zäöüß\-]+)?)\b")

# Account holder cue lines: "Inhaber:", "Kontoinhaber:", "Kunde:",
# "Auftraggeber:" followed by 1-4 capitalised tokens. We mask everything
# from the colon to the end of line.
_HOLDER_LINE_RE = re.compile(
    r"(?im)^[ \t]*(Kontoinhaber|Kunde|Inhaber|Auftraggeber|Empfänger)\s*[:：][ \t]*([^\n\r]+)$"
)


def _normalise_iban(raw: str) -> str:
    """Strip whitespace / dots and uppercase. Used for stable hashing
    and reverse-map keys."""
    return re.sub(r"[\s\.]", "", raw).upper()


def iban_hash(iban: str) -> str:
    """Stable SHA256 of a normalised IBAN — used as the dedup key in
    the accounts table even when the plaintext IBAN is never stored."""
    return hashlib.sha256(_normalise_iban(iban).encode("utf-8")).hexdigest()


class Pseudonymizer:
    """Stateful token allocator + reverse map. One instance per
    statement; do not reuse across documents."""

    def __init__(self) -> None:
        self.reverse_map: dict[str, str] = {}
        self._value_to_token: dict[tuple[str, str], str] = {}
        self._counters: dict[str, int] = {}
        # IBANs collected from the raw text so the caller can compute
        # per-IBAN hashes for dedup without re-parsing.
        self.ibans: list[str] = []

    # ----- helpers -----

    def _token_for(self, kind: str, value: str) -> str:
        """Allocate (or reuse) a token for a given kind+value. Same
        value → same token within this Pseudonymizer instance."""
        key = (kind, value)
        if key in self._value_to_token:
            return self._value_to_token[key]
        self._counters[kind] = self._counters.get(kind, 0) + 1
        token = f"{kind}_{self._counters[kind]:03d}"
        self._value_to_token[key] = token
        self.reverse_map[token] = value
        return token

    # ----- maskers -----

    def _mask_iban(self, text: str) -> str:
        def repl(m: re.Match[str]) -> str:
            normalised = _normalise_iban(m.group(0))
            # IBANs are 15-34 chars after stripping; reject obvious
            # false positives (long alphanumeric sequences).
            if not (15 <= len(normalised) <= 34):
                return m.group(0)
            if normalised not in self.ibans:
                self.ibans.append(normalised)
            return self._token_for("IBAN", normalised)
        return _IBAN_RE.sub(repl, text)

    def _mask_email(self, text: str) -> str:
        return _EMAIL_RE.sub(
            lambda m: self._token_for("EMAIL", m.group(0)), text,
        )

    def _mask_street(self, text: str) -> str:
        return _STREET_RE.sub(
            lambda m: self._token_for("ADDR", m.group(0)), text,
        )

    def _mask_zip_city(self, text: str) -> str:
        # Mask the zip + city as a single unit so the LLM still sees
        # "in ADDR_002" rather than dangling fragments.
        return _ZIP_CITY_RE.sub(
            lambda m: self._token_for("ADDR", m.group(0)), text,
        )

    def _mask_holder_line(self, text: str) -> str:
        # Replace just the value portion after "Inhaber: …" so the
        # label survives — useful context for the LLM.
        def repl(m: re.Match[str]) -> str:
            label = m.group(1)
            value = m.group(2).strip()
            if not value:
                return m.group(0)
            tok = self._token_for("NAME", value)
            return f"{label}: {tok}"
        return _HOLDER_LINE_RE.sub(repl, text)

    # ----- public -----

    def pseudonymize(self, text: str) -> str:
        """Run all maskers in a deterministic order. Returns the
        token-masked text safe to send to a third-party LLM."""
        out = self._mask_iban(text)
        out = self._mask_email(out)
        out = self._mask_holder_line(out)
        out = self._mask_street(out)
        out = self._mask_zip_city(out)
        return out

    def restore(self, value: Any) -> Any:
        """Recursively walk a string / dict / list and replace tokens
        with original values. Use after the LLM returns its JSON."""
        if isinstance(value, str):
            out = value
            # Longer tokens first so e.g. IBAN_010 doesn't get clipped
            # by an earlier IBAN_01 substitution.
            for tok in sorted(self.reverse_map, key=len, reverse=True):
                if tok in out:
                    out = out.replace(tok, self.reverse_map[tok])
            return out
        if isinstance(value, dict):
            return {k: self.restore(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.restore(v) for v in value]
        return value

    def iban_hashes(self) -> dict[str, str]:
        """For each unique IBAN seen in the source text, return
        token → SHA256 hash. Used by the DB layer to dedup accounts
        across runs without ever storing the plaintext IBAN."""
        hashes: dict[str, str] = {}
        for token, value in self.reverse_map.items():
            if token.startswith("IBAN_"):
                hashes[token] = iban_hash(value)
        return hashes

    def iban_for_token(self, token: str) -> str:
        """Reverse-lookup helper for callers that already know which
        token references the user's primary account."""
        return self.reverse_map.get(token, "")
