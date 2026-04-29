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
    # MULTILINE only (no IGNORECASE) so the cue stays case-sensitive
    # and the value can't slurp a sentence that happens to contain
    # uppercase letters mid-line.
    r"(?m)^[ \t]*(Kontoinhaber|Kunde|Inhaber|Auftraggeber|Empfänger)\s*[:：][ \t]*([^\n\r]+)$"
)

# German Anrede-style address block, common at the top of bank
# statements:
#
#     Herrn und Frau
#     Robert Manuwald
#     Steffi Manuwald
#     Musterstraße 12
#     01099 Dresden
#
# After the salutation line we mask up to four following lines that
# look like "FirstName LastName" (one or two words, both capitalised).
# The Strasse / zip-city lines are caught by the existing _STREET_RE
# and _ZIP_CITY_RE so we stop the scan as soon as we hit a digit.
_NAME_LINE = (
    r"[ \t]*"                                  # leading space
    r"[A-ZÄÖÜ][A-Za-zäöüß\-]+"                 # FirstName
    r"(?:[ \t]+[A-ZÄÖÜ][A-Za-zäöüß\-]+){0,3}"  # 0-3 more capitalised words
    r"[ \t]*"
)
_SALUTATION_BLOCK_RE = re.compile(
    # MULTILINE only — case-sensitive on purpose so the [A-ZÄÖÜ] in
    # _NAME_LINE actually requires uppercase first letters and won't
    # capture lowercase verbs / function words.
    r"(?m)^[ \t]*(Herrn und Frau|Herr und Frau|Eheleute|Familie|Herrn|Herr|Frau)[ \t]*\r?\n"
    r"((?:" + _NAME_LINE + r"\r?\n){1,4})"
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

    def _mask_salutation_block(self, text: str) -> str:
        # German bank statements typically start with a multi-line
        # address block: "Herrn und Frau\nRobert Manuwald\nSteffi
        # Manuwald\n…". The "Herr/Frau" salutation gets kept as
        # context; the name lines (1-4 of them) are each replaced
        # with their own NAME token so the LLM still sees that there
        # are N people on this account.
        def repl(m: re.Match[str]) -> str:
            salutation = m.group(1)
            block = m.group(2)
            masked_lines: list[str] = []
            for line in block.splitlines():
                stripped = line.strip()
                if not stripped:
                    masked_lines.append(line)
                    continue
                tok = self._token_for("NAME", stripped)
                # Preserve the original line's leading whitespace.
                lead = line[: len(line) - len(line.lstrip())]
                masked_lines.append(f"{lead}{tok}")
            return f"{salutation}\n" + "\n".join(masked_lines) + ("\n" if block.endswith("\n") else "")
        return _SALUTATION_BLOCK_RE.sub(repl, text)

    # ----- public -----

    def seed_household_names(self, names: list[str]) -> None:
        """Pre-register household names so they get masked even if no
        structured pattern in the OCR text captures them. Useful for
        non-statement documents (Darlehensvertrag, Karteninhaber-
        Schreiben) where the address block sits in a different layout
        than the bank-statement format we know about."""
        for name in names:
            n = (name or "").strip()
            if n:
                self._token_for("NAME", n)

    def pseudonymize(self, text: str) -> str:
        """Run all maskers in a deterministic order. Returns the
        token-masked text safe to send to a third-party LLM.

        Order matters: salutation block first so multi-line names get
        captured before the line-based street / zip-city patterns
        consume them piecewise. After the structured maskers run, we
        decompose each captured full-name into its individual tokens
        (so "Robert Manuwald" also masks "Manuwald" and "Robert" on
        their own) and finally sweep the whole text replacing every
        known value with its token. Without the token decomposition,
        "Steffi Manuwald" would slip through whenever Steffi only
        appears in a transaction line and the address block lists
        Robert alone.
        """
        out = self._mask_iban(text)
        out = self._mask_email(out)
        out = self._mask_salutation_block(out)
        out = self._mask_holder_line(out)
        out = self._mask_inline_salutation(out)
        out = self._mask_inhaber_lines(out)
        out = self._mask_street(out)
        out = self._mask_zip_city(out)
        self._decompose_name_tokens()
        out = self._sweep_known_values(out)
        return out

    # Single-line "Herrn Robert Manuwald" / "Frau Steffi Manuwald"
    # — common in card-issuance letters and credit contracts where
    # the address block isn't a clean multi-line stack. Case-sensitive
    # on purpose: "Herr" lowercase would also match German verbs and
    # nouns ("herr" doesn't exist but case-insensitive matching brings
    # in noise via the name-token capture below).
    _INLINE_SALUTATION_RE = re.compile(
        r"\b(Herrn|Herr|Frau|Familie)[ \t]+"
        r"([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[ \t]+[A-ZÄÖÜ][a-zäöüß\-]{2,}){1,3})\b"
    )

    # Cue-driven name extraction: "Karteninhaber: X Y",
    # "Gutschriftskontoinhaber: X Y" etc. We REQUIRE an explicit
    # colon between the cue and the value so legal text patterns like
    # "Hauptantragsteller zahlt das Darlehen" don't accidentally
    # capture "zahlt das Darlehen" as a name. Cue list is also
    # narrowed to formal-document anchors that always carry a colon
    # in practice. Case-sensitive on the name part — without that,
    # IGNORECASE turned the [A-ZÄÖÜ] start into "any letter" and the
    # capture group would happily grab lowercase verbs.
    _INHABER_CUE_RE = re.compile(
        r"(?m)\b(Karteninhaber(?:/in|in)?|Kontoinhaber|"
        r"Darlehensnehmer|Kreditnehmer|"
        r"Gutschriftskontoinhaber|Mitkontoinhaber|Versicherungsnehmer)"
        r"[ \t]*[:：][ \t\r\n]*"
        r"([A-ZÄÖÜ][a-zäöüß\-]{2,}(?:[ \t]+[A-ZÄÖÜ][a-zäöüß\-]{2,}){0,2})"
    )

    def _mask_inline_salutation(self, text: str) -> str:
        def repl(m: re.Match[str]) -> str:
            salutation = m.group(1)
            name = m.group(2)
            tok = self._token_for("NAME", name)
            return f"{salutation} {tok}"
        return self._INLINE_SALUTATION_RE.sub(repl, text)

    def _mask_inhaber_lines(self, text: str) -> str:
        def repl(m: re.Match[str]) -> str:
            cue = m.group(1)
            name = m.group(2)
            tok = self._token_for("NAME", name)
            # Preserve the cue + "Inhaber:" formatting.
            full = m.group(0)
            return full.replace(name, tok)
        return self._INHABER_CUE_RE.sub(repl, text)

    def _decompose_name_tokens(self) -> None:
        """For every captured NAME_xxx full-name, register each
        sub-token (>= 4 chars, capitalised in the source value) as
        its own NAME entry. This catches the case where one family
        member is detected in the salutation block and another shows
        up only in a body sentence — the common surname masks both
        without us ever seeing the second person's full name in a
        structured location.

        We skip generic stop-words so common German words like
        "Herrn" / "Steffi" (oh wait, Steffi is exactly what we want
        to mask) — we only skip the very generic ones."""
        # Words that are NEVER personal names even when they look
        # capitalised in the source — keep this list short to avoid
        # under-masking.
        SKIP = {"Herr", "Herrn", "Frau", "Familie", "Eheleute", "und"}
        new_values: list[str] = []
        existing = set(self.reverse_map.values())
        for tok, value in list(self.reverse_map.items()):
            if not tok.startswith("NAME_"):
                continue
            for word in re.split(r"[\s,;]+", value):
                word = word.strip().rstrip(".,;:")
                if len(word) < 4:
                    continue
                if not word[0].isupper():
                    continue
                if word in SKIP:
                    continue
                if word in existing:
                    continue
                new_values.append(word)
                existing.add(word)
        for w in new_values:
            self._token_for("NAME", w)

    def _sweep_known_values(self, text: str) -> str:
        """Second pass: each value already in the reverse map gets
        replaced wherever else it appears (case-insensitive — banks
        sometimes UPPERCASE names inside booking descriptions). Order
        by descending length so a full name like "Robert Manuwald"
        is handled before its component "Robert" / "Manuwald".

        Boundary uses negative letter look-arounds rather than `\\b`.
        `\\b` won't fire between a digit and a letter (both are word
        chars to the regex engine), so a German booking line like
        "MKTNR. 1220-1108-07MANUWALD BIC/IBAN…" would slip through
        — the digit-letter run "07MANUWALD" has no `\\b` between
        them. The custom boundary treats only letters as in-word, so
        digits and punctuation count as separators while still
        preventing "Manuwaldstrasse" from being mauled mid-word.
        """
        pairs = [(v, t) for t, v in self.reverse_map.items()
                 if t.startswith(("NAME_", "ADDR_"))]
        pairs.sort(key=lambda p: len(p[0]), reverse=True)
        # Letter boundary: any unicode-ish letter (Latin + German
        # umlauts) should NOT be next to a name match. Digits,
        # whitespace, punctuation are all valid neighbours.
        out = text
        for value, token in pairs:
            if not value:
                continue
            pattern = re.compile(
                r"(?<![A-Za-zÄÖÜäöüß])"
                + re.escape(value)
                + r"(?![A-Za-zÄÖÜäöüß])",
                re.IGNORECASE,
            )
            out = pattern.sub(token, out)
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
