"""Generic German bank-statement parser.

Used when none of the bank-specific layouts match. Recognises the
most common booking-row shape: a leading German date, followed by
free text, ending with a signed amount. Confidence stays low so
callers can choose to fall back to the LLM extractor when this
isn't trustworthy enough.
"""

from __future__ import annotations

import re

from ..base import (
    AMOUNT_TAIL_RE, DATE_DE_FULL_RE, DATE_DE_SHORT_RE, DATE_DE_NOYEAR_RE,
    IBAN_DE_RE, ParseResult, ParsedStatement, ParsedTransaction,
    is_page_chrome, normalize_lines, parse_amount, parse_amount_at_end,
    parse_de_date,
)


# Lines that LOOK like booking rows: start with a date.
_BOOKING_PREFIX_RE = re.compile(
    r"^(\d{1,2}\.\d{1,2}\.(?:\d{2,4})?)\s"
)

# Header-only synonyms — used to lift opening / closing balance.
_OPENING_RE = re.compile(
    r"\b(anfangssaldo|alter\s+saldo|saldo\s+vortrag|saldo\s+alt|"
    r"alter\s+kontostand|übertrag\s+vom\s+vormonat|"
    r"saldo\s+vormonat|saldo\s+aus\s+vormonat|"
    r"saldo\s+vom\s+vorauszug|beginning\s+balance|"
    r"Übertrag)\b",
    re.IGNORECASE,
)
_CLOSING_RE = re.compile(
    r"\b(endsaldo|neuer\s+saldo|saldo\s+neu|neuer\s+kontostand|"
    r"endbestand|neuer\s+kontostand|new\s+balance|"
    r"ending\s+balance)\b",
    re.IGNORECASE,
)
_PERIOD_RE = re.compile(
    r"(?:Zeitraum|Buchungszeitraum|Abrechnungszeitraum|Kontoauszug)"
    r"\D{0,40}?"
    r"(\d{1,2}\.\d{1,2}\.\d{2,4})\D{0,8}?(?:bis|–|-|—)\D{0,8}?"
    r"(\d{1,2}\.\d{1,2}\.\d{2,4})",
    re.IGNORECASE,
)
_STMT_NO_RE = re.compile(
    r"(?:Auszug(?:s)?[-\s]?Nr\.?|Auszug-Nr\.|Kontoauszug\s+Nr\.?)\s*"
    r"([0-9]+(?:\s*/\s*[0-9]+)?)",
    re.IGNORECASE,
)


# Transaction-type heuristics — applied to `purpose` AFTER the row is
# parsed so the deterministic parser still tags rows the same way the
# LLM would, without an LLM call.
_TX_TYPE_RULES = (
    (re.compile(r"\bSEPA[-\s]?Lastschrift|Lastschrifteinzug\b", re.IGNORECASE),
     "lastschrift"),
    (re.compile(r"\bDauerauftrag\b", re.IGNORECASE), "dauerauftrag"),
    (re.compile(r"\bSEPA[-\s]?Überweisung|Auftrag|Überweisung\b", re.IGNORECASE),
     "ueberweisung"),
    (re.compile(r"\b(Karten|Kreditkarten|Visa|Maestro|Girocard)[a-z]*\b", re.IGNORECASE),
     "kartenzahlung"),
    (re.compile(r"\b(GAA|Bargeldauszahlung|Geldautomat)\b", re.IGNORECASE),
     "bargeld"),
    (re.compile(r"\b(Lohn|Gehalt|Tantieme|Bonus)\b", re.IGNORECASE),
     "gehalt"),
    (re.compile(r"\b(Habenzinsen|Sollzinsen|Zinsen|Dividend)\b", re.IGNORECASE),
     "zinsen"),
    (re.compile(r"\b(Kontoführung|Entgelt|Gebühr|Postgebühr)\b", re.IGNORECASE),
     "gebuehr"),
)

_CATEGORY_RULES = (
    (re.compile(r"\b(Lidl|Aldi|Rewe|Edeka|Penny|Kaufland|Norma|Tegut|Netto)\b", re.IGNORECASE),
     "lebensmittel"),
    (re.compile(r"\b(Stadtwerke|Vattenfall|EnBW|E\.ON|Vodafone|Telekom|Glasfaser|GEZ|Rundfunkbeitrag)\b", re.IGNORECASE),
     "nebenkosten"),
    (re.compile(r"\b(DB\s|VRR|VVS|BVG|MVV|Aral|Shell|Total|Esso|Tankstelle)\b", re.IGNORECASE),
     "mobilitaet"),
    (re.compile(r"\b(Allianz|HUK|Generali|DEVK|R\+V|Krankenkasse|TK|Barmer|AOK|DAK)\b", re.IGNORECASE),
     "versicherung"),
    (re.compile(r"\b(Netflix|Spotify|Apple\s|Amazon\s+Prime|Fitness|Gym|Sky\s)\b", re.IGNORECASE),
     "abonnement"),
    (re.compile(r"\b(Apotheke|Arzt|Praxis|Krankenhaus|Klinik|Rezept|Therapie)\b", re.IGNORECASE),
     "gesundheit"),
    (re.compile(r"\b(MediaMarkt|Saturn|Cyberport|Notebooksbilliger)\b", re.IGNORECASE),
     "elektronik"),
    (re.compile(r"\b(dm[-\s]?Drogerie|Rossmann|Müller\s|IKEA|Obi|Bauhaus|Hornbach)\b", re.IGNORECASE),
     "haushalt"),
    (re.compile(r"\b(H&M|Zara|Zalando|C&A|Bekleidung)\b", re.IGNORECASE),
     "bekleidung"),
    (re.compile(r"\b(Lohn|Gehalt|Tantieme|Bonus|Arbeitgeber)\b", re.IGNORECASE),
     "gehalt"),
    (re.compile(r"\b(Miete|Vermieter|Hausverwaltung|Wohnungsbau)\b", re.IGNORECASE),
     "miete"),
    (re.compile(r"\b(Übertrag|Umbuchung)\b", re.IGNORECASE), "uebertrag"),
)


def _classify_purpose(counterparty: str, purpose: str, amount: float) -> tuple[str, str]:
    """Best-effort tx_type + category from the counterparty/purpose
    text alone — same buckets the LLM uses, just deterministic."""
    blob = f"{counterparty} {purpose}"
    tx_type = ""
    for pat, t in _TX_TYPE_RULES:
        if pat.search(blob):
            tx_type = t
            break
    category = ""
    for pat, c in _CATEGORY_RULES:
        if pat.search(blob):
            category = c
            break
    # If the row isn't matched but the amount sign + tx_type is
    # already tell-tale, default sensibly:
    if not tx_type:
        tx_type = "sonstiges"
    if not category:
        category = "sonstiges"
    return tx_type, category


def _strip_iban(s: str) -> str:
    """Pull a German IBAN out of `s`, normalised to no-spaces upper."""
    m = IBAN_DE_RE.search(s)
    if not m:
        return ""
    return re.sub(r"\s+", "", m.group(0)).upper()


class GenericLayout:
    name = "generic"

    def matches(self, text: str) -> float:
        """The generic parser is always usable. We only return a low
        baseline so any specific layout outranks it."""
        if not text or not text.strip():
            return 0.0
        # Detect "this looks at all like a German bank statement".
        score = 0.0
        if IBAN_DE_RE.search(text):
            score += 0.2
        if _OPENING_RE.search(text) or _CLOSING_RE.search(text):
            score += 0.15
        if DATE_DE_FULL_RE.search(text):
            score += 0.1
        return min(score, 0.45)  # always below the layout threshold

    def parse(self, text: str) -> ParseResult:
        lines = normalize_lines(text)
        stmt = ParsedStatement()
        warnings: list[str] = []

        # -------- Header pass --------------------------------------
        full_text = "\n".join(lines[:80])
        m = _PERIOD_RE.search(full_text)
        if m:
            ps = parse_de_date(m.group(1))
            pe = parse_de_date(m.group(2))
            if ps and pe and ps > pe:
                ps, pe = pe, ps
            stmt.period_start = ps or ""
            stmt.period_end   = pe or ""
        m = _STMT_NO_RE.search(full_text)
        if m:
            stmt.statement_no = m.group(1).strip()[:64]
        iban = _strip_iban(full_text)
        if iban:
            stmt.iban = iban
            stmt.iban_last4 = iban[-4:]

        hint_year: int | None = None
        if stmt.period_end and len(stmt.period_end) >= 4:
            try:
                hint_year = int(stmt.period_end[:4])
            except ValueError:
                hint_year = None

        # -------- Balance lines ------------------------------------
        # Search the FULL text (some statements print the saldo
        # at the bottom of page 1, not in the header block).
        opening, closing = _scan_balances(text)
        stmt.opening_balance = opening
        stmt.closing_balance = closing

        # -------- Booking rows -------------------------------------
        # Group multi-line bookings: first line carries date + amount,
        # follow-up lines without a date prefix extend the purpose.
        # This is critical for real OCR — many German banks print the
        # Verwendungszweck across 2-4 lines and naive line-by-line
        # parsing would lose those continuation lines entirely.
        blocks = _group_booking_blocks(lines)
        txs: list[ParsedTransaction] = []
        for block in blocks:
            tx = _parse_booking_block(block, hint_year=hint_year)
            if tx is not None:
                txs.append(tx)

        # Skip rows that are clearly the opening/closing balance row
        # picked up by the date+amount heuristic.
        txs = [t for t in txs if not _looks_like_balance_row(t.purpose)]

        stmt.transactions = txs

        # ---- Confidence scoring -----------------------------------
        score = 0.2  # baseline
        if stmt.period_start and stmt.period_end:
            score += 0.15
        if stmt.iban:
            score += 0.1
        if stmt.opening_balance is not None:
            score += 0.15
        if stmt.closing_balance is not None:
            score += 0.15
        if txs:
            score += 0.1
            # Saldo consistency is the strongest signal we have.
            if (stmt.opening_balance is not None
                    and stmt.closing_balance is not None):
                delta = stmt.closing_balance - stmt.opening_balance
                tx_sum = sum(t.amount for t in txs)
                if abs(tx_sum - delta) < 1.0:
                    score += 0.25
        score = min(score, 0.95)

        if not txs:
            warnings.append("no booking rows recognised")
        if stmt.opening_balance is None:
            warnings.append("no opening balance found")
        if stmt.closing_balance is None:
            warnings.append("no closing balance found")

        return ParseResult(
            statement=stmt, confidence=score,
            layout=self.name, warnings=warnings,
        )


def _scan_balances(text: str) -> tuple[float | None, float | None]:
    """Walk the text looking for opening- / closing-balance lines.
    Trick: the keyword and the amount can sit on the same line OR
    the amount can be on the next line — handle both."""
    lines = normalize_lines(text)
    opening: float | None = None
    closing: float | None = None
    for i, line in enumerate(lines):
        is_open = bool(_OPENING_RE.search(line))
        is_close = bool(_CLOSING_RE.search(line))
        if not (is_open or is_close):
            continue
        # Try amount on same line first.
        amt = parse_amount_at_end(line)
        if amt is None and i + 1 < len(lines):
            # Look at the next 1-2 lines for a stand-alone amount.
            for j in (i + 1, i + 2):
                if j < len(lines):
                    cand = lines[j].strip()
                    parsed = parse_amount(cand)
                    # Only accept if the line is mostly just the amount
                    # (not another booking row).
                    if parsed is not None and len(cand) < 30 and not _BOOKING_PREFIX_RE.match(cand):
                        amt = (parsed, "")
                        break
        if amt is None:
            continue
        value = amt[0]
        if is_open and opening is None:
            opening = value
        elif is_close:
            closing = value  # latest one wins
    return opening, closing


def _group_booking_blocks(lines: list[str]) -> list[list[str]]:
    """Group OCR lines into booking blocks. A block STARTS with a
    date-prefixed line and CONTINUES until the next date-prefixed
    line or a page-chrome line. Continuation lines (no date prefix,
    not page chrome) belong to the previous booking's purpose.

    Examples (each `→` starts a new block):

      → 03.04.2026 SEPA-Lastschrift Stadtwerke München
        Mandatsreferenz 123456 Kunden-Nr. 4711
        Verwendungszweck: Strom Q2 -89,00

    The amount can appear on the FIRST line or on a CONTINUATION
    line — the block parser handles both.
    """
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if is_page_chrome(line):
            if current:
                blocks.append(current)
                current = []
            continue
        if _BOOKING_PREFIX_RE.match(line):
            if current:
                blocks.append(current)
            current = [line]
        elif current:
            # Continuation line — only attach when it doesn't look
            # like a header / footer / standalone-balance line.
            if _looks_like_balance_row(line):
                # Treat balance row as block terminator, not part of
                # the previous booking.
                blocks.append(current)
                current = []
                continue
            # Skip empty continuation marks ("|", "_", page numbers).
            stripped = line.strip()
            if not stripped or stripped in ("|", "_", "-"):
                continue
            # Don't keep continuation lines that are absurdly long
            # (footer disclaimers etc.) — > 200 chars is suspicious.
            if len(stripped) > 200:
                continue
            current.append(line)
    if current:
        blocks.append(current)
    return blocks


def _parse_booking_block(block: list[str], *, hint_year: int | None
                         ) -> ParsedTransaction | None:
    """Parse a multi-line booking block into one transaction.

    Strategy:
    1. The first line carries the booking date (always).
    2. Look for the amount at the end of the FIRST line first; if
       missing, scan continuation lines for a stand-alone amount or
       an amount at the end of a line.
    3. Concatenate the cleaned text (sans date and amount) to form
       the purpose, with the counterparty being the most prominent
       capitalised noun phrase from the first non-prefix line.
    """
    if not block:
        return None
    first = block[0]
    m = _BOOKING_PREFIX_RE.match(first)
    if not m:
        return None
    raw_date = m.group(1)
    booking_iso = parse_de_date(raw_date, hint_year=hint_year)
    if not booking_iso:
        return None

    # Pull the date(s) off the first line. Sparkasse-style: two dates
    # back-to-back ("DD.MM. DD.MM."). The second is the value date.
    first_rest = first[m.end():].strip()
    value_iso = ""
    m2 = re.match(r"(\d{1,2}\.\d{1,2}\.(?:\d{2,4})?)\s+(.*)", first_rest)
    if m2:
        v_iso = parse_de_date(m2.group(1), hint_year=hint_year)
        if v_iso:
            value_iso = v_iso
            first_rest = m2.group(2).strip()

    # Try amount on first line.
    amount: float | None = None
    purpose_segments: list[str] = []
    amt_split = parse_amount_at_end(first_rest)
    if amt_split is not None:
        amount, prefix = amt_split
        purpose_segments.append(prefix.strip())
    else:
        purpose_segments.append(first_rest)

    # Walk continuation lines. If the amount is still missing, look
    # for it at the end of each one. Otherwise just collect text.
    for cont in block[1:]:
        c = cont.strip()
        if amount is None:
            amt_split = parse_amount_at_end(c)
            if amt_split is not None:
                amount, prefix = amt_split
                if prefix.strip():
                    purpose_segments.append(prefix.strip())
                continue
            # Standalone amount on its own line — only when the WHOLE
            # line shape passes the STRICT amount tail check (so a
            # bare "4711" reference number doesn't get accepted).
            standalone_split = parse_amount_at_end(" " + c)
            if standalone_split is not None and len(c) < 30 and not standalone_split[1].strip():
                amount = standalone_split[0]
                continue
        purpose_segments.append(c)

    if amount is None or abs(amount) > 10_000_000:
        return None

    purpose = " ".join(p for p in purpose_segments if p).strip()
    if len(purpose) < 2:
        return None

    counterparty = _extract_counterparty(purpose)
    iban_in_block = _strip_iban(purpose)
    tx_type, category = _classify_purpose(counterparty, purpose, amount)

    return ParsedTransaction(
        booking_date=booking_iso,
        value_date=value_iso,
        amount=amount,
        currency="EUR",
        counterparty=counterparty,
        counterparty_iban=iban_in_block,
        purpose=purpose[:500],
        tx_type=tx_type,
        category=category,
    )


def _extract_counterparty(purpose: str) -> str:
    """Best-effort counterparty pull from a booking purpose string.
    Heuristics, ranked:

    1. Strip well-known booking-type prefixes (SEPA-Lastschrift,
       Kartenzahlung, …).
    2. Take the first chunk before a long whitespace gap, or before
       "Mandatsreferenz" / "Kunden-Nr." / "Glaeubiger-ID" — those are
       always trailers.
    3. Drop pseudonym placeholders like NAME_001 / IBAN_001 (those
       belong to a cloud-LLM pseudonymisation scheme; the regex
       parser doesn't introduce them, but pasted samples might).
    """
    s = purpose.strip()
    # Strip booking-type prefix
    s = re.sub(
        r"^(?:SEPA[-\s]?(?:Lastschrift|Überweisung|gutschr|belast)|"
        r"Auftrag|Dauerauftrag|Kartenzahlung|Lastschrift(?:einzug)?|"
        r"Gutschrift|Bargeldauszahlung|GAA|Lohn[/\\]?Gehalt|"
        r"Habenzinsen|Sollzinsen|Kontoführungsentgelt|Übertrag)\s+",
        "", s, flags=re.IGNORECASE,
    )
    # Cut at trailer keywords
    cut = re.split(
        r"\s+(?:Mandatsreferenz|Kunden-?Nr\.?|Glaeubiger-?ID|"
        r"Glä?ubiger-?ID|End-zu-End-Referenz|Ref\.|Verwendungszweck:?|"
        r"BIC:|IBAN:|BIC\s+[A-Z]{4,}|"
        r"DE\d{2}\d{18,}|"
        r"\b\d{6,}\b)",
        s, maxsplit=1, flags=re.IGNORECASE,
    )
    head = cut[0].strip()
    # Trim trailing punctuation, drop "EUR"
    head = re.sub(r"\s+EUR\s*$", "", head, flags=re.IGNORECASE)
    head = head.rstrip(",;:.- ").strip()
    return head[:80]


def _parse_booking_line(line: str, *, hint_year: int | None) -> ParsedTransaction | None:
    """Parse ONE OCR line into a transaction or return None.

    The line must:
      - start with a German date
      - end with a parseable amount
      - have non-trivial text in between (the booking description)
    """
    if not _BOOKING_PREFIX_RE.match(line):
        return None
    # Split off the date.
    m = _BOOKING_PREFIX_RE.match(line)
    raw_date = m.group(1)
    rest = line[m.end():].strip()
    booking_iso = parse_de_date(raw_date, hint_year=hint_year)
    if not booking_iso:
        return None
    # Some layouts print "Buchungstag Wertstellung" — a SECOND date
    # right after the first. Capture it as value_date.
    value_iso = ""
    m2 = re.match(r"(\d{1,2}\.\d{1,2}\.(?:\d{2,4})?)\s+(.*)", rest)
    if m2:
        v_iso = parse_de_date(m2.group(1), hint_year=hint_year)
        if v_iso:
            value_iso = v_iso
            rest = m2.group(2).strip()

    amt_split = parse_amount_at_end(rest)
    if amt_split is None:
        return None
    amount, prefix = amt_split
    if abs(amount) > 10_000_000:
        return None
    purpose = prefix.strip()
    if len(purpose) < 2:
        return None
    counterparty = ""
    # Extract counterparty: usually the first 1-3 words after the
    # booking-type prefix. We strip well-known prefixes and take the
    # first capitalised noun-phrase as a best guess.
    cp_match = re.match(
        r"(?:SEPA[-\s]?(?:Lastschrift|Überweisung|gutschr|belast)|"
        r"Auftrag|Dauerauftrag|Kartenzahlung|Lastschrift|Gutschrift|"
        r"Bargeldauszahlung|GAA|Lohn/?Gehalt)?\s*(.{2,80}?)(?:\s{2,}|$)",
        purpose, re.IGNORECASE,
    )
    if cp_match:
        counterparty = cp_match.group(1).strip()[:80]

    iban_in_line = _strip_iban(purpose)
    tx_type, category = _classify_purpose(counterparty, purpose, amount)

    return ParsedTransaction(
        booking_date=booking_iso,
        value_date=value_iso,
        amount=amount,
        currency="EUR",
        counterparty=counterparty,
        counterparty_iban=iban_in_line,
        purpose=purpose[:500],
        tx_type=tx_type,
        category=category,
    )


def _looks_like_balance_row(text: str) -> bool:
    return bool(
        _OPENING_RE.search(text) or _CLOSING_RE.search(text)
    )
