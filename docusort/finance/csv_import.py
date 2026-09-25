"""Bank-CSV importer (Sparkasse, DKB, and other German online-banking exports).

Every bank exports the same facts under different column names, some with
a preamble (DKB, ING, comdirect: account line, period, balance) before the
real header. The importer therefore does NOT assume a layout:

1. decode (BOM / UTF-8 / CP1252 / ISO-8859-1), sniff the separator;
2. walk the first rows until one looks like a header — it must name a
   booking date and an amount (or Soll/Haben) — everything above it is
   preamble and is scanned for the account's own IBAN;
3. map the header through a synonym table onto canonical fields
   (booking_date, amount, counterparty, counterparty_iban, purpose, …);
4. parse rows: German "1.234,56", English "1234.56", "€"/"EUR" chrome,
   unicode minus, Soll/Haben pairs; dates DD.MM.YY / DD.MM.YYYY / ISO.

The account's own IBAN comes from a column (Sparkasse "Auftragskonto",
Volksbank "IBAN Auftragskonto"), else from the preamble (DKB "Girokonto";
"DE…", old DKB "Kontonummer:";"DE… / Girokonto", ING "IBAN;DE…"), else
from the `account_iban_hint` the user typed next to the upload (comdirect
and N26 files carry no IBAN at all). Without any of the three the file is
refused with a message that says exactly that.

Bank name is detected from the header signature (Sparkasse, DKB, ING,
Volksbank/GENO, comdirect, Commerzbank, Deutsche Bank, N26, Consorsbank);
unknown layouts that still map cleanly import as "Bank".

Dedup key: SHA256 of (account_iban_hash | booking date | amount |
purpose | sammlerreferenz). Same booking imported twice is recognised by
INSERT OR IGNORE on transactions.tx_hash.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from .dates import iban_hash, normalise_date, normalise_iban

logger = logging.getLogger("docusort.finance.csv_import")


def _norm_key(h: str) -> str:
    """Header cell → comparable key: lowercase, umlauts flattened, gender
    stars and unit chrome removed, whitespace collapsed."""
    h = (h or "").strip().lstrip("﻿").strip('"').strip().lower()
    for a_, b_ in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss"), ("é", "e")):
        h = h.replace(a_, b_)
    h = h.replace("*", "").replace("(eur)", "").replace("(€)", "").replace("in eur", "").replace("(end-to-end)", "")
    h = re.sub(r"\s+", " ", h).strip(" :")
    return h


# Canonical field → header spellings seen in the wild (already _norm_key'd).
_FIELD_SYNONYMS: dict[str, tuple[str, ...]] = {
    "account_iban":      ("auftragskonto", "iban auftragskonto", "iban auftraggeberkonto", "kontonummer auftragskonto",
                          "auftraggeberkonto", "eigene iban", "konto iban"),
    "booking_date":      ("buchungstag", "buchungsdatum", "buchung", "datum", "date", "booking date", "buchungs-datum",
                          "belegdatum"),
    "value_date":        ("valutadatum", "valuta", "wertstellung", "wertstellung (valuta)", "wert", "value date", "wertstellungsdatum"),
    "booking_text":      ("buchungstext", "umsatzart", "umsatztyp", "vorgang", "buchungsart", "transaction type", "art",
                          "type"),   # N26: "Credit Transfer" / "Direct Debit"
    "purpose":           ("verwendungszweck", "payment reference", "beschreibung", "buchungsdetails", "description", "zweck"),
    "counterparty":      ("beguenstigter/zahlungspflichtiger", "beguenstigter / zahlungspflichtiger", "auftraggeber / beguenstigter",
                          "auftraggeber/beguenstigter", "beguenstigter / auftraggeber", "auftraggeber/empfaenger",
                          "name zahlungsbeteiligter", "sender / empfaenger", "payee", "empfaenger/auftraggeber",
                          "name", "auftraggeber / empfaenger", "zahlungsbeteiligter",
                          # N26's newer export renamed "Payee" to "Partner Name".
                          # Without this the payee column is never found and the
                          # payment reference ends up in its place — every Telekom
                          # debit then reads "Festnetz Vertragskonto …" instead of
                          # "Telekom Deutschland GmbH".
                          "partner name"),
    "payer":             ("zahlungspflichtiger", "zahlungspflichtige/r", "zahlungspflichtiger/e", "auftraggeber"),
    "payee":             ("zahlungsempfaengerin", "zahlungsempfaenger", "empfaenger", "beguenstigter"),
    "counterparty_iban": ("kontonummer/iban", "iban zahlungsbeteiligter", "iban / konto-nr.", "iban/konto-nr.", "kontonummer",
                          "iban", "account number", "konto-nr.", "iban gegenkonto", "gegenkonto",
                          "partner iban"),   # N26 (newer export)
    "counterparty_bic":  ("bic (swift-code)", "bic", "blz", "bic (swift-code) zahlungsbeteiligter", "bic / blz"),
    "amount":            ("betrag", "umsatz", "amount", "betrag eur", "amount eur", "umsatz eur", "betrag ()", "betrag in euro"),
    "debit":             ("soll", "debit"),
    "credit":            ("haben", "credit"),
    "currency":          ("waehrung", "currency", "whg"),
    "sammlerreferenz":   ("sammlerreferenz",),
    "mandatsreferenz":   ("mandatsreferenz", "mandate reference"),
    "glaeubiger_id":     ("glaeubiger id", "glaeubiger-id", "glaeubigerid", "creditor id"),
    "kundenreferenz":    ("kundenreferenz", "kundenreferenz end-to-end", "customer reference"),
    "info":              ("info", "bemerkung", "notiz", "status"),
    "balance":           ("saldo", "saldo nach buchung", "kontostand", "balance"),
}
_KEY_TO_FIELD: dict[str, str] = {}
for _f, _names in _FIELD_SYNONYMS.items():
    for _n in _names:
        _KEY_TO_FIELD.setdefault(_n, _f)

# Header signatures → bank label (first match wins; order matters).
_BANK_SIGNATURES: tuple[tuple[str, frozenset[str]], ...] = (
    ("Sparkasse",     frozenset({"auftragskonto", "sammlerreferenz"})),
    ("DKB Visa",      frozenset({"belegdatum", "beschreibung", "umsatztyp"})),
    ("DKB Visa",      frozenset({"belegdatum", "beschreibung", "betrag eur"})),
    ("DKB",           frozenset({"zahlungsempfaengerin", "zahlungspflichtiger"})),
    ("DKB",           frozenset({"auftraggeber / beguenstigter", "kontonummer", "blz"})),
    ("Volksbank",     frozenset({"iban zahlungsbeteiligter"})),
    ("ING",           frozenset({"auftraggeber/empfaenger", "saldo"})),
    ("comdirect",     frozenset({"vorgang", "umsatz"})),
    ("Commerzbank",   frozenset({"umsatzart", "iban auftraggeberkonto"})),
    ("Deutsche Bank", frozenset({"soll", "haben", "umsatzart"})),
    ("Consorsbank",   frozenset({"sender / empfaenger"})),
    ("N26",           frozenset({"payee", "payment reference"})),
    ("N26",           frozenset({"partner name", "partner iban", "payment reference"})),
)

_IBAN_RE = re.compile(r"\b([A-Z]{2}\d{2}(?:\s?[A-Z0-9]{4}){2,7}(?:\s?[A-Z0-9]{1,4})?)\b")

# Credit cards have no IBAN. DKB writes the masked number in the preamble
# ("4998 •••• •••• 0424", older exports "4998********0424"); the card becomes
# an own account with the pseudo-IBAN "CARD-4998XXXXXXXX0424" so dedup, the
# balance hint and transfer matching (by last four digits) keep working.
_CARD_RE = re.compile(r"(\d{4})\s*[•*xX\d]{4}\s*[•*xX\d]{4}\s*(\d{4})")
CARD_PREFIX = "CARD-"


def card_key(first4: str, last4: str) -> str:
    return f"{CARD_PREFIX}{first4}XXXXXXXX{last4}"


def is_card_account(iban: str | None) -> bool:
    return (iban or "").startswith(CARD_PREFIX)


def _find_card(cells: list[str]) -> str:
    for c in cells:
        m = _CARD_RE.search(c or "")
        if m:
            return card_key(m.group(1), m.group(2))
    return ""


@dataclass
class ImportRow:
    """One CSV row, normalised but pre-deduplication."""
    account_iban: str
    booking_date: str       # ISO YYYY-MM-DD
    value_date: str         # ISO YYYY-MM-DD or ""
    booking_text: str
    purpose: str
    amount: float
    currency: str
    counterparty: str
    counterparty_iban: str
    sammlerreferenz: str
    mandatsreferenz: str
    glaeubiger_id: str
    info: str
    pending: bool = False   # bank says „vorgemerkt" — not booked yet
    tx_type: str = ""       # known from the source (Kontoauszug booking kind); "" → guessed from text


@dataclass
class ImportReport:
    file_label: str = ""
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_duplicate: int = 0
    rows_skipped: int = 0      # blank / unparseable / missing required field
    rows_invalid: int = 0      # row failed validation (bad amount, bad date)
    accounts_touched: list[str] = field(default_factory=list)
    period_start: str = ""     # earliest booking date in this import
    period_end: str = ""       # latest booking date
    transfers_tagged: int = 0  # bookings (any account) newly recognised as moves between own accounts
    bank: str = ""             # detected from the header signature ("Sparkasse", "DKB", … or "Bank")
    rows_repaired: int = 0     # earlier imports of the same rows carried a misread amount → fixed in place
    synthetic_replaced: int = 0  # DocuSort's own gap-filling counter-legs replaced by the real booking
    gaps_filled: int = 0       # export gaps closed with synthetic counter-legs after this import
    balance_date: str = ""     # "Kontostand vom …" from the file preamble (DKB, ING), ISO
    balance_amount: float | None = None
    balance_note: str = ""     # what was done with the balance: start balance set / matches / differs by X
    decimal_sep: str = ""
    # Set when the own IBAN was not in the file and we reused the only
    # known account of that bank (N26, comdirect).
    iban_from_known_account: str = ""      # "," or "." — decided once per file, not per cell
    rows_pending: int = 0      # „vorgemerkt" rows kept aside — shown, never counted
    pending_moved: int = 0     # pending rows an earlier version had imported as bookings, moved out
    rows_overlap: int = 0      # same day + amount already known from the OTHER source (CSV ↔ Kontoauszug)
    statements: int = 0        # Kontoauszug-PDFs imported (statement import only)
    statements_skipped: int = 0  # Kontoauszüge already imported / not balanced (statement import only)
    errors: list[str] = field(default_factory=list)


def _decode_bytes(data: bytes) -> str:
    """Try UTF-8, fall back to Windows-1252 (Sparkasse exports are
    usually CP1252). BOM stripped if present."""
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    for enc in ("utf-8", "cp1252", "iso-8859-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    # Last resort: latin-1 always succeeds.
    return data.decode("latin-1", errors="replace")


def _detect_dialect(text: str) -> csv.Dialect:
    """';' is the German default; DKB (new) and N26 use ',' with quoted
    cells. Take the separator every early line agrees on; fall back to ';'."""
    lines = [ln for ln in text.splitlines() if ln.strip()][:12]
    best, best_score = ";", (0, 0)
    for d in (";", ",", "\t"):
        counts = [ln.count(d) for ln in lines]
        # lines that clearly use it (≥ 2 cells) first, total occurrences second —
        # a preamble line like "Girokonto","DE…" or an empty "" line must not veto
        score = (sum(1 for c in counts if c >= 2), sum(counts))
        if score > best_score:
            best, best_score = d, score

    class _D(csv.excel):
        delimiter = best
    return _D()


def _parse_amount(raw: str, decimal: str = "") -> float | None:
    """Amount cell → float. German "1.234,56", English "1234.56",
    "-45,10 €", "45,10 EUR", unicode minus, "+" signs. None on garbage.

    `decimal` is the separator the FILE uses ("," or "."), decided once by
    `_detect_decimal`. Without it a lone "1.000" is ambiguous — the DKB
    writes round amounts without decimals ("1.000" = one thousand) and the
    per-cell guess read that as 1,00 €."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = (s.replace("−", "-").replace("–", "-").replace("€", "").replace("EUR", "")
          .replace("\xa0", "").replace(" ", "").replace("+", ""))
    if not s or s in ("-", "."):
        return None
    if decimal == ",":
        s = s.replace(".", "").replace(",", ".")
    elif decimal == ".":
        s = s.replace(",", "")
    elif "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):      # whichever comes last is the decimal separator
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def _detect_decimal(cells: list[str]) -> str:
    """Decide the decimal separator for a whole file from its amount cells.
    Any comma anywhere → German (",") and every "." is a thousands dot.
    Otherwise, a "." followed by exactly two digits at the end → English.
    Empty string when undecidable (falls back to the per-cell guess)."""
    saw_dot2 = saw_dot3 = False
    for c in cells:
        c = (c or "").strip()
        if not c:
            continue
        if "," in c:
            return ","
        if re.search(r"\.\d{2}$", c):
            saw_dot2 = True
        elif re.search(r"\.\d{3}$", c):
            saw_dot3 = True
    if saw_dot2:
        return "."
    # Only "x.yyy" cells and never a comma: a German file whose every amount
    # happens to be a round thousand ("1.000", "2.500").
    return "," if saw_dot3 else ""


_BALANCE_RE = re.compile(r"(kontostand|saldo|balance)\D{0,20}?(\d{1,2}\.\d{1,2}\.\d{2,4})", re.I)


def _find_balance(cells: list[str]) -> tuple[str, float | None]:
    """DKB/ING preamble: '"Kontostand vom 19.09.2026:";"104,77 €"'. Returns
    (ISO date, amount) or ("", None). The amount is the first parseable
    cell after the one naming the date."""
    for i, c in enumerate(cells):
        m = _BALANCE_RE.search(c or "")
        if not m:
            continue
        d = normalise_date(m.group(2))
        for nxt in cells[i + 1:]:
            v = _parse_amount(nxt, ",")
            if v is not None:
                return d, v
        return d, None
    return "", None


def _map_header(cells: list[str]) -> dict[str, int]:
    """Header row → {canonical field: column index}; first column wins."""
    out: dict[str, int] = {}
    for idx, cell in enumerate(cells):
        f = _KEY_TO_FIELD.get(_norm_key(cell))
        if f and f not in out:
            out[f] = idx
    return out


def _looks_like_header(mapping: dict[str, int]) -> bool:
    return "booking_date" in mapping and ("amount" in mapping or "debit" in mapping or "credit" in mapping)


def _detect_bank(cells: list[str]) -> str:
    keys = {_norm_key(c) for c in cells}
    for name, sig in _BANK_SIGNATURES:
        if sig <= keys:
            return name
    return "Bank"


def _find_iban(cells: list[str]) -> str:
    for c in cells:
        m = _IBAN_RE.search((c or "").upper())
        if m:
            ib = normalise_iban(m.group(1))
            if len(ib) >= 15:
                return ib
    return ""


def parse_csv(data: bytes | str, *, file_label: str = "",
              account_iban_hint: str = "") -> tuple[list[ImportRow], ImportReport]:
    """Parse the CSV bytes/text of any supported bank. Returns (rows,
    report). Doesn't touch the DB — call `import_csv` for the full
    pipeline. `account_iban_hint` is used when the file itself names no
    own IBAN (comdirect, N26)."""
    report = ImportReport(file_label=file_label)
    text = data if isinstance(data, str) else _decode_bytes(data)
    if not text.strip():
        report.errors.append("CSV is empty")
        return [], report

    dialect = _detect_dialect(text)
    reader = csv.reader(io.StringIO(text), dialect=dialect)

    # Preamble + header hunt: the header is the first row that names a
    # booking date and an amount. Rows above it may carry the own IBAN.
    mapping: dict[str, int] = {}
    header: list[str] = []
    preamble_iban = ""
    all_rows = list(reader)
    body_start = 0
    for n, cells in enumerate(all_rows):
        if n > 40:
            break
        if not cells or all(not (c or "").strip() for c in cells):
            continue
        m = _map_header(cells)
        if _looks_like_header(m):
            mapping, header = m, cells
            body_start = n + 1
            break
        if not preamble_iban:
            preamble_iban = _find_iban(cells) or _find_card(cells)
        if not report.balance_date:
            report.balance_date, report.balance_amount = _find_balance(cells)
    if not mapping:
        report.errors.append(
            "Kein Spaltenkopf mit Buchungsdatum und Betrag gefunden — ist das ein Umsatz-Export "
            "(Sparkasse, DKB, ING, Volksbank, comdirect, Commerzbank, Deutsche Bank, N26, Consorsbank)?"
        )
        return [], report
    report.bank = _detect_bank(header)
    own_hint = normalise_iban(account_iban_hint or "")

    def cell(row: list[str], field_name: str) -> str:
        idx = mapping.get(field_name)
        if idx is None or idx >= len(row):
            return ""
        return (row[idx] or "").strip()

    body = all_rows[body_start:]
    # One decision per file: which character is the decimal separator.
    amount_cells = [cell(r, f) for r in body for f in ("amount", "debit", "credit") if f in mapping]
    dec = _detect_decimal(amount_cells)
    report.decimal_sep = dec

    out: list[ImportRow] = []
    for raw_row in body:
        if not raw_row or all(not (c or "").strip() for c in raw_row):
            continue
        report.rows_seen += 1

        amount = _parse_amount(cell(raw_row, "amount"), dec) if "amount" in mapping else None
        if amount is None and ("debit" in mapping or "credit" in mapping):
            # Deutsche Bank: an (empty) "Betrag" column next to Soll/Haben
            deb = _parse_amount(cell(raw_row, "debit"), dec)
            cred = _parse_amount(cell(raw_row, "credit"), dec)
            amount = None if deb is None and cred is None else round((cred or 0.0) - abs(deb or 0.0), 2)
        if amount is None:
            report.rows_invalid += 1
            continue
        booking_date = normalise_date(cell(raw_row, "booking_date"))
        if not booking_date:
            report.rows_invalid += 1        # trailing summary / balance lines land here
            continue
        value_date = normalise_date(cell(raw_row, "value_date"))

        account_iban = normalise_iban(cell(raw_row, "account_iban")) or preamble_iban or own_hint
        if not account_iban:
            report.rows_skipped += 1
            continue

        # Counterparty: one column (most banks) or payer/payee pair (DKB new) —
        # the OTHER side of the booking is whoever isn't us.
        counterparty = cell(raw_row, "counterparty")
        if not counterparty and ("payer" in mapping or "payee" in mapping):
            counterparty = cell(raw_row, "payee") if amount < 0 else cell(raw_row, "payer")
        if not counterparty and "counterparty" not in mapping and "payee" not in mapping:
            # Credit-card exports have only a "Beschreibung" — that IS the merchant.
            counterparty = cell(raw_row, "purpose")
        cp_iban = normalise_iban(cell(raw_row, "counterparty_iban"))
        if cp_iban == account_iban:
            cp_iban = ""
        status = cell(raw_row, "info")
        # Sparkasse: Info „Umsatz vorgemerkt"; DKB: Status „Vorgemerkt". Kept
        # apart from the bookings — name, date and amount may still change.
        pending = "vorgemerkt" in status.lower()

        out.append(ImportRow(
            account_iban=account_iban,
            booking_date=booking_date,
            value_date=value_date,
            booking_text=cell(raw_row, "booking_text")[:200],
            purpose=cell(raw_row, "purpose")[:500],
            amount=amount,
            currency=(cell(raw_row, "currency").upper()[:8] or "EUR"),
            counterparty=counterparty[:200],
            counterparty_iban=cp_iban,
            sammlerreferenz=cell(raw_row, "sammlerreferenz")[:80],
            mandatsreferenz=cell(raw_row, "mandatsreferenz")[:80],
            glaeubiger_id=cell(raw_row, "glaeubiger_id")[:64],
            info=status[:200],
            pending=pending,
        ))

    return out, report


def _row_hash(account_iban_h: str, row: ImportRow) -> str:
    """Stable identity of a booking. Re-importing the same CSV
    produces the same hash → UNIQUE constraint catches duplicates."""
    parts = [
        account_iban_h or "no-iban",
        row.booking_date,
        f"{row.amount:.2f}",
        row.purpose[:200],
        row.sammlerreferenz[:80],
    ]
    key = "|".join(parts)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# ---- Light tx_type / category heuristics (no LLM) -----------------

_TX_TYPE_RULES = (
    (re.compile(r"\b(SEPA[-\s]?LASTSCHRIFT|LASTSCHRIFT|SONSTIGER\s+EINZUG)\b", re.I), "lastschrift"),
    (re.compile(r"\bDAUERAUFTRAG\b", re.I), "dauerauftrag"),
    (re.compile(r"\b(UEBERTRAG|UEBERWEISUNG|GUTSCHRIFT|EINZAHLUNG)\b", re.I), "ueberweisung"),
    (re.compile(r"\b(KARTENZAHL|KARTEN[-\s]?Z|GIROCARD|VISA|MAESTRO|EC[-\s]?POS|DIG\.\s*KARTE)\b", re.I), "kartenzahlung"),
    (re.compile(r"\b(BARGELDAUSZAHLUNG|GAA|GELDAUTOMAT|EINMAL\s*LAST)\b", re.I), "bargeld"),
    (re.compile(r"\b(LOHN|GEHALT|TANTIEME|BONUS)\b", re.I), "gehalt"),
    (re.compile(r"\b(HABENZINSEN|SOLLZINSEN|ZINSEN|DIVIDEND|ABSCHLUSS)\b", re.I), "zinsen"),
    (re.compile(r"\b(KONTOFUEHRUNG|ENTGELT|GEBUEHR)\b", re.I), "gebuehr"),
)

_CATEGORY_RULES = (
    (re.compile(r"\b(LIDL|ALDI|REWE|EDEKA|PENNY|KAUFLAND|NORMA|TEGUT|NETTO|BAECKER|BAECKEREI|MUEHLENBAECKER|OBSTHOF)\b", re.I), "lebensmittel"),
    (re.compile(r"\b(STADTWERKE|VATTENFALL|ENBW|E\.ON|VODAFONE|TELEKOM|GLASFASER|RUNDFUNK|GEZ|SACHSENENERGIE|BURGERLICHES|NEUE\s+LEBEN)\b", re.I), "nebenkosten"),
    (re.compile(r"\b(DB\b|VRR|VVS|BVG|MVV|ARAL|SHELL|TOTAL|ESSO|TANKSTELLE|RUNDKINO|CINEPLEX|CINESTAR)\b", re.I), "freizeit"),
    (re.compile(r"\b(ALLIANZ|HUK|GENERALI|DEVK|R\+V|KRANKENKASSE|TK\b|BARMER|AOK|DAK)\b", re.I), "versicherung"),
    (re.compile(r"\b(NETFLIX|SPOTIFY|APPLE|AMAZON\s+PRIME|FITNESS|GYM|SKY)\b", re.I), "abonnement"),
    (re.compile(r"\b(APOTHEKE|ARZT|PRAXIS|KRANKENHAUS|KLINIK|REZEPT|THERAPIE|DM[-\s]?DROGERIE)\b", re.I), "gesundheit"),
    (re.compile(r"\b(MEDIAMARKT|SATURN|CYBERPORT|NOTEBOOKSBILLIGER|NETTO\s+MARK)\b", re.I), "elektronik"),
    (re.compile(r"\b(ROSSMANN|MUELLER|IKEA|OBI|BAUHAUS|HORNBACH)\b", re.I), "haushalt"),
    (re.compile(r"\b(ZALANDO|H&M|ZARA|BEKLEIDUNG)\b", re.I), "bekleidung"),
    (re.compile(r"\b(WERTPAPIER|DEKABANK|DEPOT|AKTIEN|FONDS|ETF)\b", re.I), "kapital"),
)


def _classify(row: ImportRow) -> tuple[str, str]:
    blob = f"{row.booking_text} {row.counterparty} {row.purpose}"
    tx_type = "sonstiges"
    for pat, t in _TX_TYPE_RULES:
        if pat.search(blob):
            tx_type = t
            break
    category = "sonstiges"
    for pat, c in _CATEGORY_RULES:
        if pat.search(blob):
            category = c
            break
    if tx_type == "lastschrift" and category == "sonstiges" and "EINZUG" in row.booking_text.upper():
        # Generic SEPA-Lastschrift with no merchant we recognise →
        # leave category as sonstiges; the user will recategorise.
        pass
    return tx_type, category


def _ensure_csv_container_statement(db, account_id: int) -> int:
    """Return the id of the per-account container statement we use to
    satisfy the legacy `transactions.statement_id NOT NULL` constraint
    for CSV-imported bookings. Creates one on demand. The
    `statements` table also requires `doc_id NOT NULL` so we point
    at the most recent Kontoauszug document for this account; if
    there isn't one yet, we synthesise a stub document row with a
    distinctive subject so the user can recognise it.
    """
    # Look for an existing container — keyed by file_hash =
    # 'csv-import:<account_id>' so it's unique and easy to spot.
    sentinel = f"csv-import:{account_id}"
    with db._lock:
        row = db._conn.execute(
            "SELECT id FROM statements WHERE file_hash = ? LIMIT 1",
            (sentinel,),
        ).fetchone()
        if row:
            return int(row["id"])

    # Need a doc_id to anchor the statement: a stub document. (Reusing a
    # Kontoauszug document's id is not possible — `statements.doc_id` is
    # UNIQUE and every imported Kontoauszug-PDF owns its own statement row.)
    doc_id: int
    if True:
        # Synthesise a placeholder document. It must NOT be marked
        # deleted, because every finance query joins
        # `documents d` with `WHERE d.deleted_at IS NULL` — a
        # deleted stub would silently filter out every CSV-imported
        # transaction. Instead we tag it with the sentinel category
        # `_csv_container` and the library queries skip that.
        from datetime import datetime as _dt
        now = _dt.now().isoformat(timespec="seconds")
        with db._lock:
            cur = db._conn.execute(
                "INSERT INTO documents "
                "  (filename, original_name, category, subcategory, tags, "
                "   doc_date, sender, subject, confidence, library_path, "
                "   processed_path, file_size, page_count, ocr_used, model, "
                "   input_tokens, output_tokens, cost_usd, status, "
                "   content_hash, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (f".csv-container-account-{account_id}",
                 f".csv-container-account-{account_id}",
                 "_csv_container", "", "[]",
                 "", "CSV-Import",
                 f"CSV-Container Konto {account_id}",
                 0.0, "", "", 0, 0, 0, "", 0, 0, 0.0, "csv_container",
                 f"csv-stub-{account_id}", now),
            )
            db._conn.commit()
            doc_id = int(cur.lastrowid)

    from datetime import datetime as _dt
    now = _dt.now().isoformat(timespec="seconds")
    with db._lock:
        cur = db._conn.execute(
            "INSERT INTO statements "
            "  (doc_id, account_id, period_start, period_end, statement_no, "
            "   opening_balance, closing_balance, currency, file_hash, "
            "   privacy_mode, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (doc_id, account_id, "", "", "csv-container",
             None, None, "EUR", sentinel, "csv", now),
        )
        db._conn.commit()
        return int(cur.lastrowid)


def _apply_balance_hint(db, account_id: int, date_iso: str, amount: float | None) -> str:
    import json as _json
    key = f"finance.balance_hint.{account_id}"
    stored: dict = {}
    try:
        stored = _json.loads(db.meta_get(key) or "{}")
    except ValueError:
        stored = {}
    if amount is not None and date_iso:
        stored = {"date": date_iso, "amount": float(amount), "applied": stored.get("applied")}
    if not stored.get("date"):
        return ""
    hint_date, hint_amount = stored["date"], float(stored["amount"])
    with db._lock:
        booked = db._conn.execute(
            "SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE account_id = ? AND booking_date <= ?",
            (account_id, hint_date),
        ).fetchone()[0]
        cur_start = float(db._conn.execute(
            "SELECT COALESCE(start_balance, 0) FROM accounts WHERE id = ?", (account_id,)
        ).fetchone()[0] or 0.0)
    implied = round(hint_amount - float(booked), 2)
    ours = stored.get("applied")
    we_own_it = abs(cur_start) < 0.005 or (ours is not None and abs(cur_start - float(ours)) < 0.005)
    if we_own_it:
        if abs(cur_start - implied) >= 0.005:
            db.set_account_meta(account_id, start_balance=implied)
        stored["applied"] = implied
        db.meta_set(key, _json.dumps(stored))
        return (f"Kontostand laut Datei am {hint_date[8:10]}.{hint_date[5:7]}.{hint_date[0:4]}: {hint_amount:.2f} € "
                f"→ Anfangssaldo {implied:.2f} €, damit der Verlauf auf den Cent stimmt.")
    db.meta_set(key, _json.dumps(stored))
    diff = round(implied - cur_start, 2)
    if abs(diff) < 0.005:
        return f"Kontostand laut Datei ({hint_amount:.2f} €) stimmt mit Anfangssaldo + Buchungen überein."
    return (f"Kontostand laut Datei ({hint_amount:.2f} € am {hint_date[8:10]}.{hint_date[5:7]}.{hint_date[0:4]}) weicht um "
            f"{diff:+.2f} € von deinem Anfangssaldo + Buchungen ab — vermutlich fehlt ein Zeitraum im Export.")




def import_csv(db, data: bytes | str, *, file_label: str = "",
               account_holder_hint: str = "", account_iban_hint: str = "") -> ImportReport:
    """End-to-end import: parse CSV → upsert account(s) → INSERT OR
    IGNORE transactions. INSERT OR IGNORE relies on the UNIQUE
    constraint on `transactions.tx_hash` for dedup, so re-importing
    a CSV that overlaps a previous one is a safe no-op for the
    overlap rows."""
    rows, report = parse_csv(data, file_label=file_label, account_iban_hint=account_iban_hint)
    if report.errors:
        return report
    if not rows and report.rows_skipped and not account_iban_hint and report.bank:
        # N26 and comdirect never name the account the export belongs to.
        # Once such an account exists here, a later export of the same bank
        # can only mean that one — so fill the IBAN in instead of demanding
        # it again. Only when the bank has exactly ONE known account: with
        # two, guessing could book a payment onto the wrong one.
        try:
            with db._lock:  # noqa: SLF001
                known = db._conn.execute(   # noqa: SLF001
                    # Prefix match, not equality: `bank_name` doubles as the
                    # account's display name, so renaming it to
                    # "N26 Nebenkosten" must not break this lookup.
                    "SELECT iban FROM accounts "
                    "WHERE (bank_name = ? OR bank_name LIKE ?) "
                    "  AND COALESCE(iban,'') != '' "
                    "  AND iban NOT LIKE 'CARD-%'",
                    (report.bank, report.bank + " %"),
                ).fetchall()
        except Exception:  # noqa: BLE001
            known = []
        if len(known) == 1:
            guessed = known[0]["iban"]
            logger.info("CSV %r: no own IBAN in the file — using the only known "
                        "%s account %s", file_label, report.bank, guessed)
            rows, report = parse_csv(data, file_label=file_label,
                                     account_iban_hint=guessed)
            report.iban_from_known_account = guessed
    if not rows:
        if report.rows_skipped and not report.rows_inserted:
            report.errors.append(
                "Die Datei nennt keine IBAN des eigenen Kontos — bitte die Konto-IBAN im Feld neben dem Import angeben."
            )
        return report
    import_rows(db, rows, report, account_holder_hint=account_holder_hint)
    logger.info(
        "CSV import %r: %d inserted, %d duplicates, %d overlap, %d invalid, %d skipped "
        "(across %d accounts, period %s..%s).",
        file_label, report.rows_inserted, report.rows_duplicate, report.rows_overlap,
        report.rows_invalid, report.rows_skipped,
        len(report.accounts_touched), report.period_start, report.period_end,
    )
    return report


def _other_source_counts(db, account_id: int, rows: list[ImportRow], *, from_statement: bool) -> dict[tuple[str, float], int]:
    """Bookings of this account that the OTHER source already holds, keyed by
    (day, amount). A CSV export and a Kontoauszug describe the same money
    with different words, so the text hash cannot see the overlap — day and
    amount can. Counted, not set-based: two identical bookings on one day
    stay two."""
    if not rows:
        return {}
    lo = min(r.booking_date for r in rows)
    hi = max(r.booking_date for r in rows)
    # Rows coming from a Kontoauszug look for CSV rows and vice versa.
    cond = ("COALESCE(s.file_hash, '') LIKE 'csv-import:%'" if from_statement
            else "COALESCE(s.file_hash, '') NOT LIKE 'csv-import:%'")
    out: dict[tuple[str, float], int] = {}
    with db._lock:
        cur = db._conn.execute(
            f"SELECT t.booking_date, t.amount FROM transactions t "
            f"JOIN statements s ON s.id = t.statement_id "
            f"WHERE t.account_id = ? AND t.booking_date BETWEEN ? AND ? "
            f"  AND COALESCE(t.synthetic, 0) = 0 AND {cond}",
            (account_id, lo, hi),
        )
        for r in cur.fetchall():
            k = (str(r["booking_date"]), round(float(r["amount"]), 2))
            out[k] = out.get(k, 0) + 1
    return out


def _cards_covering(db, own_ibans: set[str]) -> dict[str, tuple[str, str]]:
    """{last4 → (first, last booking day)} of the own credit cards. A card
    settlement on the Giro is only a transfer while the card's own bookings
    are imported for that time — before the card export begins, the
    settlement is the only trace of that spending and counts as
    `kreditkarte`."""
    cards = [i for i in own_ibans if is_card_account(i)]
    if not cards:
        return {}
    out: dict[str, tuple[str, str]] = {}
    with db._lock:
        for r in db._conn.execute(
            "SELECT a.iban, MIN(t.booking_date) lo, MAX(t.booking_date) hi FROM accounts a "
            "JOIN transactions t ON t.account_id = a.id WHERE a.iban LIKE 'CARD-%' GROUP BY a.iban"
        ).fetchall():
            out[str(r["iban"])[-4:]] = (str(r["lo"] or ""), str(r["hi"] or ""))
    return out


def cards_for_day(coverage: dict[str, tuple[str, str]], day: str, *, slack_days: int = 45) -> set[str]:
    """Own cards whose imported bookings cover `day` (± settlement slack)."""
    from datetime import date as _d, timedelta as _td
    if not coverage:
        return set()
    try:
        d = _d.fromisoformat(day[:10])
    except ValueError:
        return set(coverage)
    out: set[str] = set()
    for last4, (lo, hi) in coverage.items():
        if not lo:
            continue
        try:
            a = _d.fromisoformat(lo[:10]) - _td(days=slack_days)
            b = _d.fromisoformat(hi[:10]) + _td(days=slack_days)
        except ValueError:
            out.add(last4)
            continue
        if a <= d <= b:
            out.add(last4)
    return out


def _ensure_pdf_statement(db, account_id: int, statement: dict) -> int:
    """One `statements` row per Kontoauszug-PDF (keyed by the file hash)."""
    from datetime import datetime as _dt
    with db._lock:
        row = db._conn.execute(
            "SELECT id FROM statements WHERE file_hash = ? LIMIT 1", (statement["file_hash"],)
        ).fetchone()
        if row:
            return int(row["id"])
        now = _dt.now().isoformat(timespec="seconds")
        cur = db._conn.execute(
            "INSERT INTO statements (doc_id, account_id, period_start, period_end, statement_no, "
            "  opening_balance, closing_balance, currency, file_hash, privacy_mode, extra_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'EUR', ?, 'local', ?, ?)",
            (int(statement["doc_id"]), account_id, statement.get("opening_date") or "",
             statement.get("closing_date") or "", statement.get("statement_no") or "",
             statement.get("opening_balance"), statement.get("closing_balance"),
             statement["file_hash"], statement.get("extra_json") or None, now),
        )
        db._conn.commit()
        return int(cur.lastrowid)


def import_rows(db, rows: list[ImportRow], report: ImportReport, *,
                account_holder_hint: str = "", statement: dict | None = None) -> ImportReport:
    """Insert parsed rows (CSV or Kontoauszug) for their accounts.

    Dedup happens twice: the text hash catches a re-imported file, the
    day+amount overlap catches the same booking arriving from the other
    source (CSV ↔ Kontoauszug). `statement` (Kontoauszug import) carries
    doc_id, file_hash, statement_no, opening/closing date+balance and
    is_savings — the rows then hang off a real `statements` row instead of
    the per-account CSV container."""
    # Group rows by account IBAN — usually one file = one account but
    # the importer doesn't assume that.
    by_account: dict[str, list[ImportRow]] = {}
    for r in rows:
        by_account.setdefault(r.account_iban, []).append(r)

    from .classify import classify as _decide, RuleIndex
    rules = RuleIndex(db.finance_rules_list()) if hasattr(db, "finance_rules_list") else RuleIndex()
    own_ibans = {a.get("iban") for a in db.list_accounts() if a.get("iban")} | set(by_account)
    card_cov = _cards_covering(db, own_ibans)
    pinned = db.tx_overrides_map() if hasattr(db, "tx_overrides_map") else {}
    from_statement = statement is not None
    holders = db.finance_holder_tokens() if hasattr(db, "finance_holder_tokens") else set()
    if from_statement and statement.get("holder"):
        holders |= db._name_tokens(statement["holder"]) if hasattr(db, "_name_tokens") else set()

    period_start = ""
    period_end = ""
    for ib, rs in by_account.items():
        h = iban_hash(ib)
        last4 = ib[-4:] if len(ib) >= 4 else ""
        account_id = db.upsert_account(
            bank_name=report.bank or "Bank",
            iban=ib, iban_last4=last4, iban_hash=h,
            account_holder=account_holder_hint or (statement or {}).get("holder") or "",
            currency=rs[0].currency or "EUR",
        )
        if from_statement and statement.get("is_savings") and not any(
            int(a.get("is_savings") or 0) for a in db.list_accounts() if a.get("id") == account_id
        ):
            # A Tagesgeld-/Sparkonto statement says what the account is.
            try:
                db.set_account_meta(account_id, is_savings=True)
            except Exception as exc:  # noqa: BLE001
                logger.warning("import: is_savings failed: %s", exc)
        if ib not in report.accounts_touched:
            report.accounts_touched.append(ib)

        # transactions.statement_id is NOT NULL (legacy schema from
        # the v0.13 statement-extraction era). CSV rows hang off ONE
        # container "statement" per account; Kontoauszug rows off a real
        # statement row with period and balances.
        if from_statement:
            stmt_id = _ensure_pdf_statement(db, account_id, statement)
        else:
            stmt_id = _ensure_csv_container_statement(db, account_id)

        other = _other_source_counts(db, account_id, [r for r in rs if not r.pending], from_statement=from_statement)
        pending_rows: list[dict] = []
        booked_max = max((r.booking_date for r in rs if not r.pending), default="")
        # Two identical bookings on one day (0,99 € Apple twice) are two
        # bookings, not a duplicate: the second occurrence of a hash within
        # ONE file gets a running number. Re-importing the same file yields
        # the same numbers, so overlap detection still holds.
        seen_hash: dict[str, int] = {}
        with db._lock:
            for r in rs:
                tx_hash = _row_hash(h, r)
                n = seen_hash.get(tx_hash, 0)
                seen_hash[tx_hash] = n + 1
                if n:
                    tx_hash = f"{tx_hash}#{n + 1}"
                if r.pending:
                    pending_rows.append({
                        "booking_date": r.booking_date, "amount": r.amount, "currency": r.currency,
                        "counterparty": r.counterparty, "purpose": (r.booking_text + " " + r.purpose).strip()[:500],
                        "tx_hash": tx_hash,
                    })
                    continue
                k = (r.booking_date, round(r.amount, 2))
                if other.get(k, 0) > 0:
                    # The other source already holds this booking (same day,
                    # same amount) — a Kontoauszug re-telling a CSV month.
                    other[k] -= 1
                    report.rows_overlap += 1
                    continue
                tx_type = r.tx_type or _classify(r)[0]
                if is_card_account(ib) and tx_type == "sonstiges" and r.amount < 0:
                    tx_type = "kartenzahlung"
                # Compose a richer "purpose" so the existing /finance
                # search keeps finding things — booking_text first
                # (gives type context) then verwendungszweck.
                full_purpose = (r.booking_text + " " + r.purpose).strip()[:500]
                decision = _decide(
                    {"tx_hash": tx_hash, "amount": r.amount, "counterparty": r.counterparty,
                     "counterparty_iban": r.counterparty_iban, "purpose": full_purpose,
                     "booking_text": r.booking_text, "tx_type": tx_type, "account_iban": ib},
                    rules=rules, own_ibans=own_ibans, pinned=pinned,
                    own_cards=cards_for_day(card_cov, r.booking_date), holders=holders,
                )
                try:
                    cur = db._conn.execute(
                        "INSERT OR IGNORE INTO transactions "
                        "  (statement_id, account_id, booking_date, value_date, "
                        "   amount, currency, counterparty, counterparty_iban, "
                        "   purpose, tx_type, category, tx_hash, category_source, category_reason) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (stmt_id, account_id, r.booking_date, r.value_date or r.booking_date,
                         r.amount, r.currency, r.counterparty, r.counterparty_iban,
                         full_purpose, tx_type, decision.category, tx_hash,
                         decision.source, decision.reason),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("import: insert failed: %s", exc)
                    report.errors.append(str(exc))
                    continue
                if cur.rowcount > 0:
                    # A booking that is new by hash may still exist with a
                    # misread amount from an earlier import (DKB "1.000" read
                    # as 1,00 €): same day, same text, amount ×1000. Heal that
                    # row instead of keeping both.
                    victim = db._conn.execute(
                        "SELECT id FROM transactions WHERE account_id = ? AND booking_date = ? "
                        "  AND purpose = ? AND COALESCE(counterparty,'') = ? AND id != last_insert_rowid() "
                        "  AND ABS(amount * 1000 - ?) < 0.005 AND ABS(amount) < 100",
                        (account_id, r.booking_date, full_purpose, r.counterparty, r.amount),
                    ).fetchone()
                    if victim:
                        db._conn.execute("DELETE FROM transactions WHERE id = ?", (int(victim["id"]),))
                        report.rows_repaired += 1
                    # A real booking replaces the counter-leg DocuSort had
                    # synthesised to close an export gap (same day, amount).
                    twin = db._conn.execute(
                        "SELECT id FROM transactions WHERE account_id = ? AND synthetic = 1 "
                        "  AND booking_date = ? AND ABS(amount - ?) < 0.005 LIMIT 1",
                        (account_id, r.booking_date, r.amount),
                    ).fetchone()
                    if twin:
                        db._conn.execute("DELETE FROM transactions WHERE id = ?", (int(twin["id"]),))
                        report.synthetic_replaced += 1
                    report.rows_inserted += 1
                    if not period_start or r.booking_date < period_start:
                        period_start = r.booking_date
                    if not period_end or r.booking_date > period_end:
                        period_end = r.booking_date
                else:
                    report.rows_duplicate += 1
            db._conn.commit()

        # The file's pending rows replace what we knew was pending for this
        # account — they show on /transactions, count nowhere, and vanish
        # with the export in which they have booked.
        if not from_statement:
            try:
                pr = db.pending_replace(account_id, booked_max, pending_rows)
                report.rows_pending += pr["added"]
                report.pending_moved += pr["moved"]
            except Exception as exc:  # noqa: BLE001
                logger.warning("CSV import: pending rows failed: %s", exc)

        # "Kontostand vom <Datum>" in the preamble pins the balance history:
        # start balance = stated balance − everything booked up to that day.
        # The hint is remembered per account and re-applied after EVERY import
        # (three DKB year-files all state today's balance; only with all
        # three loaded is the implied start final). A start balance the user
        # typed is never overwritten — then we report the difference, which is
        # a gap in the exports rather than a rounding error.
        if not from_statement:
            try:
                report.balance_note = _apply_balance_hint(
                    db, account_id, report.balance_date, report.balance_amount,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("CSV import: balance hint failed: %s", exc)

    # Now that every account in this file exists, bookings whose counterparty
    # is one of OUR accounts are transfers, not income/expense — on this
    # import and on everything imported before it (the Giro's "to Tagesgeld"
    # rows become recognisable the moment the Tagesgeld CSV arrives).
    if not from_statement or statement.get("finalize", True):
        finalize_import(db, report)

    report.period_start = period_start
    report.period_end = period_end
    return report


def finalize_import(db, report: ImportReport | None = None) -> None:
    """Cross-account bookkeeping after rows landed: name-only transfer legs
    get their IBAN from the counterpart, own-account transfers are tagged,
    export gaps closed. Cheap and idempotent; the statement backfill calls it
    once at the end instead of once per PDF."""
    rep = report or ImportReport()
    try:
        db.finance_pair_name_transfers()
    except Exception as exc:  # noqa: BLE001
        logger.warning("import: name-transfer pairing failed: %s", exc)
    try:
        rep.transfers_tagged = int(db.finance_retag_transfers())
    except Exception as exc:  # noqa: BLE001
        logger.warning("import: transfer re-tag failed: %s", exc)
    try:
        rep.gaps_filled = len(db.finance_fill_transfer_gaps().get("added", []))
    except Exception as exc:  # noqa: BLE001
        logger.warning("import: gap fill failed: %s", exc)
    try:
        db.finance_reconcile_statements()
    except Exception as exc:  # noqa: BLE001
        logger.warning("import: statement reconcile failed: %s", exc)
    # Fresh bookings are exactly what an open bill was waiting for, so the
    # deadline card learns about a payment in the same pass that imported it.
    # Through deadline_match, so the amounts are read first (a bill whose
    # amount nobody ever read cannot be matched, no matter how many bookings
    # arrive) and so this cannot run beside another pass.
    try:
        from .. import deadline_match
        matched = deadline_match.run_now(db).get("matched", 0)
        if matched:
            logger.info("import: %d open deadline(s) settled by the new bookings", matched)
    except Exception as exc:  # noqa: BLE001
        logger.warning("import: deadline payment matching failed: %s", exc)
