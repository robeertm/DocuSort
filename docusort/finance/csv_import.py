"""Sparkasse-CSV importer.

Header layout (Sparkasse Online-Banking export):

    Auftragskonto;Buchungstag;Valutadatum;Buchungstext;Verwendungszweck;
    Glaeubiger ID;Mandatsreferenz;Kundenreferenz (End-to-End);
    Sammlerreferenz;Lastschrift Ursprungsbetrag;
    Auslagenersatz Rueckslastschrift;Beguenstigter/Zahlungspflichtiger;
    Kontonummer/IBAN;BIC (SWIFT-Code);Betrag;Waehrung;Info

Both Girokonto and Tagesgeldkonto exports use the same shape.

Robust to:
- BOM at the start of the file
- ; or , as separator
- ISO-8859-1 / Windows-1252 / UTF-8 encoding
- German amount format ("-39,98" with comma as decimal)
- DD.MM.YY booking dates

Dedup key: SHA256 of (account_iban_hash | buchungstag | betrag |
verwendungszweck | sammlerreferenz). Same booking imported twice is
recognised by INSERT OR IGNORE on transactions.tx_hash.
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


# Header aliases — Sparkasse occasionally renames a column between
# product variants (e.g. "Glaeubiger-ID" vs. "Glaeubiger ID"). We
# normalise to lowercase + collapse whitespace + strip umlaut chrome
# so all variants map to the same canonical key.
_HEADER_ALIASES: dict[str, str] = {
    "auftragskonto": "auftragskonto",
    "buchungstag": "buchungstag",
    "valutadatum": "valutadatum",
    "buchungstext": "buchungstext",
    "verwendungszweck": "verwendungszweck",
    "glaeubiger id": "glaeubiger_id",
    "glaeubigerid": "glaeubiger_id",
    "glaeubiger-id": "glaeubiger_id",
    "mandatsreferenz": "mandatsreferenz",
    "kundenreferenz (end-to-end)": "kundenreferenz",
    "kundenreferenz end-to-end": "kundenreferenz",
    "kundenreferenz": "kundenreferenz",
    "sammlerreferenz": "sammlerreferenz",
    "lastschrift ursprungsbetrag": "lastschrift_betrag",
    "auslagenersatz rueckslastschrift": "rueckslastschrift",
    "beguenstigter/zahlungspflichtiger": "counterparty",
    "kontonummer/iban": "counterparty_iban",
    "bic (swift-code)": "counterparty_bic",
    "betrag": "betrag",
    "waehrung": "waehrung",
    "info": "info",
}


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
    """Sparkasse uses ';' but some exports come comma-separated. Probe
    the first line."""
    sample = "\n".join(text.splitlines()[:5])
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,\t")
    except csv.Error:
        # Default to ; (Sparkasse standard).
        class _D(csv.excel):
            delimiter = ";"
        return _D()


def _normalise_header(h: str) -> str:
    h = (h or "").strip().lower()
    # Strip BOM remnants and surrounding quotes
    h = h.lstrip("﻿").strip('"').strip()
    h = re.sub(r"\s+", " ", h)
    return _HEADER_ALIASES.get(h, h.replace(" ", "_").replace("/", "_"))


def _parse_amount(raw: str) -> float | None:
    """German-shaped amount → float. Returns None on garbage."""
    if raw is None:
        return None
    s = str(raw).strip().replace(" ", "")
    if not s:
        return None
    # "1.234,56" → 1234.56  (strip thousands "."; "," → ".")
    if "," in s and s.count(",") == 1:
        s = s.replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return None


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


def parse_csv(data: bytes | str, *, file_label: str = "") -> tuple[list[ImportRow], ImportReport]:
    """Parse the CSV bytes/text. Returns (rows, report). Doesn't
    touch the DB — call `import_csv` for the full pipeline.
    """
    report = ImportReport(file_label=file_label)
    text = data if isinstance(data, str) else _decode_bytes(data)
    if not text.strip():
        report.errors.append("CSV is empty")
        return [], report

    dialect = _detect_dialect(text)
    reader = csv.reader(io.StringIO(text), dialect=dialect)
    try:
        header = next(reader)
    except StopIteration:
        report.errors.append("CSV has no header line")
        return [], report

    norm_header = [_normalise_header(h) for h in header]
    required = {"auftragskonto", "buchungstag", "betrag"}
    missing = required - set(norm_header)
    if missing:
        report.errors.append(
            f"CSV header is missing required columns: {sorted(missing)}. "
            f"Got: {norm_header}"
        )
        return [], report

    out: list[ImportRow] = []
    for raw_row in reader:
        if not raw_row or all(not (c or "").strip() for c in raw_row):
            continue
        report.rows_seen += 1
        # Pad short rows so zip doesn't drop trailing fields.
        while len(raw_row) < len(norm_header):
            raw_row.append("")
        rec = dict(zip(norm_header, raw_row))

        amount = _parse_amount(rec.get("betrag", ""))
        if amount is None:
            report.rows_invalid += 1
            continue
        booking_date = normalise_date(rec.get("buchungstag", ""))
        if not booking_date:
            report.rows_invalid += 1
            continue
        value_date = normalise_date(rec.get("valutadatum", ""))
        account_iban = normalise_iban(rec.get("auftragskonto", ""))
        if not account_iban:
            # Without an account there's no useful dedup key — skip.
            report.rows_skipped += 1
            continue

        out.append(ImportRow(
            account_iban=account_iban,
            booking_date=booking_date,
            value_date=value_date,
            booking_text=str(rec.get("buchungstext", "") or "").strip()[:200],
            purpose=str(rec.get("verwendungszweck", "") or "").strip()[:500],
            amount=amount,
            currency=(str(rec.get("waehrung", "") or "EUR").strip().upper()[:8] or "EUR"),
            counterparty=str(rec.get("counterparty", "") or "").strip()[:200],
            counterparty_iban=normalise_iban(rec.get("counterparty_iban", "")),
            sammlerreferenz=str(rec.get("sammlerreferenz", "") or "").strip()[:80],
            mandatsreferenz=str(rec.get("mandatsreferenz", "") or "").strip()[:80],
            glaeubiger_id=str(rec.get("glaeubiger_id", "") or "").strip()[:64],
            info=str(rec.get("info", "") or "").strip()[:200],
        ))

    return out, report


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

    # Need a doc_id to anchor the statement. Either reuse an existing
    # Kontoauszug doc that's already linked to the same account (via a
    # prior statement, if any), OR create a stub document.
    with db._lock:
        existing = db._conn.execute(
            """SELECT s.doc_id FROM statements s
               WHERE s.account_id = ?
               ORDER BY s.id DESC LIMIT 1""",
            (account_id,),
        ).fetchone()
    doc_id: int
    if existing and existing["doc_id"]:
        doc_id = int(existing["doc_id"])
    else:
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


def import_csv(db, data: bytes | str, *, file_label: str = "",
               account_holder_hint: str = "") -> ImportReport:
    """End-to-end import: parse CSV → upsert account(s) → INSERT OR
    IGNORE transactions. INSERT OR IGNORE relies on the UNIQUE
    constraint on `transactions.tx_hash` for dedup, so re-importing
    a CSV that overlaps a previous one is a safe no-op for the
    overlap rows."""
    rows, report = parse_csv(data, file_label=file_label)
    if report.errors:
        return report
    if not rows:
        return report

    # Group rows by account IBAN — usually one CSV = one account but
    # the importer doesn't assume that.
    by_account: dict[str, list[ImportRow]] = {}
    for r in rows:
        by_account.setdefault(r.account_iban, []).append(r)

    period_start = ""
    period_end = ""
    for ib, rs in by_account.items():
        h = iban_hash(ib)
        last4 = ib[-4:] if len(ib) >= 4 else ""
        account_id = db.upsert_account(
            bank_name="Sparkasse",   # the CSV format itself is Sparkasse-specific
            iban=ib, iban_last4=last4, iban_hash=h,
            account_holder=account_holder_hint or "",
            currency=rs[0].currency or "EUR",
        )
        if ib not in report.accounts_touched:
            report.accounts_touched.append(ib)

        # transactions.statement_id is NOT NULL (legacy schema from
        # the v0.13 statement-extraction era). For CSV-imported rows
        # we maintain ONE container "statement" per account so the
        # FK constraint stays happy without a schema migration. The
        # container is purely structural: no period, no balances, no
        # extracted_text.
        stmt_id = _ensure_csv_container_statement(db, account_id)

        with db._lock:
            for r in rs:
                tx_hash = _row_hash(h, r)
                tx_type, category = _classify(r)
                # Compose a richer "purpose" so the existing /finance
                # search keeps finding things — booking_text first
                # (gives type context) then verwendungszweck.
                full_purpose = (r.booking_text + " " + r.purpose).strip()[:500]
                try:
                    cur = db._conn.execute(
                        "INSERT OR IGNORE INTO transactions "
                        "  (statement_id, account_id, booking_date, value_date, "
                        "   amount, currency, counterparty, counterparty_iban, "
                        "   purpose, tx_type, category, tx_hash) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (stmt_id, account_id, r.booking_date, r.value_date or r.booking_date,
                         r.amount, r.currency, r.counterparty, r.counterparty_iban,
                         full_purpose, tx_type, category, tx_hash),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("CSV import: insert failed: %s", exc)
                    report.errors.append(str(exc))
                    continue
                if cur.rowcount > 0:
                    report.rows_inserted += 1
                    if not period_start or r.booking_date < period_start:
                        period_start = r.booking_date
                    if not period_end or r.booking_date > period_end:
                        period_end = r.booking_date
                else:
                    report.rows_duplicate += 1
            db._conn.commit()

    report.period_start = period_start
    report.period_end = period_end
    logger.info(
        "CSV import %r: %d inserted, %d duplicates, %d invalid, %d skipped "
        "(across %d accounts, period %s..%s).",
        file_label, report.rows_inserted, report.rows_duplicate,
        report.rows_invalid, report.rows_skipped,
        len(report.accounts_touched), period_start, period_end,
    )
    return report
