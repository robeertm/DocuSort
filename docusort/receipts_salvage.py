"""Recover Kassenzettel docs that were misclassified into Rechnungen
(or other adjacent categories) by the LLM at ingest time.

A receipt is supposed to land in `category = "Kassenzettel"` so the
receipt extractor can pull line items. But the classifier sometimes
picks "Rechnungen" because it sees "GmbH", "USt-IdNr", a SUMME and a
total — superficial overlap with a real invoice. Here we scan the
stored OCR text of docs in the wrong categories and bulk-revert the
ones that carry strong receipt signals.

Heuristic-only, no LLM call. The user reviews the candidate list and
confirms the revert, so false positives are recoverable.
"""

from __future__ import annotations

import logging
import re
from typing import Any


logger = logging.getLogger("docusort.receipts_salvage")


# Categories the classifier sometimes picks for an actual Kassenzettel.
# Sonstiges is a fallback the LLM uses when confidence is low; Quittung
# isn't a real category (no longer in the yaml) but old data might
# still have it.
_PROMOTABLE_CATEGORIES = (
    "Rechnungen",
    "Sonstiges",
    "Bank",
    "Vertraege",
    "Versand",
    "Quittung",
)


# Each entry: a regex (case-insensitive) that matches a strong receipt
# signal. We require at least TWO distinct hits to promote — single
# hits (e.g. just "girocard" appearing in a banking letter) are not
# enough.
_RECEIPT_SIGNALS: list[tuple[str, re.Pattern]] = [
    ("bon_nr",        re.compile(r"\bbon[\s\-]?nr\b|\bbon\s*[:\.]\s*\d", re.IGNORECASE)),
    ("ta_nr",         re.compile(r"\bta[\s\-]?nr\b", re.IGNORECASE)),
    ("bnr",           re.compile(r"\bbnr\b\s*\d", re.IGNORECASE)),
    ("beleg_nr",      re.compile(r"\bbeleg[\s\-]?nr\b", re.IGNORECASE)),
    ("kasse",         re.compile(r"\bkasse\s*[:\.]?\s*\d", re.IGNORECASE)),
    ("kassierer",     re.compile(r"\bkassierer\b", re.IGNORECASE)),
    ("terminal_id",   re.compile(r"\bterminal[\s\-]?(?:id|nummer)\b", re.IGNORECASE)),
    ("girocard",      re.compile(r"\bgirocard\b|\bec[\s\-]?cash\b|\btelecash\b|\bkontaktlos\b", re.IGNORECASE)),
    ("kartenzahlung", re.compile(r"\bkartenzahlung\b", re.IGNORECASE)),
    ("kundenbeleg",   re.compile(r"K[\s\-]+U[\s\-]+N[\s\-]+D[\s\-]+E[\s\-]+N[\s\-]+B[\s\-]+E[\s\-]+L[\s\-]+E[\s\-]+G", re.IGNORECASE)),
    ("zu_zahlen",     re.compile(r"\bzu\s+zahlen\b", re.IGNORECASE)),
    ("summe_eur",     re.compile(r"\bsumme\s+eur\b", re.IGNORECASE)),
    ("posten_n",      re.compile(r"\bposten\s*[:\.]?\s*\d", re.IGNORECASE)),
    ("vkst",          re.compile(r"\bvkst\s*[:\.]?\s*\d", re.IGNORECASE)),
    ("mwst_table",    re.compile(r"mwst\s*[a-z0-9]?\s*=?\s*\d{1,2}[,.]\d{2}\s*%", re.IGNORECASE)),
    ("steuer_table",  re.compile(r"steuer\s*%\s*brutto\s*netto", re.IGNORECASE)),
    ("tse",           re.compile(r"\btse[-\s]?(signatur|seriennummer|transaktion|hashalgorithm|publickey)", re.IGNORECASE)),
    ("pfand",         re.compile(r"\bpfand(wert|r[üu]ckgabe)?\b|leergut", re.IGNORECASE)),
    ("payback",       re.compile(r"\bpayback\b", re.IGNORECASE)),
    ("emv_aid",       re.compile(r"\bemv[\s\-]?(aid|daten)\b", re.IGNORECASE)),
    ("genehmigung",   re.compile(r"genehmigungs[\s\-]?nr|autorisierungsnr", re.IGNORECASE)),
    ("trafic_shop",   re.compile(
        r"\b(rewe|edeka|aldi|lidl|kaufland|penny|netto|tegut|norma|"
        r"rossmann|dm[\s\-]drogerie|m[üu]ller\s+drogerie|budni|"
        r"obi|bauhaus|hornbach|hagebau|toom|globus|"
        r"aral|shell|esso|jet|total|"
        r"deichmann|h&m|c&a|zara|primark|tom\s+tailor|"
        r"mediamarkt|saturn|"
        r"thalia|hugendubel|"
        r"cineplex|cinestar|uci\s+kinowelt"
        r")\b",
        re.IGNORECASE,
    )),
]


_INVOICE_BLOCKERS: list[re.Pattern] = [
    re.compile(r"\brechnungs?[\s\-]?nr(?:\.|ummer)?\s*[:\.]\s*[A-Z0-9\-]+", re.IGNORECASE),
    re.compile(r"\bzahlbar\s+bis\b|\bf[äa]lligkeit\s+am\b|\bzahlungsziel\b", re.IGNORECASE),
    re.compile(r"\biban\s+f[üu]r\s+(?:die\s+)?[üu]berweisung\b", re.IGNORECASE),
    re.compile(r"\bversicherungs?[\s\-]?nr(?:\.|ummer)?\b", re.IGNORECASE),
]


def text_looks_like_kassenzettel(text: str) -> tuple[bool, list[str]]:
    """Return (is_receipt, matched_signal_names).

    A doc qualifies when it has at least 2 distinct strong receipt
    signals AND no obvious invoice-only blocker (Rechnungsnummer +
    Fälligkeit / IBAN für Überweisung)."""
    if not text:
        return False, []
    matched: list[str] = []
    for name, rx in _RECEIPT_SIGNALS:
        if rx.search(text):
            matched.append(name)
    if len(matched) < 2:
        return False, matched
    # If the OCR also has clear invoice-only structure, don't promote
    # — even though some signals match (e.g. a real Vodafone-Rechnung
    # also has "Kartenzahlung" if paid by card, but it has a
    # Rechnungsnummer + Fälligkeit on top).
    blockers_hit = sum(1 for rx in _INVOICE_BLOCKERS if rx.search(text))
    if blockers_hit >= 2:
        return False, matched
    return True, matched


def scan_misclassified(db, *, limit: int = 200) -> list[dict[str, Any]]:
    """Find candidates for promotion to Kassenzettel. Returns one dict
    per doc with the matched signals, so the user can sanity-check
    before bulk-applying."""
    placeholders = ",".join("?" for _ in _PROMOTABLE_CATEGORIES)
    sql = (
        "SELECT id, category, subcategory, sender, subject, "
        "       library_path, filename, doc_date, extracted_text "
        "FROM documents "
        "WHERE deleted_at IS NULL "
        f"  AND category IN ({placeholders}) "
        "  AND extracted_text IS NOT NULL AND extracted_text != '' "
        "ORDER BY id DESC "
        "LIMIT ?"
    )
    params = list(_PROMOTABLE_CATEGORIES) + [limit * 5]
    with db._lock:
        rows = db._conn.execute(sql, params).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        is_receipt, signals = text_looks_like_kassenzettel(r["extracted_text"] or "")
        if not is_receipt:
            continue
        out.append({
            "id":          int(r["id"]),
            "category":    r["category"],
            "subcategory": r["subcategory"] or "",
            "sender":      r["sender"] or "",
            "subject":     r["subject"] or "",
            "doc_date":    r["doc_date"] or "",
            "filename":    r["filename"] or "",
            "library_path": r["library_path"] or "",
            "signals":     signals,
            "signal_count": len(signals),
        })
        if len(out) >= limit:
            break
    out.sort(key=lambda d: (-d["signal_count"], d["id"]))
    return out


def promote_to_kassenzettel(db, doc_ids: list[int]) -> dict[str, Any]:
    """Bulk-update the given doc IDs to category='Kassenzettel'.

    Subcategory is intentionally left untouched; the user can fine-tune
    it via the existing edit form. Library_path stays in place — the
    file rename is a separate concern (and rewriting paths breaks
    Synology / external sync targets), so the directory bucket on disk
    may diverge from the DB category until the user re-saves the doc.

    `status` is reset to 'review' so the doc shows up in the user's
    review queue, prompting them to verify the metadata."""
    if not doc_ids:
        return {"updated": 0}
    with db._lock:
        # Status='review' nudges the user; we deliberately do NOT
        # delete an existing receipt row, since a previous run might
        # already have populated it with valid items.
        db._conn.executemany(
            "UPDATE documents SET category = 'Kassenzettel', "
            "  subcategory = '', status = 'review' "
            "WHERE id = ? AND deleted_at IS NULL",
            [(int(i),) for i in doc_ids],
        )
        db._conn.commit()
    logger.warning(
        "Promoted %d misclassified docs to Kassenzettel.", len(doc_ids),
    )
    return {"updated": len(doc_ids)}
