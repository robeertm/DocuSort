"""Recover transactions from `extra_json` when they never made it
into the transactions table.

Some statements end up in a state where the LLM successfully extracted
bookings (visible in `extra_json`) but the transactions table has 0
rows for that statement_id — caused by a re-extraction that returned
an empty list and overwrote the previous good data, or a parser bug
along the way. The raw response is still on disk and parses fine, so
we can salvage the data without burning another LLM call.

This module is purely read-extra-json + recompute-hash + re-insert.
No pseudonymization, no model calls. Safe to run on a quiet DB.
"""

from __future__ import annotations

import json
import logging
from hashlib import sha256
from typing import Any

from .extractor import _coerce_float, _normalise_date, _normalise_tx
from .pseudonymizer import iban_hash as _iban_hash


logger = logging.getLogger("docusort.finance.salvage")


def promote_bank_to_kontoauszug(db, *, dry_run: bool = False) -> dict[str, Any]:
    """Find documents that look like Kontoauszüge but were classified
    as `Bank` (with subcategory `Konto` / `Karte` or a kontoauszug-
    shaped subject) and switch their category to `Kontoauszug`.

    Older releases left this promotion to the bulk re-analyze worker,
    so anything that was never re-analysed sat at `Bank/Konto` —
    invisible to /finance and the day-picker. This is a one-shot
    cleanup that mirrors what the new ingest-time promotion does for
    fresh uploads.

    File renames are intentionally NOT performed: changing the on-disk
    library_path is a separate operation with much higher risk
    (broken sync targets, dead bookmarks). The DB row update is what
    /finance actually queries on."""
    sql = (
        "SELECT id, category, subcategory, subject, filename, library_path "
        "FROM documents "
        "WHERE deleted_at IS NULL AND category = 'Bank' "
        "  AND ("
        "      LOWER(COALESCE(subcategory,'')) IN ('konto', 'karte') "
        "      OR LOWER(COALESCE(subject,'')) LIKE '%kontoauszug%' "
        "      OR LOWER(COALESCE(subject,'')) LIKE '%girokonto%' "
        "      OR LOWER(COALESCE(subject,'')) LIKE '%tagesgeld%' "
        "      OR LOWER(COALESCE(subject,'')) LIKE '%kreditkart%' "
        "      OR LOWER(COALESCE(subject,'')) LIKE '%paypal%auszug%' "
        "      OR LOWER(COALESCE(subject,'')) LIKE '%depotauszug%' "
        "  )"
    )
    with db._lock:
        rows = db._conn.execute(sql).fetchall()
    items = [dict(r) for r in rows]
    if dry_run or not items:
        return {"candidates": len(items), "promoted": 0,
                "items": items, "dry_run": dry_run}
    with db._lock:
        ids = [(r["id"],) for r in items]
        db._conn.executemany(
            "UPDATE documents SET category = 'Kontoauszug', subcategory = '' "
            "WHERE id = ?", ids,
        )
        db._conn.commit()
    logger.info("Promoted %d Bank-classified docs to Kontoauszug.", len(items))
    return {"candidates": len(items), "promoted": len(items),
            "items": items, "dry_run": False}


def delete_absurd_amounts(db, *, threshold: float = 10_000_000.0,
                          dry_run: bool = False) -> dict[str, Any]:
    """Delete transactions whose absolute amount is over `threshold` €.

    Live data caught an LLM that misparsed a date into the amount
    field, producing a -322,147,719 € row that dominated every
    aggregate. The new sanity-cap in `_normalise_tx` keeps fresh
    extractions clean; this is the one-off cleanup for what already
    sits in the table.
    """
    with db._lock:
        rows = db._conn.execute(
            "SELECT id, statement_id, booking_date, amount, counterparty, purpose "
            "FROM transactions WHERE ABS(amount) > ?", (threshold,),
        ).fetchall()
    items = [dict(r) for r in rows]
    if not items:
        return {"candidates": 0, "deleted": 0, "items": [], "dry_run": dry_run}
    if dry_run:
        return {"candidates": len(items), "deleted": 0,
                "items": items, "dry_run": True}
    with db._lock:
        db._conn.executemany(
            "DELETE FROM transactions WHERE id = ?",
            [(r["id"],) for r in items],
        )
        db._conn.commit()
    logger.warning(
        "Deleted %d absurd-amount transaction(s) above %.2f € threshold.",
        len(items), threshold,
    )
    return {"candidates": len(items), "deleted": len(items),
            "items": items, "dry_run": False}


def normalise_existing_dates(db, *, dry_run: bool = False) -> dict[str, Any]:
    """One-off pass over the transactions table that converts every
    non-ISO booking_date / value_date into ISO YYYY-MM-DD.

    Up to v0.27.4 the extractor stored whatever shape the LLM emitted
    — for small local models that's often the source PDF's DD.MM.YYYY
    instead of ISO. Those rows break SQLite's strftime + every monthly
    chart on /finance. This is a pure data fix; no LLM calls.
    """
    with db._lock:
        rows = db._conn.execute(
            "SELECT id, booking_date, value_date FROM transactions "
            "WHERE NOT (booking_date GLOB '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]') "
            "   OR (value_date != '' AND NOT "
            "       (value_date GLOB '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]'))"
        ).fetchall()
    fixed = 0
    cleared = 0
    samples: list[dict[str, Any]] = []
    if dry_run:
        for r in rows[:20]:
            samples.append({
                "id": r["id"],
                "from_booking": r["booking_date"],
                "to_booking":   _normalise_date(r["booking_date"] or ""),
                "from_value":   r["value_date"],
                "to_value":     _normalise_date(r["value_date"] or ""),
            })
    else:
        with db._lock:
            for r in rows:
                old_b = r["booking_date"] or ""
                old_v = r["value_date"] or ""
                new_b = _normalise_date(old_b)
                new_v = _normalise_date(old_v)
                if not new_b and old_b:
                    cleared += 1
                if (new_b != old_b) or (new_v != old_v):
                    db._conn.execute(
                        "UPDATE transactions SET booking_date = ?, value_date = ? "
                        "WHERE id = ?",
                        (new_b, new_v, r["id"]))
                    fixed += 1
            db._conn.commit()

    return {
        "candidates": len(rows),
        "fixed": fixed,
        "cleared": cleared,
        "dry_run": dry_run,
        "samples": samples,
    }


def _empty_statement_ids(db) -> list[dict[str, Any]]:
    """Statements where transactions table has 0 rows AND
    `acknowledged_empty` is not set — those are the ones we want to
    try to recover."""
    with db._lock:
        rows = db._conn.execute(
            """SELECT s.id AS stmt_id, s.doc_id AS doc_id,
                      s.account_id AS account_id,
                      s.extra_json AS extra_json,
                      COALESCE(s.acknowledged_empty, 0) AS ack
               FROM statements s
               JOIN documents d ON d.id = s.doc_id
               WHERE d.deleted_at IS NULL
                 AND COALESCE(s.acknowledged_empty, 0) = 0
                 AND s.id NOT IN (SELECT statement_id FROM transactions)"""
        ).fetchall()
    return [dict(r) for r in rows]


def _try_parse_extra_json(blob: str) -> dict[str, Any] | None:
    """Parse extra_json. Returns None if blob is empty / unparseable
    (truncated responses fall here — those need re-extraction, not
    salvage)."""
    if not blob:
        return None
    try:
        return json.loads(blob)
    except (ValueError, TypeError):
        return None


def salvage_one(db, stmt_id: int, *, dry_run: bool = False) -> dict[str, Any]:
    """Re-insert transactions for one statement from its extra_json.

    Returns a small report dict so the caller can render per-row
    feedback. `dry_run=True` parses + counts but doesn't write.
    """
    with db._lock:
        s = db._conn.execute(
            "SELECT id, doc_id, account_id, extra_json, period_start, period_end, "
            "       opening_balance, closing_balance, currency, statement_no, "
            "       privacy_mode, file_hash, extraction_warning "
            "FROM statements WHERE id = ?",
            (stmt_id,),
        ).fetchone()
    if not s:
        return {"stmt_id": stmt_id, "ok": False, "reason": "statement not found"}

    data = _try_parse_extra_json(s["extra_json"] or "")
    if data is None:
        return {"stmt_id": stmt_id, "ok": False,
                "reason": "extra_json missing or unparseable"}

    txs_raw = data.get("transactions") or []
    if not isinstance(txs_raw, list) or not txs_raw:
        return {"stmt_id": stmt_id, "ok": False,
                "reason": "extra_json has no transactions"}

    iban = str(data.get("account_iban_token") or data.get("account_iban") or "").strip()
    holder_iban_hash = _iban_hash(iban) if iban else ""

    # Reconstruct Transaction objects, then convert back to dicts in the
    # shape upsert_statement expects.
    tx_payload: list[dict[str, Any]] = []
    skipped = 0
    for d in txs_raw:
        if not isinstance(d, dict):
            skipped += 1
            continue
        if "counterparty_iban" not in d and "counterparty_iban_token" in d:
            d["counterparty_iban"] = d.pop("counterparty_iban_token")
        tx = _normalise_tx(d)
        if tx is None:
            skipped += 1
            continue
        # Same hash scheme as bulk_reanalyze so we don't duplicate rows
        # if the user re-runs analyze later.
        key = (
            (holder_iban_hash or "no-iban") + "|" +
            tx.booking_date + "|" +
            f"{tx.amount:.2f}" + "|" +
            tx.purpose
        )
        tx_d = tx.as_dict()
        tx_d["tx_hash"] = sha256(key.encode("utf-8")).hexdigest()
        tx_payload.append(tx_d)

    if dry_run:
        return {
            "stmt_id": stmt_id, "ok": True, "dry_run": True,
            "would_insert": len(tx_payload), "skipped": skipped,
        }

    # Resolve account_id: prefer existing on the statement, fall back to
    # creating one from the iban we just parsed if needed.
    account_id = s["account_id"]
    if account_id is None and iban:
        account_id = db.upsert_account(
            bank_name=str(data.get("bank_name") or "Unbekannt"),
            iban=iban,
            iban_last4=iban[-4:] if len(iban) >= 4 else "",
            iban_hash=holder_iban_hash,
            account_holder=str(data.get("account_holder_token") or
                               data.get("account_holder") or ""),
            currency=str(data.get("currency") or "EUR").upper(),
        )

    # Reuse upsert_statement: it deletes-then-inserts for stmt_id, which
    # is what we want — there are 0 rows currently, so the delete is a
    # no-op and we get clean inserts with the new tx_hashes.
    db.upsert_statement(
        s["doc_id"],
        account_id=account_id,
        period_start=s["period_start"] or "",
        period_end=s["period_end"] or "",
        statement_no=s["statement_no"] or "",
        opening_balance=_coerce_float(s["opening_balance"]),
        closing_balance=_coerce_float(s["closing_balance"]),
        currency=s["currency"] or "EUR",
        file_hash=s["file_hash"] or "",
        privacy_mode=s["privacy_mode"] or "",
        transactions=tx_payload,
        extra_json=s["extra_json"] or "",
        extraction_warning=s["extraction_warning"] or "",
    )

    # Verify the insert took.
    with db._lock:
        n = db._conn.execute(
            "SELECT COUNT(*) AS n FROM transactions WHERE statement_id = ?",
            (stmt_id,),
        ).fetchone()["n"]

    if int(n) > 0:
        return {"stmt_id": stmt_id, "ok": True, "inserted": int(n),
                "skipped": skipped}

    # Inserted 0 rows even though we computed a payload — every tx_hash
    # collided with an existing transaction. This happens when the user
    # uploaded the same Kontoauszug twice (two PDFs, same period, same
    # account → same hashes), and the bookings already live on the older
    # statement_id. The current row is genuinely a duplicate; auto-ack
    # so the diag banner stops complaining about it, and surface the
    # sibling stmt_id so the user can double-check.
    sibling = _find_duplicate_sibling(db, stmt_id, account_id, s["period_start"] or "")
    if sibling is not None:
        with db._lock:
            db._conn.execute(
                "UPDATE statements SET acknowledged_empty = 1 WHERE id = ?",
                (stmt_id,),
            )
            db._conn.commit()
        return {
            "stmt_id": stmt_id, "ok": True, "inserted": 0,
            "skipped": skipped, "duplicate_of": sibling,
            "acknowledged": True,
        }

    return {"stmt_id": stmt_id, "ok": False, "inserted": 0,
            "skipped": skipped,
            "reason": "tx_hash collisions but no sibling found — manual investigation needed"}


def _find_duplicate_sibling(db, stmt_id: int, account_id: int | None,
                            period_start: str) -> int | None:
    """Find a statement on the same account + period_start that has
    transactions in the table. Returns its id or None."""
    if account_id is None or not period_start:
        return None
    with db._lock:
        row = db._conn.execute(
            "SELECT id FROM statements "
            "WHERE id != ? AND account_id = ? AND period_start = ? "
            "  AND id IN (SELECT DISTINCT statement_id FROM transactions) "
            "LIMIT 1",
            (stmt_id, account_id, period_start),
        ).fetchone()
    return int(row["id"]) if row else None


def salvage_all_empty(db, *, dry_run: bool = False) -> dict[str, Any]:
    """Try salvage on every statement with 0 transactions in the table.

    Three buckets in the summary:
      - recovered: we re-inserted bookings (or would, in dry-run mode)
      - duplicates: 0 inserted because every booking already lives on a
        sibling statement; auto-marked acknowledged_empty
      - unrecoverable: extra_json was empty, truncated, or contained no
        parseable transactions — those need a fresh LLM run
    """
    candidates = _empty_statement_ids(db)
    recovered: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    unrecoverable: list[dict[str, Any]] = []
    for c in candidates:
        report = salvage_one(db, c["stmt_id"], dry_run=dry_run)
        if report.get("duplicate_of"):
            duplicates.append(report)
        elif report.get("ok") and (report.get("inserted") or report.get("would_insert", 0) > 0):
            recovered.append(report)
        else:
            unrecoverable.append(report)
    return {
        "candidates": len(candidates),
        "recovered_count": len(recovered),
        "duplicate_count": len(duplicates),
        "unrecoverable_count": len(unrecoverable),
        "recovered": recovered,
        "duplicates": duplicates,
        "unrecoverable": unrecoverable,
        "dry_run": dry_run,
    }
