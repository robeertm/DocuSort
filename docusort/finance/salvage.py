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

from .extractor import _coerce_float, _normalise_tx
from .pseudonymizer import iban_hash as _iban_hash


logger = logging.getLogger("docusort.finance.salvage")


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

    return {"stmt_id": stmt_id, "ok": True, "inserted": int(n),
            "skipped": skipped}


def salvage_all_empty(db, *, dry_run: bool = False) -> dict[str, Any]:
    """Try salvage on every statement with 0 transactions in the table.

    Returns a summary with per-statement results split into `recovered`
    (we re-inserted bookings) and `unrecoverable` (extra_json was empty,
    truncated, or contained no parseable transactions — those need a
    fresh LLM run)."""
    candidates = _empty_statement_ids(db)
    recovered: list[dict[str, Any]] = []
    unrecoverable: list[dict[str, Any]] = []
    for c in candidates:
        report = salvage_one(db, c["stmt_id"], dry_run=dry_run)
        if report.get("ok") and (report.get("inserted") or report.get("would_insert", 0) > 0):
            recovered.append(report)
        else:
            unrecoverable.append(report)
    return {
        "candidates": len(candidates),
        "recovered_count": len(recovered),
        "unrecoverable_count": len(unrecoverable),
        "recovered": recovered,
        "unrecoverable": unrecoverable,
        "dry_run": dry_run,
    }
