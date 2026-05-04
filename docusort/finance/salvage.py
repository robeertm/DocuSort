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
    as something else (Bank, Sonstiges, Rechnungen, Vertraege, Steuer)
    and switch their category to `Kontoauszug`.

    Two-tier match:

    1. Cheap SQL hints — `Bank` with subcategory Konto/Karte or any
       category whose subject already mentions "Kontoauszug" /
       "Girokonto" / etc. These are unambiguous.
    2. OCR-text scan — any other promotable category gets its
       `extracted_text` checked against the same heuristic the
       classifier uses at ingest time
       (`classifier.text_looks_like_kontoauszug`). Catches the case
       where an LLM dumped a clear bank-statement OCR into Sonstiges
       because it ran out of output budget or returned low confidence.

    File renames are intentionally NOT performed: changing the on-disk
    library_path is a separate operation with much higher risk
    (broken sync targets, dead bookmarks). The DB row update is what
    /finance actually queries on."""
    from ..classifier import text_looks_like_kontoauszug

    sql_obvious = (
        "SELECT id, category, subcategory, subject, filename, library_path "
        "FROM documents "
        "WHERE deleted_at IS NULL "
        "  AND category != 'Kontoauszug' "
        "  AND ("
        "       (category = 'Bank' AND LOWER(COALESCE(subcategory,'')) IN ('konto','karte')) "
        "    OR LOWER(COALESCE(subject,'')) LIKE '%kontoauszug%' "
        "    OR LOWER(COALESCE(subject,'')) LIKE '%girokonto%' "
        "    OR LOWER(COALESCE(subject,'')) LIKE '%tagesgeld%' "
        "    OR LOWER(COALESCE(subject,'')) LIKE '%kreditkart%' "
        "    OR LOWER(COALESCE(subject,'')) LIKE '%paypal%auszug%' "
        "    OR LOWER(COALESCE(subject,'')) LIKE '%depotauszug%' "
        "  )"
    )
    # Tier-2 candidates: any document in a promotable category whose
    # subject didn't already trigger the cheap SQL match. We pull
    # extracted_text and run the heuristic on each one. Limit the
    # candidate set by skipping rows that obviously don't qualify
    # (image-only docs with no OCR text, deleted rows, …).
    sql_text = (
        "SELECT id, category, subcategory, subject, filename, library_path, "
        "       extracted_text "
        "FROM documents "
        "WHERE deleted_at IS NULL "
        "  AND category IN ('Bank','Sonstiges','Rechnungen','Vertraege','Steuer') "
        "  AND category != 'Kontoauszug' "
        "  AND extracted_text IS NOT NULL AND extracted_text != '' "
        "  AND LOWER(COALESCE(subject,'')) NOT LIKE '%kontoauszug%' "
        "  AND LOWER(COALESCE(subject,'')) NOT LIKE '%girokonto%' "
    )
    with db._lock:
        obvious = db._conn.execute(sql_obvious).fetchall()
        text_candidates = db._conn.execute(sql_text).fetchall()
    items_by_id: dict[int, dict] = {}
    for r in obvious:
        items_by_id[int(r["id"])] = {
            k: r[k] for k in (
                "id", "category", "subcategory", "subject",
                "filename", "library_path",
            )
        }
    for r in text_candidates:
        if int(r["id"]) in items_by_id:
            continue
        if not text_looks_like_kontoauszug(r["extracted_text"] or ""):
            continue
        items_by_id[int(r["id"])] = {
            k: r[k] for k in (
                "id", "category", "subcategory", "subject",
                "filename", "library_path",
            )
        }
    items = list(items_by_id.values())
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
    logger.info("Promoted %d misclassified docs to Kontoauszug.", len(items))
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


def bulk_deterministic_parse(
    db, *, dry_run: bool = False,
    only_low_confidence: bool = True,
    min_confidence: float = 0.85,
) -> dict[str, Any]:
    """Run the deterministic regex parser over every Kontoauszug
    that has OCR text on file. Apply the result when the parser is
    confident AND the saldo reconciles.

    `only_low_confidence`: skip documents whose existing statement
    already came from the deterministic parser (their `extra_json`
    starts with `deterministic:`). Default true so re-runs don't
    fight themselves.

    Reports counts per outcome:
      - `applied`         : statement overwritten with parser output
      - `skipped_lowconf` : parser ran but confidence < threshold
      - `skipped_existing`: doc already had a deterministic statement
      - `skipped_no_ocr`  : no OCR text → can't parse
      - `errors`          : parser blew up (logged, not raised)
    """
    from .parser import parse as _parse_det
    from .pseudonymizer import iban_hash as _iban_hash
    from hashlib import sha256

    with db._lock:
        rows = db._conn.execute(
            """SELECT d.id          AS doc_id,
                      d.extracted_text,
                      d.category,
                      s.id          AS stmt_id,
                      s.extra_json
               FROM documents d
               LEFT JOIN statements s ON s.doc_id = d.id
               WHERE d.deleted_at IS NULL
                 AND d.category = 'Kontoauszug'"""
        ).fetchall()

    counts = {
        "found": len(rows),
        "applied": 0,
        "skipped_lowconf": 0,
        "skipped_existing": 0,
        "skipped_no_ocr": 0,
        "errors": 0,
    }
    samples: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        doc_id = int(d["doc_id"])
        if not d["extracted_text"]:
            counts["skipped_no_ocr"] += 1
            continue
        if only_low_confidence and (d["extra_json"] or "").startswith("deterministic:"):
            counts["skipped_existing"] += 1
            continue
        try:
            result = _parse_det(d["extracted_text"])
        except Exception as exc:  # noqa: BLE001
            logger.exception("Bulk parse failed for doc %d: %s", doc_id, exc)
            counts["errors"] += 1
            continue
        ok = result.confidence >= min_confidence and result.saldo_consistent
        if not ok:
            counts["skipped_lowconf"] += 1
            if len(samples) < 20:
                samples.append({
                    "doc_id": doc_id,
                    "applied": False,
                    "layout": result.layout,
                    "confidence": round(result.confidence, 2),
                    "saldo_consistent": result.saldo_consistent,
                    "tx_count": len(result.statement.transactions),
                })
            continue
        if dry_run:
            counts["applied"] += 1
            if len(samples) < 20:
                samples.append({
                    "doc_id": doc_id,
                    "applied": True,
                    "layout": result.layout,
                    "confidence": round(result.confidence, 2),
                    "saldo_consistent": True,
                    "tx_count": len(result.statement.transactions),
                })
            continue
        # Apply
        s = result.statement
        account_id = None
        if s.iban:
            account_id = db.upsert_account(
                bank_name=s.bank_name or "Unbekannt",
                iban=s.iban, iban_last4=s.iban_last4,
                iban_hash=_iban_hash(s.iban),
                account_holder=s.account_holder or "",
                currency=s.currency or "EUR",
            )
        h_iban = _iban_hash(s.iban) if s.iban else "no-iban"
        tx_payload = []
        for t in s.transactions:
            key = f"{h_iban}|{t.booking_date}|{t.amount:.2f}|{t.purpose}"
            td = t.as_dict()
            td["tx_hash"] = sha256(key.encode("utf-8")).hexdigest()
            tx_payload.append(td)
        try:
            db.upsert_statement(
                doc_id, account_id=account_id,
                period_start=s.period_start, period_end=s.period_end,
                statement_no=s.statement_no,
                opening_balance=s.opening_balance,
                closing_balance=s.closing_balance,
                currency=s.currency or "EUR",
                file_hash="",
                privacy_mode="local",
                transactions=tx_payload,
                extra_json=f"deterministic:{result.layout}:conf={result.confidence:.2f}",
                extraction_warning=("; ".join(result.warnings)
                                    if result.warnings else ""),
            )
            with db._lock:
                if s.period_end:
                    db._conn.execute(
                        "UPDATE documents SET doc_date = ? WHERE id = ?",
                        (s.period_end, doc_id),
                    )
                db._conn.commit()
            counts["applied"] += 1
            if len(samples) < 20:
                samples.append({
                    "doc_id": doc_id,
                    "applied": True,
                    "layout": result.layout,
                    "confidence": round(result.confidence, 2),
                    "saldo_consistent": True,
                    "tx_count": len(s.transactions),
                })
        except Exception as exc:  # noqa: BLE001
            logger.exception("Bulk parse: upsert failed for doc %d: %s", doc_id, exc)
            counts["errors"] += 1

    logger.info(
        "Bulk deterministic parse: %d applied, %d low-confidence, "
        "%d already deterministic, %d no OCR, %d errors of %d total.",
        counts["applied"], counts["skipped_lowconf"],
        counts["skipped_existing"], counts["skipped_no_ocr"],
        counts["errors"], counts["found"],
    )
    return {**counts, "dry_run": dry_run, "samples": samples}


def scan_suspicious_amounts(db, *, limit: int = 50) -> dict[str, Any]:
    """Read-only diagnostic: list statements whose transactions look
    suspiciously big — likely OCR-comma damage that the strict
    `rescale_broken_amounts` heuristic couldn't auto-correct (because
    the saldo itself is also x100, or the saldo wasn't extracted at
    all).

    Surfaces, per statement:
      - `stmt_id`, `doc_id`, `period`, `bank_name`, `subject`
      - `tx_count`, opening / closing balance (raw, possibly broken)
      - `max_abs`, `sum_abs` of transactions
      - `all_integer` — every booking is a whole number (typical OCR
        fingerprint: "199143" instead of "1991,43")
      - `saldo_consistent` — opening + Σtx ≈ closing
      - `saldo_consistent_div100` — Σtx / 100 fits Δsaldo (the strict
        heuristic would have caught this)
      - `severity` — 'high' / 'medium' / 'low' so the UI can sort

    Top `limit` rows by `severity` × `sum_abs` so the worst offenders
    show first. No writes."""
    with db._lock:
        rows = db._conn.execute(
            """SELECT s.id          AS stmt_id,
                      s.doc_id      AS doc_id,
                      s.period_start, s.period_end,
                      s.opening_balance, s.closing_balance,
                      a.bank_name, d.subject,
                      COUNT(t.id)         AS tx_count,
                      COALESCE(MAX(ABS(t.amount)), 0) AS max_abs,
                      COALESCE(SUM(ABS(t.amount)), 0) AS sum_abs,
                      COALESCE(SUM(t.amount), 0)      AS sum_signed,
                      SUM(CASE
                          WHEN ABS(t.amount - ROUND(t.amount)) < 0.005
                          THEN 1 ELSE 0 END) AS integer_count
               FROM statements s
               JOIN documents  d ON d.id = s.doc_id AND d.deleted_at IS NULL
               LEFT JOIN accounts a ON a.id = s.account_id
               LEFT JOIN transactions t ON t.statement_id = s.id
               GROUP BY s.id
               HAVING tx_count > 0"""
        ).fetchall()

    items: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        n = int(d["tx_count"] or 0)
        if n == 0:
            continue
        max_abs = float(d["max_abs"] or 0.0)
        sum_abs = float(d["sum_abs"] or 0.0)
        sum_signed = float(d["sum_signed"] or 0.0)
        integer_count = int(d["integer_count"] or 0)
        all_integer = integer_count == n
        # Saldo consistency (only meaningful when both sides are present).
        opening = d["opening_balance"]
        closing = d["closing_balance"]
        saldo_consistent = False
        saldo_consistent_div100 = False
        if opening is not None and closing is not None:
            delta = float(closing) - float(opening)
            if abs(delta) > 0.01:
                err_raw = abs(sum_signed - delta)
                err_div100 = abs((sum_signed / 100.0) - delta)
                ratio_raw = err_raw / max(abs(delta), 1.0)
                ratio_div100 = err_div100 / max(abs(delta), 1.0)
                saldo_consistent = ratio_raw < 0.05
                saldo_consistent_div100 = (ratio_div100 < 0.05 and ratio_raw > 0.5)
            else:
                saldo_consistent = abs(sum_signed) < 1.0
        # Severity scoring — rough, just for sorting:
        #  - HIGH: max_abs > 100k, or saldo_div100 fingerprint
        #  - MED:  all_integer + max_abs > 10k
        #  - LOW:  big sum_abs alone (could be a real high-volume account)
        severity = "low"
        if saldo_consistent_div100:
            severity = "high"
        elif max_abs > 100_000:
            severity = "high"
        elif all_integer and max_abs > 10_000:
            severity = "medium"
        elif sum_abs > 500_000:
            severity = "medium"
        # Only return statements that look at least suspicious.
        if severity == "low" and not (all_integer and max_abs > 5000):
            continue
        items.append({
            "stmt_id": int(d["stmt_id"]),
            "doc_id":  int(d["doc_id"]),
            "bank_name":   d["bank_name"] or "",
            "subject":     d["subject"] or "",
            "period_start": d["period_start"] or "",
            "period_end":   d["period_end"] or "",
            "tx_count":    n,
            "max_abs":     round(max_abs, 2),
            "sum_abs":     round(sum_abs, 2),
            "opening":     None if opening is None else round(float(opening), 2),
            "closing":     None if closing is None else round(float(closing), 2),
            "all_integer":             all_integer,
            "saldo_consistent":        saldo_consistent,
            "saldo_consistent_div100": saldo_consistent_div100,
            "severity":    severity,
        })
    sev_order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda it: (sev_order.get(it["severity"], 9), -it["sum_abs"]))
    return {"items": items[:limit], "total_found": len(items)}


def rescale_statement_amounts(db, stmt_id: int, *, factor: float = 0.01,
                              also_saldo: bool = True) -> dict[str, Any]:
    """Manually rescale every transaction (and optionally the
    opening/closing balance) of a single statement by `factor`.

    Used by the UI when the user looks at a suspicious statement and
    decides "yes, divide everything in here by 100" — for the cases
    the auto-rescale heuristic can't reach because the saldo itself
    is corrupted too.

    `factor=0.01` is the typical /100 fix. The function recomputes
    `tx_hash` so future re-extracts dedup against the corrected rows
    instead of re-inserting the broken originals."""
    if factor <= 0 or factor >= 100:
        raise ValueError(f"factor out of safe range: {factor}")
    with db._lock:
        s = db._conn.execute(
            """SELECT s.id, s.opening_balance, s.closing_balance,
                      a.iban_hash
               FROM statements s
               LEFT JOIN accounts a ON a.id = s.account_id
               WHERE s.id = ?""",
            (stmt_id,),
        ).fetchone()
    if s is None:
        raise ValueError(f"statement {stmt_id} not found")
    iban_h = s["iban_hash"] or "no-iban"
    with db._lock:
        txs = db._conn.execute(
            "SELECT id, amount, booking_date, purpose "
            "FROM transactions WHERE statement_id = ?",
            (stmt_id,),
        ).fetchall()
    updated_rows = 0
    for t in txs:
        new_amount = round(float(t["amount"]) * factor, 2)
        key = (
            iban_h + "|" +
            (t["booking_date"] or "") + "|" +
            f"{new_amount:.2f}" + "|" +
            (t["purpose"] or "")
        )
        new_hash = sha256(key.encode("utf-8")).hexdigest()
        with db._lock:
            db._conn.execute(
                "UPDATE OR REPLACE transactions "
                "SET amount = ?, tx_hash = ? WHERE id = ?",
                (new_amount, new_hash, int(t["id"])),
            )
        updated_rows += 1
    saldo_changed = False
    if also_saldo:
        ob = s["opening_balance"]
        cb = s["closing_balance"]
        new_ob = None if ob is None else round(float(ob) * factor, 2)
        new_cb = None if cb is None else round(float(cb) * factor, 2)
        if (new_ob is not None) or (new_cb is not None):
            with db._lock:
                db._conn.execute(
                    "UPDATE statements "
                    "SET opening_balance = ?, closing_balance = ? "
                    "WHERE id = ?",
                    (new_ob, new_cb, stmt_id),
                )
            saldo_changed = True
    with db._lock:
        db._conn.commit()
    logger.warning(
        "Manually rescaled statement %d by factor %.4f: %d tx rows, "
        "saldo_changed=%s.", stmt_id, factor, updated_rows, saldo_changed,
    )
    return {
        "stmt_id": stmt_id,
        "factor":  factor,
        "updated_rows": updated_rows,
        "saldo_changed": saldo_changed,
    }


def rescale_broken_amounts(db, *, dry_run: bool = False) -> dict[str, Any]:
    """Apply the v0.29.0 saldo-mismatch / 100x-scale heuristic to rows
    already in the transactions table.

    For every statement that has an `opening_balance` and
    `closing_balance` recorded:

    - sum the existing transactions
    - if `Σtx / 100 ≈ closing - opening` (within 5 % residual) AND the
      raw sum is way off (> 50 % residual) AND every booking is
      integer-valued AND there are at least 2 bookings,
    - divide each amount by 100 and rewrite `tx_hash` so future
      re-extracts dedup correctly.

    Same rule as the live extractor — kept in lock-step so a
    re-extraction won't disagree with the migration result.

    No LLM calls. Pure data fix. Idempotent: a second run after a
    successful first run is a no-op (the residual collapses below the
    trigger threshold)."""
    with db._lock:
        stmts = db._conn.execute(
            """SELECT s.id        AS stmt_id,
                      s.opening_balance AS opening,
                      s.closing_balance AS closing,
                      a.iban_hash  AS iban_hash
               FROM statements s
               LEFT JOIN accounts a ON a.id = s.account_id
               WHERE s.opening_balance IS NOT NULL
                 AND s.closing_balance IS NOT NULL"""
        ).fetchall()
    candidates: list[dict[str, Any]] = []
    for s in stmts:
        opening = float(s["opening"])
        closing = float(s["closing"])
        delta = closing - opening
        if abs(delta) < 0.01:
            continue
        with db._lock:
            txs = db._conn.execute(
                "SELECT id, amount, tx_hash, booking_date, purpose "
                "FROM transactions WHERE statement_id = ?",
                (s["stmt_id"],),
            ).fetchall()
        if len(txs) < 2:
            continue
        all_integer = all(abs(t["amount"] - round(t["amount"])) < 0.005 for t in txs)
        if not all_integer:
            continue
        tx_sum = sum(float(t["amount"]) for t in txs)
        err_raw = abs(tx_sum - delta)
        err_div100 = abs((tx_sum / 100.0) - delta)
        ratio_raw = err_raw / max(abs(delta), 1.0)
        ratio_div100 = err_div100 / max(abs(delta), 1.0)
        if not (ratio_div100 < 0.05 and ratio_raw > 0.5):
            continue
        candidates.append({
            "stmt_id": int(s["stmt_id"]),
            "iban_hash": s["iban_hash"] or "no-iban",
            "tx_count": len(txs),
            "tx_sum_before": round(tx_sum, 2),
            "tx_sum_after":  round(tx_sum / 100.0, 2),
            "delta_target":  round(delta, 2),
            "txs": txs,
        })
    if dry_run:
        return {
            "candidates": len(candidates),
            "would_update_rows": sum(c["tx_count"] for c in candidates),
            "samples": [
                {k: v for k, v in c.items() if k != "txs"}
                for c in candidates[:20]
            ],
            "dry_run": True,
        }
    updated_rows = 0
    for c in candidates:
        iban_h = c["iban_hash"]
        for t in c["txs"]:
            new_amount = round(float(t["amount"]) / 100.0, 2)
            key = (
                iban_h + "|" +
                (t["booking_date"] or "") + "|" +
                f"{new_amount:.2f}" + "|" +
                (t["purpose"] or "")
            )
            new_hash = sha256(key.encode("utf-8")).hexdigest()
            with db._lock:
                # OR REPLACE handles the (extremely unlikely) case where
                # rescaling produces a tx_hash collision with another
                # row — the colliding row is a logical duplicate, so
                # collapsing them is the right behaviour.
                db._conn.execute(
                    "UPDATE OR REPLACE transactions "
                    "SET amount = ?, tx_hash = ? WHERE id = ?",
                    (new_amount, new_hash, int(t["id"])),
                )
            updated_rows += 1
    with db._lock:
        db._conn.commit()
    logger.warning(
        "Rescaled %d transactions across %d statements (OCR-comma "
        "recovery).", updated_rows, len(candidates),
    )
    return {
        "candidates": len(candidates),
        "updated_statements": len(candidates),
        "updated_rows": updated_rows,
        "dry_run": False,
    }


def dedupe_cross_statement_transactions(db, *, dry_run: bool = False) -> dict[str, Any]:
    """Drop transactions that appear in multiple statements of the
    same account on the same booking_date / amount / counterparty.

    Live data showed bookings from one Kontoauszug bleeding into a
    sibling: re-extracting an overlapping period sometimes inserted
    the row again under a slightly different tx_hash, so the unique
    constraint didn't catch it. This pass groups by
    (account_id, booking_date, ROUND(amount,2), LOWER(counterparty),
     LOWER(purpose)) and keeps only the FIRST tx in each group
    (lowest tx.id), deletes the rest.

    Conservative: requires a non-trivial counterparty (≥2 chars) so
    we don't collapse legitimate "—" placeholder rows. Idempotent."""
    with db._lock:
        groups = db._conn.execute(
            """SELECT MIN(t.id)        AS keep_id,
                      COUNT(*)         AS dup_count,
                      GROUP_CONCAT(t.id) AS all_ids,
                      t.account_id,
                      t.booking_date,
                      ROUND(t.amount, 2) AS amt,
                      LOWER(COALESCE(t.counterparty, '')) AS cp,
                      LOWER(COALESCE(t.purpose, ''))      AS pp
               FROM transactions t
               JOIN statements s ON s.id = t.statement_id
               JOIN documents  d ON d.id = s.doc_id AND d.deleted_at IS NULL
               WHERE t.account_id IS NOT NULL
                 AND t.booking_date IS NOT NULL AND t.booking_date != ''
                 AND LENGTH(COALESCE(t.counterparty,'')) >= 2
               GROUP BY t.account_id, t.booking_date,
                        ROUND(t.amount, 2),
                        LOWER(COALESCE(t.counterparty, '')),
                        LOWER(COALESCE(t.purpose, ''))
               HAVING dup_count > 1"""
        ).fetchall()
    delete_ids: list[int] = []
    samples: list[dict[str, Any]] = []
    for g in groups:
        all_ids = [int(x) for x in (g["all_ids"] or "").split(",") if x]
        keep = int(g["keep_id"])
        drop = [i for i in all_ids if i != keep]
        delete_ids.extend(drop)
        if len(samples) < 20:
            samples.append({
                "keep_id": keep,
                "drop_ids": drop,
                "booking_date": g["booking_date"],
                "amount": float(g["amt"]),
                "counterparty": g["cp"],
            })
    if dry_run or not delete_ids:
        return {
            "groups": len(groups),
            "would_delete": len(delete_ids),
            "deleted": 0,
            "samples": samples,
            "dry_run": dry_run,
        }
    with db._lock:
        db._conn.executemany(
            "DELETE FROM transactions WHERE id = ?",
            [(i,) for i in delete_ids],
        )
        db._conn.commit()
    logger.warning(
        "Cross-statement dedup: dropped %d duplicate booking(s) "
        "across %d group(s).", len(delete_ids), len(groups),
    )
    return {
        "groups": len(groups),
        "would_delete": len(delete_ids),
        "deleted": len(delete_ids),
        "samples": samples,
        "dry_run": False,
    }


def audit_statements(db, *, limit: int = 100) -> dict[str, Any]:
    """Read-only audit: every statement gets a health score plus a
    list of concrete issues so the user can see at a glance which
    Kontoauszüge are trustworthy and which need re-extraction.

    Issues we flag per statement:
      - `saldo_mismatch`: opening + Σtx differs from closing by > 1 €
      - `out_of_period`: a transaction's booking_date sits outside
        [period_start, period_end] (likely a row from a sibling
        statement that bled in)
      - `duplicate_in_other_stmt`: a tx_hash from this statement also
        appears in a different statement of the same account → the
        same booking got captured twice across overlapping uploads
      - `no_balance`: neither opening nor closing balance present
        (extractor missed the totals row entirely)
      - `no_period`: no period_start/period_end (year filter on
        /library will misplace the doc)

    Severity ranking:
      - HIGH: saldo_mismatch | duplicate_in_other_stmt | out_of_period
      - MED : no_balance | no_period
      - LOW : everything else
    """
    with db._lock:
        # All statements with rolled-up tx aggregates, plus the doc
        # subject so the UI can identify each row at a glance.
        rows = db._conn.execute(
            """SELECT s.id            AS stmt_id,
                      s.doc_id        AS doc_id,
                      s.account_id    AS account_id,
                      s.period_start, s.period_end,
                      s.opening_balance, s.closing_balance,
                      d.subject, d.doc_date,
                      a.bank_name,
                      COUNT(t.id)                AS tx_count,
                      COALESCE(SUM(t.amount),0)  AS tx_sum,
                      COALESCE(MIN(t.booking_date), '') AS first_tx,
                      COALESCE(MAX(t.booking_date), '') AS last_tx
               FROM statements s
               JOIN documents  d ON d.id = s.doc_id AND d.deleted_at IS NULL
               LEFT JOIN accounts a ON a.id = s.account_id
               LEFT JOIN transactions t ON t.statement_id = s.id
               GROUP BY s.id"""
        ).fetchall()

        # Build a tx_hash → list of statement_ids index so we can find
        # duplicates across statements without an N×N scan in Python.
        # Same hash in 2+ statements = same booking captured twice.
        dup_rows = db._conn.execute(
            """SELECT t.tx_hash, COUNT(DISTINCT t.statement_id) AS n,
                      GROUP_CONCAT(DISTINCT t.statement_id)     AS stmts
               FROM transactions t
               JOIN statements s ON s.id = t.statement_id
               JOIN documents  d ON d.id = s.doc_id AND d.deleted_at IS NULL
               WHERE t.tx_hash IS NOT NULL AND t.tx_hash != ''
               GROUP BY t.tx_hash
               HAVING n > 1"""
        ).fetchall()
    # stmt_id → set of duplicate-with stmt ids
    dup_index: dict[int, set[int]] = {}
    for r in dup_rows:
        ids = [int(x) for x in (r["stmts"] or "").split(",") if x]
        for sid in ids:
            dup_index.setdefault(sid, set()).update(i for i in ids if i != sid)

    items: list[dict[str, Any]] = []
    counts = {"high": 0, "medium": 0, "low": 0, "ok": 0}
    for r in rows:
        d = dict(r)
        issues: list[str] = []
        opening = d["opening_balance"]
        closing = d["closing_balance"]
        tx_sum = float(d["tx_sum"] or 0.0)
        tx_count = int(d["tx_count"] or 0)
        ps = (d["period_start"] or "").strip()
        pe = (d["period_end"]   or "").strip()

        if not ps and not pe:
            issues.append("no_period")
        if opening is None and closing is None:
            issues.append("no_balance")
        elif opening is not None and closing is not None and tx_count > 0:
            delta = float(closing) - float(opening)
            if abs(tx_sum - delta) > 1.0:
                issues.append("saldo_mismatch")
        # Out-of-period check: only when we know the period AND have
        # at least one tx — a run of out-of-period rows is the typical
        # "bookings from a sibling statement bled in" footprint.
        if ps and pe and tx_count > 0:
            first = d["first_tx"]
            last  = d["last_tx"]
            if first and first < ps:
                issues.append("out_of_period")
            elif last and last > pe:
                issues.append("out_of_period")
        # Duplicate-in-other-statement
        if int(d["stmt_id"]) in dup_index:
            issues.append("duplicate_in_other_stmt")

        if not issues:
            counts["ok"] += 1
            continue
        if any(i in issues for i in (
            "saldo_mismatch", "out_of_period", "duplicate_in_other_stmt"
        )):
            severity = "high"
        elif any(i in issues for i in ("no_balance", "no_period")):
            severity = "medium"
        else:
            severity = "low"
        counts[severity] += 1

        items.append({
            "stmt_id":       int(d["stmt_id"]),
            "doc_id":        int(d["doc_id"]),
            "subject":       d["subject"] or "",
            "doc_date":      d["doc_date"] or "",
            "bank_name":     d["bank_name"] or "",
            "period_start":  ps, "period_end": pe,
            "opening":       None if opening is None else round(float(opening), 2),
            "closing":       None if closing is None else round(float(closing), 2),
            "tx_count":      tx_count,
            "tx_sum":        round(tx_sum, 2),
            "first_tx":      d["first_tx"] or "",
            "last_tx":       d["last_tx"]  or "",
            "issues":        issues,
            "severity":      severity,
            "duplicate_with": sorted(dup_index.get(int(d["stmt_id"]), [])),
        })
    sev_order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda it: (sev_order[it["severity"]], -it["tx_count"]))
    return {
        "items":         items[:limit],
        "total_flagged": len(items),
        "counts":        counts,
    }


def compute_missing_opening_balances(db, *, dry_run: bool = False) -> dict[str, Any]:
    """Fill `opening_balance` from arithmetic on existing rows.

    Whenever a statement has `closing_balance` set and at least one
    transaction but `opening_balance IS NULL`, we know the answer:
    `opening = closing - Σtx`. Local 7B models often miss the
    "Anfangssaldo" line on the cover page even when it's clearly
    printed; this back-fills without another LLM call.

    Same arithmetic in the other direction for the rare statements
    where opening is set but closing isn't (model truncated before
    the totals row). Idempotent."""
    with db._lock:
        rows = db._conn.execute(
            """SELECT s.id        AS stmt_id,
                      s.opening_balance,
                      s.closing_balance,
                      COALESCE(SUM(t.amount), 0)      AS tx_sum,
                      COUNT(t.id)                     AS tx_count
               FROM statements s
               JOIN documents  d ON d.id = s.doc_id AND d.deleted_at IS NULL
               LEFT JOIN transactions t ON t.statement_id = s.id
               GROUP BY s.id
               HAVING tx_count > 0
                  AND (
                       (s.opening_balance IS NULL AND s.closing_balance IS NOT NULL)
                    OR (s.closing_balance IS NULL AND s.opening_balance IS NOT NULL)
                  )"""
        ).fetchall()
    items: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        if d["opening_balance"] is None:
            new_opening = round(float(d["closing_balance"]) - float(d["tx_sum"]), 2)
            items.append({
                "stmt_id": int(d["stmt_id"]),
                "fixed":   "opening",
                "value":   new_opening,
                "tx_sum":  round(float(d["tx_sum"]), 2),
                "closing": round(float(d["closing_balance"]), 2),
            })
        else:
            new_closing = round(float(d["opening_balance"]) + float(d["tx_sum"]), 2)
            items.append({
                "stmt_id": int(d["stmt_id"]),
                "fixed":   "closing",
                "value":   new_closing,
                "tx_sum":  round(float(d["tx_sum"]), 2),
                "opening": round(float(d["opening_balance"]), 2),
            })
    if dry_run or not items:
        return {
            "candidates": len(items),
            "updated": 0,
            "samples": items[:20],
            "dry_run": dry_run,
        }
    with db._lock:
        for it in items:
            col = "opening_balance" if it["fixed"] == "opening" else "closing_balance"
            db._conn.execute(
                f"UPDATE statements SET {col} = ? WHERE id = ?",
                (it["value"], it["stmt_id"]),
            )
        db._conn.commit()
    logger.info(
        "Filled missing balance for %d statement(s) via arithmetic.",
        len(items),
    )
    return {
        "candidates": len(items),
        "updated": len(items),
        "samples": items[:20],
        "dry_run": False,
    }


def align_doc_dates_to_statement_period(db, *, dry_run: bool = False) -> dict[str, Any]:
    """One-off migration: for every Kontoauszug whose `doc_date`
    differs from its statement's `period_end`, set `doc_date =
    period_end`.

    The classifier reads the print / generation date off the
    letterhead (e.g. "2026-04-30") and stores that as `doc_date`,
    even when the actual booking period is months earlier
    (10/2025). The /library year + month filter ranks docs by
    `doc_date`, so that mismatch makes statements show up in the
    wrong year. The statement extractor stores the real booking
    period in `statements.period_end`, which is the truer answer
    for "when does this Kontoauszug belong".

    Idempotent — once aligned, subsequent runs find nothing to
    update."""
    with db._lock:
        rows = db._conn.execute(
            """SELECT d.id AS doc_id, d.doc_date AS old_date,
                      s.period_end AS new_date,
                      d.subject
               FROM documents d
               JOIN statements s ON s.doc_id = d.id
               WHERE d.deleted_at IS NULL
                 AND d.category = 'Kontoauszug'
                 AND s.period_end IS NOT NULL
                 AND s.period_end != ''
                 AND COALESCE(d.doc_date, '') != s.period_end"""
        ).fetchall()
    items = [dict(r) for r in rows]
    if dry_run or not items:
        return {
            "candidates": len(items),
            "updated": 0,
            "samples": items[:20],
            "dry_run": dry_run,
        }
    with db._lock:
        db._conn.executemany(
            "UPDATE documents SET doc_date = ? WHERE id = ?",
            [(r["new_date"], r["doc_id"]) for r in items],
        )
        db._conn.commit()
    logger.warning(
        "Aligned doc_date to statement.period_end for %d Kontoauszüge.",
        len(items),
    )
    return {
        "candidates": len(items),
        "updated": len(items),
        "samples": items[:20],
        "dry_run": False,
    }


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
