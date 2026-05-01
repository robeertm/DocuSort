"""Module-level worker for the bulk statement re-extraction job.

Lifted out of `web/app.py` so the same code path can be triggered both
from the `/api/finance/reanalyze-all` HTTP endpoint and from the
post-upgrade hook in `main.py`, without going through a self-call to
the local web server.

The job key (`analyze-statements`) is shared with the existing
`/api/finance/analyze-all` endpoint, which means the dashboard banner,
progress polling, and notification fan-out all keep working without
changes. Only one of the two jobs can run at a time — the helper
short-circuits with `started=False` when one is already in flight.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from ..config import AppSettings
from ..db import Database
from ..classifier import Classifier


logger = logging.getLogger("docusort.bulk_reanalyze")


def start_reanalyze_all_statements(
    settings: AppSettings,
    db: Database,
    classifier: Classifier,
    *,
    force_all: bool = True,
) -> dict[str, Any]:
    """Find every Kontoauszug-shaped document with stored OCR text and
    queue a fresh extraction for each one. Existing statement rows get
    replaced; user-set transaction categories are restored from the
    override table after re-insert."""
    from .. import activity

    is_local = settings.ai.provider in ("openai_compat", "bridge")
    if settings.finance.local_only and not is_local:
        return {"started": False, "reason": "finance.local_only is on but provider is not local"}
    existing = activity.get_job("analyze-statements")
    if existing.running:
        return {"started": False, "reason": "already running",
                **existing.as_dict()}

    with db._lock:
        rows = db._conn.execute(
            """SELECT d.id AS doc_id, d.category AS category,
                      COALESCE(d.subject, d.filename) AS subject,
                      d.extracted_text AS text
               FROM documents d
               WHERE d.deleted_at IS NULL
                 AND d.extracted_text IS NOT NULL AND d.extracted_text != ''
                 AND (
                   d.category = 'Kontoauszug'
                   OR (d.category = 'Bank' AND (
                     d.subcategory = 'Konto'
                     OR d.subcategory = 'Karte'
                     OR LOWER(COALESCE(d.subject,'')) LIKE '%kontoauszug%'
                     OR LOWER(COALESCE(d.subject,'')) LIKE '%girokonto%'
                     OR LOWER(COALESCE(d.subject,'')) LIKE '%tagesgeld%'
                     OR LOWER(COALESCE(d.subject,'')) LIKE '%kreditkart%'
                     OR LOWER(COALESCE(d.subject,'')) LIKE '%paypal%auszug%'
                   ))
                 )
               ORDER BY d.doc_date DESC, d.id DESC"""
        ).fetchall()

    targets: list[tuple[int, str, str, str]] = []
    for r in rows:
        doc_id = int(r["doc_id"])
        cat = (r["category"] if "category" in r.keys() else None) or ""
        targets.append((doc_id, r["subject"] or f"doc {doc_id}", r["text"], cat))

    if not targets:
        return {"started": False, "reason": "no Kontoauszug documents found",
                "total": 0, "approved": [], "failed": []}

    do_pseudo = settings.finance.pseudonymize and not is_local
    activity.start_job("analyze-statements", total=len(targets))

    def worker() -> None:
        from ..finance import StatementExtractor
        from hashlib import sha256
        import time as _time

        extractor = StatementExtractor(
            classifier.provider, settings.ai.model,
            max_text_chars=max(settings.ai.max_text_chars, 32000),
            holder_names=settings.finance.holder_names,
        )
        for idx, (doc_id, subject, text, category) in enumerate(targets):
            if idx > 0:
                _time.sleep(0.6)
            activity.update_job(
                "analyze-statements",
                current=str(subject)[:120], current_doc_id=doc_id,
            )
            try:
                stmt = extractor.extract(text, pseudonymize=do_pseudo)
                account_id = None
                if stmt.iban_hash:
                    account_id = db.upsert_account(
                        bank_name=stmt.bank_name or "Unbekannt",
                        iban=stmt.iban, iban_last4=stmt.iban_last4,
                        iban_hash=stmt.iban_hash,
                        account_holder=stmt.account_holder,
                        currency=stmt.currency,
                    )
                tx_payload = []
                for tx in stmt.transactions:
                    key = (
                        (stmt.iban_hash or "no-iban") + "|" +
                        tx.booking_date + "|" + f"{tx.amount:.2f}" +
                        "|" + tx.purpose
                    )
                    d = tx.as_dict()
                    d["tx_hash"] = sha256(key.encode("utf-8")).hexdigest()
                    tx_payload.append(d)
                db.upsert_statement(
                    doc_id, account_id=account_id,
                    period_start=stmt.period_start,
                    period_end=stmt.period_end,
                    statement_no=stmt.statement_no,
                    opening_balance=stmt.opening_balance,
                    closing_balance=stmt.closing_balance,
                    currency=stmt.currency, file_hash="",
                    privacy_mode=stmt.privacy_mode,
                    transactions=tx_payload,
                    extra_json=stmt.raw_response,
                )
                if category == "Bank" and stmt.transactions:
                    with db._lock:
                        db._conn.execute(
                            "UPDATE documents SET category = 'Kontoauszug', "
                            "subcategory = '' WHERE id = ?",
                            (doc_id,),
                        )
                        db._conn.commit()
                job = activity.get_job("analyze-statements")
                if not stmt.transactions:
                    job.failed.append({"doc_id": doc_id,
                                       "error": "no transactions extracted"})
                    activity.update_job(
                        "analyze-statements", done=idx + 1,
                        last_error="empty result",
                    )
                else:
                    job.approved.append(doc_id)
                    activity.update_job("analyze-statements", done=idx + 1)
            except Exception as exc:
                job = activity.get_job("analyze-statements")
                job.failed.append({"doc_id": doc_id, "error": str(exc)})
                activity.update_job(
                    "analyze-statements", done=idx + 1,
                    last_error=str(exc),
                )
        activity.finish_job("analyze-statements", current="")
        try:
            from .. import notifier as _n
            job = activity.get_job("analyze-statements")
            ok, fail_n = len(job.approved), len(job.failed)
            _n.fire(_n.NotificationEvent(
                kind="bulk_done",
                title=f"Statement re-analysis done — {ok} ok, {fail_n} failed",
                body=f"Re-processed {ok + fail_n} of {len(targets)} Kontoauszüge.",
            ))
        except Exception:
            pass

    threading.Thread(
        target=worker, name="analyze-statements", daemon=True,
    ).start()
    return {"started": True, "force_all": force_all,
            **activity.get_job("analyze-statements").as_dict()}
