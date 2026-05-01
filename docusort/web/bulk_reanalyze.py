"""Module-level worker for the bulk statement re-extraction job.

Lifted out of `web/app.py` so the same code path can be triggered both
from the `/api/finance/reanalyze-all` HTTP endpoint and from the
post-upgrade hook in `main.py`, without going through a self-call to
the local web server.

The job key (`analyze-statements`) is shared with the existing
`/api/finance/analyze-all` endpoint, which means the dashboard banner,
progress polling, and notification fan-out all keep working without
changes. Only one of the two jobs can run at a time.

Pause / resume: the worker checks `activity.is_pause_requested(...)`
before every iteration. On a pending pause request it persists the
list of doc_ids it hasn't gotten to yet under
`meta.analyze_statements_pending` (JSON list of ints) and exits.
A resume call rebuilds targets from that list and starts a fresh
worker thread, picking up exactly where the previous run left off —
even across a service restart.
"""

from __future__ import annotations

import json
import logging
import threading
from hashlib import sha256
from pathlib import Path
from typing import Any

from ..config import AppSettings
from ..db import Database
from ..classifier import Classifier


logger = logging.getLogger("docusort.bulk_reanalyze")

_RESUME_META_KEY = "analyze_statements_pending"


def has_resumable_run(db: Database) -> bool:
    """Cheap pointer-check used by the UI to decide whether to show a
    Resume button. True iff a previous worker stopped with a non-empty
    pending-doc-ids list still in meta."""
    raw = db.meta_get(_RESUME_META_KEY) or ""
    if not raw:
        return False
    try:
        return bool(json.loads(raw))
    except (ValueError, TypeError):
        return False


def _persist_pending(db: Database, doc_ids: list[int]) -> None:
    db.meta_set(_RESUME_META_KEY, json.dumps([int(i) for i in doc_ids]))


def _clear_pending(db: Database) -> None:
    db.meta_set(_RESUME_META_KEY, "")


def _select_all_statement_targets(db: Database) -> list:
    with db._lock:
        return db._conn.execute(
            """SELECT d.id AS doc_id, d.category AS category,
                      COALESCE(d.subject, d.filename) AS subject,
                      d.extracted_text AS text,
                      d.library_path AS library_path
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


def _select_targets_by_id(db: Database, doc_ids: list[int]) -> list:
    if not doc_ids:
        return []
    placeholders = ",".join("?" * len(doc_ids))
    with db._lock:
        return db._conn.execute(
            f"""SELECT d.id AS doc_id, d.category AS category,
                       COALESCE(d.subject, d.filename) AS subject,
                       d.extracted_text AS text,
                       d.library_path AS library_path
                FROM documents d
                WHERE d.id IN ({placeholders})
                  AND d.deleted_at IS NULL
                  AND d.extracted_text IS NOT NULL AND d.extracted_text != ''""",
            [int(i) for i in doc_ids],
        ).fetchall()


def start_reanalyze_all_statements(
    settings: AppSettings,
    db: Database,
    classifier: Classifier,
    *,
    force_all: bool = True,
    resume: bool = False,
) -> dict[str, Any]:
    """Find every Kontoauszug-shaped document with stored OCR text and
    queue a fresh extraction for each one. Existing statement rows get
    replaced; user-set transaction categories are restored from the
    override table after re-insert.

    `resume=True` picks up where a previous paused run stopped: the
    pending doc_id list from `meta` becomes the target list instead
    of the regular WHERE-clause scan.
    """
    from .. import activity

    is_local = settings.ai.provider in ("openai_compat", "bridge")
    if settings.finance.local_only and not is_local:
        return {"started": False,
                "reason": "finance.local_only is on but provider is not local"}
    existing = activity.get_job("analyze-statements")
    if existing.running:
        return {"started": False, "reason": "already running",
                **existing.as_dict()}

    if resume:
        raw = db.meta_get(_RESUME_META_KEY) or ""
        try:
            pending_ids = json.loads(raw) if raw else []
        except (ValueError, TypeError):
            pending_ids = []
        if not pending_ids:
            return {"started": False, "reason": "nothing to resume",
                    "total": 0, "approved": [], "failed": []}
        rows = _select_targets_by_id(db, pending_ids)
    else:
        rows = _select_all_statement_targets(db)

    targets: list[tuple[int, str, str, str, str]] = []
    for r in rows:
        doc_id = int(r["doc_id"])
        cat = (r["category"] if "category" in r.keys() else None) or ""
        targets.append((
            doc_id, r["subject"] or f"doc {doc_id}",
            r["text"], cat, r["library_path"] or "",
        ))

    if not targets:
        return {"started": False, "reason": "no Kontoauszug documents found",
                "total": 0, "approved": [], "failed": []}

    do_pseudo = settings.finance.pseudonymize and not is_local

    activity.start_job("analyze-statements", total=len(targets))
    activity.clear_paused("analyze-statements")
    # Persist the full pending list up front. If the service crashes mid-
    # run, the next start with resume=True picks up the rest.
    _persist_pending(db, [t[0] for t in targets])

    def worker() -> None:
        from ..finance import StatementExtractor
        import time as _time

        extractor = StatementExtractor(
            classifier.provider, settings.ai.model,
            max_text_chars=max(settings.ai.max_text_chars, 32000),
            holder_names=settings.finance.holder_names,
        )
        # remaining tracks the still-untouched doc_ids so that a pause
        # request can persist the rest in one shot.
        remaining = [t[0] for t in targets]

        for idx, (doc_id, subject, text, category, library_path) in enumerate(targets):
            if activity.is_pause_requested("analyze-statements"):
                _persist_pending(db, remaining)
                activity.mark_paused("analyze-statements")
                logger.info(
                    "analyze-statements paused at %d/%d (%d remaining)",
                    idx, len(targets), len(remaining),
                )
                return
            if idx > 0:
                _time.sleep(0.6)
            activity.update_job(
                "analyze-statements",
                current=str(subject)[:120], current_doc_id=doc_id,
            )
            try:
                pdf_path = None
                if library_path:
                    p = Path(library_path)
                    if p.exists() and p.suffix.lower() == ".pdf":
                        pdf_path = p
                stmt = extractor.extract(
                    text, pseudonymize=do_pseudo,
                    pdf_path=pdf_path, ocr_settings=settings.ocr,
                )
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
                    extraction_warning=stmt.extraction_warning,
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
                if stmt.extraction_warning:
                    job.failed.append({"doc_id": doc_id,
                                       "error": stmt.extraction_warning})
                    activity.update_job(
                        "analyze-statements", done=idx + 1,
                        last_error="suspicious empty result",
                    )
                elif not stmt.transactions:
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
            # Pop only after the iteration finished — if we crashed
            # before reaching this line the doc_id stays in the
            # persisted list for a future resume to retry.
            try:
                remaining.remove(doc_id)
                _persist_pending(db, remaining)
            except ValueError:
                pass

        activity.finish_job("analyze-statements", current="")
        _clear_pending(db)
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
    return {"started": True, "force_all": force_all, "resumed": resume,
            **activity.get_job("analyze-statements").as_dict()}
