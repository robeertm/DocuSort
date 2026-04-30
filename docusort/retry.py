"""Re-classify a document that previously failed or landed in review.

Uses stored extracted_text when present so we don't re-pay for OCR; falls
back to a fresh OCR run if the text is empty. On success the physical file
is moved to its new category folder and the DB row is updated in place
(keeping the same `id` but accumulating token usage).
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from .classifier import Classifier
from .config import AppSettings
from .db import Database
from .ocr import extract_text
from .organizer import _parse_iso_date, _slug, build_filename, _uniquify  # type: ignore


logger = logging.getLogger("docusort.retry")


def retry_document(
    doc_id: int,
    settings: AppSettings,
    classifier: Classifier,
    db: Database,
) -> dict[str, Any]:
    doc = db.get(doc_id)
    if not doc:
        raise ValueError(f"document {doc_id} not found")

    text = doc.get("extracted_text") or ""
    if not text:
        # extracted_text was never stored — re-OCR from whichever file we still have.
        source = Path(doc.get("library_path") or doc.get("processed_path") or "")
        if not source.exists():
            raise ValueError(f"source file missing: {source}")
        logger.info("retry %d: no stored text, re-OCRing %s", doc_id, source)
        ocr_res = extract_text(source, settings.ocr)
        text = ocr_res.text

    if not text:
        raise ValueError("no extractable text")

    cls = classifier.classify(text)
    logger.info(
        "retry %d classified -> %s / %s (conf=%.2f, $%.4f)",
        doc_id, cls.category, cls.date, cls.confidence, cls.cost_usd,
    )

    # Move the file to its new home.
    current = Path(doc["library_path"])
    if not current.exists():
        raise ValueError(f"library file missing: {current}")

    year = _parse_iso_date(cls.date).strftime("%Y")
    if cls.is_confident:
        target_dir = settings.paths.library / year / cls.category
        if cls.subcategory:
            target_dir = target_dir / cls.subcategory
    else:
        target_dir = settings.paths.review
    target_dir.mkdir(parents=True, exist_ok=True)
    target = _uniquify(
        target_dir / build_filename(
            cls, settings.filename_template, settings.max_filename_length,
            current.suffix,
        )
    )
    shutil.move(str(current), str(target))

    status = "filed" if cls.is_confident else "review"
    db.update_classification(
        doc_id, cls,
        library_path=str(target),
        filename=target.name,
        status=status,
        extracted_text=text[: settings.claude.max_text_chars],
    )

    # If the reclassification landed on Kontoauszug (or a Bank-tagged
    # statement-lookalike), kick off the second-pass extraction in the
    # same call. Without this step the user has to click "Auswerten"
    # on every single doc — defeats the whole "Re-queue all" workflow.
    statement_result: dict[str, Any] | None = None
    is_bank_lookalike = False
    if cls.category == "Bank":
        subj_l = (cls.subject or "").lower()
        sub_l  = (cls.subcategory or "").lower()
        if (sub_l in ("konto", "karte")
                or "kontoauszug" in subj_l
                or "girokonto"   in subj_l
                or "tagesgeld"   in subj_l
                or "kreditkart"  in subj_l):
            is_bank_lookalike = True

    if cls.category == "Kontoauszug" or is_bank_lookalike:
        local_providers = ("openai_compat", "bridge")
        is_local = settings.ai.provider in local_providers
        # Same gates as the primary pipeline in main.py: respect the
        # user's local-only and review-before-send settings.
        if settings.finance.local_only and not is_local:
            logger.info(
                "retry %d: skipped statement extraction (local_only=true, "
                "provider=%s)", doc_id, settings.ai.provider,
            )
        elif settings.finance.review_before_send and not is_local:
            with db._lock:
                db._conn.execute(
                    "UPDATE documents SET status = 'pending_review' WHERE id = ?",
                    (doc_id,),
                )
            logger.info("retry %d: paused for user review (cloud provider)", doc_id)
        else:
            try:
                statement_result = _extract_statement_inline(
                    doc_id=doc_id, text=text, classifier=classifier,
                    settings=settings, db=db, target=target,
                    is_local=is_local, was_lookalike=is_bank_lookalike,
                )
                logger.info("retry %d: statement extracted (%d transactions)",
                            doc_id, statement_result.get("transactions", 0))
            except Exception as exc:
                logger.warning("retry %d: statement extraction failed: %s",
                               doc_id, exc)

    return {
        "doc_id": doc_id,
        "status": status,
        "category": cls.category,
        "confidence": cls.confidence,
        "cost_usd": cls.cost_usd,
        "library_path": str(target),
        "statement": statement_result,
    }


def _extract_statement_inline(*, doc_id: int, text: str,
                              classifier: Classifier,
                              settings: AppSettings, db: Database,
                              target: Path, is_local: bool,
                              was_lookalike: bool) -> dict[str, Any]:
    """Run the second-pass statement extraction and persist the result.
    Mirrors the equivalent block in main.py — kept inline here so a
    bulk re-queue can complete without a separate user click."""
    from hashlib import sha256
    from .finance import StatementExtractor

    extractor = StatementExtractor(
        classifier.provider, settings.ai.model,
        max_text_chars=max(settings.ai.max_text_chars, 32000),
        holder_names=settings.finance.holder_names,
    )
    do_pseudo = settings.finance.pseudonymize and not is_local
    stmt = extractor.extract(text, pseudonymize=do_pseudo)

    account_id: int | None = None
    if stmt.iban_hash:
        account_id = db.upsert_account(
            bank_name=stmt.bank_name or "Unbekannt",
            iban=stmt.iban,
            iban_last4=stmt.iban_last4,
            iban_hash=stmt.iban_hash,
            account_holder=stmt.account_holder,
            currency=stmt.currency,
        )

    tx_payload: list[dict] = []
    for tx in stmt.transactions:
        key = (
            (stmt.iban_hash or "no-iban") + "|" +
            (tx.booking_date or "") + "|" +
            f"{tx.amount:.2f}" + "|" +
            (tx.purpose or "")
        )
        d = tx.as_dict()
        d["tx_hash"] = sha256(key.encode("utf-8")).hexdigest()
        tx_payload.append(d)

    file_hash = ""
    try:
        with open(target, "rb") as fh:
            file_hash = sha256(fh.read()).hexdigest()
    except OSError:
        pass

    db.upsert_statement(
        doc_id,
        account_id=account_id,
        period_start=stmt.period_start,
        period_end=stmt.period_end,
        statement_no=stmt.statement_no,
        opening_balance=stmt.opening_balance,
        closing_balance=stmt.closing_balance,
        currency=stmt.currency,
        file_hash=file_hash,
        privacy_mode=stmt.privacy_mode,
        transactions=tx_payload,
        extra_json=stmt.raw_response,
    )

    # Promote a Bank lookalike to Kontoauszug so /finance picks it up
    # everywhere else (mirrors what backfill_statements + main.py do).
    if was_lookalike:
        with db._lock:
            db._conn.execute(
                "UPDATE documents SET category = 'Kontoauszug' WHERE id = ?",
                (doc_id,),
            )
            db._conn.commit()

    return {
        "transactions":  len(tx_payload),
        "period_start":  stmt.period_start,
        "period_end":    stmt.period_end,
        "iban_last4":    stmt.iban_last4,
    }
