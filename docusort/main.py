"""DocuSort entrypoint.

Default mode starts BOTH the folder watcher and the FastAPI web UI in the same
process (web UI on port from config.yaml, default 8080).

    python -m docusort             # watcher + web UI
    python -m docusort --once      # process existing inbox files and exit
    python -m docusort --dry-run   # classify + log, no moves
    python -m docusort --no-web    # watcher only (legacy behaviour)

DOCUSORT_LOG_LEVEL (DEBUG/INFO/WARNING/ERROR) controls verbosity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import threading
from pathlib import Path

from . import __version__
from .classifier import Classification, Classifier
from .config import AppSettings, get_api_key, is_configured, load_config
from .db import Database, DocumentRecord, open_db
from .logger import setup_logger
from .ocr import OcrResult, extract_text, is_supported
from .organizer import organize
from .watcher import process_existing, run_forever, watch


def _sha256(path: Path, chunk_size: int = 1 << 16) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _build_pipeline(settings: AppSettings, classifier: Classifier | None, db: Database):
    log = logging.getLogger("docusort.pipeline")
    sem = threading.BoundedSemaphore(max(1, settings.ocr.max_parallel))

    def process(path: Path) -> None:
        if not path.exists() or not is_supported(path):
            return
        if classifier is None:
            log.info("Skipping %s — AI provider not configured. "
                     "Finish /setup, then restart docusort.", path.name)
            return
        with sem:
            _process_one(path)

    def _process_one(path: Path) -> None:
        if not path.exists():
            return
        log.info("Processing %s (%.1f KB)", path.name, path.stat().st_size / 1024)
        original_name = path.name
        original_size = path.stat().st_size

        # Duplicate check BEFORE OCR/Claude — SHA256 of raw file bytes.
        content_hash = _sha256(path)
        existing = db.find_by_hash(content_hash)
        if existing:
            log.info(
                "Duplicate of doc %d (%s) — skipping OCR+Claude, recording as duplicate",
                existing["id"], existing["filename"],
            )
            # Keep original in _Processed, don't re-file in library, register row.
            try:
                if settings.keep_original:
                    settings.paths.processed.mkdir(parents=True, exist_ok=True)
                    dup_target = settings.paths.processed / original_name
                    if dup_target.exists():
                        dup_target = settings.paths.processed / f"{content_hash[:8]}-{original_name}"
                    path.rename(dup_target)
                else:
                    path.unlink(missing_ok=True)

                dup_rec = DocumentRecord(
                    filename=existing["filename"],
                    original_name=original_name,
                    category=existing["category"],
                    subcategory=existing.get("subcategory") or "",
                    tags=existing.get("tags") or "[]",
                    doc_date=existing["doc_date"] or "",
                    sender=existing["sender"] or "",
                    subject=existing["subject"] or "",
                    confidence=existing["confidence"] or 0.0,
                    reasoning=f"Duplicate of doc {existing['id']}",
                    library_path=existing["library_path"],
                    processed_path="",
                    file_size=original_size,
                    page_count=existing.get("page_count"),
                    ocr_used=False,
                    model="",
                    input_tokens=0,
                    output_tokens=0,
                    cost_usd=0.0,
                    status="duplicate",
                    content_hash=content_hash,
                )
                db.insert_document(dup_rec)
            except Exception as exc:
                log.exception("Failed to record duplicate: %s", exc)
            return

        ocr_res: OcrResult = extract_text(path, settings.ocr)

        if not ocr_res.text:
            log.warning("No text extracted from %s – routing to review", path.name)
            cls = Classification(
                category="Sonstiges", date="", sender="Unbekannt",
                subject="OCR-fehlgeschlagen", confidence=0.0,
                reasoning="No text could be extracted",
            )
        else:
            try:
                cls = classifier.classify(ocr_res.text)
                log.info(
                    "Classified %s -> %s / %s / %s (conf=%.2f, $%.4f)",
                    path.name, cls.category, cls.date, cls.sender,
                    cls.confidence, cls.cost_usd,
                )
            except Exception as exc:
                log.exception("Classification failed for %s: %s", path.name, exc)
                cls = Classification(
                    category="Sonstiges", date="", sender="Unbekannt",
                    subject="Klassifizierung-fehlgeschlagen", confidence=0.0,
                    reasoning=str(exc),
                )

        target = organize(path, ocr_res.path, cls, settings)

        if settings.dry_run:
            return

        status = "filed" if cls.is_confident and cls.confidence > 0 else "review"
        if cls.confidence == 0 and cls.reasoning.startswith("No text"):
            status = "failed"

        import json as _json
        rec = DocumentRecord(
            filename=target.name,
            original_name=original_name,
            category=cls.category,
            subcategory=cls.subcategory,
            tags=_json.dumps(cls.tags),
            doc_date=cls.date,
            sender=cls.sender,
            subject=cls.subject,
            confidence=cls.confidence,
            reasoning=cls.reasoning,
            library_path=str(target),
            processed_path=str(settings.paths.processed / original_name)
                if settings.keep_original else "",
            file_size=original_size,
            page_count=ocr_res.page_count,
            ocr_used=ocr_res.ocr_used,
            model=cls.model,
            input_tokens=cls.input_tokens,
            output_tokens=cls.output_tokens,
            cache_creation_tokens=cls.cache_creation_tokens,
            cache_read_tokens=cls.cache_read_tokens,
            cost_usd=cls.cost_usd,
            status=status,
            content_hash=content_hash,
            # Store the full OCR text rather than truncating at the
            # LLM-input limit. Keeping more characters in the DB lets
            # second-pass extractors (Kontoauszug, Kassenzettel) work
            # on multi-page documents whose booking tables would
            # otherwise sit past the cutoff. The LLM call itself still
            # truncates to settings.ai.max_text_chars at send time.
            # We cap at a generous 200k-char ceiling to bound runaway
            # rows; a single bank statement is typically well under
            # that.
            extracted_text=ocr_res.text[:200_000],
        )
        try:
            doc_id = db.insert_document(rec)
            log.info("DB row %d written for %s", doc_id, target.name)
        except Exception as exc:
            log.exception("DB insert failed for %s: %s", target.name, exc)
            return

        # Receipts get a second-pass LLM extraction for the line items so
        # the analytics dashboard can do per-item aggregation. We only run
        # this for category=Kassenzettel to keep the cost bounded.
        if cls.category == "Kassenzettel" and ocr_res.text:
            try:
                from .receipts import ReceiptExtractor
                extractor = ReceiptExtractor(
                    classifier.provider, settings.ai.model,
                    max_text_chars=settings.ai.max_text_chars,
                    holder_names=settings.finance.holder_names,
                    pseudonymize=settings.finance.pseudonymize,
                )
                receipt = extractor.extract(ocr_res.text)
                db.upsert_receipt(
                    doc_id,
                    shop_name=receipt.shop_name,
                    shop_type=receipt.shop_type,
                    payment_method=receipt.payment_method,
                    total_amount=receipt.total_amount,
                    currency=receipt.currency,
                    receipt_date=receipt.receipt_date or cls.date,
                    items=[i.as_dict() for i in receipt.items],
                    extra_json=receipt.raw_response,
                )
                log.info(
                    "Receipt extracted for %s: shop=%s items=%d total=%s",
                    target.name, receipt.shop_name, len(receipt.items),
                    receipt.total_amount,
                )
            except Exception as exc:
                log.warning("Receipt extraction failed for %d: %s", doc_id, exc)

        # Bank statements get the same second-pass treatment, with the
        # privacy twist: by default we pseudonymise IBANs / addresses /
        # holder names before sending text to a cloud provider. Users
        # who flipped finance.local_only get statement extraction skipped
        # if the active provider isn't local.
        #
        # Also catch the case where the classifier picked the legacy
        # `Bank` category for what is clearly a statement (the subject
        # mentions Kontoauszug / Girokonto / Tagesgeld / Kreditkarte,
        # or the subcategory says Konto / Karte). Without this branch
        # those docs sit unprocessed in /finance — same path as the
        # heuristic in `backfill_statements`. After successful
        # extraction we promote the doc to category=Kontoauszug so
        # /finance picks it up everywhere else, mirroring the backfill.
        is_bank_statement_lookalike = False
        if cls.category == "Bank" and ocr_res.text:
            subj_l = (cls.subject or "").lower()
            sub_l  = (cls.subcategory or "").lower()
            if (sub_l in ("konto", "karte")
                    or "kontoauszug" in subj_l
                    or "girokonto"   in subj_l
                    or "tagesgeld"   in subj_l
                    or "kreditkart"  in subj_l):
                is_bank_statement_lookalike = True

        if (cls.category == "Kontoauszug" or is_bank_statement_lookalike) and ocr_res.text:
            local_providers = ("openai_compat",)
            is_local = settings.ai.provider in local_providers
            if settings.finance.local_only and not is_local:
                log.warning(
                    "Skipping statement extraction for %s: finance.local_only=true "
                    "but active provider %s is not local",
                    target.name, settings.ai.provider,
                )
            elif settings.finance.review_before_send and not is_local:
                # User opted in to manually approve every Kontoauszug before
                # the second-pass LLM call. Mark the doc as pending so it
                # appears in the /finance review queue; extraction runs
                # later when the user clicks "send" on the preview page.
                with db._lock:
                    db._conn.execute(
                        "UPDATE documents SET status = 'pending_review' WHERE id = ?",
                        (doc_id,),
                    )
                log.info(
                    "Statement extraction paused for %s — waiting for user "
                    "approval on /finance",
                    target.name,
                )
            else:
                try:
                    from .finance import StatementExtractor
                    from hashlib import sha256
                    extractor = StatementExtractor(
                        classifier.provider, settings.ai.model,
                        max_text_chars=max(settings.ai.max_text_chars, 32000),
                        holder_names=settings.finance.holder_names,
                    )
                    # Pseudonymise unless we're already on a local provider
                    # (no leak risk) OR the user explicitly turned it off.
                    do_pseudo = settings.finance.pseudonymize and not is_local
                    stmt = extractor.extract(ocr_res.text, pseudonymize=do_pseudo)

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

                    # Per-tx hash for dedup against overlapping statements.
                    tx_payload: list[dict] = []
                    for tx in stmt.transactions:
                        key = (
                            (stmt.iban_hash or "no-iban") + "|" +
                            tx.booking_date + "|" +
                            f"{tx.amount:.2f}" + "|" +
                            tx.purpose
                        )
                        tx_hash_val = sha256(key.encode("utf-8")).hexdigest()
                        d = tx.as_dict()
                        d["tx_hash"] = tx_hash_val
                        tx_payload.append(d)

                    # File-level dedup: if a statement with the same PDF
                    # bytes already exists, the document layer already
                    # rejected it; we only see new docs here. We still
                    # record the file hash so /finance can show "this
                    # statement is the original of these other docs".
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
                    # If the doc was filed under the legacy `Bank`
                    # category but the extractor actually pulled
                    # transactions, promote it to `Kontoauszug` so
                    # /finance and the diagnostics banner pick it up
                    # alongside everything else.
                    if is_bank_statement_lookalike and stmt.transactions:
                        with db._lock:
                            db._conn.execute(
                                "UPDATE documents SET category = 'Kontoauszug', "
                                "subcategory = '' WHERE id = ?",
                                (doc_id,),
                            )
                    log.info(
                        "Statement extracted for %s: bank=%s period=%s..%s tx=%d privacy=%s",
                        target.name, stmt.bank_name, stmt.period_start,
                        stmt.period_end, len(stmt.transactions),
                        stmt.privacy_mode,
                    )
                except Exception as exc:
                    log.warning("Statement extraction failed for %d: %s", doc_id, exc)

    return process


def _ensure_dirs(settings: AppSettings) -> None:
    for p in (
        settings.paths.inbox,
        settings.paths.library,
        settings.paths.review,
        settings.paths.processed,
        settings.paths.logs,
    ):
        p.mkdir(parents=True, exist_ok=True)


def _start_web(settings: AppSettings, db: Database, classifier: Classifier) -> None:
    """Run the FastAPI app with uvicorn in the current thread.

    If `web.ssl_cert` and `web.ssl_key` point to readable files, uvicorn is
    booted in TLS mode — required by browsers to register the upload
    service worker over a non-localhost URL.
    """
    import uvicorn
    from .web.app import create_app

    log = logging.getLogger("docusort.main")
    app = create_app(settings, db, classifier)

    ssl_kwargs: dict = {}
    cert, key = settings.web.ssl_cert, settings.web.ssl_key
    if cert and key:
        if Path(cert).exists() and Path(key).exists():
            ssl_kwargs = {"ssl_certfile": cert, "ssl_keyfile": key}
            log.info("Starting HTTPS on port %d (cert=%s)", settings.web.port, cert)
        else:
            log.warning(
                "SSL cert configured but not readable (%s / %s) — falling back to HTTP",
                cert, key,
            )

    uvicorn.run(
        app, host=settings.web.host, port=settings.web.port,
        log_level=os.environ.get("DOCUSORT_LOG_LEVEL", "info").lower(),
        access_log=False,
        **ssl_kwargs,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="docusort", description=__doc__)
    parser.add_argument("--once", action="store_true",
                        help="Process existing inbox files and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Classify and log but don't move anything")
    parser.add_argument("--no-web", action="store_true",
                        help="Run watcher only, no web UI")
    parser.add_argument("--check-update", action="store_true",
                        help="Check GitHub for a newer release and exit")
    parser.add_argument("--update", action="store_true",
                        help="Install the latest GitHub release and exit")
    parser.add_argument("--backfill-tags", action="store_true",
                        help="Re-classify existing docs to attach subcategory + tags, then exit")
    parser.add_argument("--backfill-dry-run", action="store_true",
                        help="Same as --backfill-tags but only prints what would change")
    parser.add_argument("--backfill-receipts", action="store_true",
                        help="Extract line items from existing Kassenzettel docs that don't have them yet")
    parser.add_argument("--backfill-statements", action="store_true",
                        help="Extract transactions from existing Kontoauszug docs that don't have them yet")
    parser.add_argument("--reocr-statements", action="store_true",
                        help="Re-read PDFs for Kontoauszug/Bank docs and refresh stored OCR text — fixes truncated text from earlier installs")
    parser.add_argument("--version", action="version", version=f"docusort {__version__}")
    args = parser.parse_args(argv)

    if args.check_update or args.update:
        from . import updater
        if args.check_update:
            info = updater.version_info()
            if info.get("error"):
                print(f"error: {info['error']}", file=sys.stderr)
                return 2
            print(f"current: {info['current']}")
            print(f"latest:  {info['latest']}")
            print("update available" if info["has_update"] else "up to date")
            return 0 if info["has_update"] else 1
        if args.update:
            try:
                result = updater.install_latest()
            except Exception as exc:
                print(f"update failed: {exc}", file=sys.stderr)
                return 2
            if not result.get("updated"):
                print(f"no update: {result.get('reason')}")
                return 0
            print(f"updated {result['from']} -> {result['to']}")
            print(f"pip: {result['pip']}")
            print("restart required: run `sudo systemctl restart docusort` or re-run start script")
            return 0

    settings = load_config()
    if args.dry_run:
        settings.dry_run = True

    setup_logger(settings.paths.logs, level=os.environ.get("DOCUSORT_LOG_LEVEL", "INFO"))
    log = logging.getLogger("docusort.main")
    log.info("DocuSort %s starting (dry_run=%s, once=%s, web=%s)",
             __version__, settings.dry_run, args.once, not args.no_web)

    _ensure_dirs(settings)

    db = open_db(settings.paths.db)

    classifier: Classifier | None = None
    if is_configured(settings):
        try:
            api_key = get_api_key(settings)
            classifier = Classifier(
                api_key, settings.ai, settings.categories,
                holder_names=settings.finance.holder_names,
                pseudonymize=settings.finance.pseudonymize,
            )
        except Exception as exc:
            log.error("Classifier init failed (provider=%s): %s",
                      settings.ai.provider, exc)
            classifier = None
    else:
        log.warning("AI provider not configured — web UI starts in setup mode, "
                    "watcher will skip classification until /setup is completed.")

    if (args.backfill_tags or args.backfill_dry_run
            or args.backfill_receipts or args.backfill_statements) and classifier is None:
        log.error("Cannot run backfill: AI provider not configured. "
                  "Open the web UI and finish /setup first.")
        return 2

    if args.backfill_tags or args.backfill_dry_run:
        from . import backfill as backfill_mod
        result = backfill_mod.backfill(
            settings, db, classifier, dry_run=args.backfill_dry_run,
        )
        log.info("Backfill done: %s", result)
        print(json.dumps(result, indent=2))
        return 0

    if args.backfill_receipts:
        from .receipts import backfill_receipts
        result = backfill_receipts(settings, db, classifier, dry_run=False)
        log.info("Receipt backfill done: %s", result)
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.reocr_statements:
        # No LLM call here — pure local re-OCR. Walks every Kontoauszug
        # / Bank document, re-reads the PDF, and overwrites the stored
        # OCR text with the full content. Earlier installs truncated at
        # ~12k chars, which cut off the booking table in multi-page
        # statements; refreshing the text unblocks --backfill-statements
        # and the /finance "re-extract empty" button.
        from .ocr import extract_text as _extract_text
        rows = db._conn.execute(
            """SELECT id, library_path, length(extracted_text) AS old_len
               FROM documents
               WHERE deleted_at IS NULL
                 AND category IN ('Kontoauszug', 'Bank')
                 AND library_path IS NOT NULL"""
        ).fetchall()
        processed: list[dict] = []
        failed: list[dict] = []
        for r in rows:
            doc_id = int(r["id"])
            path = Path(r["library_path"])
            if not path.exists():
                failed.append({"doc_id": doc_id, "error": f"file missing: {path}"})
                continue
            try:
                ocr_res = _extract_text(path, settings.ocr)
            except Exception as exc:
                failed.append({"doc_id": doc_id, "error": str(exc)})
                continue
            new_text = ocr_res.text[:200_000]
            db._conn.execute(
                "UPDATE documents SET extracted_text = ?, ocr_used = ? WHERE id = ?",
                (new_text, 1 if ocr_res.ocr_used else 0, doc_id),
            )
            processed.append({
                "doc_id": doc_id,
                "old_len": int(r["old_len"] or 0),
                "new_len": len(new_text),
                "ocr_used": ocr_res.ocr_used,
            })
        result = {"processed": processed, "failed": failed, "found": len(rows)}
        log.info("Re-OCR done: %s", result)
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.backfill_statements:
        from .finance.extractor import backfill_statements
        local_only = settings.finance.local_only and settings.ai.provider == "openai_compat"
        result = backfill_statements(
            settings, db, classifier, dry_run=False, local_only=local_only,
        )
        log.info("Statement backfill done: %s", result)
        print(json.dumps(result, indent=2, default=str))
        return 0

    pipeline = _build_pipeline(settings, classifier, db)

    if args.once:
        process_existing(settings.paths.inbox, pipeline)
        log.info("One-shot mode finished.")
        return 0

    observer = watch(settings.paths.inbox, pipeline, settings.stable_seconds)

    # Drain the inbox in the background so the web UI comes up immediately.
    # The OCR+Claude semaphore inside the pipeline keeps memory bounded even
    # when this thread races with watcher-spawned per-file threads.
    threading.Thread(
        target=process_existing,
        args=(settings.paths.inbox, pipeline),
        name="process-existing",
        daemon=True,
    ).start()

    if args.no_web:
        run_forever(observer)
        return 0

    try:
        _start_web(settings, db, classifier)
    finally:
        observer.stop()
        observer.join(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
