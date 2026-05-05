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


def _doc_url(settings: AppSettings, doc_id: int) -> str | None:
    """Best-effort clickable URL for a document — used in notifications.
    The watcher has no incoming HTTP request to derive the public host
    from, so we rely on settings.web.host/port and assume HTTP unless
    an SSL cert is configured. None if we can't make a sensible guess."""
    host = (settings.web.host or "").strip()
    if not host or host in ("0.0.0.0", "::"):
        return None
    scheme = "https" if (settings.web.ssl_cert and settings.web.ssl_key) else "http"
    return f"{scheme}://{host}:{settings.web.port}/document/{doc_id}"


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

        # Fire a notification (if any channel is configured + the
        # corresponding event is enabled). Failures here can never
        # break the pipeline — the dispatcher catches and logs.
        try:
            from . import notifier as _n
            ev_kind = (
                "doc_failed" if status == "failed"
                else "doc_review" if status == "review"
                else "doc_filed"
            )
            subject_line = cls.subject or target.name
            sender_line  = cls.sender or "?"
            body = (
                f"Category:    {cls.category}"
                + (f" / {cls.subcategory}" if cls.subcategory else "")
                + f"\nSender:      {sender_line}"
                f"\nSubject:     {subject_line}"
                f"\nDate:        {cls.date or '—'}"
                f"\nConfidence:  {cls.confidence:.0%}"
                f"\nFile:        {target.name}"
            )
            title_prefix = {
                "doc_failed": "❌ Classification failed",
                "doc_review": "⚠️  New review document",
                "doc_filed":  "📄 Document filed",
            }[ev_kind]
            _n.fire(_n.NotificationEvent(
                kind=ev_kind,
                title=f"{title_prefix}: {subject_line[:80]}",
                body=body,
                doc_id=doc_id,
                url=_doc_url(settings, doc_id),
            ))
        except Exception as exc:
            log.warning("Notification fire failed: %s", exc)

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

        # v0.33.0 removed the second-pass Kontoauszug extraction.
        # Bank statements are stored as documents like everything
        # else; transaction data comes from CSV exports the user
        # uploads on /finance, not from OCR.

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

    # v0.33.0 hard reset: the old LLM-extracted statement data was
    # never trustworthy. Run once per DB to start with a clean slate
    # before the user re-imports their CSV exports. Subsequent
    # restarts find the meta flag set and skip the wipe.
    try:
        from .finance.reset import maybe_run_one_time_reset
        report = maybe_run_one_time_reset(db)
        if report:
            log.warning(
                "v0.33.0 finance hard-reset: dropped %d transactions, "
                "%d statements, %d accounts. Re-import CSVs on /finance.",
                report["transactions"], report["statements"], report["accounts"],
            )
    except Exception:
        log.exception("Finance reset failed — continuing startup")

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
        # Local-AI-Bridge requests can stay in flight for many minutes
        # (a 16k-token Kontoauszug extraction on a 7B/14B model). Default
        # ws_ping_timeout=20s would kill the WebSocket mid-inference even
        # though both sides are still alive. Push both interval and
        # timeout up so a slow model never causes a phantom disconnect.
        ws_ping_interval=30.0,
        ws_ping_timeout=300.0,
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

    # Spin up the notification dispatcher early so even watcher events
    # before the web UI starts can be delivered.
    try:
        from . import notifier as _notifier
        _notifier.configure(settings)
    except Exception as exc:
        log.warning("Notifier setup failed (continuing without): %s", exc)

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
            or args.backfill_receipts) and classifier is None:
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
        log.warning(
            "--reocr-statements was removed in v0.33.0. Bank-statement "
            "OCR text is no longer fed into a second-pass extractor; "
            "import CSV exports on /finance for transaction data."
        )
        return 2

    if args.backfill_statements:
        log.warning(
            "--backfill-statements was removed in v0.33.0. "
            "Bank statements are no longer extracted from PDF; "
            "import CSV exports on /finance instead."
        )
        return 2

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
