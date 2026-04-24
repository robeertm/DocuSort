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
import logging
import os
import sys
from pathlib import Path

from . import __version__
from .classifier import Classification, Classifier
from .config import AppSettings, get_api_key, load_config
from .db import Database, DocumentRecord, open_db
from .logger import setup_logger
from .ocr import OcrResult, extract_text, is_supported
from .organizer import organize
from .watcher import process_existing, run_forever, watch


def _build_pipeline(settings: AppSettings, classifier: Classifier, db: Database):
    log = logging.getLogger("docusort.pipeline")

    def process(path: Path) -> None:
        if not path.exists() or not is_supported(path):
            return
        log.info("Processing %s (%.1f KB)", path.name, path.stat().st_size / 1024)
        original_name = path.name
        original_size = path.stat().st_size

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

        rec = DocumentRecord(
            filename=target.name,
            original_name=original_name,
            category=cls.category,
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
            cost_usd=cls.cost_usd,
            status=status,
            extracted_text=ocr_res.text[: settings.claude.max_text_chars],
        )
        try:
            doc_id = db.insert_document(rec)
            log.info("DB row %d written for %s", doc_id, target.name)
        except Exception as exc:
            log.exception("DB insert failed for %s: %s", target.name, exc)

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


def _start_web(settings: AppSettings, db: Database) -> None:
    """Run the FastAPI app with uvicorn in the current thread."""
    import uvicorn
    from .web.app import create_app

    app = create_app(settings, db)
    uvicorn.run(
        app, host=settings.web.host, port=settings.web.port,
        log_level=os.environ.get("DOCUSORT_LOG_LEVEL", "info").lower(),
        access_log=False,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="docusort", description=__doc__)
    parser.add_argument("--once", action="store_true",
                        help="Process existing inbox files and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Classify and log but don't move anything")
    parser.add_argument("--no-web", action="store_true",
                        help="Run watcher only, no web UI")
    parser.add_argument("--version", action="version", version=f"docusort {__version__}")
    args = parser.parse_args(argv)

    settings = load_config()
    if args.dry_run:
        settings.dry_run = True

    setup_logger(settings.paths.logs, level=os.environ.get("DOCUSORT_LOG_LEVEL", "INFO"))
    log = logging.getLogger("docusort.main")
    log.info("DocuSort %s starting (dry_run=%s, once=%s, web=%s)",
             __version__, settings.dry_run, args.once, not args.no_web)

    _ensure_dirs(settings)

    try:
        api_key = get_api_key()
    except RuntimeError as exc:
        log.error("%s", exc)
        return 2

    db = open_db(settings.paths.db)
    classifier = Classifier(api_key, settings.claude, settings.categories)
    pipeline = _build_pipeline(settings, classifier, db)

    process_existing(settings.paths.inbox, pipeline)

    if args.once:
        log.info("One-shot mode finished.")
        return 0

    observer = watch(settings.paths.inbox, pipeline, settings.stable_seconds)

    if args.no_web:
        run_forever(observer)
        return 0

    try:
        _start_web(settings, db)
    finally:
        observer.stop()
        observer.join(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
