"""DocuSort entrypoint.

Run as a long-running service (default) or in one-shot mode via
`python -m docusort --once`. Environment variable DOCUSORT_LOG_LEVEL controls
logging verbosity (DEBUG/INFO/WARNING/ERROR).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from . import __version__
from .classifier import Classifier
from .config import AppSettings, get_api_key, load_config
from .logger import setup_logger
from .ocr import extract_text, is_supported
from .organizer import organize
from .watcher import process_existing, run_forever, watch


def _build_pipeline(settings: AppSettings, classifier: Classifier):
    log = logging.getLogger("docusort.pipeline")

    def process(path: Path) -> None:
        if not path.exists() or not is_supported(path):
            return
        log.info("Processing %s (%.1f KB)", path.name, path.stat().st_size / 1024)
        text, processed_path = extract_text(path, settings.ocr)
        if not text:
            log.warning("No text extracted from %s – routing to review", path.name)
            from .classifier import Classification
            cls = Classification(
                category="Sonstiges", date="",
                sender="Unbekannt", subject="OCR-fehlgeschlagen",
                confidence=0.0, reasoning="No text could be extracted",
            )
        else:
            try:
                cls = classifier.classify(text)
                log.info(
                    "Classified %s -> %s / %s / %s (conf=%.2f)",
                    path.name, cls.category, cls.date, cls.sender, cls.confidence,
                )
            except Exception as exc:
                log.exception("Classification failed for %s: %s", path.name, exc)
                from .classifier import Classification
                cls = Classification(
                    category="Sonstiges", date="", sender="Unbekannt",
                    subject="Klassifizierung-fehlgeschlagen",
                    confidence=0.0, reasoning=str(exc),
                )
        organize(path, processed_path, cls, settings)

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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="docusort", description=__doc__)
    parser.add_argument("--once", action="store_true",
                        help="Process existing inbox files and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Classify and log but don't move anything")
    parser.add_argument("--version", action="version", version=f"docusort {__version__}")
    args = parser.parse_args(argv)

    settings = load_config()
    if args.dry_run:
        settings.dry_run = True

    setup_logger(settings.paths.logs, level=os.environ.get("DOCUSORT_LOG_LEVEL", "INFO"))
    log = logging.getLogger("docusort.main")
    log.info("DocuSort %s starting (dry_run=%s)", __version__, settings.dry_run)

    _ensure_dirs(settings)

    try:
        api_key = get_api_key()
    except RuntimeError as exc:
        log.error("%s", exc)
        return 2

    classifier = Classifier(api_key, settings.claude, settings.categories)
    pipeline = _build_pipeline(settings, classifier)

    process_existing(settings.paths.inbox, pipeline)

    if args.once:
        log.info("One-shot mode finished.")
        return 0

    observer = watch(settings.paths.inbox, pipeline, settings.stable_seconds)
    run_forever(observer)
    return 0


if __name__ == "__main__":
    sys.exit(main())
