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

    return {
        "doc_id": doc_id,
        "status": status,
        "category": cls.category,
        "confidence": cls.confidence,
        "cost_usd": cls.cost_usd,
        "library_path": str(target),
    }
