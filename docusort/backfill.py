"""Re-classify existing documents to attach subcategory + tags.

Run via `python -m docusort --backfill-tags`. For every doc that has
`extracted_text` and is missing a subcategory (and the parent category
HAS subcategories) or has no tags, send the cached OCR text back to
Claude. The full classification result is applied: the file is renamed
and moved to library/<year>/<category>/<subcategory>/, and the DB row
is updated. OCR is NOT rerun — costs are just the Claude call (~$0.001
per doc with prompt cache).

Documents in trash are skipped. Documents without extracted_text are
skipped (they failed OCR and need a manual retry).
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

from .classifier import Classifier
from .config import AppSettings
from .db import Database
from .organizer import target_path


logger = logging.getLogger("docusort.backfill")


def _needs_backfill(doc: dict, sub_map: dict[str, list[str]]) -> bool:
    has_subs = bool(sub_map.get(doc["category"], []))
    if has_subs and not (doc.get("subcategory") or "").strip():
        return True
    try:
        tags = json.loads(doc.get("tags") or "[]") or []
    except Exception:
        tags = []
    return not tags


def backfill(
    settings: AppSettings, db: Database, classifier: Classifier,
    *, dry_run: bool = False, limit: int | None = None,
) -> dict:
    sub_map = {c["name"]: list(c.get("subcategories") or [])
               for c in settings.categories}

    docs = db.list_documents(limit=10_000, trash=False)
    candidates = [d for d in docs if d.get("extracted_text") and _needs_backfill(d, sub_map)]
    if limit:
        candidates = candidates[:limit]

    logger.info(
        "Backfill: %d candidates (out of %d total non-trashed docs)",
        len(candidates), len(docs),
    )
    if not candidates:
        return {"updated": 0, "skipped": 0, "moved": 0, "cost_usd": 0.0}

    updated = 0
    moved = 0
    skipped = 0
    total_cost = 0.0

    for doc in candidates:
        doc_id = doc["id"]
        try:
            cls = classifier.classify(doc["extracted_text"])
        except Exception as exc:
            logger.exception("Classifier failed for doc %d: %s", doc_id, exc)
            skipped += 1
            continue

        total_cost += cls.cost_usd

        # Preserve manual edits: if the user already set a category that
        # disagrees with the model, keep the user's category and just take
        # the model's subcategory (if it fits) and tags.
        new_category = doc["category"]
        new_subcategory = ""
        if cls.category in sub_map:
            new_category = cls.category
            if cls.subcategory and cls.subcategory in sub_map.get(new_category, []):
                new_subcategory = cls.subcategory
        elif cls.subcategory and cls.subcategory in sub_map.get(new_category, []):
            new_subcategory = cls.subcategory

        old_path = Path(doc["library_path"])
        new_path = old_path
        if old_path.exists():
            new_path = target_path(
                settings.paths.library,
                doc["doc_date"] or doc["created_at"][:10],
                new_category, doc["sender"] or "", doc["subject"] or "",
                settings.filename_template,
                settings.max_filename_length,
                old_path.suffix,
                subcategory=new_subcategory,
                current_path=old_path,
            )
            if new_path != old_path:
                if dry_run:
                    logger.info("[dry-run] would move %s -> %s", old_path.name, new_path)
                else:
                    shutil.move(str(old_path), str(new_path))
                moved += 1
        else:
            logger.warning("Doc %d library_path missing on disk: %s", doc_id, old_path)

        if dry_run:
            logger.info(
                "[dry-run] would set doc %d: cat=%s sub=%s tags=%s",
                doc_id, new_category, new_subcategory, cls.tags,
            )
        else:
            db.update_metadata(
                doc_id,
                category=new_category,
                subcategory=new_subcategory,
                tags=cls.tags,
                doc_date=doc["doc_date"] or "",
                sender=doc["sender"] or "",
                subject=doc["subject"] or "",
                filename=new_path.name,
                library_path=str(new_path),
                status=doc["status"] or "filed",
            )
        updated += 1
        if updated % 10 == 0:
            logger.info("Backfill progress: %d/%d (cost so far $%.4f)",
                        updated, len(candidates), total_cost)

    return {
        "updated": updated, "moved": moved, "skipped": skipped,
        "cost_usd": round(total_cost, 4),
    }
