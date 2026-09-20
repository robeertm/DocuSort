"""Soft-delete helpers.

`delete` moves the file into a `_Trash/` tree that mirrors the category
layout. `restore` moves it back. `purge` removes the file from disk and
the row from the DB. All three keep the physical layout in sync with the
DB so the user can drop a backup on rclone at any time without weird state.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from .config import AppSettings
from .db import Database


logger = logging.getLogger("docusort.trash")

TRASH_DIR = "_Trash"


def _trash_root(settings: AppSettings) -> Path:
    return settings.paths.library / TRASH_DIR


def _restore_target(settings: AppSettings, trash_path: Path) -> Path:
    """Map a path inside _Trash back to its original library location by
    dropping the '_Trash/' prefix."""
    rel = trash_path.relative_to(_trash_root(settings))
    return settings.paths.library / rel


def _uniquify(target: Path) -> Path:
    if not target.exists():
        return target
    stem, suffix, parent = target.stem, target.suffix, target.parent
    i = 2
    while True:
        cand = parent / f"{stem}-{i}{suffix}"
        if not cand.exists():
            return cand
        i += 1


def delete_document(doc_id: int, settings: AppSettings, db: Database) -> dict:
    doc = db.get(doc_id)
    if not doc:
        raise ValueError(f"document {doc_id} not found")
    if doc.get("deleted_at"):
        raise ValueError("document already in trash")

    source_str = doc.get("library_path") or ""
    source = Path(source_str) if source_str else None

    # If the underlying file is gone (e.g. an aborted classifier never
    # actually moved the upload, or the user deleted it on disk), still
    # let the user clean up the DB row. Without this branch the doc
    # would stay in the review queue forever — exactly the "wie bekomme
    # ich das weg?" case.
    if source is None or not source.exists():
        db.mark_deleted(doc_id, source_str)
        logger.info("Trashed doc %d (file already missing): %s", doc_id, source_str)
        return {"doc_id": doc_id, "trash_path": source_str,
                "file_missing": True}

    # Preserve the original sub-path (YYYY/Category/filename) under _Trash/
    library_root = settings.paths.library
    try:
        rel = source.relative_to(library_root)
    except ValueError:
        # File lives outside the library root (e.g. /review/...); flatten
        # under _Trash/ instead of crashing on the relative_to call.
        rel = Path(source.name)
    target = _uniquify(_trash_root(settings) / rel)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(target))

    db.mark_deleted(doc_id, str(target))
    logger.info("Trashed doc %d: %s -> %s", doc_id, source, target)
    return {"doc_id": doc_id, "trash_path": str(target)}


def restore_document(doc_id: int, settings: AppSettings, db: Database) -> dict:
    doc = db.get(doc_id)
    if not doc:
        raise ValueError(f"document {doc_id} not found")
    if not doc.get("deleted_at"):
        raise ValueError("document is not in trash")

    source = Path(doc["library_path"])
    if not source.exists():
        raise ValueError(f"trash file missing: {source}")

    target = _uniquify(_restore_target(settings, source))
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(target))

    db.mark_restored(doc_id, str(target))
    logger.info("Restored doc %d: %s -> %s", doc_id, source, target)
    return {"doc_id": doc_id, "library_path": str(target)}


def purge_document(doc_id: int, settings: AppSettings, db: Database) -> dict:
    doc = db.get(doc_id)
    if not doc:
        raise ValueError(f"document {doc_id} not found")
    source = Path(doc["library_path"])
    if source.exists():
        source.unlink()
    # Also clean up the processed-copy if it was set.
    proc = doc.get("processed_path")
    if proc:
        p = Path(proc)
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass
    db.purge(doc_id)
    logger.info("Purged doc %d (%s)", doc_id, source)
    return {"doc_id": doc_id, "purged": True}


def empty_trash(settings: AppSettings, db: Database) -> dict:
    """Permanent-delete everything currently in trash."""
    docs = db.list_documents(trash=True, limit=100000)
    count = 0
    for d in docs:
        try:
            purge_document(d["id"], settings, db)
            count += 1
        except Exception as exc:
            logger.warning("purge failed for %d: %s", d["id"], exc)
    return {"purged": count}
