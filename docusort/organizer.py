"""File naming and moving logic.

Turns a Classification + source file into a properly named file in the
library, with collision handling and review-folder routing when confidence
is low.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

from .classifier import Classification
from .config import AppSettings


logger = logging.getLogger("docusort.organizer")


_SLUG_RE = re.compile(r"[^A-Za-z0-9\-]+")
_UMLAUT_MAP = str.maketrans({
    "ä": "ae", "ö": "oe", "ü": "ue",
    "Ä": "Ae", "Ö": "Oe", "Ü": "Ue",
    "ß": "ss",
})


def _slug(value: str) -> str:
    """Make a filesystem-safe slug, preserving German readability."""
    if not value:
        return ""
    value = value.translate(_UMLAUT_MAP)
    value = _SLUG_RE.sub("-", value).strip("-")
    return value or ""


def _parse_iso_date(value: str) -> datetime:
    """Parse YYYY-MM-DD, falling back to today on failure."""
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d")
    except Exception:
        logger.warning("Invalid date %r – using today", value)
        return datetime.now()


def build_filename_from_parts(
    date: str, category: str, sender: str, subject: str,
    template: str, max_len: int, suffix: str,
) -> str:
    parts = {
        "date": _parse_iso_date(date).strftime("%Y-%m-%d"),
        "category": _slug(category),
        "sender": _slug(sender) or "Unbekannt",
        "subject": _slug(subject) or "Dokument",
    }
    name = template.format(**parts)
    name = re.sub(r"-+", "-", name).strip("-_")
    if len(name) > max_len:
        name = name[:max_len].rstrip("-_")
    return f"{name}{suffix.lower()}"


def build_filename(cls: Classification, template: str, max_len: int, suffix: str) -> str:
    return build_filename_from_parts(
        cls.date, cls.category, cls.sender, cls.subject,
        template, max_len, suffix,
    )


def target_path(
    library_root: Path, date: str, category: str, sender: str, subject: str,
    template: str, max_len: int, suffix: str,
    subcategory: str = "",
    current_path: Path | None = None,
) -> Path:
    """Return the canonical, collision-free library path for given metadata.

    When `current_path` is given and points at the natural target, return it
    unchanged — otherwise we'd uniquify against ourselves and end up renaming
    `foo.pdf` to `foo-2.pdf` for no reason.
    """
    year = _parse_iso_date(date).strftime("%Y")
    if subcategory:
        target_dir = library_root / year / category / subcategory
    else:
        target_dir = library_root / year / category
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = build_filename_from_parts(
        date, category, sender, subject, template, max_len, suffix,
    )
    natural = target_dir / filename
    if current_path is not None and natural == current_path:
        return current_path
    return _uniquify(natural)


def _uniquify(target: Path) -> Path:
    """If target exists, append -2, -3, … before the suffix.

    NOTE: This only *predicts* a free name — there is a gap between the
    exists() check and whoever writes the file. Use `_reserve_unique`
    for the real write path so two concurrent documents can never race
    onto the same name. `_uniquify` is fine for dry-run previews and
    read-only callers.
    """
    if not target.exists():
        return target
    stem, suffix, parent = target.stem, target.suffix, target.parent
    i = 2
    while True:
        candidate = parent / f"{stem}-{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def _reserve_unique(target: Path) -> Path:
    """Atomically claim a free path near `target` and return it.

    Creates an empty placeholder with O_CREAT|O_EXCL so the name is
    reserved on disk the instant we win it. Two worker threads filing
    documents that slugify to the same name can therefore never both
    pick `foo.pdf` — the loser gets FileExistsError and rolls on to
    `foo-2.pdf`. The caller then writes the real bytes *into* the
    placeholder it now owns (shutil.copyfile / os.replace both
    overwrite it safely). This closes the check-then-write gap that a
    plain exists()-based uniquify leaves open, which could otherwise
    let one document silently clobber another (the whole point of the
    library being Robert's real archive).
    """
    stem, suffix, parent = target.stem, target.suffix, target.parent
    parent.mkdir(parents=True, exist_ok=True)
    candidate = target
    i = 2
    while True:
        try:
            fd = os.open(candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.close(fd)
            return candidate
        except FileExistsError:
            candidate = parent / f"{stem}-{i}{suffix}"
            i += 1


def organize(
    source: Path,
    processed_source: Path,
    cls: Classification,
    settings: AppSettings,
) -> Path:
    """Move file to its final location and return that location.

    - `source` is the ORIGINAL file that arrived in the inbox.
    - `processed_source` is the (possibly OCR'd) file that should end up in the
      library. When OCR created a new file, these are different paths.
    """
    if cls.is_confident:
        year = _parse_iso_date(cls.date).strftime("%Y")
        sub = getattr(cls, "subcategory", "") or ""
        target_dir = settings.paths.library / year / cls.category
        if sub:
            target_dir = target_dir / sub
    else:
        target_dir = settings.paths.review
        logger.info(
            "Low confidence (%.2f) for %s – routing to review",
            cls.confidence, source.name,
        )

    target_dir.mkdir(parents=True, exist_ok=True)
    filename = build_filename(
        cls, settings.filename_template, settings.max_filename_length,
        processed_source.suffix,
    )

    if settings.dry_run:
        target = _uniquify(target_dir / filename)
        logger.info("[dry-run] would move %s -> %s", source.name, target)
        return target

    # Atomically reserve the name before writing so a concurrent worker
    # filing a same-named document can't overwrite this one (or vice versa).
    target = _reserve_unique(target_dir / filename)
    shutil.copyfile(processed_source, target)
    shutil.copystat(processed_source, target)
    logger.info("Filed %s -> %s", source.name, target)

    if processed_source != source:
        # OCR produced a temp file – clean it up.
        processed_source.unlink(missing_ok=True)

    if settings.keep_original:
        settings.paths.processed.mkdir(parents=True, exist_ok=True)
        archived = _reserve_unique(settings.paths.processed / source.name)
        # os.replace overwrites the placeholder we just reserved; fall back
        # to shutil.move when the original lives on a different filesystem.
        try:
            os.replace(str(source), str(archived))
        except OSError:
            shutil.copyfile(str(source), str(archived))
            shutil.copystat(str(source), str(archived))
            source.unlink(missing_ok=True)
        logger.debug("Archived original to %s", archived)
    else:
        source.unlink(missing_ok=True)

    return target
