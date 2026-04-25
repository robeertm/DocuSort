"""File naming and moving logic.

Turns a Classification + source file into a properly named file in the
library, with collision handling and review-folder routing when confidence
is low.
"""

from __future__ import annotations

import logging
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
    """If target exists, append -2, -3, … before the suffix."""
    if not target.exists():
        return target
    stem, suffix, parent = target.stem, target.suffix, target.parent
    i = 2
    while True:
        candidate = parent / f"{stem}-{i}{suffix}"
        if not candidate.exists():
            return candidate
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
    target = _uniquify(target_dir / filename)

    if settings.dry_run:
        logger.info("[dry-run] would move %s -> %s", source.name, target)
        return target

    shutil.copy2(processed_source, target)
    logger.info("Filed %s -> %s", source.name, target)

    if processed_source != source:
        # OCR produced a temp file – clean it up.
        processed_source.unlink(missing_ok=True)

    if settings.keep_original:
        settings.paths.processed.mkdir(parents=True, exist_ok=True)
        archived = _uniquify(settings.paths.processed / source.name)
        shutil.move(str(source), archived)
        logger.debug("Archived original to %s", archived)
    else:
        source.unlink(missing_ok=True)

    return target
