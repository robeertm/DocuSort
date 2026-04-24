"""Stream a ZIP of the library (or a filtered subset) over HTTP.

We can't write a multi-hundred-megabyte archive into memory, so we use
`zipfile.ZipFile` over a generator-backed writer that yields each chunk
as FastAPI streams it to the client.
"""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path
from typing import Iterable, Iterator

from .config import AppSettings
from .db import Database


logger = logging.getLogger("docusort.export")


class _StreamingBuffer(io.RawIOBase):
    """An append-only buffer that yields chunks as bytes are written to it."""

    def __init__(self):
        self._buf = bytearray()

    def writable(self) -> bool:
        return True

    def write(self, data) -> int:
        self._buf.extend(data)
        return len(data)

    def drain(self) -> bytes:
        chunk = bytes(self._buf)
        self._buf.clear()
        return chunk


def _pick_files(
    settings: AppSettings,
    db: Database,
    *,
    category: str | None = None,
    year: str | None = None,
    include_trash: bool = False,
) -> Iterable[tuple[Path, str]]:
    """Yield (disk_path, archive_name) pairs for documents matching the filter.

    Dedup by absolute file path — `duplicate` rows share the library_path with
    the original, so without this we'd write the same bytes twice under the
    same archive name.
    """
    docs = db.list_documents(
        category=category or None, year=year or None,
        limit=1_000_000, trash=include_trash,
    )
    library_root = settings.paths.library
    seen: set[str] = set()
    for d in docs:
        src = Path(d["library_path"])
        if not src.exists() or str(src) in seen:
            continue
        seen.add(str(src))
        try:
            rel = src.relative_to(library_root)
        except ValueError:
            rel = Path(src.name)
        yield src, str(rel)


def stream_zip(
    settings: AppSettings,
    db: Database,
    *,
    category: str | None = None,
    year: str | None = None,
    include_trash: bool = False,
    chunk_size: int = 64 * 1024,
) -> Iterator[bytes]:
    buf = _StreamingBuffer()
    zf = zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True)

    for src, arcname in _pick_files(
        settings, db, category=category, year=year, include_trash=include_trash,
    ):
        try:
            with src.open("rb") as f, zf.open(arcname, mode="w", force_zip64=True) as dst:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    dst.write(chunk)
                    data = buf.drain()
                    if data:
                        yield data
        except Exception as exc:
            logger.warning("skipping %s: %s", src, exc)
            continue
        tail = buf.drain()
        if tail:
            yield tail

    zf.close()
    yield buf.drain()


def suggested_filename(
    *, category: str | None = None, year: str | None = None, trash: bool = False
) -> str:
    parts = ["docusort"]
    if trash:
        parts.append("trash")
    if year:
        parts.append(year)
    if category:
        parts.append(category.replace(" ", "-"))
    return "_".join(parts) + ".zip"
