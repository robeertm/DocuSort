"""Inbox watcher.

Uses watchdog to react to new files in the inbox. Before handing a file to the
pipeline we wait until its size has been stable for a few seconds so we don't
try to read a file that's still being scanned/copied.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .ocr import is_supported


logger = logging.getLogger("docusort.watcher")

Handler = Callable[[Path], None]


class _Handler(FileSystemEventHandler):
    def __init__(self, process: Handler, stable_seconds: int):
        super().__init__()
        self._process = process
        self._stable = stable_seconds
        self._pending: dict[Path, threading.Timer] = {}
        self._lock = threading.Lock()

    def _schedule(self, path: Path) -> None:
        if not is_supported(path):
            return
        with self._lock:
            existing = self._pending.pop(path, None)
            if existing:
                existing.cancel()
            timer = threading.Timer(self._stable, self._check_and_run, args=(path, 0))
            self._pending[path] = timer
            timer.start()

    def _check_and_run(self, path: Path, last_size: int) -> None:
        try:
            if not path.exists():
                with self._lock:
                    self._pending.pop(path, None)
                return
            size = path.stat().st_size
            if size != last_size:
                # still growing – re-check
                timer = threading.Timer(
                    self._stable, self._check_and_run, args=(path, size),
                )
                with self._lock:
                    self._pending[path] = timer
                timer.start()
                return
            with self._lock:
                self._pending.pop(path, None)
            self._process(path)
        except Exception:
            logger.exception("Error while processing %s", path)

    def on_created(self, event):
        if event.is_directory:
            return
        self._schedule(Path(event.src_path))

    def on_moved(self, event):
        if event.is_directory:
            return
        self._schedule(Path(event.dest_path))


def process_existing(inbox: Path, process: Handler) -> None:
    """Process files that were already in the inbox at startup."""
    for item in sorted(inbox.iterdir()):
        if item.is_file() and is_supported(item):
            logger.info("Picking up pre-existing file %s", item.name)
            try:
                process(item)
            except Exception:
                logger.exception("Startup processing failed for %s", item)


def watch(inbox: Path, process: Handler, stable_seconds: int = 5) -> Observer:
    """Start the watchdog observer. The caller must keep it alive."""
    inbox.mkdir(parents=True, exist_ok=True)
    handler = _Handler(process, stable_seconds)
    observer = Observer()
    observer.schedule(handler, str(inbox), recursive=False)
    observer.start()
    logger.info("Watching %s for new documents…", inbox)
    return observer


def run_forever(observer: Observer) -> None:
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down…")
    finally:
        observer.stop()
        observer.join()
