"""Cloud sync via rclone.

We don't ship rclone and we don't try to reimplement iCloud/Drive/Dropbox
auth. rclone is the mature, well-maintained multi-backend tool; users run
`rclone config` once to authorise whichever remote they want, and this
module just shells out to `rclone sync` on demand (or on a systemd timer).

The `sync` section of config.yaml controls what gets uploaded:

    sync:
      enabled: true
      remote: "icloud:DocuSort"      # <remote-name>:<path>
      source: library                # 'library' | 'library_and_trash'
      extra_flags: ["--transfers=4"] # optional rclone flags

Supported remotes = whatever rclone supports. For iCloud Drive: follow
    https://rclone.org/iclouddrive/
On Debian: `sudo apt install rclone` and then `rclone config`.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import AppSettings


logger = logging.getLogger("docusort.sync")

_state_lock = threading.Lock()
_state: dict[str, Any] = {
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_duration_seconds": None,
    "last_result": None,       # 'ok' | 'error'
    "last_message": None,
    "last_transferred": None,  # bytes
    "last_files": None,
    "last_errors": None,
}


def rclone_available() -> bool:
    return shutil.which("rclone") is not None


def status(settings: AppSettings) -> dict[str, Any]:
    with _state_lock:
        snapshot = dict(_state)
    cfg = getattr(settings, "sync", None)
    return {
        **snapshot,
        "enabled": bool(cfg.enabled) if cfg else False,
        "remote": cfg.remote if cfg else None,
        "source": cfg.source if cfg else None,
        "rclone_installed": rclone_available(),
    }


def _source_path(settings: AppSettings) -> Path:
    """Return the directory to push. 'library' excludes _Trash; the rclone
    --exclude flag handles that filter."""
    return settings.paths.library


def _parse_rclone_stats(lines: list[str]) -> dict[str, Any]:
    """rclone --stats-one-line outputs lines like:
        Transferred: 2.345 MiB / 2.345 MiB, 100%, 1.234 MiB/s, ETA 0s
        Errors: 0
        Files: 12
    We grep the final summary block for byte/file/error counts.
    """
    info: dict[str, Any] = {}
    for line in reversed(lines):
        low = line.lower().strip()
        if low.startswith("transferred:"):
            # "Transferred:    512.345 KiB / 512.345 KiB, 100%, ..."
            try:
                head = low.split(",")[0]
                info["transferred"] = head.split(":", 1)[1].strip()
            except Exception:
                pass
        elif low.startswith("errors:"):
            try:
                info["errors"] = int(low.split(":")[1].strip().split()[0])
            except Exception:
                pass
        elif low.startswith("files:"):
            try:
                info["files"] = int(low.split(":")[1].strip().split()[0])
            except Exception:
                pass
        if "transferred" in info and "errors" in info and "files" in info:
            break
    return info


def run_sync(settings: AppSettings) -> dict[str, Any]:
    """Execute `rclone sync` once. Blocks until finished. Updates the shared
    state dict. Returns the final state snapshot."""
    cfg = getattr(settings, "sync", None)
    if not cfg or not cfg.enabled:
        return {"ok": False, "error": "sync disabled in config"}
    if not cfg.remote:
        return {"ok": False, "error": "no remote configured"}
    if not rclone_available():
        return {"ok": False, "error": "rclone not installed on PATH"}

    src = _source_path(settings)
    if not src.exists():
        return {"ok": False, "error": f"source missing: {src}"}

    started = time.monotonic()
    started_at = datetime.now().isoformat(timespec="seconds")
    with _state_lock:
        if _state["running"]:
            return {"ok": False, "error": "a sync is already running"}
        _state["running"] = True
        _state["last_started_at"] = started_at

    cmd = [
        "rclone", "sync",
        str(src), cfg.remote,
        "--exclude", "_Trash/**",
        "--stats=5s", "--stats-one-line",
    ] + list(cfg.extra_flags or [])

    logger.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=cfg.timeout_seconds,
        )
        duration = round(time.monotonic() - started, 1)
        ok = (result.returncode == 0)
        stats = _parse_rclone_stats(result.stderr.splitlines()[-50:])
        with _state_lock:
            _state["running"] = False
            _state["last_finished_at"] = datetime.now().isoformat(timespec="seconds")
            _state["last_duration_seconds"] = duration
            _state["last_result"] = "ok" if ok else "error"
            _state["last_message"] = (
                "synced" if ok else (result.stderr or "unknown error")[-800:]
            )
            _state["last_transferred"] = stats.get("transferred")
            _state["last_files"] = stats.get("files")
            _state["last_errors"] = stats.get("errors")
        return {"ok": ok, **stats, "duration_seconds": duration}
    except subprocess.TimeoutExpired:
        with _state_lock:
            _state["running"] = False
            _state["last_result"] = "error"
            _state["last_message"] = "timeout"
        return {"ok": False, "error": "rclone sync timed out"}
    except Exception as exc:
        logger.exception("rclone sync failed")
        with _state_lock:
            _state["running"] = False
            _state["last_result"] = "error"
            _state["last_message"] = str(exc)
        return {"ok": False, "error": str(exc)}


def run_sync_async(settings: AppSettings) -> dict[str, str]:
    """Fire-and-forget — spawns a thread and returns immediately."""
    threading.Thread(target=run_sync, args=(settings,), daemon=True).start()
    return {"status": "started"}
