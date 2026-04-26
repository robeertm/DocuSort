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


def rsync_available() -> bool:
    return shutil.which("rsync") is not None


def status(settings: AppSettings) -> dict[str, Any]:
    with _state_lock:
        snapshot = dict(_state)
    cfg = getattr(settings, "sync", None)
    return {
        **snapshot,
        "enabled":     bool(cfg.enabled) if cfg else False,
        "target_type": cfg.target_type if cfg else "local",
        "local_path":  cfg.local_path if cfg else "",
        "remote":      cfg.remote if cfg else None,
        "source":      cfg.source if cfg else None,
        "rclone_installed": rclone_available(),
        "rsync_installed":  rsync_available(),
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
    """Execute one sync run. Dispatches between the rclone backend and the
    rsync-based local-folder backend based on settings.sync.target_type.
    Blocks until finished, updates the shared state dict, returns a result
    snapshot."""
    cfg = getattr(settings, "sync", None)
    if not cfg or not cfg.enabled:
        return {"ok": False, "error": "sync disabled in config"}

    src = _source_path(settings)
    if not src.exists():
        return {"ok": False, "error": f"source missing: {src}"}

    with _state_lock:
        if _state["running"]:
            return {"ok": False, "error": "a sync is already running"}
        _state["running"] = True
        _state["last_started_at"] = datetime.now().isoformat(timespec="seconds")

    if cfg.target_type == "local":
        return _run_local_sync(cfg, src)
    return _run_rclone_sync(cfg, src)


def _finalise_state(*, ok: bool, message: str, duration: float,
                    stats: dict[str, Any] | None = None) -> None:
    with _state_lock:
        _state["running"] = False
        _state["last_finished_at"] = datetime.now().isoformat(timespec="seconds")
        _state["last_duration_seconds"] = duration
        _state["last_result"] = "ok" if ok else "error"
        _state["last_message"] = message
        if stats:
            _state["last_transferred"] = stats.get("transferred")
            _state["last_files"]       = stats.get("files")
            _state["last_errors"]      = stats.get("errors")


def _run_rclone_sync(cfg, src) -> dict[str, Any]:
    if not cfg.remote:
        _finalise_state(ok=False, message="no remote configured", duration=0)
        return {"ok": False, "error": "no remote configured"}
    if not rclone_available():
        _finalise_state(ok=False, message="rclone not installed", duration=0)
        return {"ok": False, "error": "rclone not installed on PATH"}

    started = time.monotonic()
    cmd = [
        "rclone", "sync",
        str(src), cfg.remote,
        "--exclude", "_Trash/**",
        "--stats=5s", "--stats-one-line",
    ] + list(cfg.extra_flags or [])

    logger.info("Running rclone: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=cfg.timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        _finalise_state(ok=False, message="timeout", duration=cfg.timeout_seconds)
        return {"ok": False, "error": "rclone sync timed out"}
    except Exception as exc:
        logger.exception("rclone sync failed")
        _finalise_state(ok=False, message=str(exc), duration=round(time.monotonic()-started,1))
        return {"ok": False, "error": str(exc)}

    duration = round(time.monotonic() - started, 1)
    ok = (result.returncode == 0)
    stats = _parse_rclone_stats(result.stderr.splitlines()[-50:])
    _finalise_state(
        ok=ok,
        message="synced" if ok else (result.stderr or "unknown error")[-800:],
        duration=duration, stats=stats,
    )
    return {"ok": ok, **stats, "duration_seconds": duration}


def _run_local_sync(cfg, src) -> dict[str, Any]:
    """Mirror the library to a local path with rsync. Falls back to a pure-
    Python copy when rsync isn't available (slower but works everywhere)."""
    target = cfg.local_path.strip() if cfg.local_path else ""
    if not target:
        _finalise_state(ok=False, message="no local target path", duration=0)
        return {"ok": False, "error": "sync.local_path is empty"}
    target_path = Path(target).expanduser()

    # Refuse to sync onto the library itself or any of its parents — that
    # would either be a no-op (same path) or destroy unrelated files.
    try:
        if target_path == src or src in target_path.parents or target_path in src.parents:
            _finalise_state(ok=False, message="target overlaps source", duration=0)
            return {"ok": False, "error":
                    f"target {target_path} overlaps source {src}"}
    except Exception:
        pass

    try:
        target_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        _finalise_state(ok=False, message=f"cannot create target: {exc}", duration=0)
        return {"ok": False, "error": str(exc)}

    started = time.monotonic()

    if rsync_available():
        # Trailing slash on src means "copy contents of src into target",
        # not "copy src dir into target". --delete-excluded removes files
        # that no longer exist in the source so the mirror stays clean.
        cmd = [
            "rsync", "-a", "--delete", "--delete-excluded",
            "--exclude=_Trash/", "--exclude=_Trash/**",
            "--info=stats2",
            f"{src}/", f"{target_path}/",
        ]
        logger.info("Running rsync: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=cfg.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            _finalise_state(ok=False, message="timeout", duration=cfg.timeout_seconds)
            return {"ok": False, "error": "local sync timed out"}
        duration = round(time.monotonic() - started, 1)
        ok = (result.returncode == 0)
        stats = _parse_rsync_stats(result.stdout)
        _finalise_state(
            ok=ok,
            message="synced" if ok else (result.stderr or "rsync failed")[-800:],
            duration=duration, stats=stats,
        )
        return {"ok": ok, **stats, "duration_seconds": duration}

    # Pure-Python fallback — copytree with dirs_exist_ok. Slower and not
    # incremental; only used when rsync isn't available (rare on Linux/Mac).
    logger.warning("rsync not found — falling back to shutil.copytree (slower)")
    try:
        for child in target_path.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
        copied = 0
        for entry in src.rglob("*"):
            if entry.is_dir():
                continue
            if "_Trash" in entry.relative_to(src).parts:
                continue
            dest = target_path / entry.relative_to(src)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(entry, dest)
            copied += 1
        duration = round(time.monotonic() - started, 1)
        _finalise_state(
            ok=True, message="synced (python fallback)", duration=duration,
            stats={"files": copied, "errors": 0},
        )
        return {"ok": True, "files": copied, "duration_seconds": duration}
    except Exception as exc:
        logger.exception("local sync (python fallback) failed")
        _finalise_state(ok=False, message=str(exc),
                        duration=round(time.monotonic()-started, 1))
        return {"ok": False, "error": str(exc)}


def _parse_rsync_stats(stdout: str) -> dict[str, Any]:
    """Pull the few numbers we care about out of `rsync --info=stats2`."""
    info: dict[str, Any] = {}
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("Number of regular files transferred:"):
            try:
                info["files"] = int(line.split(":")[1].strip().replace(",", ""))
            except Exception:
                pass
        elif line.startswith("Total transferred file size:"):
            try:
                info["transferred"] = line.split(":", 1)[1].split("(")[0].strip()
            except Exception:
                pass
    info.setdefault("errors", 0)
    return info


def run_sync_async(settings: AppSettings) -> dict[str, str]:
    """Fire-and-forget — spawns a thread and returns immediately."""
    threading.Thread(target=run_sync, args=(settings,), daemon=True).start()
    return {"status": "started"}
