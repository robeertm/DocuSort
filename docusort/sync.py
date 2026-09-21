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
import sqlite3
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import AppSettings


logger = logging.getLogger("docusort.sync")

# Below this file count a local mirror is considered "populated" enough
# that a sudden drop to (near) empty in the source is treated as a
# vanished mount rather than a legitimate delete-everything.
_MIN_MIRROR_FILES = 20
# If the source shrank to less than this fraction of the existing mirror,
# refuse the --delete run. Protects against a half-mounted / partially
# populated library nuking the backup.
_SHRINK_ABORT_RATIO = 0.5

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
    """Return the directory to push. Whether _Trash is included is decided
    by sync.source (see `_include_trash`); the --exclude flags handle it."""
    return settings.paths.library


def _include_trash(cfg) -> bool:
    """True when the user asked to back up the trash too (sync.source ==
    'library_and_trash'). Historically this setting existed but was
    ignored — _Trash was always excluded regardless."""
    return getattr(cfg, "source", "library") == "library_and_trash"


# SQLite sidecar files. A backup that captures the main .db mid-write
# together with a mismatched -wal is exactly what SQLite later rejects as
# "malformed". We never mirror the volatile sidecars and instead ship a
# clean snapshot (see _snapshot_db).
# 🔴 `preview-cache/**` gehört dazu: die Seitenbilder der Vorschau liegen
# neben der Datenbank — also innerhalb der Bibliothek — und wären sonst
# in jeder Sicherung. Sie sind jederzeit aus dem PDF neu zu rendern.
_DB_SIDECAR_GLOBS = ("*.db-wal", "*.db-shm", "*.db-journal", "preview-cache/**")


def _snapshot_db(settings: AppSettings) -> Path | None:
    """Write a consistent snapshot of the metadata DB into the library so it
    rides along with the mirror. Uses the official SQLite backup API, which
    produces a coherent copy even while the app keeps writing (WAL mode).

    The snapshot is named `<db>.backup` and is the authoritative file to
    restore from — the live `docusort.db` in the mirror can be torn, its
    `.backup` sibling never is. Returns the snapshot path, or None when
    there's nothing to snapshot / it failed (never raises: a backup must
    still run even if the snapshot can't be taken)."""
    db_path = settings.paths.db
    try:
        if not db_path.exists():
            return None
        dest = settings.paths.library / (db_path.name + ".backup")
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        src = sqlite3.connect(str(db_path))
        try:
            out = sqlite3.connect(str(tmp))
            try:
                src.backup(out)
            finally:
                out.close()
        finally:
            src.close()
        import os
        os.replace(tmp, dest)
        logger.info("DB snapshot written for backup: %s", dest)
        return dest
    except Exception as exc:
        logger.warning("Could not snapshot DB before sync (%s) — "
                       "continuing without a clean copy", exc)
        return None


def _count_files(root: Path, *, include_trash: bool) -> int:
    """Count regular files under `root`, optionally skipping _Trash. Used by
    the shrink-guard; returns 0 on any error (treated as 'empty')."""
    n = 0
    try:
        for entry in root.rglob("*"):
            try:
                if not entry.is_file():
                    continue
                if not include_trash and "_Trash" in entry.relative_to(root).parts:
                    continue
                n += 1
            except OSError:
                continue
    except OSError:
        return 0
    return n


# ---- Persisted sync state (survives restarts; drives the stale alarm) ----

def _sync_state_path(settings: AppSettings) -> Path:
    return Path(getattr(settings, "config_dir", Path("."))) / "sync_state.json"


def read_sync_state(settings: AppSettings) -> dict[str, Any]:
    try:
        return json.loads(_sync_state_path(settings).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_sync_state(settings: AppSettings, **updates: Any) -> None:
    try:
        state = read_sync_state(settings)
        state.update(updates)
        _sync_state_path(settings).write_text(
            json.dumps(state, indent=2), encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("Could not persist sync state: %s", exc)


def _fire_sync_alarm(settings: AppSettings, title: str, body: str) -> None:
    """Send a backup-problem notification (throttled elsewhere for the
    stale case; run failures fire every time)."""
    try:
        from . import notifier as _n
        _n.fire(_n.NotificationEvent(
            kind="sync_failed", title=title, body=body,
        ))
    except Exception as exc:
        logger.warning("Could not fire sync alarm: %s", exc)


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

    # Ship a guaranteed-consistent copy of the metadata DB alongside the
    # documents so a restore can't end up with a torn / "malformed" db.
    _snapshot_db(settings)

    if cfg.target_type == "local":
        result = _run_local_sync(cfg, src)
    else:
        result = _run_rclone_sync(cfg, src)
    _post_sync(settings, result)
    return result


def _post_sync(settings: AppSettings, result: dict[str, Any]) -> None:
    """Persist the outcome and raise a backup alarm on failure."""
    now = datetime.now().isoformat(timespec="seconds")
    if result.get("ok"):
        # Clear any pending stale-alarm throttle on a good run.
        _write_sync_state(settings, last_success_at=now,
                          last_result="ok", last_alarm_at=None)
    else:
        _write_sync_state(settings, last_result="error",
                          last_error=str(result.get("error", "")))
        _fire_sync_alarm(
            settings,
            "❌ DocuSort-Backup fehlgeschlagen",
            "Das letzte Backup ist nicht durchgelaufen:\n"
            + str(result.get("error", "unbekannter Fehler")),
        )


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
    excludes: list[str] = []
    for glob in _DB_SIDECAR_GLOBS:
        excludes += ["--exclude", glob]
    if not _include_trash(cfg):
        excludes += ["--exclude", "_Trash/**"]
    cmd = [
        "rclone", "sync",
        str(src), cfg.remote,
        *excludes,
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

    # --- Shrink guard (protects the --delete mirror) --------------------
    # If the library disk briefly drops out, the source looks empty and a
    # plain `rsync --delete` would faithfully wipe the entire backup. Bail
    # out when the source is empty, or has shrunk to less than half of what
    # the backup currently holds. Both the rsync and pure-Python paths
    # delete, so the guard sits before either.
    include_trash = _include_trash(cfg)
    src_count = _count_files(src, include_trash=include_trash)
    if src_count == 0:
        msg = ("source library is empty — refusing to sync with --delete "
               "(is the library disk mounted?)")
        _finalise_state(ok=False, message=msg, duration=0)
        return {"ok": False, "error": msg}
    tgt_count = _count_files(target_path, include_trash=include_trash)
    if tgt_count >= _MIN_MIRROR_FILES and src_count < tgt_count * _SHRINK_ABORT_RATIO:
        msg = (f"source shrank drastically ({src_count} files now vs "
               f"{tgt_count} in backup) — refusing --delete to avoid wiping "
               f"the mirror")
        _finalise_state(ok=False, message=msg, duration=0)
        return {"ok": False, "error": msg}

    started = time.monotonic()

    if rsync_available():
        # Trailing slash on src means "copy contents of src into target",
        # not "copy src dir into target". --delete-excluded removes files
        # that no longer exist in the source so the mirror stays clean.
        exclude_args = [f"--exclude={g}" for g in _DB_SIDECAR_GLOBS]
        if not include_trash:
            exclude_args += ["--exclude=_Trash/", "--exclude=_Trash/**"]
        cmd = [
            "rsync", "-a", "--delete", "--delete-excluded",
            *exclude_args,
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
            rel = entry.relative_to(src)
            if not include_trash and "_Trash" in rel.parts:
                continue
            # 🔴 `Path.match` versteht „**" nicht wie rclone — der
            # Vorschau-Zwischenspeicher wird deshalb über die Pfadteile
            # ausgeschlossen, nicht über das Muster.
            if "preview-cache" in rel.parts:
                continue
            if any(entry.match(g) for g in _DB_SIDECAR_GLOBS if "**" not in g):
                continue
            dest = target_path / rel
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
