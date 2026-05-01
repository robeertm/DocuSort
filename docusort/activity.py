"""Lightweight in-memory activity tracker for the web UI.

Two uses:

1. Long-running background jobs (the bulk approve-pending run) push
   their progress into this module so a polling endpoint can read it
   back without re-doing the work synchronously inside the HTTP
   request.
2. Provider call sites bump a counter on entry / exit so the header
   indicator can show "AI working" while LLM extractions are in
   flight, regardless of whether the work was triggered from the
   watcher, the classifier, or a manual UI action.

State is process-local — DocuSort runs as a single uvicorn worker
so a dict + a Lock is all we need. Restart wipes it, which is fine:
nothing here outlives the running pipeline anyway.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class JobState:
    """Progress snapshot for a single named job (e.g. "approve-pending")."""
    name: str
    running: bool = False
    started_at: float = 0.0
    finished_at: float = 0.0
    total: int = 0
    done: int = 0
    current: str = ""        # human label of what's being worked on
    current_doc_id: int = 0
    approved: list[int] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)
    last_error: str = ""
    # Cooperative pause: the worker checks pause_requested before
    # every iteration; on True it persists pending state and exits.
    pause_requested: bool = False
    paused: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "name":      self.name,
            "running":   self.running,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "total":     self.total,
            "done":      self.done,
            "current":   self.current,
            "current_doc_id": self.current_doc_id,
            "approved":  list(self.approved),
            "failed":    list(self.failed),
            "last_error": self.last_error,
            "pause_requested": self.pause_requested,
            "paused":    self.paused,
        }


_lock = threading.Lock()
_jobs: dict[str, JobState] = {}
_in_flight = 0
_last_call_at: float = 0.0


def get_job(name: str) -> JobState:
    """Return the (snapshot of the) job state for `name`, creating an
    empty entry if it doesn't exist yet."""
    with _lock:
        if name not in _jobs:
            _jobs[name] = JobState(name=name)
        return _jobs[name]


def start_job(name: str, total: int) -> JobState:
    with _lock:
        job = JobState(name=name, running=True, started_at=time.time(), total=total)
        _jobs[name] = job
        return job


def update_job(name: str, **fields: Any) -> None:
    with _lock:
        job = _jobs.get(name)
        if job is None:
            return
        for k, v in fields.items():
            setattr(job, k, v)


def finish_job(name: str, **fields: Any) -> None:
    with _lock:
        job = _jobs.get(name)
        if job is None:
            return
        for k, v in fields.items():
            setattr(job, k, v)
        job.running = False
        job.finished_at = time.time()


def request_pause(name: str) -> bool:
    """Cooperative pause request — returns True if the named job was
    running. Worker has to actually check the flag."""
    with _lock:
        job = _jobs.get(name)
        if job is None or not job.running:
            return False
        job.pause_requested = True
        return True


def is_pause_requested(name: str) -> bool:
    with _lock:
        job = _jobs.get(name)
        return bool(job and job.pause_requested)


def mark_paused(name: str) -> None:
    """Worker acknowledges pause: running flips False, paused flips True."""
    with _lock:
        job = _jobs.get(name)
        if job is None:
            return
        job.running = False
        job.pause_requested = False
        job.paused = True
        job.finished_at = time.time()


def clear_paused(name: str) -> None:
    """Reset paused flag — called when resume actually starts."""
    with _lock:
        job = _jobs.get(name)
        if job is None:
            return
        job.paused = False
        job.pause_requested = False


def begin_call() -> None:
    """Called when an LLM extraction starts. Drives the header dot."""
    global _in_flight, _last_call_at
    with _lock:
        _in_flight += 1
        _last_call_at = time.time()


def end_call() -> None:
    global _in_flight, _last_call_at
    with _lock:
        if _in_flight > 0:
            _in_flight -= 1
        _last_call_at = time.time()


def snapshot() -> dict[str, Any]:
    """Read-only view of every tracked job + the global in-flight counter.
    Cheap — used by the header polling endpoint."""
    with _lock:
        return {
            "in_flight":   _in_flight,
            "last_call_at": _last_call_at,
            "jobs":        {n: j.as_dict() for n, j in _jobs.items()},
        }
