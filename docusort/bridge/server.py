"""In-process hub for the local-AI bridge.

A single Mac client connects via WebSocket and stays connected. When
a worker thread on the server needs an LLM call routed through the
client, it submits a request through this hub. The hub posts the
request onto the WebSocket via the running event loop, then blocks
the calling thread on a `threading.Event` until the reply (or an
error, or a timeout) arrives.

The hub is deliberately a process-wide singleton. There is exactly one
client in this design — if a second one connects with the same token,
the older one is dropped. That keeps the routing and accounting
trivial: every request finds exactly one place to go, and a stale
client never silently lingers.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


logger = logging.getLogger("docusort.bridge")


@dataclass
class _PendingRequest:
    """One in-flight request, waiting for the Mac client to answer."""
    event: threading.Event = field(default_factory=threading.Event)
    response: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    submitted_at: float = field(default_factory=time.time)


class BridgeServer:
    """Holds the active client + the request/response routing table."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: dict[str, _PendingRequest] = {}
        self._client = None  # FastAPI WebSocket; typed as Any to avoid hard dep
        self._client_info: dict[str, Any] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._connected_at: float = 0.0
        self._last_request_at: float = 0.0
        self._calls_total: int = 0
        self._calls_failed: int = 0
        self._tokens_in: int = 0
        self._tokens_out: int = 0
        self._latency_sum_s: float = 0.0  # for avg latency
        self._latency_n: int = 0
        # Last token-rejected attempt — surfaced in the UI so the user
        # gets a clear "you're using the wrong token" instead of a
        # silent "offline".
        self._last_reject: dict[str, Any] = {}

    # ------------------------------------------------------------------ status
    def is_connected(self) -> bool:
        return self._client is not None

    def info(self) -> dict[str, Any]:
        with self._lock:
            avg_latency = (self._latency_sum_s / self._latency_n) if self._latency_n else 0.0
            return {
                "connected": self.is_connected(),
                "client":    dict(self._client_info),
                "connected_at":     self._connected_at,
                "last_request_at":  self._last_request_at,
                "calls_total":      self._calls_total,
                "calls_failed":     self._calls_failed,
                "tokens_in":        self._tokens_in,
                "tokens_out":       self._tokens_out,
                "avg_latency_s":    avg_latency,
                "pending":          len(self._pending),
                "last_reject":      dict(self._last_reject),
            }

    # -------------------------------------------------------------- ws hooks
    async def attach_client(self, ws, hello: dict[str, Any]) -> None:
        """Called from the WebSocket handler once the client has said hello.
        Replaces any previous client (single-client model)."""
        prev = None
        with self._lock:
            prev = self._client
            self._client = ws
            self._client_info = {
                "host":     str(hello.get("host", "")),
                "platform": str(hello.get("platform", "")),
                "machine":  str(hello.get("machine", "")),
                "python":   str(hello.get("python", "")),
                "model":    str(hello.get("model", "")),
                "ollama":   str(hello.get("ollama", "")),
            }
            self._loop = asyncio.get_running_loop()
            self._connected_at = time.time()
        if prev is not None and prev is not ws:
            try:
                await prev.close(code=1000, reason="replaced by new client")
            except Exception:
                pass
            logger.info("Bridge: previous client dropped (replaced).")
        logger.info("Bridge: client attached: %s", self._client_info)

    async def detach_client(self, ws) -> None:
        """Drop the client and fail every still-pending request immediately."""
        with self._lock:
            if self._client is not ws:
                return
            self._client = None
            self._client_info = {}
            self._loop = None
            pending = list(self._pending.items())
            self._pending.clear()
        for _, p in pending:
            p.error = "bridge client disconnected"
            p.event.set()
        logger.info("Bridge: client detached. Failed %d pending request(s).",
                    len(pending))

    async def handle_message(self, msg: dict[str, Any]) -> None:
        """Route an incoming message from the Mac client. Today only
        responses to pending requests come back this way; pings are
        handled at the protocol layer (FastAPI's WebSocket)."""
        kind = msg.get("type")
        if kind != "response":
            return
        req_id = str(msg.get("request_id", ""))
        if not req_id:
            return
        with self._lock:
            p = self._pending.pop(req_id, None)
        if p is None:
            logger.debug("Bridge: stale response for %s (already gone)", req_id)
            return
        if msg.get("error"):
            p.error = str(msg["error"])
        else:
            p.response = msg.get("data") or {}
            latency = max(time.time() - p.submitted_at, 0.0)
            with self._lock:
                self._tokens_in  += int(p.response.get("input_tokens", 0) or 0)
                self._tokens_out += int(p.response.get("output_tokens", 0) or 0)
                self._latency_sum_s += latency
                self._latency_n     += 1
        p.event.set()

    # --------------------------------------------------------------- submit
    def call(self, *, system_prompt: str, user_prompt: str, model: str,
             max_output_tokens: int = 600,
             timeout: float = 180.0) -> dict[str, Any]:
        """Synchronously dispatch one request through the bridge. Blocks the
        calling thread until the Mac client responds, the connection drops,
        or the timeout fires. Returns the `data` payload directly."""
        if not self.is_connected():
            raise RuntimeError(
                "Local AI bridge not connected — start the bridge client "
                "on your Mac (Settings → Local AI Bridge)."
            )
        req_id = uuid.uuid4().hex
        request = {
            "type": "request",
            "request_id": req_id,
            "system_prompt": system_prompt,
            "user_prompt":   user_prompt,
            "model":         model,
            "max_output_tokens": int(max_output_tokens),
            "timeout":       float(timeout),
        }
        p = _PendingRequest()

        with self._lock:
            self._pending[req_id] = p
            self._calls_total += 1
            self._last_request_at = time.time()
            ws   = self._client
            loop = self._loop

        if ws is None or loop is None:
            with self._lock:
                self._pending.pop(req_id, None)
                self._calls_failed += 1
            raise RuntimeError("Bridge client gone before send")

        # Marshal the WebSocket .send_json() call onto the asyncio loop
        # that owns the connection — calling it from this worker thread
        # directly would race with the loop's own state machine.
        async def _send() -> None:
            await ws.send_json(request)

        try:
            future = asyncio.run_coroutine_threadsafe(_send(), loop)
            future.result(timeout=10.0)
        except Exception as exc:
            with self._lock:
                self._pending.pop(req_id, None)
                self._calls_failed += 1
            raise RuntimeError(f"Bridge send failed: {exc}") from exc

        # Now block this worker thread until the Mac answers (or doesn't).
        # The caller already picked a generous timeout for long extractions;
        # pad with a small grace margin so a Mac that's almost finished
        # still wins over the local clock.
        wait_s = max(timeout + 5.0, 10.0)
        if not p.event.wait(timeout=wait_s):
            with self._lock:
                self._pending.pop(req_id, None)
                self._calls_failed += 1
            raise TimeoutError(
                f"Local AI bridge timed out after {wait_s:.0f}s"
            )

        if p.error:
            with self._lock:
                self._calls_failed += 1
            raise RuntimeError(p.error)
        return p.response or {}


# ------------------------------------------------------------- module singleton
_singleton: Optional[BridgeServer] = None
_singleton_lock = threading.Lock()


def get_bridge() -> BridgeServer:
    global _singleton
    with _singleton_lock:
        if _singleton is None:
            _singleton = BridgeServer()
        return _singleton


# ------------------------------------------------------------- token storage
def _token_path(config_dir: Path) -> Path:
    return Path(config_dir) / "bridge_token"


def get_or_create_token(config_dir: Path) -> str:
    """Read the persisted shared secret (or mint a new one). The token is
    a 32-byte url-safe base64 string — long enough that brute force is
    not a meaningful threat even over the public internet."""
    p = _token_path(config_dir)
    try:
        existing = p.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    except FileNotFoundError:
        pass
    except OSError as exc:
        logger.warning("Could not read bridge token at %s: %s", p, exc)

    token = secrets.token_urlsafe(32)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(token, encoding="utf-8")
        try:
            os.chmod(p, 0o600)
        except OSError:
            pass
    except OSError as exc:
        logger.warning("Could not persist bridge token at %s: %s", p, exc)
    return token


def regenerate_token(config_dir: Path) -> str:
    """Replace the existing token with a fresh one. Any client still
    holding the old token will be rejected on its next reconnect."""
    p = _token_path(config_dir)
    try:
        p.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        logger.warning("Could not delete old bridge token at %s", p)
    return get_or_create_token(config_dir)
