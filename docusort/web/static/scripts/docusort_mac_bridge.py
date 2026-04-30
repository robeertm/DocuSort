#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DocuSort Local AI Bridge — Mac client.

Runs on the user's Mac, connects outbound to the DocuSort server, and
turns every incoming LLM request into a local Ollama call. No firewall
configuration, no port forwarding — the Mac is the one initiating the
connection, so anything that can reach the DocuSort URL works (home
network, Tailscale, public DNS, doesn't matter).

Designed to be runnable straight from the terminal with one command:

    python3 docusort_mac_bridge.py \\
        --server https://your-docusort.example \\
        --token  <token-from-settings>

The script will:
  1. Make sure Ollama is installed (uses Homebrew if available, otherwise
     downloads the official installer).
  2. Make sure ``ollama serve`` is running locally.
  3. Make sure the requested model is pulled.
  4. Open a WebSocket to ``/api/llm-bridge/ws`` and process incoming
     requests until the user hits Ctrl-C.

Only stdlib + `websockets` is required. The script self-installs
`websockets` into the user site-packages on first run if missing.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


# ----------------------------------------------------------------- defaults
DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434"
DEFAULT_MODEL      = "qwen2.5:7b-instruct"
USER_AGENT         = "DocuSortMacBridge/1.0"


# --------------------------------------------------------------- terminal UI
def _supports_color() -> bool:
    return sys.stdout.isatty() and os.environ.get("TERM", "") != "dumb"


_C = {
    "reset":  "\033[0m",
    "bold":   "\033[1m",
    "dim":    "\033[2m",
    "green":  "\033[32m",
    "yellow": "\033[33m",
    "red":    "\033[31m",
    "blue":   "\033[34m",
    "cyan":   "\033[36m",
}
if not _supports_color():
    _C = {k: "" for k in _C}


def info(msg: str)  -> None:
    print(f"{_C['cyan']}•{_C['reset']} {msg}", flush=True)
def good(msg: str)  -> None:
    print(f"{_C['green']}✓{_C['reset']} {msg}", flush=True)
def warn(msg: str)  -> None:
    print(f"{_C['yellow']}!{_C['reset']} {msg}", flush=True)
def fail(msg: str)  -> None:
    print(f"{_C['red']}✗{_C['reset']} {msg}", flush=True)
def step(msg: str)  -> None:
    print(f"{_C['bold']}→{_C['reset']} {msg}", flush=True)


# ------------------------------------------------------------ self-bootstrap
def _ensure_websockets() -> None:
    """The only third-party dep. We self-install into the user's
    site-packages on first run so the user does not need to know what
    pip is."""
    try:
        import websockets  # noqa: F401
        return
    except ImportError:
        pass
    info("Installing the 'websockets' library (one time only) …")
    cmd = [sys.executable, "-m", "pip", "install", "--user", "--quiet",
           "websockets>=12.0"]
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as exc:
        fail(f"pip install failed: {exc}")
        sys.exit(2)
    # Ensure the user-site is on sys.path for this run
    import site, importlib  # noqa: E401
    site.main()
    importlib.invalidate_caches()
    try:
        import websockets  # noqa: F401
    except ImportError:
        fail("websockets still not importable after install — try: "
             f"{sys.executable} -m pip install --user websockets")
        sys.exit(2)


# ------------------------------------------------------------------ ollama
def _have(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def _ollama_health(base: str = DEFAULT_OLLAMA_URL, timeout: float = 2.0) -> bool:
    try:
        req = urllib.request.Request(base + "/api/tags",
                                     headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def _ollama_models(base: str = DEFAULT_OLLAMA_URL) -> list[str]:
    try:
        req = urllib.request.Request(base + "/api/tags",
                                     headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read().decode("utf-8"))
        return [m.get("name", "") for m in (data.get("models") or [])]
    except Exception:
        return []


def install_ollama_macos(non_interactive: bool) -> None:
    """Best-effort install path: Homebrew when present, official script
    otherwise. We deliberately don't try to silently elevate privileges
    or run an arbitrary remote shell script unless the user is OK with
    it (`non_interactive=False` keeps the prompt)."""
    if _have("ollama"):
        return
    if _have("brew"):
        step("Installing Ollama via Homebrew …")
        subprocess.check_call(["brew", "install", "ollama"])
        return
    msg = ("Ollama is not installed and Homebrew was not found. "
           "I can run the official installer (curl https://ollama.com/install.sh | sh).")
    if non_interactive:
        warn(msg)
        warn("--non-interactive set — running the official installer …")
        proceed = True
    else:
        print(msg)
        ans = input("Run the installer now? [Y/n] ").strip().lower()
        proceed = ans in ("", "y", "yes")
    if not proceed:
        fail("Cannot continue without Ollama. Install it manually from "
             "https://ollama.com and re-run this script.")
        sys.exit(1)
    step("Running the Ollama installer …")
    subprocess.check_call(["bash", "-c",
                           "curl -fsSL https://ollama.com/install.sh | sh"])


def ensure_ollama_running() -> subprocess.Popen | None:
    """Return a Popen handle if we started ollama, else None."""
    if _ollama_health():
        good("Ollama is already running.")
        return None
    step("Starting `ollama serve` in the background …")
    proc = subprocess.Popen(
        ["ollama", "serve"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    # Wait up to 15 s for the daemon to come up.
    for _ in range(30):
        if _ollama_health():
            good("Ollama is up.")
            return proc
        time.sleep(0.5)
    fail("Ollama did not come up within 15 s. Check `ollama serve` manually.")
    proc.terminate()
    sys.exit(1)


def ensure_model(model: str) -> None:
    if model in _ollama_models():
        good(f"Model '{model}' already pulled.")
        return
    step(f"Pulling model '{model}' (one-time download — this is the slow step) …")
    subprocess.check_call(["ollama", "pull", model])
    good(f"Model '{model}' is ready.")


def call_ollama_blocking(*, model: str, system_prompt: str, user_prompt: str,
                         max_output_tokens: int, timeout: float,
                         force_json: bool) -> dict:
    """Synchronous Ollama chat call. Returns dict with raw_text + token
    counts. We deliberately stay on stdlib (urllib) so the bridge has
    no extra dependencies."""
    body: dict = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        "stream": False,
        "options": {
            "num_predict": int(max_output_tokens),
            "temperature": 0.0,
        },
    }
    if force_json:
        # Ollama 0.1.30+ honours format=json: the server constrains the
        # generation to valid JSON and saves us a parse-or-bust round-trip.
        body["format"] = "json"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        DEFAULT_OLLAMA_URL + "/api/chat",
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent":   USER_AGENT,
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        out = json.loads(r.read().decode("utf-8"))
    raw = (out.get("message") or {}).get("content", "") or ""
    return {
        "raw_text":      raw,
        "model":         model,
        "input_tokens":  int(out.get("prompt_eval_count", 0) or 0),
        "output_tokens": int(out.get("eval_count", 0) or 0),
    }


# ---------------------------------------------------------------- main loop
async def run_loop(*, server_url: str, token: str, model: str,
                   force_json: bool, insecure: bool) -> None:
    import asyncio
    import websockets  # type: ignore

    parsed = urllib.parse.urlparse(server_url.rstrip("/"))
    if parsed.scheme not in ("http", "https"):
        fail(f"--server must start with http(s)://, got: {server_url!r}")
        sys.exit(1)
    ws_scheme = "wss" if parsed.scheme == "https" else "ws"
    ws_url = (
        f"{ws_scheme}://{parsed.netloc}/api/llm-bridge/ws"
        f"?token={urllib.parse.quote(token, safe='')}"
    )

    # Tailscale-issued certs are trusted; self-signed local servers
    # need --insecure. We never accept invalid certs by default.
    ssl_ctx = None
    if ws_scheme == "wss":
        if insecure:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        else:
            ssl_ctx = ssl.create_default_context()

    backoff = 1.0
    while True:
        try:
            info(f"Connecting to {parsed.netloc} …")
            async with websockets.connect(
                ws_url,
                ssl=ssl_ctx,
                ping_interval=20, ping_timeout=20,
                max_size=8 * 1024 * 1024,  # 8 MB — long bank statements
            ) as ws:
                # Hello envelope — first message after accept must be this.
                hello = {
                    "type": "hello",
                    "client": {
                        "host":     socket.gethostname(),
                        "platform": platform.platform(),
                        "machine":  platform.machine(),
                        "python":   platform.python_version(),
                        "model":    model,
                        "ollama":   _ollama_version_or_blank(),
                    },
                }
                await ws.send(json.dumps(hello))
                welcome = json.loads(await ws.recv())
                good(f"Connected to DocuSort {welcome.get('version', '?')} — "
                     f"model={model}. Waiting for requests …")
                backoff = 1.0  # reset

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        warn(f"Ignoring non-JSON frame ({len(raw)} bytes)")
                        continue
                    if msg.get("type") != "request":
                        continue
                    req_id = str(msg.get("request_id", ""))
                    short  = req_id[:8] or "??"
                    # Server may request a specific model. We honour the
                    # request when it looks like an Ollama tag; if the
                    # server is still on a cloud-style identifier
                    # (claude-…, gpt-4o, gemini-…) we silently fall back
                    # to the script's --model arg, so a misconfigured
                    # server can't break the bridge.
                    requested = str(msg.get("model") or "").strip()
                    active_model = requested if (requested and not _looks_cloudy(requested)) else model
                    started = time.time()
                    try:
                        loop = asyncio.get_running_loop()
                        m = active_model  # bind for the closure
                        data = await loop.run_in_executor(
                            None,
                            lambda: call_ollama_blocking(
                                model=m,
                                system_prompt=msg.get("system_prompt", ""),
                                user_prompt=msg.get("user_prompt", ""),
                                max_output_tokens=int(msg.get("max_output_tokens", 600)),
                                timeout=float(msg.get("timeout", 180.0)),
                                force_json=force_json,
                            ),
                        )
                        await ws.send(json.dumps({
                            "type": "response",
                            "request_id": req_id,
                            "data": data,
                        }))
                        dt = time.time() - started
                        good(f"{short}  ✓  {data['input_tokens']:>5} → "
                             f"{data['output_tokens']:>4} tok in "
                             f"{dt:5.1f}s")
                    except Exception as exc:
                        await ws.send(json.dumps({
                            "type": "response",
                            "request_id": req_id,
                            "error": str(exc),
                        }))
                        dt = time.time() - started
                        fail(f"{short}  ✗  {exc} ({dt:5.1f}s)")
        except (websockets.exceptions.InvalidStatus,
                websockets.exceptions.InvalidStatusCode) as exc:
            # 4401 = invalid token → no point retrying
            code = getattr(exc, "status_code", None) or \
                   getattr(getattr(exc, "response", None), "status_code", None)
            fail(f"Server rejected the connection (HTTP {code}). "
                 "Likely an invalid or regenerated token.")
            sys.exit(2)
        except (websockets.exceptions.ConnectionClosed,
                ConnectionRefusedError, OSError) as exc:
            warn(f"Connection lost: {exc}. Retrying in {backoff:.1f}s …")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
        except KeyboardInterrupt:
            info("Shutting down (Ctrl-C).")
            return


_CLOUD_MODEL_PREFIXES = ("claude-", "gpt-", "gpt4", "o1-", "o3-",
                         "gemini-", "models/gemini", "command-",
                         "anthropic.", "openai.", "google.")

def _looks_cloudy(name: str) -> bool:
    """Heuristic: does this look like a cloud-API model id rather than
    an Ollama tag? Used to ignore stale provider settings on the
    server when we know we are running locally."""
    n = name.strip().lower()
    return any(n.startswith(p) for p in _CLOUD_MODEL_PREFIXES)


def _ollama_version_or_blank() -> str:
    try:
        out = subprocess.check_output(
            ["ollama", "--version"], text=True, stderr=subprocess.STDOUT,
        )
        return out.strip().splitlines()[0]
    except Exception:
        return ""


# ------------------------------------------------------------------- entry
def main() -> None:
    ap = argparse.ArgumentParser(
        prog="docusort_mac_bridge",
        description="Local AI bridge for DocuSort — runs LLM inference "
                    "on your Mac and forwards answers back to the server.",
    )
    ap.add_argument("--server", default=os.environ.get("DOCUSORT_SERVER", ""),
                    help="DocuSort URL, e.g. https://docusort.lan:9876")
    ap.add_argument("--token",  default=os.environ.get("DOCUSORT_BRIDGE_TOKEN", ""),
                    help="Bridge token from Settings → Local AI Bridge")
    ap.add_argument("--model",  default=os.environ.get("DOCUSORT_MODEL", DEFAULT_MODEL),
                    help=f"Ollama model name (default: {DEFAULT_MODEL})")
    ap.add_argument("--no-format-json", action="store_true",
                    help="Don't ask Ollama for format=json (use if a model "
                         "doesn't support it cleanly)")
    ap.add_argument("--non-interactive", action="store_true",
                    help="Don't prompt for confirmations; fail instead.")
    ap.add_argument("--insecure", action="store_true",
                    help="Accept self-signed TLS certs (e.g. local dev)")
    ap.add_argument("--no-auto-install", action="store_true",
                    help="Skip the Ollama auto-install step")
    args = ap.parse_args()

    if not args.server or not args.token:
        fail("Missing --server or --token. Both are shown in DocuSort under "
             "Settings → Local AI Bridge.")
        sys.exit(1)

    print(f"{_C['bold']}DocuSort Mac Bridge{_C['reset']} → {args.server}")
    print(f"  model:   {args.model}")
    print(f"  python:  {sys.executable}")
    print()

    if platform.system() != "Darwin":
        warn(f"You're running on {platform.system()} — the bridge works "
             "anywhere, but the auto-install path is tuned for macOS.")

    _ensure_websockets()

    if not args.no_auto_install:
        install_ollama_macos(args.non_interactive)
    elif not _have("ollama"):
        fail("Ollama not found and --no-auto-install was set.")
        sys.exit(1)

    proc = ensure_ollama_running()

    try:
        ensure_model(args.model)
    except subprocess.CalledProcessError as exc:
        fail(f"Could not pull model '{args.model}': {exc}")
        if proc is not None:
            proc.terminate()
        sys.exit(2)

    print()
    try:
        import asyncio
        asyncio.run(run_loop(
            server_url=args.server,
            token=args.token,
            model=args.model,
            force_json=not args.no_format_json,
            insecure=args.insecure,
        ))
    except KeyboardInterrupt:
        info("Bye.")
    finally:
        if proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass


if __name__ == "__main__":
    main()
