#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DocuSort Local AI Bridge — client.

Runs on a computer you control (Mac, Linux box, or Windows machine),
connects outbound to the DocuSort server, and turns every incoming
LLM request into a local Ollama call. No firewall configuration, no
port forwarding — the client is the one initiating the connection,
so anything that can reach the DocuSort URL works (home network,
Tailscale, public DNS, doesn't matter).

Designed to be runnable straight from the terminal with one command:

    python3 docusort_bridge.py \\
        --server https://your-docusort.example \\
        --token  <token-from-settings>

The script will:
  1. Make sure Ollama is installed (Homebrew on macOS, the official
     install script on Linux, winget on Windows; falls back to a
     manual prompt when no automated path works).
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


def _confirm(msg: str, non_interactive: bool, default_yes: bool = True) -> bool:
    if non_interactive:
        return True
    suffix = " [Y/n] " if default_yes else " [y/N] "
    ans = input(msg + suffix).strip().lower()
    if not ans:
        return default_yes
    return ans in ("y", "yes", "j", "ja")


def install_ollama(non_interactive: bool) -> None:
    """Cross-platform install path. macOS prefers Homebrew, Linux uses
    the official install script, Windows uses winget when available
    and otherwise points the user at the installer download. We never
    silently elevate privileges or run remote shell scripts without
    explicit confirmation."""
    if _have("ollama"):
        return
    sysname = platform.system()

    if sysname == "Darwin":
        if _have("brew"):
            step("Installing Ollama via Homebrew …")
            subprocess.check_call(["brew", "install", "ollama"])
            return
        if _confirm("Ollama not found, Homebrew missing. Run the official "
                    "macOS installer (curl https://ollama.com/install.sh | sh)?",
                    non_interactive):
            step("Running the Ollama installer …")
            subprocess.check_call(["bash", "-c",
                                   "curl -fsSL https://ollama.com/install.sh | sh"])
            return
    elif sysname == "Linux":
        if _confirm("Ollama not found. Run the official Linux installer "
                    "(curl https://ollama.com/install.sh | sh)?",
                    non_interactive):
            step("Running the Ollama installer …")
            subprocess.check_call(["bash", "-c",
                                   "curl -fsSL https://ollama.com/install.sh | sh"])
            return
    elif sysname == "Windows":
        if _have("winget") and _confirm(
                "Ollama not found. Install via winget (Ollama.Ollama)?",
                non_interactive):
            step("Installing Ollama via winget …")
            subprocess.check_call(
                ["winget", "install", "--silent", "--accept-source-agreements",
                 "--accept-package-agreements", "Ollama.Ollama"])
            return
        fail("Ollama not found. Download and run the Windows installer "
             "from https://ollama.com/download/windows, then re-run this "
             "script.")
        sys.exit(1)
    else:
        fail(f"Unsupported platform: {sysname}. Install Ollama manually "
             "from https://ollama.com and re-run this script.")
        sys.exit(1)

    fail("Cannot continue without Ollama. Install it manually from "
         "https://ollama.com and re-run this script.")
    sys.exit(1)


# Backwards-compat alias — older copies of the install command in
# people's terminal history still call install_ollama_macos.
install_ollama_macos = install_ollama


def maybe_upgrade_ollama(*, non_interactive: bool, disabled: bool) -> None:
    """Check for and apply Ollama updates at script startup, before
    `ollama serve` comes up. Doing it pre-serve guarantees no in-flight
    request can be interrupted by the upgrade restart.

    We only auto-upgrade through package managers that are quiet,
    non-interactive, and idempotent (Homebrew on macOS, winget on
    Windows). Hosts without a package manager just skip the upgrade —
    a manual `curl | sh` re-run would work but it's an intrusive
    interactive script and we don't want to surprise anyone.

    A failed upgrade check NEVER aborts the bridge — it logs a warning
    and we continue with whatever Ollama version is already there.
    """
    if disabled or not _have("ollama"):
        return
    sysname = platform.system()
    try:
        if sysname == "Darwin" and _have("brew"):
            # `brew outdated --json=v2` gives us a structured answer in
            # ~1 s; we only run `brew upgrade ollama` when it's needed.
            out = subprocess.run(
                ["brew", "outdated", "--json=v2"],
                capture_output=True, text=True, timeout=20, check=False,
            )
            data = {}
            if out.returncode == 0 and out.stdout.strip():
                try:
                    data = json.loads(out.stdout)
                except Exception:
                    data = {}
            outdated_names = {f.get("name") for f in (data.get("formulae") or [])}
            outdated_names |= {f.get("name") for f in (data.get("casks") or [])}
            if "ollama" in outdated_names:
                step("Ollama update available — upgrading via Homebrew …")
                subprocess.run(
                    ["brew", "upgrade", "ollama"],
                    timeout=600, check=False,
                )
                good("Ollama upgraded.")
            else:
                info("Ollama is up to date (Homebrew).")
            return

        if sysname == "Windows" and _have("winget"):
            # winget exits 0 even when nothing was upgraded; --silent
            # keeps the install UI from popping up. Long timeout in
            # case there's a real download (~250 MB).
            step("Checking for Ollama updates via winget …")
            r = subprocess.run(
                ["winget", "upgrade", "--silent",
                 "--accept-source-agreements", "--accept-package-agreements",
                 "--id", "Ollama.Ollama"],
                capture_output=True, text=True, timeout=900, check=False,
            )
            txt = (r.stdout or "") + (r.stderr or "")
            if "No applicable update" in txt or "No installed package" in txt:
                info("Ollama is up to date (winget).")
            else:
                good("winget run finished — any pending Ollama update applied.")
            return

        # macOS-without-brew, Linux, BSD, etc: the official install.sh
        # is the upgrade path but it's interactive and intrusive.
        # Surface a hint instead of running it silently.
        info("Auto-upgrade only runs through Homebrew (macOS) or winget "
             "(Windows). Run `curl -fsSL https://ollama.com/install.sh | sh` "
             "manually to upgrade on this host.")
    except subprocess.TimeoutExpired:
        warn("Ollama upgrade check timed out — keeping the current version.")
    except Exception as exc:
        warn(f"Ollama upgrade check failed: {exc}")


def ensure_ollama_running() -> subprocess.Popen | None:
    """Return a Popen handle if we started ollama, else None."""
    if _ollama_health():
        good("Ollama is already running.")
        return None
    step("Starting `ollama serve` in the background …")
    popen_kwargs: dict[str, Any] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    # On POSIX detach via start_new_session; on Windows the equivalent
    # is CREATE_NEW_PROCESS_GROUP so closing this script doesn't take
    # the daemon down with it.
    if platform.system() == "Windows":
        popen_kwargs["creationflags"] = (
            subprocess.CREATE_NEW_PROCESS_GROUP
            | getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
        )
    else:
        popen_kwargs["start_new_session"] = True
    proc = subprocess.Popen(["ollama", "serve"], **popen_kwargs)
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


def _estimate_ctx_size(system_prompt: str, user_prompt: str,
                       max_output_tokens: int) -> int:
    """Pick a num_ctx that is big enough for prompt + output without
    blowing up unified memory. Ollama defaults to 4096 across versions,
    which silently truncates anything longer — that quietly destroys
    bank-statement extraction (10k+ chars of OCR text). We size up to
    fit the whole exchange and cap at 32k."""
    chars = len(system_prompt) + len(user_prompt)
    # German legal/finance text averages ~3.3 chars/token for Qwen and
    # similar tokenizers. Leave a chunky margin for tokenizer surprises
    # and for the chat template overhead.
    prompt_tokens = int(chars / 3.0) + 256
    needed = prompt_tokens + int(max_output_tokens) + 512
    # Round up to a power-of-two-ish step so the KV cache buffer is a
    # familiar size and we don't wobble between near-identical values
    # for similar inputs.
    for cap in (8192, 12288, 16384, 24576, 32768):
        if needed <= cap:
            return cap
    return 32768  # hard cap


def call_ollama_blocking(*, model: str, system_prompt: str, user_prompt: str,
                         max_output_tokens: int, timeout: float,
                         force_json: bool) -> dict:
    """Synchronous Ollama chat call. Returns dict with raw_text + token
    counts. We deliberately stay on stdlib (urllib) so the bridge has
    no extra dependencies."""
    num_ctx = _estimate_ctx_size(system_prompt, user_prompt, max_output_tokens)
    body: dict = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt},
        ],
        "stream": False,
        "options": {
            "num_predict": int(max_output_tokens),
            "num_ctx":     num_ctx,
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
    in_tok  = int(out.get("prompt_eval_count", 0) or 0)
    out_tok = int(out.get("eval_count", 0) or 0)
    # Ollama's `truncated` flag is unreliable across versions, so we
    # compare the prompt token count against the requested ctx window.
    # When they meet exactly, the prompt was clipped — surface that to
    # the server so the user sees a real warning instead of silent
    # quality loss.
    truncated = (in_tok >= num_ctx - 8)
    return {
        "raw_text":      raw,
        "model":         model,
        "input_tokens":  in_tok,
        "output_tokens": out_tok,
        "num_ctx":       num_ctx,
        "truncated":     truncated,
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

    # Survives across reconnect attempts: responses we computed but
    # couldn't send (or weren't able to send fully) before the WS died.
    # On the next connection we hand them to the server inside the
    # hello envelope, so an extraction whose answer was lost in transit
    # still ends up where it belongs.
    queued_responses: list[dict] = []
    # IDs we've already processed (or are processing). On reconnect the
    # server may resend its still-pending requests; any redelivery we
    # already have in flight or queued is silently dropped to avoid
    # double-running expensive 10-min inference jobs.
    seen_request_ids: set[str] = set()

    backoff = 1.0
    while True:
        try:
            info(f"Connecting to {parsed.netloc} …")
            async with websockets.connect(
                ws_url,
                ssl=ssl_ctx,
                # Long local inference (10–30 min for a big statement
                # on a 14B/32B model) must not trigger a phantom
                # disconnect. Both sides are bumped to a 5-minute
                # ping_timeout so a model that takes its time keeps
                # the WebSocket open. The interval stays short enough
                # to detect a *real* dead connection within a few
                # minutes.
                ping_interval=30, ping_timeout=300,
                # 16 MB — covers very long bank statements
                # (300+ booking lines) plus some headroom.
                max_size=16 * 1024 * 1024,
            ) as ws:
                # Hello envelope — first message after accept must be this.
                # Carries any responses we computed but couldn't deliver
                # before the previous WS died.
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
                    "queued_responses": list(queued_responses),
                }
                await ws.send(json.dumps(hello))
                welcome = json.loads(await ws.recv())
                # Responses are now delivered to the server (or will be
                # by the time the welcome reply lands) — clear the queue.
                if queued_responses:
                    info(f"Replayed {len(queued_responses)} queued response(s).")
                queued_responses = []
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
                    redelivery = bool(msg.get("redelivery"))
                    # Dedupe redeliveries: the server resends still-
                    # pending requests after a reconnect. If we've seen
                    # the id (already done, currently running, or in
                    # the queued_responses backlog) drop it silently.
                    already_queued = any(
                        r.get("request_id") == req_id for r in queued_responses
                    )
                    if req_id in seen_request_ids or already_queued:
                        if redelivery:
                            info(f"{short}  · skip redelivery (already handled)")
                        continue
                    seen_request_ids.add(req_id)
                    # Bound the dedup set so a long-running bridge doesn't
                    # leak memory (request ids are 32-byte hex; the bound
                    # is generous).
                    if len(seen_request_ids) > 5000:
                        seen_request_ids = set(list(seen_request_ids)[-2500:])
                    # Server may request a specific model. We honour the
                    # request when it looks like an Ollama tag; if the
                    # server is still on a cloud-style identifier
                    # (claude-…, gpt-4o, gemini-…) we silently fall back
                    # to the script's --model arg, so a misconfigured
                    # server can't break the bridge.
                    requested = str(msg.get("model") or "").strip()
                    active_model = requested if (requested and not _looks_cloudy(requested)) else model
                    started = time.time()
                    response: dict
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
                        response = {"type": "response", "request_id": req_id, "data": data}
                        dt = time.time() - started
                        flag = " (truncated!)" if data.get("truncated") else ""
                        ok_log = (f"{short}  ✓  {data['input_tokens']:>5} → "
                                  f"{data['output_tokens']:>4} tok "
                                  f"(ctx={data.get('num_ctx', '?')}) in "
                                  f"{dt:5.1f}s{flag}")
                    except Exception as exc:
                        response = {"type": "response", "request_id": req_id,
                                    "error": str(exc)}
                        dt = time.time() - started
                        ok_log = None
                        fail(f"{short}  ✗  {exc} ({dt:5.1f}s)")

                    # Best-effort send. If the WS just died we keep the
                    # response in queued_responses so the next connect's
                    # hello envelope delivers it — never lose computed
                    # work to a network blip.
                    try:
                        await ws.send(json.dumps(response))
                        if ok_log: good(ok_log)
                    except Exception as exc:
                        queued_responses.append(response)
                        warn(f"{short}  ✎ queued for redelivery (send failed: {exc})")
                        # Bubble up so the outer reconnect logic kicks in.
                        raise
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
        prog="docusort_bridge",
        description="Local AI bridge for DocuSort — runs LLM inference "
                    "on your computer and forwards answers back to the server.",
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
    ap.add_argument("--no-auto-upgrade-ollama", action="store_true",
                    help="Skip the per-startup check for Ollama updates")
    args = ap.parse_args()

    if not args.server or not args.token:
        fail("Missing --server or --token. Both are shown in DocuSort under "
             "Settings → Local AI Bridge.")
        sys.exit(1)

    print(f"{_C['bold']}DocuSort Local Bridge{_C['reset']} → {args.server}")
    print(f"  os:      {platform.system()} {platform.release()}")
    print(f"  model:   {args.model}")
    print(f"  python:  {sys.executable}")
    print()

    _ensure_websockets()

    if not args.no_auto_install:
        install_ollama(args.non_interactive)
    elif not _have("ollama"):
        fail("Ollama not found and --no-auto-install was set.")
        sys.exit(1)

    # Run the upgrade check BEFORE serving — guarantees no in-flight
    # request gets interrupted by an Ollama restart mid-extraction.
    maybe_upgrade_ollama(
        non_interactive=args.non_interactive,
        disabled=args.no_auto_upgrade_ollama,
    )

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
