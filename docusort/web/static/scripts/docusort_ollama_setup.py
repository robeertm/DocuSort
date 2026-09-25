#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DocuSort — one-click setup for a local model (Ollama).

Downloaded from your own DocuSort and started with a double-click. It does the
four things that otherwise take a manual afternoon:

  1. install Ollama if it isn't there (Homebrew on macOS, the official install
     script on Linux, winget on Windows),
  2. make it listen where DocuSort can actually reach it,
  3. pull a model,
  4. write the setting and then ask DOCUSORT whether it works — not this
     machine. DocuSort.

Usage (the launcher fills all of this in for you):

    python3 docusort_ollama_setup.py --docusort https://docusort.local:9876 \\
                                     --ticket <short-lived setup ticket>

🔴 Nothing here reads your documents and nothing is uploaded. The only thing
that leaves this machine is the address of this machine, sent to the DocuSort
you downloaded the script from.

This is the sibling of the Postwache's setup script — same idea, same order,
same refusal to call anything "done" before the other side says so.
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
import urllib.error
import urllib.request

OLLAMA_PORT = 11434
DEFAULT_MODEL = "qwen2.5:7b-instruct"     # what DocuSort's docs recommend
SMALL_MODEL = "llama3.2:3b"               # for machines with little memory
WISH = ("qwen2.5:7b-instruct", "qwen2.5:14b-instruct", "llama3.1:8b",
        "llama3.2:3b", "mistral:7b", "gemma2:9b")
UNUSABLE = ("embed", "bge-", "minilm", "clip", "rerank", "nomic-",
            "llava", "moondream")
SERVE_WAIT = 40
VERIFY_WAIT = 180                          # the first answer loads the model

RED, GREEN, YELLOW, GREY, OFF = ("\033[31m", "\033[32m", "\033[33m",
                                 "\033[90m", "\033[0m")
if not sys.stdout.isatty() or os.environ.get("NO_COLOR"):
    RED = GREEN = YELLOW = GREY = OFF = ""


def step(t): print("\n%s▸ %s%s" % (YELLOW, t, OFF), flush=True)
def good(t): print("%s  ✓ %s%s" % (GREEN, t, OFF), flush=True)
def info(t): print("%s    %s%s" % (GREY, t, OFF), flush=True)
def warn(t): print("%s  ! %s%s" % (YELLOW, t, OFF), flush=True)


def stop(t: str, code: int = 1):
    print("\n%s✋ %s%s" % (RED, t, OFF), flush=True)
    if sys.stdin.isatty():
        try:
            input("\nPress Enter to close this window. ")
        except (EOFError, KeyboardInterrupt):
            pass
    sys.exit(code)


def ask_yes_no(text: str, default_yes: bool = True) -> bool:
    """Without a terminal (double-clicked in some desktops) take the default
    rather than hang forever."""
    if not sys.stdin.isatty():
        info("%s  → %s (no terminal, taking the default)"
             % (text, "yes" if default_yes else "no"))
        return default_yes
    hint = "[Y/n]" if default_yes else "[y/N]"
    try:
        a = input("\n  %s %s " % (text, hint)).strip().lower()
    except (EOFError, KeyboardInterrupt):
        return False
    return default_yes if not a else a.startswith("y")


def have(prog: str) -> bool:
    return shutil.which(prog) is not None


# ---------------------------------------------------------------------- HTTP
def _ctx(insecure: bool):
    if not insecure:
        return None
    c = ssl.create_default_context()
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    return c


def get(url: str, timeout: float = 5.0, insecure: bool = False):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout, context=_ctx(insecure)) as r:
        return json.loads(r.read().decode("utf-8", "replace") or "{}")


def post(url: str, body: dict, timeout: float = 30.0, insecure: bool = False):
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout, context=_ctx(insecure)) as r:
        return json.loads(r.read().decode("utf-8", "replace") or "{}")


def detail(exc: Exception) -> str:
    """FastAPI puts the real reason in the body, not in the status line."""
    if isinstance(exc, urllib.error.HTTPError):
        try:
            return str(json.loads(exc.read().decode("utf-8", "replace")).get("detail")
                       or exc)
        except Exception:
            return "HTTP %s" % exc.code
    return str(exc)[:200]


# -------------------------------------------------------------------- Ollama
def ollama_models(base: str, timeout: float = 2.0) -> list:
    try:
        return [str((m or {}).get("name") or "")
                for m in (get(base.rstrip("/") + "/api/tags", timeout).get("models") or [])]
    except Exception:
        return []


def pick_model(models: list) -> str:
    usable = [m for m in models if not any(u in m.lower() for u in UNUSABLE)]
    for want in WISH:
        for m in usable:
            if m == want or m.split(":")[0] == want.split(":")[0]:
                return m
    return usable[0] if usable else ""


def install_ollama() -> None:
    if have("ollama"):
        return good("Ollama is already installed.")
    system = platform.system()
    step("Installing Ollama")
    if system == "Darwin":
        if have("brew"):
            info("via Homebrew …")
            subprocess.check_call(["brew", "install", "ollama"])
            return good("Ollama installed.")
        if ask_yes_no("Homebrew not found. Run the official installer "
                      "(curl -fsSL https://ollama.com/install.sh | sh)?"):
            subprocess.check_call(["/bin/bash", "-c",
                                   "curl -fsSL https://ollama.com/install.sh | sh"])
            return good("Ollama installed.")
    elif system == "Linux":
        if ask_yes_no("Run the official installer "
                      "(curl -fsSL https://ollama.com/install.sh | sh)?"):
            subprocess.check_call(["/bin/bash", "-c",
                                   "curl -fsSL https://ollama.com/install.sh | sh"])
            return good("Ollama installed.")
    elif system == "Windows":
        if have("winget") and ask_yes_no("Install Ollama via winget?"):
            subprocess.check_call(
                ["winget", "install", "--silent", "--accept-source-agreements",
                 "--accept-package-agreements", "Ollama.Ollama"])
            return good("Ollama installed.")
        stop("Ollama not found. Install it from "
             "https://ollama.com/download/windows and run this file again.")
    else:
        stop("Unsupported platform: %s. Install Ollama from https://ollama.com "
             "and run this file again." % system)
    stop("Cannot continue without Ollama. Install it from https://ollama.com "
         "and run this file again.")


def serve(bind: str) -> bool:
    """Start `ollama serve` in the background, bound to `bind`.

    🔴 The bind address is the whole point. Ollama listens on 127.0.0.1 by
    default — perfectly right, and perfectly useless when DocuSort runs on a
    different machine."""
    env = dict(os.environ, OLLAMA_HOST="%s:%d" % (bind, OLLAMA_PORT))
    log = os.path.join(os.path.expanduser("~"), "ollama-docusort.log")
    step("Starting Ollama (listening on %s:%d)" % (bind, OLLAMA_PORT))
    try:
        with open(log, "ab") as fh:
            kw = {"stdout": fh, "stderr": fh, "env": env}
            if platform.system() == "Windows":
                kw["creationflags"] = 0x00000008        # DETACHED_PROCESS
            else:
                kw["start_new_session"] = True
            subprocess.Popen(["ollama", "serve"], **kw)
    except Exception as exc:
        warn("Could not start Ollama: %s" % exc)
        return False
    info("log: %s" % log)
    for _ in range(SERVE_WAIT):
        if ollama_models("http://127.0.0.1:%d" % OLLAMA_PORT, 1.0):
            good("Ollama is up.")
            return True
        time.sleep(1)
    warn("Ollama did not answer within %d s." % SERVE_WAIT)
    return False


def bind_permanently(bind: str) -> None:
    """Make the bind address survive a restart. Every system has its own way,
    and none of them is `ollama serve` — on macOS the menu-bar app wins, on
    Linux systemd does."""
    system = platform.system()
    value = "%s:%d" % (bind, OLLAMA_PORT)
    if system == "Darwin":
        try:
            subprocess.check_call(["launchctl", "setenv", "OLLAMA_HOST", value])
            good("OLLAMA_HOST=%s set for this login session." % value)
            info("To keep it across reboots, add this to your shell profile:")
            info("  launchctl setenv OLLAMA_HOST %s" % value)
        except Exception as exc:
            warn("Could not set OLLAMA_HOST: %s" % exc)
    elif system == "Linux":
        info("If Ollama runs as a systemd service, it needs the same setting:")
        info("  sudo systemctl edit ollama")
        info("  [Service]")
        info('  Environment="OLLAMA_HOST=%s"' % value)
        info("  sudo systemctl restart ollama")
    elif system == "Windows":
        try:
            subprocess.check_call(["setx", "OLLAMA_HOST", value],
                                  stdout=subprocess.DEVNULL)
            good("OLLAMA_HOST=%s stored for your user account." % value)
            info("Quit Ollama in the system tray and start it again to apply it.")
        except Exception as exc:
            warn("Could not store OLLAMA_HOST: %s" % exc)


def pull(model: str) -> bool:
    step("Pulling model %s — several GB the first time" % model)
    try:
        return subprocess.call(["ollama", "pull", model]) == 0
    except Exception as exc:
        warn("Pull failed: %s" % exc)
        return False


# ------------------------------------------------------------- the way there
def split(origin: str):
    from urllib.parse import urlsplit
    t = urlsplit(origin if "://" in origin else "https://" + origin)
    return t.scheme, (t.hostname or "127.0.0.1"), (t.port or
                                                   (443 if t.scheme == "https" else 80))


def my_address(host: str, port: int) -> str:
    """The address THIS machine has from DocuSort's point of view.

    🔴 Asking the hostname gives the wrong answer on any machine with more
    than one network — a VPN, a docker bridge, two Wi-Fi adapters. The routing
    table knows; a connected UDP socket asks it without sending a packet."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((host, port))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def main() -> int:
    p = argparse.ArgumentParser(description="Set up a local model for DocuSort.")
    p.add_argument("--docusort", required=True, help="e.g. https://docusort.local:9876")
    p.add_argument("--ticket", required=True, help="short-lived setup ticket")
    p.add_argument("--model", default="", help="model to pull (default: %s)" % DEFAULT_MODEL)
    p.add_argument("--insecure", action="store_true",
                   help="accept a self-signed certificate on DocuSort")
    a = p.parse_args()
    origin = a.docusort.rstrip("/")
    scheme, host, port = split(origin)

    print("%s\nDocuSort — local model setup%s" % (YELLOW, OFF))
    print("  DocuSort: %s" % origin)

    # 1. Can we reach DocuSort at all?
    step("Reaching DocuSort")
    try:
        ver = get(origin + "/api/version", 8.0, a.insecure)
    except Exception as exc:
        stop("Cannot reach %s (%s).\nIs DocuSort running, and is this machine "
             "on the same network?" % (origin, detail(exc)))
    good("DocuSort %s answers." % (ver.get("current") or ver.get("version") or "?"))

    # 2. Is DocuSort on THIS machine?
    mine = my_address(host, port)
    here = mine.startswith("127.") or host in ("localhost", "127.0.0.1", "::1")
    if here:
        bind, visible = "127.0.0.1", "127.0.0.1"
        info("DocuSort runs on this machine — nothing needs to be exposed.")
    else:
        bind, visible = "0.0.0.0", mine
        info("DocuSort runs elsewhere; it will reach this machine at %s." % mine)

    # 3. Ollama
    install_ollama()

    step("Checking Ollama")
    local = ollama_models("http://127.0.0.1:%d" % OLLAMA_PORT)
    target = "http://%s:%d" % (visible, OLLAMA_PORT)
    reachable = local if here else ollama_models(target)

    if reachable:
        good("Ollama answers at %s." % target)
    elif local and not here:
        warn("Ollama runs, but only for this machine (127.0.0.1).")
        print("\n  DocuSort sits on another computer, so Ollama has to listen")
        print("  on the network as well. That means: anyone on your local")
        print("  network can then use this model — Ollama has no password.")
        print("  On a home network that is usually fine; on a shared or")
        print("  public one it is not.")
        if not ask_yes_no("Let Ollama listen on the network (0.0.0.0:%d)?"
                          % OLLAMA_PORT, default_yes=False):
            stop("Nothing changed. You can also run DocuSort and Ollama on the "
                 "same machine, then none of this is needed.", 0)
        bind_permanently("0.0.0.0")
        if platform.system() == "Darwin":
            info("Restarting the Ollama app so it picks up the new setting …")
            subprocess.call(["osascript", "-e", 'quit app "Ollama"'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2)
            subprocess.call(["open", "-a", "Ollama"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(4)
        reachable = ollama_models(target, 4.0)
        if not reachable:
            serve("0.0.0.0")
            reachable = ollama_models(target, 4.0)
    else:
        if not here and not ask_yes_no(
                "Ollama has to listen on the network (0.0.0.0:%d) so DocuSort "
                "can reach it. Anyone on your local network can then use the "
                "model — Ollama has no password. Continue?" % OLLAMA_PORT,
                default_yes=False):
            stop("Nothing changed.", 0)
        if not here:
            bind_permanently("0.0.0.0")
        serve(bind)
        reachable = ollama_models(target, 4.0)

    if not reachable:
        stop("Ollama is not reachable at %s.\nIf a firewall is in the way, "
             "allow port %d for your local network." % (target, OLLAMA_PORT))
    good("%d model(s) available." % len(reachable))

    # 4. Model — take what is already there before downloading gigabytes.
    model = a.model.strip() or pick_model(reachable) or DEFAULT_MODEL
    if model not in reachable:
        if not pull(model):
            if model != SMALL_MODEL and ask_yes_no(
                    "Pull failed. Try the smaller %s instead?" % SMALL_MODEL):
                model = SMALL_MODEL
                if not pull(model):
                    stop("Could not pull a model.")
            else:
                stop("Could not pull a model.")
        good("Model %s ready." % model)
    else:
        good("Model %s is already there." % model)

    # 5. Tell DocuSort — and let DOCUSORT say whether it works.
    step("Telling DocuSort, and asking it to try the model")
    info("The first answer loads the model into memory — this can take a minute.")
    try:
        res = post(origin + "/api/local-ai/adopt",
                   {"ticket": a.ticket, "url": target, "model": model},
                   VERIFY_WAIT, a.insecure)
    except Exception as exc:
        stop("DocuSort refused the setting: %s" % detail(exc))
    if not res.get("verified"):
        stop("The setting is saved, but DocuSort cannot use the model yet:\n  %s"
             "\n\nMost often a firewall on this machine is blocking port %d."
             % (res.get("answer") or "?", OLLAMA_PORT))
    good("DocuSort answers: %s" % res.get("answer"))

    # 6. The classifier only switches over on a restart.
    if res.get("restart_required"):
        print("\n  DocuSort has saved the setting, but the running classifier")
        print("  still uses the old provider until the service restarts.")
        if ask_yes_no("Restart DocuSort now?"):
            try:
                post(origin + "/api/local-ai/finish", {"ticket": a.ticket},
                     30.0, a.insecure)
                good("Restart triggered.")
            except Exception as exc:
                warn("Could not restart automatically: %s" % detail(exc))
                info("Restart it yourself — Settings → Update → Restart.")
        else:
            info("Restart it when it suits you — Settings → Update → Restart.")

    print("\n%s✓ Done.%s" % (GREEN, OFF))
    print("\n  From now on DocuSort classifies your documents on this machine.")
    print("  Nothing leaves your home.")
    print("\n  Address : %s" % target)
    print("  Model   : %s" % model)
    print("  DocuSort: %s" % origin)
    if sys.stdin.isatty():
        try:
            input("\nPress Enter to close this window. ")
        except (EOFError, KeyboardInterrupt):
            pass
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nStopped.")
        sys.exit(130)
