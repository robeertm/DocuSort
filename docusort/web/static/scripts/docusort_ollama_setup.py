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
import tempfile
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


def why_it_failed(log: str, seit: int) -> None:
    """Say WHY, instead of only that it did not work.

    🔴 This is the whole reason this function exists. `ollama serve` writes its
    reason into the log and the setup used to answer „did not answer within
    40 s" — the answer was lying on the user's own disk and nobody showed it to
    them. Whatever goes wrong here, the next person sees the reason on their
    screen and can act on it.
    """
    zeilen = []
    try:
        with open(log, "rb") as fh:
            try:                                   # only what THIS run wrote
                fh.seek(seit)
            except Exception:
                pass
            zeilen = [z for z in fh.read().decode("utf-8", "replace").splitlines()
                      if z.strip()]
    except Exception:
        pass
    if zeilen:
        warn("What Ollama itself said:")
        for z in zeilen[-12:]:
            info(z[:200])
    else:
        info("The log stayed empty — Ollama did not even get as far as a message.")

    # The two answers that come up again and again, named rather than guessed at.
    ganz = " ".join(zeilen).lower()
    if "address already in use" in ganz or "bind" in ganz and "in use" in ganz:
        warn("Something is already holding port %d." % OLLAMA_PORT)
        info("Most often that is Ollama's own service, listening on 127.0.0.1")
        info("only. On Linux it has to be told the new address and restarted:")
        for z in systemd_rezept("0.0.0.0"):
            info(z)
    elif platform.system() == "Linux" and systemd_hat_ollama():
        warn("There is an `ollama` service on this machine.")
        info("Starting a second copy by hand fights it. Set the address on the")
        info("service instead and restart it:")
        for z in systemd_rezept("0.0.0.0"):
            info(z)


def systemd_hat_ollama() -> bool:
    """Is Ollama a systemd service here? Asked, not assumed — the official
    Linux installer creates one, a distribution package may not."""
    try:
        r = subprocess.run(["systemctl", "list-unit-files", "ollama.service"],
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                           timeout=5)
        return b"ollama.service" in (r.stdout or b"")
    except Exception:
        return False


def systemd_rezept(bind: str) -> list:
    return ["sudo systemctl edit ollama",
            "  [Service]",
            '  Environment="OLLAMA_HOST=%s:%d"' % (bind, OLLAMA_PORT),
            "sudo systemctl restart ollama",
            "then run this file again."]


def wurzelweg() -> list:
    """How does one become root ON THIS machine? Asked, not assumed.

    🔴 `sudo` is not a law of nature. Debian without sudo, Alpine and the BSD
    school use `doas`, a desktop session has `pkexec` with a graphical password
    box, and a live system may already be root. Every Linux has at least one of
    them; which one is a question, not a constant.
    """
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        return []                                   # nothing to do
    # A terminal can take a typed password; a double-clicked window cannot, and
    # there `pkexec` is the only one that can ask at all.
    reihe = ["sudo", "pkexec", "doas"] if sys.stdin.isatty() \
        else ["pkexec", "sudo", "doas"]
    for w in reihe:
        if have(w):
            return [w]
    return []


def als_root(args: list, timeout: int = 120) -> bool:
    """Run one command with root rights.

    🔑 The person in front of this is not here to learn systemd. Typing a
    password once is something everybody knows how to do; opening an editor on
    a unit file is not. So the setup does the work and asks only for the
    password — and only when there is actually something to do.
    """
    weg = wurzelweg()
    if weg == ["sudo"]:
        # -p: say WHY the password is wanted, right where it is typed.
        ruf = ["sudo", "-p", "  Your login password: "] + args
    elif weg:
        ruf = weg + args
    elif hasattr(os, "geteuid") and os.geteuid() == 0:
        ruf = args
    else:
        return False
    try:
        return subprocess.call(ruf, timeout=timeout) == 0
    except Exception:
        return False


def schreibe_als_root(inhalt: str, ziel: str) -> bool:
    """Put a file where only root may write.

    `install -D` makes the directory on the way and is one command, one
    password — but it is GNU coreutils. Busybox and the leaner systems get the
    two-step way instead. Both are plain, and the second is tried only if the
    first really failed.
    """
    tmp = os.path.join(tempfile.gettempdir(), "docusort-ollama.conf")
    try:
        with open(tmp, "w") as fh:
            fh.write(inhalt)
    except Exception as exc:
        warn("Could not prepare the setting: %s" % exc)
        return False
    try:
        if als_root(["install", "-D", "-m", "644", tmp, ziel]):
            return True
        return (als_root(["mkdir", "-p", os.path.dirname(ziel)])
                and als_root(["cp", tmp, ziel])
                and als_root(["chmod", "644", ziel]))
    finally:
        try:
            os.remove(tmp)
        except Exception:
            pass


def systemd_binden(bind: str) -> bool:
    """Tell the `ollama` service the address and restart it — no typing.

    🔴 The old setup printed a recipe and started a second `ollama serve` of
    its own next to the service. That second copy can only ever lose: the
    service already holds the port, so it dies with „address already in use"
    and the user is left with 40 seconds of silence. There is exactly one owner
    of that port on a systemd machine, and it is the service.
    """
    conf = "/etc/systemd/system/ollama.service.d/docusort.conf"
    inhalt = ("# Written by the DocuSort setup.\n"
              "# DocuSort runs on another machine, so Ollama has to listen on\n"
              "# the network instead of on 127.0.0.1 only.\n"
              "# Delete this file and restart ollama to undo it.\n"
              "[Service]\n"
              'Environment="OLLAMA_HOST=%s:%d"\n' % (bind, OLLAMA_PORT))
    step("Setting up the Ollama service to listen on %s:%d" % (bind, OLLAMA_PORT))
    if not wurzelweg() and not (hasattr(os, "geteuid") and os.geteuid() == 0):
        # 🔴 No way to become root at all. Then the recipe IS the help, and it
        #    is better than pretending the work was done.
        warn("This needs administrator rights, and this machine offers no way "
             "to ask for them (no sudo, no pkexec, no doas).")
        info("Ask whoever administers it to run:")
        for z in systemd_rezept(bind):
            info(z)
        return False
    info("This needs your password once — the one you use to log in.")
    if not schreibe_als_root(inhalt, conf):
        warn("Could not write %s." % conf)
        info("If you would rather do it by hand:")
        for z in systemd_rezept(bind):
            info(z)
        return False
    als_root(["systemctl", "daemon-reload"])
    if not als_root(["systemctl", "restart", "ollama"]):
        warn("The ollama service did not restart.")
        info("Its own words:  systemctl status ollama")
        return False
    good("The service now listens on %s:%d." % (bind, OLLAMA_PORT))
    # 🔑 „restarted" is not „answering". Ask it, do not assume it.
    for _ in range(SERVE_WAIT):
        if ollama_models("http://127.0.0.1:%d" % OLLAMA_PORT, 1.0):
            good("Ollama is up.")
            return True
        time.sleep(1)
    warn("The service restarted but does not answer yet.")
    info("Its own words:  systemctl status ollama")
    return False


def _sagt(args: list, timeout: int = 5) -> str:
    try:
        r = subprocess.run(args, stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, timeout=timeout)
        return (r.stdout or b"").decode("utf-8", "replace").strip()
    except Exception:
        return ""


def firewall_art() -> str:
    """Which firewall is actually RUNNING here — not which one is installed.

    🔴 Every distribution answers this differently and not all of them have
    systemd, so each one is asked in its own words first and only then through
    the service manager. Two are handled: firewalld (Fedora, RHEL, openSUSE,
    CachyOS) and ufw (Ubuntu, Mint, Debian desktops). Between them they cover
    what a desktop Linux normally runs.
    """
    if have("firewall-cmd") and _sagt(["firewall-cmd", "--state"]) == "running":
        return "firewalld"
    if have("ufw"):
        # Readable by everyone, so no password is needed just to look.
        try:
            with open("/etc/ufw/ufw.conf") as fh:
                if "ENABLED=yes" in fh.read().replace(" ", ""):
                    return "ufw"
        except Exception:
            pass
    if have("systemctl"):
        for dienst in ("firewalld", "ufw"):
            if have(dienst.replace("firewalld", "firewall-cmd")) \
                    and _sagt(["systemctl", "is-active", dienst]) == "active":
                return dienst
    return ""


def firewall_fremd() -> bool:
    """A rule set that is nobody's to edit blindly.

    🔴 nftables and plain iptables have no safe, persistent, distribution-
    independent „open this port" — where the rule has to go depends on the
    setup, and a wrong one can lock the machine out of its own network. So this
    is only DETECTED, and said out loud. Doing less here is doing the user a
    favour.
    """
    for w, args in (("nft", ["nft", "list", "ruleset"]),
                    ("iptables", ["iptables", "-S"])):
        if have(w) and _sagt(args, 8):
            return True
    return False


def firewall_oeffnen(art: str) -> bool:
    """Open the port, rather than telling somebody else to."""
    step("Opening port %d in the firewall (%s)" % (OLLAMA_PORT, art))
    if art == "firewalld":
        ok = als_root(["firewall-cmd", "--permanent",
                       "--add-port=%d/tcp" % OLLAMA_PORT])
        ok = als_root(["firewall-cmd", "--reload"]) and ok
    elif art == "ufw":
        ok = als_root(["ufw", "allow", "%d/tcp" % OLLAMA_PORT])
    else:
        return False
    if ok:
        good("Port %d is open for your local network." % OLLAMA_PORT)
    else:
        warn("Could not change the firewall.")
    return ok


def serve(bind: str) -> bool:
    """Start `ollama serve` in the background, bound to `bind`.

    🔴 The bind address is the whole point. Ollama listens on 127.0.0.1 by
    default — perfectly right, and perfectly useless when DocuSort runs on a
    different machine."""
    # 🔑 Ask FIRST whether a service already owns this. Starting a second copy
    #    by hand only produces „address already in use", and the user is left
    #    with a failure whose cause was knowable before the attempt.
    if platform.system() == "Linux" and systemd_hat_ollama():
        return systemd_binden(bind)

    env = dict(os.environ, OLLAMA_HOST="%s:%d" % (bind, OLLAMA_PORT))
    log = os.path.join(os.path.expanduser("~"), "ollama-docusort.log")
    seit = os.path.getsize(log) if os.path.exists(log) else 0
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
    why_it_failed(log, seit)
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
        # A service is set up and restarted in `serve()` — nothing to say here.
        # Without one, Ollama is started by hand and there is nothing to make
        # permanent either; the next run of this file does it again.
        if not systemd_hat_ollama():
            info("Ollama is not a service here; this file starts it when needed.")
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

    # 🔑 Ollama answers on this machine but not from outside: that is a firewall
    #    and nothing else. Telling somebody „allow port 11434" is telling them
    #    to go and learn their firewall. Ask once, then do it.
    if not reachable and not here and ollama_models(
            "http://127.0.0.1:%d" % OLLAMA_PORT, 3.0):
        art = firewall_art()
        if art:
            warn("Ollama runs, but your firewall (%s) is blocking port %d."
                 % (art, OLLAMA_PORT))
            if ask_yes_no("Open port %d so DocuSort can reach it?" % OLLAMA_PORT):
                firewall_oeffnen(art)
                reachable = ollama_models(target, 4.0)

    if not reachable:
        hinweis = ""
        if not here and ollama_models("http://127.0.0.1:%d" % OLLAMA_PORT, 3.0):
            hinweis = ("\nOllama answers on this machine but not from the "
                       "network, so something in between is blocking port %d."
                       % OLLAMA_PORT)
            if firewall_fremd():
                hinweis += ("\nThis machine filters with nftables/iptables. "
                            "There is no safe way for this setup to edit those "
                            "rules — a wrong one can cut the machine off its "
                            "own network — so port %d has to be allowed by "
                            "whoever set them up." % OLLAMA_PORT)
        stop("Ollama is not reachable at %s.%s" % (target, hinweis))
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
