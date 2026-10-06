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
def ollama_models(base: str, timeout: float = 2.0):
    """The models at `base` — or ``None`` when nothing answered there.

    🔴 THIS RETURNED `[]` FOR BOTH CASES AND IT COST SOMEBODY AN EVENING.
    A fresh Ollama has no models yet. `/api/tags` then answers, politely and
    correctly, with an empty list — and an empty list is false. So the setup
    read „no models" as „no Ollama", announced

        ✋ Ollama is not reachable at http://192.0.2.38:11434

    and stopped — one step before the very thing that would have fixed it, which
    is pulling a model. The owner meanwhile opened that exact address in a
    browser and read „Ollama is running".

    „It answered" and „it has something" are two different questions. Asking one
    and reporting the other is how a setup tells somebody their network is
    broken when nothing is.
    """
    try:
        antwort = get(base.rstrip("/") + "/api/tags", timeout)
    except Exception:
        return None                      # niemand da
    return [str((m or {}).get("name") or "")
            for m in (antwort.get("models") or [])]


def ollama_antwortet(base: str, timeout: float = 2.0) -> bool:
    """Antwortet dort ueberhaupt ein Ollama? Unabhaengig davon, ob es schon
    ein Modell hat."""
    return ollama_models(base, timeout) is not None


def pick_model(models: list) -> str:
    """The model DocuSort is most likely to get on with.

    🔴 ZWEI DURCHGAENGE, UND DIE REIHENFOLGE IST DER GANZE PUNKT. Vorher lief
    nur EINE Schleife, die auch auf die FAMILIE passte: `want.split(":")[0]`
    macht aus `qwen2.5:7b-instruct` ein `qwen2.5`, und darauf passt auch
    `qwen2.5:3b-instruct`. Lagen beide auf einem Rechner, gewann das kleinere —
    einfach weil Ollama es zuerst auflistet.

    Das ist kein Schoenheitsfehler. Gemessen an derselben Stromrechnung auf dem
    echten Weg des Programms: das 3B-Modell brauchte 186 s und legte sie unter
    „Haus", das 7B 465 s und legte sie unter „Rechnungen" — dorthin, wo sie
    hingehoert. Das kleinere Modell ist nicht die schnellere Variante derselben
    Arbeit, es ist eine schlechtere Arbeit.

    Also: erst ein GENAUER Treffer ueber die ganze Wunschliste, und nur wenn
    keiner dabei ist, ein Familientreffer.
    """
    usable = [m for m in models if not any(u in m.lower() for u in UNUSABLE)]
    # 🔑 `:latest` ist Ollamas stillschweigende Marke — `qwen2.5:7b-instruct`
    #    und `qwen2.5:7b-instruct:latest` sind dasselbe Modell.
    def _blank(n: str) -> str:
        return n[:-7] if n.endswith(":latest") else n
    for want in WISH:
        for m in usable:
            if _blank(m) == want:
                return m
    for want in WISH:
        for m in usable:
            if _blank(m).split(":")[0] == want.split(":")[0]:
                return m
    return usable[0] if usable else ""


# --------------------------------------------------------------- hardware
# "und wenn die hardware es nicht kann muss das auch klar kommuniziert werden"
#
# 🔴 A LANGUAGE MODEL THAT DOES NOT FIT IS NOT A SLOW MODEL, IT IS A BROKEN
#    ONE. Without enough memory the system starts swapping and a single
#    document takes hours, or the server is killed outright — and from the
#    outside that looks like DocuSort being broken.
#
# 🔑 Measured, never assumed, and on every platform the same three questions:
#    how much memory, how many cores, is there a GPU. The answer decides which
#    model is suggested, and it is SAID OUT LOUD either way.

MIN_GB_7B = 8.0      # a 7B model at Q4 occupies ~5 GB plus room to work
MIN_GB_3B = 4.5      # a 3B model at Q4 occupies ~2 GB
MIN_CORES = 4        # fewer than this and even a 3B model is painful on CPU


def _ram_gb() -> float:
    """Total memory in GB. 🔴 Returns 0.0 when it cannot be measured — and
    then nothing is claimed about it, rather than a guess being printed."""
    system = platform.system()
    try:
        if system == "Darwin":
            out = subprocess.check_output(["sysctl", "-n", "hw.memsize"],
                                          text=True).strip()
            return int(out) / (1024 ** 3)
        if system == "Linux":
            with open("/proc/meminfo", "r") as fh:
                for zeile in fh:
                    if zeile.startswith("MemTotal:"):
                        return int(zeile.split()[1]) * 1024 / (1024 ** 3)
        if system == "Windows":
            import ctypes

            class _MS(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong),
                            ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            st = _MS()
            st.dwLength = ctypes.sizeof(_MS)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))
            return st.ullTotalPhys / (1024 ** 3)
    except Exception:
        pass
    return 0.0


def _gpu() -> str:
    """A name when there is a usable GPU, "" when there is none, and "?" when
    the question could not be answered."""
    system = platform.system()
    try:
        if system == "Darwin":
            # Apple Silicon shares memory with the GPU and Ollama uses Metal.
            if platform.machine() in ("arm64", "aarch64"):
                return "Apple Silicon (Metal)"
            return ""
        out = subprocess.run(["nvidia-smi", "--query-gpu=name",
                              "--format=csv,noheader"],
                             capture_output=True, text=True, timeout=6)
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip().splitlines()[0]
        if system == "Linux":
            lspci = subprocess.run(["lspci"], capture_output=True, text=True,
                                   timeout=6)
            if lspci.returncode == 0:
                for z in lspci.stdout.splitlines():
                    if "VGA" in z and ("AMD" in z or "NVIDIA" in z):
                        return z.split(":")[-1].strip()
        return ""
    except FileNotFoundError:
        return ""
    except Exception:
        return "?"


def hardware_check() -> dict:
    """Measure the machine and say plainly what it can do.

    Returns {ram, cores, gpu, model, verdict}. `model` is what this machine
    should actually run; `verdict` is one of ok / small / tight / no.
    """
    ram = _ram_gb()
    cores = os.cpu_count() or 0
    gpu = _gpu()

    step("Checking what this machine can do")
    info("Memory: %s" % ("%.1f GB" % ram if ram else "could not be measured"))
    info("Cores:  %s" % (cores or "could not be measured"))
    info("GPU:    %s" % (gpu if gpu and gpu != "?" else
                         ("could not be determined" if gpu == "?"
                          else "none — the CPU does the work")))

    # 🔴 Unmeasurable is NOT the same as insufficient. When the numbers could
    #    not be read, nothing is claimed and the default is used — refusing to
    #    continue on a failed measurement would be worse than continuing.
    if not ram:
        warn("Could not measure memory; continuing with the default model.")
        return {"ram": 0, "cores": cores, "gpu": gpu,
                "model": DEFAULT_MODEL, "verdict": "unknown"}

    if ram < MIN_GB_3B:
        print("")
        warn("%.1f GB of memory is not enough to run a language model here."
             % ram)
        info("Even the small model needs about %.1f GB. What you can do:"
             % MIN_GB_3B)
        info("  · point DocuSort at another machine on your network that has")
        info("    Ollama (Settings → AI), or")
        info("  · use a cloud provider (Settings → AI), or")
        info("  · add memory to this machine.")
        return {"ram": ram, "cores": cores, "gpu": gpu,
                "model": "", "verdict": "no"}

    if ram < MIN_GB_7B:
        warn("%.1f GB is enough for the small model, not for the recommended "
             "one." % ram)
        info("Using %s instead of %s." % (SMALL_MODEL, DEFAULT_MODEL))
        info("It is quicker but files documents less accurately — measured on "
             "a real invoice, the small model picked the wrong folder where "
             "the larger one got it right.")
        verdict, modell = "small", SMALL_MODEL
    else:
        good("Enough memory for the recommended model (%s)." % DEFAULT_MODEL)
        verdict, modell = "ok", DEFAULT_MODEL

    if not gpu and cores and cores < MIN_CORES:
        warn("%d cores and no GPU — expect several minutes per document."
             % cores)
        info("That is workable for a few documents a day, not for a bulk "
             "import. DocuSort can use a second machine for that; you set "
             "them up under Settings → AI.")
        verdict = "tight"
    elif not gpu:
        info("No GPU: the CPU does the work. Expect minutes per document, "
             "not seconds. This file measures it for real further down.")
    return {"ram": ram, "cores": cores, "gpu": gpu,
            "model": modell, "verdict": verdict}


def speed_check(base: str, model: str) -> None:
    """Measure what a document will actually cost HERE.

    🔴 An estimate from core counts would be a guess. Ollama reports the real
    figures for every answer, so the machine is asked instead: how fast did it
    read, how fast did it write. DocuSort's own prompt carries all categories
    and runs about 2000 tokens, which is what the estimate is based on.
    """
    step("Measuring how fast this machine answers")
    try:
        antwort = post(base + "/api/generate",
                       {"model": model, "prompt": "Reply with the word ok.",
                        "stream": False, "keep_alive": -1},
                       timeout=VERIFY_WAIT)
    except Exception as exc:
        return warn("Could not measure: %s" % detail(exc))
    try:
        lesen = antwort.get("prompt_eval_count") or 0
        lese_ns = antwort.get("prompt_eval_duration") or 0
        schreiben = antwort.get("eval_count") or 0
        schreib_ns = antwort.get("eval_duration") or 0
        lese_rate = lesen / (lese_ns / 1e9) if lese_ns else 0
        schreib_rate = schreiben / (schreib_ns / 1e9) if schreib_ns else 0
    except Exception:
        return warn("The server did not report timings.")
    if not lese_rate or not schreib_rate:
        return warn("The server did not report timings.")

    info("Reading: %.0f tokens/s   ·   Writing: %.0f tokens/s"
         % (lese_rate, schreib_rate))
    # A DocuSort classification: ~2000 tokens in, ~120 out.
    sekunden = 2000 / lese_rate + 120 / schreib_rate
    if sekunden < 20:
        good("A document will take about %.0f seconds here." % sekunden)
    elif sekunden < 90:
        good("A document will take about %.0f seconds here." % sekunden)
        info("Fine for everyday use.")
    elif sekunden < 600:
        warn("A document will take about %.0f minutes here." % (sekunden / 60))
        info("Workable for a few documents a day. For a bulk import of a few "
             "hundred, this machine will run for many hours — DocuSort can "
             "send those to a second machine instead (Settings → AI).")
    else:
        warn("A document will take roughly %.0f minutes here." % (sekunden / 60))
        info("That is too slow for regular use. Either point DocuSort at a "
             "faster machine or at a cloud provider (Settings → AI).")


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


# 🔴 Als Konstante, nicht als Zeichenkette mitten im Code: ein Pruefstand muss
# das Verzeichnis umbiegen koennen, sonst liest er das echte /etc des Rechners,
# auf dem er laeuft — und misst damit wieder die Maschine statt den Fall.
DROPIN_DIR = "/etc/systemd/system/ollama.service.d"


def systemd_dropins() -> list:
    """Which drop-in files does the `ollama` service already have?

    🔴 Asked because systemd merges drop-ins in FILENAME order and the LAST
    one wins. A machine that followed the older printed recipe has an
    `override.conf` from `systemctl edit` — and `docusort.conf` sorts BEFORE
    it, so our file would be written, loaded, and quietly outvoted.
    """
    try:
        return sorted(n for n in os.listdir(DROPIN_DIR)
                      if n.endswith(".conf"))
    except Exception:
        return []


def systemd_host() -> str:
    """The address the service would REALLY use — merged from the unit and all
    its drop-ins. Asked of systemd, never read back out of our own file: our
    file is one voice among several, and not the loudest by default.
    """
    for stueck in _sagt(["systemctl", "show", "-p", "Environment",
                         "ollama"]).split():
        if stueck.startswith("Environment="):
            stueck = stueck[len("Environment="):]
        if stueck.startswith("OLLAMA_HOST="):
            return stueck[len("OLLAMA_HOST="):].strip('"').replace("http://", "")
    return ""


def systemd_angeschaltet(dienst: str = "ollama", nutzer: bool = False) -> str:
    """Was systemd ueber das HOCHFAHREN sagt: `enabled`, `disabled`, `static`, …

    🔴 DIE FRAGE, DIE HIER GEFEHLT HAT. Ein Paket darf einen Dienst mitbringen,
    ohne ihn anzuschalten — in der Arch-Familie ist das die Regel, nicht die
    Ausnahme. Dann laeuft Ollama, solange es jemand startet, und ist nach dem
    naechsten Hochfahren weg. Der Einrichter hat den Dienst bisher nur NEU
    GESTARTET. Das haelt genau bis zum Ausschalten.
    """
    return _sagt(["systemctl"] + (["--user"] if nutzer else [])
                 + ["is-enabled", dienst])


def systemd_lebt(dienst: str = "ollama", nutzer: bool = False) -> bool:
    """Laeuft der Dienst JETZT? Nicht dasselbe wie „es gibt ihn"."""
    return _sagt(["systemctl"] + (["--user"] if nutzer else [])
                 + ["is-active", dienst]) == "active"


def systemd_anschalten(dienst: str = "ollama") -> bool:
    """Dafuer sorgen, dass der Dienst beim Hochfahren mitkommt.

    Gibt zurueck, ob er es danach WIRKLICH tut — nicht, ob der Befehl abgesetzt
    wurde. „enable gelaufen" und „kommt wieder" sind zwei Aussagen.
    """
    zustand = systemd_angeschaltet(dienst)
    if zustand in ("enabled", "enabled-runtime", "static", "indirect",
                   "alias", "generated"):
        return True
    if not als_root(["systemctl", "enable", dienst]):
        return False
    return systemd_angeschaltet(dienst) not in ("disabled", "masked", "")


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
    want = "%s:%d" % (bind, OLLAMA_PORT)
    # 🔑 Perhaps there is nothing to do at all. Restarting somebody's Ollama to
    #    write a setting it already has is noise, and it hides the real cause:
    #    if the address is right and DocuSort still cannot reach it, the port is
    #    blocked — a firewall question, not a bind question.
    vorher = systemd_host()
    if vorher == want:
        good("The service is already set to %s — leaving the setting alone."
             % want)
        # 🔴 „DIE ADRESSE STIMMT" IST NICHT „ES LAEUFT". Hier meldete der
        #    Einrichter Erfolg, obwohl der Dienst gestoppt war: die Adresse
        #    steht im drop-in vom letzten Lauf, der Dienst war nach dem
        #    Hochfahren nur nie gestartet. Der Lauf starb drei Schritte spaeter
        #    an „Ollama is not reachable" — und sagte nie, dass niemand horcht.
        if not systemd_lebt():
            warn("But the service is not running.")
            info("This needs your password once — the one you use to log in.")
            if not als_root(["systemctl", "start", "ollama"]):
                warn("The ollama service did not start.")
                info("Its own words:  systemctl status ollama")
                return False
            good("Started it.")
        dienst_bleibt()
        if warte_auf_ollama():
            good("Ollama is up.")
            return True
        warn("The service is running but does not answer yet.")
        info("Its own words:  systemctl status ollama")
        info("If DocuSort still cannot reach it, something between the two "
             "machines is blocking port %d." % OLLAMA_PORT)
        return False

    # 🔴 Drop-ins are merged in filename order, last one wins. If a file that
    #    sorts AFTER ours already sets the address, ours would be outvoted in
    #    silence — so take a name that comes last, and say so. Their file is
    #    left untouched; deleting ours undoes everything.
    datei = "docusort.conf"
    # 🔴 Die ganze Liste behalten. Beim ersten Entwurf filterte sie „docusort.conf"
    #    sofort heraus — und die Frage „liegt da noch eine alte eigene Datei?"
    #    konnte danach nie mehr wahr werden.
    vorhanden = systemd_dropins()
    staerker = [n for n in vorhanden
                if n > datei and n not in ("zz-docusort.conf",)]
    if staerker and vorher:
        datei = "zz-docusort.conf"
        warn("This machine already sets the address itself, in %s."
             % ", ".join(staerker))
        info("That file says %s and would outvote ours, so ours goes in as %s."
             % (vorher, datei))
        info("Your own file stays exactly as it is.")
    conf = os.path.join(DROPIN_DIR, datei)
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
    # A stale file of ours from an earlier run would only confuse the next
    # person reading that directory; the one we just wrote outvotes it anyway.
    if datei != "docusort.conf" and "docusort.conf" in vorhanden:
        als_root(["rm", "-f", os.path.join(DROPIN_DIR, "docusort.conf")])
    als_root(["systemctl", "daemon-reload"])
    if not als_root(["systemctl", "restart", "ollama"]):
        warn("The ollama service did not restart.")
        info("Its own words:  systemctl status ollama")
        return False
    # 🔑 „The file is written" is not „the setting is in force". Ask systemd
    #    what it ended up with — otherwise a drop-in we did not expect makes
    #    this report a success that never happened.
    nachher = systemd_host()
    if nachher and nachher != want:
        warn("The service still uses %s, not %s." % (nachher, want))
        info("Something else is setting it. These files have a say, the last")
        info("one wins:")
        for n in systemd_dropins():
            info("  %s" % os.path.join(DROPIN_DIR, n))
        return False
    good("The service now listens on %s." % want)
    dienst_bleibt()
    # 🔑 „restarted" is not „answering". Ask it, do not assume it.
    if warte_auf_ollama():
        good("Ollama is up.")
        return True
    warn("The service restarted but does not answer yet.")
    info("Its own words:  systemctl status ollama")
    return False


def dienst_bleibt() -> bool:
    """Kommt der `ollama`-DIENST nach einem Neustart von allein zurueck?

    🔴 Das ist die Haelfte, die hier gefehlt hat. `systemctl restart` holt
    Ollama fuer HEUTE zurueck; ob es morgen wieder da ist, entscheidet
    `enable`. Ein Paket, das den Dienst mitbringt, ohne ihn anzuschalten, ist
    in der Arch-Familie der Normalfall — und der Nutzer sieht den Unterschied
    erst beim naechsten Hochfahren.
    """
    if systemd_anschalten():
        good("It also comes back by itself after a restart.")
        return True
    warn("Ollama runs now, but it is switched OFF for the next restart.")
    info("Nothing on this machine would start it again. Whoever administers")
    info("it can switch that on once:")
    info("  sudo systemctl enable ollama")
    return False


def warte_auf_ollama(sekunden: int = 0) -> bool:
    """Antwortet Ollama hier, innerhalb der Wartezeit?"""
    for _ in range(sekunden or SERVE_WAIT):
        if ollama_antwortet("http://127.0.0.1:%d" % OLLAMA_PORT, 1.0):
            return True
        time.sleep(1)
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
    # 🔴 Kein Systemdienst — und HIER lag „immer wieder die Datei holen". Von
    #    Hand gestartet haelt Ollama bis zum Abmelden. Hat dieser Rechner ein
    #    systemd fuer den Benutzer (fast jedes Linux mit Oberflaeche), bekommt
    #    Ollama einen eigenen Dienst und kommt von allein zurueck.
    if platform.system() == "Linux" and systemd_nutzer_da():
        return nutzerdienst(bind)
    return _von_hand(bind)


# ----------------------------------------- Ollama als Dienst DIESES Benutzers
# 🔴 WARUM DIESER ABSCHNITT EXISTIERT (06.10.2026)
#
# Gemeldet wurde, dass die Einrichtungsdatei auf Linux IMMER WIEDER geholt
# werden muss, damit das Modell wieder laeuft, und dass sonst eine
# Fehlermeldung kommt. Das war kein Raetsel, es stand als Absicht im Quelltext
# dieser Datei:
#
#     „Without one, Ollama is started by hand and there is nothing to make
#      permanent either; the next run of this file does it again."
#
# Also: ohne systemd-Dienst lief Ollama als KIND dieses Skripts. Mit dem
# naechsten Abmelden oder Hochfahren war es weg, DocuSort meldete einen Fehler,
# und der Weg zurueck fuehrte ueber einen NEUEN Starter — der alte traegt einen
# Zettel, der beim ersten Erfolg verbraucht wird.
#
# Auf dem Mac stand die Antwort seit Monaten daneben: ein LaunchAgent, der beim
# Anmelden startet und nachstartet. Linux hat denselben Mechanismus; er heisst
# anders und braucht nicht einmal root.
NUTZER_UNIT = "docusort-ollama.service"


def nutzer_unit_pfad() -> str:
    basis = os.environ.get("XDG_CONFIG_HOME") or os.path.join(
        os.path.expanduser("~"), ".config")
    return os.path.join(basis, "systemd", "user", NUTZER_UNIT)


def systemd_nutzer_da() -> bool:
    """Gibt es auf diesem Rechner ein systemd FUER DEN BENUTZER?

    🔑 Gefragt, nicht angenommen: in einem Container, auf einem nackten Server
    ohne Sitzung oder unter einem anderen Init-System gibt es keinen
    Benutzer-Bus — dann ist `systemctl --user` nicht die Antwort, und eine
    Fehlermeldung darueber waere Laerm.
    """
    if not have("systemctl"):
        return False
    try:
        r = subprocess.run(["systemctl", "--user", "list-unit-files",
                            "--type=service"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=5)
        return r.returncode == 0
    except Exception:
        return False


def nutzerdienst_da() -> bool:
    return os.path.exists(nutzer_unit_pfad())


def fremder_starter() -> str:
    """Startet auf diesem Rechner schon irgendetwas anderes Ollama?

    🔴 Sonst stellt der Einrichter einen ZWEITEN Starter daneben, und nach dem
    naechsten Hochfahren streiten sich zwei um denselben Port. Einer besitzt
    ihn, und wer das ist, wird gefragt und nicht geraten.
    """
    for n in _sagt(["systemctl", "--user", "list-unit-files", "ollama*"]).split():
        if n.endswith(".service") and n != NUTZER_UNIT:
            return "the user service %s" % n
    auto = os.path.join(os.environ.get("XDG_CONFIG_HOME") or os.path.join(
        os.path.expanduser("~"), ".config"), "autostart")
    try:
        for n in sorted(os.listdir(auto)):
            if not n.endswith(".desktop"):
                continue
            try:
                with open(os.path.join(auto, n), encoding="utf-8",
                          errors="replace") as fh:
                    if "ollama" in fh.read().lower():
                        return "the autostart entry %s" % n
            except OSError:
                pass
    except OSError:
        pass
    return ""


def linger_an() -> bool:
    """Darf dieser Benutzer Dienste haben, OHNE angemeldet zu sein?

    🔑 Ohne das startet ein Benutzerdienst erst beim Anmelden. Auf einem
    Schreibtischrechner ist das genau richtig; auf einem Rechner, der nur
    hochfaehrt und arbeitet, ist es der Unterschied zwischen wiederkommen und
    wiederkommen, sobald sich jemand anmeldet. Also wird es gesetzt, und wenn
    es nicht geht, wird genau das gesagt.
    """
    wer = os.environ.get("USER") or os.environ.get("LOGNAME") or ""
    if not wer or not have("loginctl"):
        return False
    if "yes" in _sagt(["loginctl", "show-user", wer, "-p", "Linger"]).lower():
        return True
    try:
        if subprocess.call(["loginctl", "enable-linger", wer],
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=30) == 0:
            return True
    except Exception:
        pass
    if als_root(["loginctl", "enable-linger", wer], timeout=30):
        return True
    return "yes" in _sagt(["loginctl", "show-user", wer, "-p", "Linger"]).lower()


def nutzerdienst(bind: str) -> bool:
    """Ollama einen eigenen Dienst dieses Benutzers geben — und ihn starten.

    Der Zwilling von `_launchagent_darwin`: startet beim Anmelden, startet nach
    einem Absturz nach, traegt seine Adresse bei sich und braucht kein root.
    Zum Rueckbau reicht das Loeschen EINER Datei; der Befehl dazu steht in ihr.
    """
    pfad = shutil.which("ollama") or ""
    if not os.path.isabs(pfad):
        # 🔴 Eine Unit erbt KEIN PATH. Ohne vollen Pfad waere der Dienst
        #    geschrieben und wuerde bei jedem Start scheitern.
        warn("Could not find the full path to the ollama program.")
        return _von_hand(bind)
    ziel = nutzer_unit_pfad()
    inhalt = (
        "# Written by the DocuSort setup.\n"
        "# It keeps Ollama running: at login, after a crash, after a reboot.\n"
        "# To undo it:\n"
        "#   systemctl --user disable --now %s\n"
        "#   rm %s\n"
        "[Unit]\n"
        "Description=Ollama (set up by DocuSort)\n"
        "After=network-online.target\n"
        "\n"
        "[Service]\n"
        "ExecStart=%s serve\n"
        'Environment="OLLAMA_HOST=%s:%d"\n'
        # Ein Modell im Speicher halten: es neu zu laden kostet auf einer NAS
        # gemessene 30 s, und zwar genau dann, wenn nach einer Pause der erste
        # Scan hereinfaellt.
        'Environment="OLLAMA_KEEP_ALIVE=-1"\n'
        "Restart=always\n"
        "RestartSec=3\n"
        "\n"
        "[Install]\n"
        "WantedBy=default.target\n"
        % (NUTZER_UNIT, ziel, pfad, bind, OLLAMA_PORT))
    step("Setting Ollama up as your own service (listening on %s:%d)"
         % (bind, OLLAMA_PORT))
    try:
        os.makedirs(os.path.dirname(ziel), exist_ok=True)
        with open(ziel, "w", encoding="utf-8") as fh:
            fh.write(inhalt)
    except Exception as exc:
        warn("Could not write %s: %s" % (ziel, exc))
        return _von_hand(bind)
    # 🔴 Eine von Hand gestartete Kopie haelt den Port, und der Dienst wuerde
    #    daran scheitern — mit „address already in use" im Journal, wo niemand
    #    nachsieht. Diese Kopie gehoert uns: der vorige Lauf dieser Datei hat
    #    sie gestartet.
    if ollama_antwortet("http://127.0.0.1:%d" % OLLAMA_PORT, 2.0):
        info("Stopping the copy that was started by hand — the service takes "
             "over the port.")
        subprocess.run(["pkill", "-f", "ollama serve"], capture_output=True)
        time.sleep(1)
    _sagt(["systemctl", "--user", "daemon-reload"])
    try:
        rc = subprocess.call(["systemctl", "--user", "enable", "--now",
                              NUTZER_UNIT], timeout=60)
    except Exception as exc:
        warn("Could not start the service: %s" % exc)
        return _von_hand(bind)
    if rc != 0:
        warn("systemd refused the service.")
        for z in _sagt(["journalctl", "--user", "-u", NUTZER_UNIT, "-n", "12",
                        "--no-pager"]).splitlines()[-12:]:
            info(z[:200])
        return _von_hand(bind)
    if not warte_auf_ollama(SERVE_WAIT * 2):
        warn("The service was started but Ollama does not answer yet.")
        for z in _sagt(["journalctl", "--user", "-u", NUTZER_UNIT, "-n", "12",
                        "--no-pager"]).splitlines()[-12:]:
            info(z[:200])
        return False
    good("Ollama is up, and it starts again with your session.")
    if linger_an():
        good("It also comes back after a reboot without anyone logging in.")
    else:
        info("It starts as soon as you log in. To have it come back on a")
        info("machine nobody logs into, an administrator can run once:")
        info("  sudo loginctl enable-linger %s"
             % (os.environ.get("USER") or "<your user>"))
    info("Undo: systemctl --user disable --now %s && rm %s"
         % (NUTZER_UNIT, ziel))
    return True


def nutzerdienst_abraeumen() -> None:
    """Hat der Rechner inzwischen einen Systemdienst, gehoert ihm der Port.

    🔑 Dann muss unser Benutzerdienst weg — zwei Besitzer eines Ports sind
    einer zu viel, und der zweite stirbt still.
    """
    if not nutzerdienst_da():
        return
    info("This machine now has an ollama system service, so the one DocuSort "
         "set up for your account is no longer needed.")
    _sagt(["systemctl", "--user", "disable", "--now", NUTZER_UNIT])
    try:
        os.remove(nutzer_unit_pfad())
    except OSError:
        pass
    _sagt(["systemctl", "--user", "daemon-reload"])


def bleibt_ollama(bind: str) -> None:
    """Kommt Ollama nach einem Neustart von allein zurueck?

    🔴 DIE FRAGE, DIE NIEMAND GESTELLT HAT. Bisher endete der Einrichter mit
    „✓ Done", und das stimmte auch — fuer heute. Ob es morgen noch gilt, hing
    davon ab, wie Ollama auf diesen Rechner gekommen war. Das ist kein Detail,
    sondern der Unterschied zwischen einer Einrichtung und einer Gewohnheit.
    """
    system = platform.system()
    step("Checking that Ollama comes back on its own")
    if system == "Linux":
        if systemd_hat_ollama():
            dienst_bleibt()
            nutzerdienst_abraeumen()
            return
        if nutzerdienst_da() and systemd_angeschaltet(
                NUTZER_UNIT, nutzer=True).startswith("enabled"):
            good("Ollama runs as your own service and starts with your session.")
            return
        fremd = fremder_starter()
        if fremd:
            good("Something already starts Ollama here: %s." % fremd)
            info("Left alone — two starters would fight over port %d."
                 % OLLAMA_PORT)
            return
        if systemd_nutzer_da():
            nutzerdienst(bind)
            return
        warn("Nothing on this machine will start Ollama again after a reboot.")
        info("There is no systemd here, so this file cannot set that up.")
        info("Until then: start it with `ollama serve` after a restart, or run")
        info("this file again — it is the same amount of work either way.")
        return
    if system == "Darwin":
        if os.path.exists(os.path.expanduser(
                "~/Library/LaunchAgents/%s.plist" % AGENT_LABEL)):
            return good("Ollama starts at login (startup item already there).")
        if os.path.isdir("/Applications/Ollama.app"):
            return good("The Ollama app starts itself at login.")
        # Homebrew's ollama brings no startup item of its own — measured on a
        # real Mac: after a reboot nothing was running at all.
        _launchagent_darwin("%s:%d" % (bind, OLLAMA_PORT))
        return
    if system == "Windows":
        good("The Windows installer starts Ollama with your account.")


def _von_hand(bind: str) -> bool:
    """Der letzte Ausweg: Ollama als Kind dieses Skripts.

    🔴 Haelt nur, solange diese Sitzung haelt — darum steht das hier unten und
    wird auch so gesagt, statt als Erfolg verbucht zu werden.
    """
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
    if warte_auf_ollama():
        good("Ollama is up.")
        # 🔴 Nicht mehr behaupten als wahr ist: so gestartet haelt es, bis
        #    sich jemand abmeldet oder den Rechner neu startet.
        info("Started by hand, so it runs until you log out or reboot.")
        return True
    warn("Ollama did not answer within %d s." % SERVE_WAIT)
    why_it_failed(log, seit)
    return False


AGENT_LABEL = "com.docusort.ollama"


def _launchagent_darwin(value: str) -> None:
    """Start Ollama at login and keep it running — with its environment.

    Writes ~/Library/LaunchAgents/<label>.plist. Nothing is written outside
    the user's own home, and nothing needs administrator rights.
    """
    import plistlib
    pfad_ollama = shutil.which("ollama") or "/usr/local/bin/ollama"
    ziel = os.path.expanduser(
        "~/Library/LaunchAgents/%s.plist" % AGENT_LABEL)
    plist = {
        "Label": AGENT_LABEL,
        # 🔑 The FULL path: a LaunchAgent inherits no PATH from any shell.
        "ProgramArguments": [pfad_ollama, "serve"],
        "EnvironmentVariables": {
            "OLLAMA_HOST": value,
            # Keep the model resident: reloading it costs about 30 seconds on
            # top of every request after an idle period.
            "OLLAMA_KEEP_ALIVE": "-1",
        },
        "RunAtLoad": True,
        # The difference between "running because I started it today" and
        # "running".
        "KeepAlive": True,
        "StandardOutPath": "/tmp/ollama.log",
        "StandardErrorPath": "/tmp/ollama.err",
    }
    try:
        os.makedirs(os.path.dirname(ziel), exist_ok=True)
        with open(ziel, "wb") as fh:
            plistlib.dump(plist, fh)
    except Exception as exc:
        warn("Could not write the startup item: %s" % exc)
        info("Ollama will keep working until you log out.")
        return
    # Reload it, so an older copy does not keep the port.
    subprocess.run(["launchctl", "unload", ziel],
                   capture_output=True)
    # 🔴 An Ollama started by hand holds port 11434 and the agent would fail
    #    silently. The one we are replacing is ours to stop.
    subprocess.run(["pkill", "-f", "ollama serve"], capture_output=True)
    time.sleep(1)
    r = subprocess.run(["launchctl", "load", "-w", ziel], capture_output=True,
                       text=True)
    if r.returncode != 0:
        warn("Could not enable the startup item: %s"
             % (r.stderr or "").strip()[:200])
        return
    good("Ollama now starts at login and restarts itself if it stops.")
    info("Listening on %s. Startup item: %s" % (value, ziel))


def _neustart_windows() -> None:
    """Restart Ollama so it picks up the new OLLAMA_HOST."""
    try:
        subprocess.run(["taskkill", "/IM", "ollama.exe", "/F"],
                       capture_output=True)
        subprocess.run(["taskkill", "/IM", "ollama app.exe", "/F"],
                       capture_output=True)
        time.sleep(2)
        pfad = shutil.which("ollama")
        if pfad:
            subprocess.Popen([pfad, "serve"],
                             creationflags=getattr(subprocess,
                                                   "CREATE_NO_WINDOW", 0))
            time.sleep(2)
            good("Ollama restarted with the new setting.")
        else:
            info("Start Ollama again from the Start menu to apply it.")
    except Exception as exc:
        warn("Could not restart Ollama automatically: %s" % exc)
        info("Quit Ollama in the system tray and start it again.")


def bind_permanently(bind: str) -> None:
    """Make the bind address survive a restart. Every system has its own way,
    and none of them is `ollama serve` — on macOS the menu-bar app wins, on
    Linux systemd does."""
    system = platform.system()
    value = "%s:%d" % (bind, OLLAMA_PORT)
    if system == "Darwin":
        # 🔴 `launchctl setenv` ALONE LASTS UNTIL THE NEXT REBOOT, and the old
        #    version of this file said so and then handed the problem to the
        #    user: "add this to your shell profile". Nobody does. The result,
        #    measured on a real machine: after a restart Ollama was not running
        #    at all, and when started by hand it listened on 127.0.0.1 only —
        #    so DocuSort on another box could not reach it, and the message it
        #    showed was "not reachable", which says nothing about why.
        #
        # 🔑 A LaunchAgent answers both halves at once: it starts Ollama at
        #    login, restarts it if it dies, and carries OLLAMA_HOST with it so
        #    the setting cannot drift away from the process it belongs to.
        _launchagent_darwin(value)
    elif system == "Linux":
        # 🔴 HIER STAND DER FEHLER. Wortwoertlich: „Without one, Ollama is
        #    started by hand and there is nothing to make permanent either; the
        #    next run of this file does it again." Das war die Ursache der
        #    gemeldeten Lage, in der die Datei immer wieder geholt werden
        #    musste — als Absicht aufgeschrieben. Auf dem Mac stand zwei Zeilen
        #    darueber die richtige Antwort und auf Linux keine.
        #
        # Die Adresse DAUERHAFT zu machen heisst auf Linux: Ollama gehoert
        # einem Dienst. Welchem, entscheidet `serve()` (Systemdienst) bzw.
        # `nutzerdienst()` (Dienst dieses Benutzers) — beide tragen die Adresse
        # bei sich, und `bleibt_ollama()` prueft am Ende, dass es wirklich einen
        # gibt. Hier ist darum nichts zu tun, und das ist keine Luecke mehr.
        pass
    elif system == "Windows":
        try:
            subprocess.check_call(["setx", "OLLAMA_HOST", value],
                                  stdout=subprocess.DEVNULL)
            good("OLLAMA_HOST=%s stored for your user account." % value)
        except Exception as exc:
            return warn("Could not store OLLAMA_HOST: %s" % exc)
        # 🔴 `setx` writes the value for FUTURE processes. The Ollama already
        #    running in the tray keeps the old one, so without this the setting
        #    looks applied and nothing changes until the next reboot — the kind
        #    of "it says it worked" that costs an evening.
        _neustart_windows()
        # The Windows installer registers Ollama to start with the account, so
        # there is nothing further to make permanent. Said out loud rather than
        # left for the reader to wonder about.
        info("Ollama starts with your account; the setting is kept.")


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


def my_addresses(host: str, port: int) -> list:
    """Every IPv4 address this machine could be reached at — best guess first.

    🔴 WHY A LIST AND NOT ONE ADDRESS. `my_address()` below asks the routing
    table which of our addresses would be used to reach DocuSort, and returns
    it. That is correct from THIS machine's point of view — and it was still
    wrong, measured on a real install:

    DocuSort was opened over Tailscale, so the routing table answered with
    this machine's Tailscale address. DocuSort itself, however, runs in a
    container, and a container does not reach the host's Tailnet — only the
    host does. The setup downloaded 4.7 GB, measured the speed, wrote the
    address and then died at the handover with "Connection timed out", with
    everything on this side working perfectly.

    🔑 ONLY DOCUSORT CAN ANSWER THIS. It is the one that has to reach us. So
    we stop guessing, send every address we have, and let it try them.
    """
    raus = []

    def add(ip: str) -> None:
        ip = (ip or "").strip()
        if ip and not ip.startswith("127.") and ip not in raus:
            raus.append(ip)

    add(my_address(host, port))        # the routing table's answer goes first
    try:
        import subprocess as _sp
        if platform.system() == "Windows":
            out = _sp.check_output(["ipconfig"], text=True, timeout=10)
            for zeile in out.splitlines():
                if "IPv4" in zeile and ":" in zeile:
                    add(zeile.split(":")[-1].strip())
        else:
            for befehl in (["ifconfig"], ["ip", "-4", "addr"]):
                try:
                    out = _sp.check_output(befehl, text=True, timeout=10,
                                           stderr=_sp.DEVNULL)
                except Exception:
                    continue
                for zeile in out.split():
                    pass
                import re as _re
                for treffer in _re.findall(
                        r"inet\s+(?:addr:)?(\d{1,3}(?:\.\d{1,3}){3})", out):
                    add(treffer)
                break
    except Exception:
        pass
    # 🔑 Tailnet- und CGNAT-Adressen ans ENDE: sie funktionieren oft zwischen
    #    zwei Rechnern und gerade nicht aus einem Container heraus. Sie werden
    #    nicht weggeworfen — manchmal sind sie der einzige Weg.
    def spaet(ip: str) -> int:
        teile = ip.split(".")
        try:
            return 1 if (teile[0] == "100" and 64 <= int(teile[1]) <= 127) else 0
        except (IndexError, ValueError):
            return 0
    raus.sort(key=spaet)
    return raus or ["127.0.0.1"]


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

    # 🔑 Den Zettel JETZT pruefen, nicht am Ende. Bisher stellte sich erst nach
    #    dem Herunterladen eines Modells heraus, dass er abgelaufen war — und
    #    dann war die ganze Arbeit getan und nichts gespeichert. Die Frage
    #    kostet eine Zehntelsekunde.
    try:
        gueltig = bool(post(origin + "/api/local-ai/ticket-check",
                            {"ticket": a.ticket}, 10.0, a.insecure).get("ok"))
    except Exception:
        gueltig = True          # Aeltere Fassung kennt den Weg nicht — weiter.
    if not gueltig:
        warn("The ticket in this launcher is no longer valid.")
        print("")
        info("A launcher carries a ticket from the moment you downloaded it,")
        info("and that ticket does not last for ever. Nothing is wrong with")
        info("your machine — this file is simply too old.")
        print("")
        info("Open DocuSort, go to Settings, Local AI, and download the")
        info("launcher again. Then run the new one.")
        stop("Stopped before doing any work — nothing was changed.", 0)

    # 2. Is DocuSort on THIS machine?
    mine = my_address(host, port)
    here = mine.startswith("127.") or host in ("localhost", "127.0.0.1", "::1")
    if here:
        bind, visible = "127.0.0.1", "127.0.0.1"
        info("DocuSort runs on this machine — nothing needs to be exposed.")
    else:
        bind, visible = "0.0.0.0", mine
        info("DocuSort runs elsewhere; it will reach this machine at %s." % mine)

    # 3. What can this machine actually do?
    # 🔴 BEFORE installing anything. Somebody whose machine cannot run a model
    #    should learn that before several gigabytes come down the line, not
    #    after. And they should be told what to do instead.
    hw = hardware_check()
    if hw["verdict"] == "no":
        stop("This machine cannot run a local model. Nothing was installed.",
             code=0)

    # 4. Ollama
    install_ollama()

    step("Checking Ollama")
    # 🔑 „Antwortet es?" und „hat es Modelle?" sind ZWEI Fragen. `modelle` kann
    #    eine leere Liste sein — das ist ein frisch installiertes Ollama, kein
    #    Netzproblem. Nur `None` heisst: da antwortet niemand.
    local = ollama_models("http://127.0.0.1:%d" % OLLAMA_PORT)
    target = "http://%s:%d" % (visible, OLLAMA_PORT)
    modelle = local if here else ollama_models(target)
    reachable = modelle is not None

    if reachable:
        good("Ollama answers at %s." % target)
    elif local is not None and not here:
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
        modelle = ollama_models(target, 4.0)
        if modelle is None:
            serve("0.0.0.0")
            modelle = ollama_models(target, 4.0)
        reachable = modelle is not None
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
        modelle = ollama_models(target, 4.0)
        reachable = modelle is not None

    # 🔑 Ollama answers on this machine but not from outside: that is a firewall
    #    and nothing else. Telling somebody „allow port 11434" is telling them
    #    to go and learn their firewall. Ask once, then do it.
    if not reachable and not here and ollama_antwortet(
            "http://127.0.0.1:%d" % OLLAMA_PORT, 3.0):
        art = firewall_art()
        if art:
            warn("Ollama runs, but your firewall (%s) is blocking port %d."
                 % (art, OLLAMA_PORT))
            if ask_yes_no("Open port %d so DocuSort can reach it?" % OLLAMA_PORT):
                firewall_oeffnen(art)
                modelle = ollama_models(target, 4.0)
                reachable = modelle is not None

    if not reachable:
        hinweis = ""
        if not here and ollama_antwortet("http://127.0.0.1:%d" % OLLAMA_PORT, 3.0):
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
    modelle = modelle or []
    if modelle:
        good("%d model(s) available." % len(modelle))
    else:
        # 🔑 Kein Mangel, sondern der Normalzustand eines frischen Ollama.
        good("Ollama answers. No model on it yet — fetching one now.")

    # 🔴 „Es laeuft" ist nicht „es laeuft auch morgen". Diese Frage kommt
    #    bewusst HIER und nicht nur im Zweig, der Ollama selbst gestartet hat:
    #    wer es vor diesem Lauf von Hand gestartet hatte, war am schlimmsten
    #    dran — fuer ihn sah alles fertig aus, und nach dem naechsten
    #    Hochfahren war nichts mehr da.
    bleibt_ollama(bind)

    # 4. Model — take what is already there before downloading gigabytes.
    # 🔑 The measurement decides, not a fixed default: on a machine with
    #    little memory the recommended model would swap and take hours.
    #    An explicit --model always wins — the person asking knows their box.
    model = (a.model.strip() or pick_model(modelle)
             or hw.get("model") or DEFAULT_MODEL)
    if model not in modelle:
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

    # Measure what a document really costs HERE, now that the model is there.
    # 🔑 A number from the machine itself beats any estimate from core counts,
    #    and it is the one thing that tells somebody whether this is going to
    #    work for them before they import three hundred documents.
    speed_check("http://127.0.0.1:%d" % OLLAMA_PORT, model)

    # 6. Tell DocuSort — and let DOCUSORT say whether it works.
    step("Telling DocuSort, and asking it to try the model")
    info("The first answer loads the model into memory — this can take a minute.")
    try:
        # 🔑 Alle Adressen mitschicken; DocuSort probiert sie durch und nimmt
        #    die, die es WIRKLICH erreicht. `url` bleibt fuer aeltere Stände.
        alle = ["http://%s:%d" % (ip, OLLAMA_PORT)
                for ip in my_addresses(host, port)] if not here else [target]
        res = post(origin + "/api/local-ai/adopt",
                   {"ticket": a.ticket, "url": target, "urls": alle,
                    "model": model},
                   VERIFY_WAIT, a.insecure)
        if res.get("url") and res["url"].rstrip("/v1").rstrip("/") != target:
            info("DocuSort reaches this machine at %s" % res["url"])
    except Exception as exc:
        # 🔴 Hier war alles schon getan: Ollama laeuft, gebunden, Modell
        #    heruntergeladen — und dann starb es an der UEBERGABE. Wer
        #    gigabyteweise gewartet hat, darf nicht mit „abgelehnt" alleine
        #    dastehen. Die zwei Werte, die noch fehlen, stehen hier.
        warn("DocuSort refused the setting: %s" % detail(exc))
        print("")
        info("Nothing of your work is lost — Ollama is running and the model")
        info("is on this machine. Only the handover failed. Two ways on:")
        print("")
        info("  a) Download the setup again in DocuSort (Settings, Local AI)")
        info("     and run it. The model is already here, so it is quick.")
        info("  b) Or type these two values in DocuSort yourself, under")
        info("     Settings, AI, provider \"OpenAI-compatible\":")
        print("")
        info("       Address :  %s/v1" % target)
        info("       Model   :  %s" % model)
        print("")
        stop("The setting was not saved.")
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
