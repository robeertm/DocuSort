"""Finding a local model, without anyone typing an address.

🔴 The search runs from the SERVER's side, never from the browser's. The
browser sits on the machine where Ollama is installed — it would happily
report "reachable" while DocuSort, on a VM or in a container somewhere else,
cannot get there at all. The question is never whether *you* can reach it.

Only addresses that are already known get asked. There is no scan of the
network here and there must never be one: a document server that probes its
neighbourhood is a document server nobody will install.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import time
import urllib.request

OLLAMA_PORT = 11434
PROBE_TIMEOUT = 2.0        # per address; they are asked side by side
PROBE_DEADLINE = 3.5       # hard ceiling for the whole search
ASK_TIMEOUT = 150.0        # the first answer loads the model into memory

# Order of preference among models that are already pulled. DocuSort's own
# documentation recommends qwen2.5:7b-instruct, so it comes first.
WISH = ("qwen2.5:7b-instruct", "qwen2.5:14b-instruct", "llama3.1:8b",
        "llama3.2:3b", "mistral:7b", "gemma2:9b")
# Embedders and vision models cannot classify a document — offering one as
# the suggestion would be the worst possible default.
UNUSABLE = ("embed", "bge-", "minilm", "clip", "rerank", "nomic-",
            "llava", "moondream")


def models_at(base: str, timeout: float = PROBE_TIMEOUT) -> list[str]:
    """The model list of an Ollama at `base`, or an empty list.

    Never raises: a search across several addresses must not end at the
    first one nobody is listening on."""
    try:
        req = urllib.request.Request(base.rstrip("/") + "/api/tags")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read().decode("utf-8", "replace") or "{}")
    except Exception:
        return []
    names = [str((m or {}).get("name") or "") for m in (data.get("models") or [])]
    return [n for n in names if n]


def usable_model(models: list[str]) -> str:
    """The model DocuSort is most likely to get on with."""
    usable = [m for m in models if not any(u in m.lower() for u in UNUSABLE)]
    for want in WISH:
        for m in usable:
            if m == want or m.split(":")[0] == want.split(":")[0]:
                return m
    return usable[0] if usable else ""


def _candidates(configured: str = "", client_ip: str = "") -> list[str]:
    """Every address worth asking — and nothing beyond that."""
    out: list[str] = []

    def add(u: str) -> None:
        u = (u or "").strip().rstrip("/")
        # The provider is configured with an OpenAI-style /v1 base; the
        # Ollama tag list lives one level up.
        if u.endswith("/v1"):
            u = u[:-3]
        if u and u not in out:
            out.append(u)

    add(configured)
    add(f"http://127.0.0.1:{OLLAMA_PORT}")
    if os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"):
        add(f"http://host.docker.internal:{OLLAMA_PORT}")
    ip = (client_ip or "").strip()
    # The machine that has the settings page open is the likeliest place for
    # a local model — and its address is the one thing the browser cannot
    # tell us but the connection already knows.
    if ip and not ip.startswith("127.") and ip != "::1":
        add("http://%s:%d" % (("[%s]" % ip) if ":" in ip else ip, OLLAMA_PORT))
    return out


def discover(configured: str = "", client_ip: str = "") -> list[dict]:
    """Where can DocuSort reach an Ollama? Asked side by side, with a ceiling —
    four unreachable addresses in a row would otherwise be four waits."""
    cands = _candidates(configured, client_ip)
    found: list[dict] = []
    lock = threading.Lock()
    threads: list[threading.Thread] = []

    def check(u: str) -> None:
        models = models_at(u)
        if models:
            with lock:
                found.append({"url": u, "models": models,
                              "suggested": usable_model(models)})

    for u in cands:
        t = threading.Thread(target=check, args=(u,), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=PROBE_DEADLINE)
    found.sort(key=lambda e: cands.index(e["url"]))
    return found


def ask(base: str, model: str, timeout: float = ASK_TIMEOUT) -> tuple[bool, str]:
    """One real, tiny question to the model.

    🔴 "Saved" is not "works". A service that answers 200 on its front page
    but does not know the model would otherwise show up green."""
    url = base.rstrip("/")
    if not url.endswith("/v1"):
        url += "/v1"
    body = json.dumps({
        "model": model, "temperature": 0, "max_tokens": 16,
        "messages": [{"role": "system", "content": "Answer with exactly one word."},
                     {"role": "user", "content": "Say: ready"}],
    }).encode()
    started = time.monotonic()
    try:
        req = urllib.request.Request(
            url + "/chat/completions", data=body,
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer ollama"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read().decode("utf-8", "replace") or "{}")
    except Exception as exc:
        return False, str(exc)[:200]
    choice = (data.get("choices") or [{}])[0]
    word = str((choice.get("message") or {}).get("content") or "").strip()
    word = word.splitlines()[0][:60] if word else ""
    if not word:
        return False, "the model answered nothing"
    return True, "%s (%.1f s)" % (word, time.monotonic() - started)


# --------------------------------------------------------------- setup ticket
# The setup script runs on the user's own machine and has no session cookie.
# It gets a short-lived ticket, baked into the launcher the administrator
# downloads, good for exactly two things: writing the AI setting and
# restarting the service afterwards.
#
# 🔴 Kept in memory on purpose. A ticket that survives a restart of the
# service is a ticket lying around on disk, and there is no reason for one to
# outlive the setup it belongs to.
SETUP_TTL = 30 * 60
_tickets: dict[str, float] = {}
_ticket_lock = threading.Lock()


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def new_setup_ticket() -> str:
    token = secrets.token_urlsafe(32)
    now = time.time()
    with _ticket_lock:
        for h, exp in list(_tickets.items()):     # expired ones go on the way out
            if exp < now:
                _tickets.pop(h, None)
        _tickets[_hash(token)] = now + SETUP_TTL
    return token


def check_setup_ticket(token: str) -> bool:
    if not token:
        return False
    h = _hash(token)
    now = time.time()
    with _ticket_lock:
        exp = _tickets.get(h)
        if exp is None or exp < now:
            _tickets.pop(h, None)
            return False
    return True


def spend_setup_ticket(token: str) -> None:
    """Called once the setup is finished — nothing is left to reuse."""
    with _ticket_lock:
        _tickets.pop(_hash(token), None)
