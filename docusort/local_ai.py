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
import logging
import os
import secrets
import threading
import time
import urllib.request

logger = logging.getLogger(__name__)

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


def antwortet(base: str, timeout: float = PROBE_TIMEOUT) -> bool:
    """Antwortet an dieser Adresse ueberhaupt ein Ollama?

    🔴 NICHT DASSELBE WIE „hat es Modelle". `models_at()` gibt in beiden
    Faellen eine leere Liste zurueck: wenn niemand lauscht UND wenn ein frisch
    installiertes Ollama noch kein Modell hat. Wer die beiden verwechselt,
    erklaert ein fertiges Ollama fuer tot — genau der Fehler, der weiter unten
    in dieser Datei schon einmal kommentiert ist.
    """
    try:
        req = urllib.request.Request(base.rstrip("/") + "/api/tags")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return 200 <= r.status < 300
    except Exception:
        return False


def usable_model(models: list[str]) -> str:
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
        # 🔑 Die Kiste nebenan. `docker-compose.yml` bringt einen optionalen
        # `ollama`-Dienst mit; mit `--profile ki` gestartet ist er unter seinem
        # Dienstnamen im Docker-Netz erreichbar. Das ist die einzige Adresse, für
        # die niemand einen Port veröffentlichen und niemand etwas eintippen
        # muss — darum wird sie vor dem Wirt gefragt.
        add(f"http://ollama:{OLLAMA_PORT}")
        # 🔴 Ein Modell auf dem Rechner, auf dem der Container sitzt. Diesen
        # Namen gibt es auf Linux NICHT von sich aus — nur Docker Desktop
        # erfindet ihn. Die ausgelieferte Compose-Datei bildet ihn mit
        # `extra_hosts: host.docker.internal:host-gateway` ab; ohne diese Zeile
        # antwortet der Kandidat auf einer Synology, einem Pi und einem VPS
        # gleichermaßen „Name or service not known" — gemessen —, und die Suche
        # bleibt leer, obwohl das Modell direkt daneben lief.
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
# 🔴 THIS USED TO SAY: kept in memory on purpose, because a ticket that
# survives a restart is a ticket lying around on disk, and none should outlive
# the setup it belongs to. The reasoning was fine. Both of its premises are not:
#
#   * „the setup it belongs to" is not a short thing. The script installs
#     Ollama and pulls a model — gigabytes over whatever line the user has.
#     Thirty minutes is a fast download, not a generous allowance, and the
#     ticket was only ever checked at the very END, after that download. One
#     user watched a model finish and was then told the ticket had expired.
#   * „a restart" used to be rare. Since updates arrive hourly, the service may
#     restart at the top of any hour — right through the middle of a setup.
#
# So it is written down now: the SHA-256 of the token and its expiry, nothing
# that identifies anyone, in the config directory with owner-only permissions,
# removed the moment it is spent or expires.
SETUP_TTL = 4 * 60 * 60
_tickets: dict[str, float] = {}
_ticket_lock = threading.Lock()
_ticket_store: str = ""


def set_ticket_store(pfad: str) -> None:
    """Where tickets are written down. Called once when the app is built."""
    global _ticket_store
    _ticket_store = pfad
    with _ticket_lock:
        _tickets.update(_lies_tickets())


def _lies_tickets() -> dict:
    if not _ticket_store:
        return {}
    try:
        with open(_ticket_store, encoding="utf-8") as fh:
            roh = json.load(fh)
        jetzt = time.time()
        return {str(h): float(e) for h, e in roh.items() if float(e) > jetzt}
    except Exception:
        return {}


def _schreibe_tickets() -> None:
    """Der Aufrufer haelt bereits `_ticket_lock`."""
    if not _ticket_store:
        return
    tmp = _ticket_store + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(_tickets, fh)
        os.chmod(tmp, 0o600)
        os.replace(tmp, _ticket_store)
    except Exception:
        try:
            os.remove(tmp)
        except Exception:
            pass


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
        _schreibe_tickets()
    return token


def check_setup_ticket(token: str) -> bool:
    if not token:
        return False
    h = _hash(token)
    now = time.time()
    with _ticket_lock:
        exp = _tickets.get(h)
        if exp is None:
            # Die Fassung von der Platte kann neuer sein als unsere — nach
            # einem Neustart ist der Speicher leer, die Datei nicht.
            _tickets.update(_lies_tickets())
            exp = _tickets.get(h)
        if exp is None or exp < now:
            if _tickets.pop(h, None) is not None:
                _schreibe_tickets()
            return False
    return True


def spend_setup_ticket(token: str) -> None:
    """Called once the setup is finished — nothing is left to reuse."""
    with _ticket_lock:
        _tickets.pop(_hash(token), None)
        _schreibe_tickets()


# ------------------------------------------------------- Suche im eigenen Netz
# „es wird ermittelt was gibt es für Hardware in der Umgebung"
#
# 🔴 EIN NETZSCAN IST NICHTS, WAS MAN NEBENBEI TUT. 254 Verbindungen in ein
#    fremdes Netz sehen von außen aus wie ein Portscan, und in einem Firmennetz
#    ist das ein Vorfall. Deshalb:
#      · nur auf ausdrücklichen Knopfdruck, nie beim Laden einer Seite,
#      · nur der EINE Port, auf dem Ollama lauscht,
#      · nur das eigene /24, nie ein größerer Bereich,
#      · kurze Zeitgrenze, damit es Sekunden dauert und nicht Minuten.
#
# 🔑 Und zuerst wird ohne Scan gefragt: `discover()` kennt schon die Adressen,
#    die etwas über sich verraten haben — die Gegenstelle des Browsers vor
#    allem. Wer die Seite von seinem Arbeitsrechner aus offen hat, wird dort
#    gefunden, ohne dass ein einziges fremdes Gerät angefasst wird.

SCAN_PORT = OLLAMA_PORT
SCAN_TIMEOUT = 0.35          # je Adresse; parallel, also nicht die Summe
SCAN_WORKERS = 64


def _ist_docker_netz(ip: str) -> bool:
    """Liegt diese Adresse in einem Docker-Bereich (172.16.0.0/12)?

    🔴 DAS IST DER UNTERSCHIED ZWISCHEN „mein Haus" UND „mein Container".
    DocuSort läuft meistens in einem Container, und dessen eigene Adresse ist
    die des Docker-Netzes (irgendwo in 172.16/12). Wer von dort „das eigene
    /24" absucht, durchsucht das Docker-Netz und findet darin genau eine
    Adresse: das Gateway, also den eigenen Wirt. Als Fund angezeigt heißt das
    eine Adresse, die zu keinem Geraet im Haus passt — und die Frage des
    Benutzers lautete zu Recht: welches Geraet soll das sein? Die Rechner im
    Haus findet man so NIE.
    """
    teile = ip.split(".")
    if len(teile) != 4 or teile[0] != "172":
        return False
    try:
        return 16 <= int(teile[1]) <= 31
    except ValueError:
        return False


def _eigene_adresse() -> str:
    """Die Adresse, mit der dieser Rechner nach draußen ginge — oder ""."""
    import socket as _s
    try:
        s = _s.socket(_s.AF_INET, _s.SOCK_DGRAM)
        try:
            # Verbindet nichts, fragt nur die Routing-Tabelle, welche eigene
            # Adresse für ein Ziel draußen benutzt würde.
            s.connect(("192.0.2.1", 9))
            return s.getsockname()[0] or ""
        finally:
            s.close()
    except Exception:
        return ""


def _eigenes_netz(client_ip: str = "") -> tuple[str, str] | None:
    """Welches /24 soll durchsucht werden, und welche Adresse ist die eigene.

    🔑 DIE ADRESSE DES BROWSERS ENTSCHEIDET. Wer die Seite offen hat, sitzt im
    richtigen Netz — das ist die einzige verlässliche Auskunft darüber, wo
    „das Haus" liegt, und der Container bekommt sie geschenkt, weil die
    Verbindung sie ohnehin mitbringt. Die eigene Adresse wird nur genommen,
    wenn sie NICHT aus einem Docker-Netz stammt.

    🔴 Ohne brauchbares Netz wird nicht gescannt, statt ein falsches zu raten.
    """
    eigene = _eigene_adresse()
    kandidaten = []
    k = (client_ip or "").strip()
    if k and not k.startswith("127.") and ":" not in k:
        kandidaten.append(k)
    if eigene and not eigene.startswith("127.") and not _ist_docker_netz(eigene):
        kandidaten.append(eigene)
    for ip in kandidaten:
        teile = ip.split(".")
        if len(teile) == 4:
            return eigene or ip, ".".join(teile[:3])
    if eigene and _ist_docker_netz(eigene):
        logger.info("Netzsuche: die eigene Adresse liegt in einem "
                    "Docker-Netz — ohne die Adresse des Browsers lässt sich "
                    "das Hausnetz nicht bestimmen, es wird nicht gescannt")
    return None


def _name_zu(ip: str) -> str:
    """Der Rechnername zu einer Adresse, wenn das Netz ihn kennt.

    🔑 „welches gerät soll das sein?" — eine nackte Zahl beantwortet das
    nicht. Reverse-DNS beantwortet es oft, und wo nicht, bleibt es ehrlich
    leer statt geraten.
    """
    import socket as _s
    alt = _s.getdefaulttimeout()
    try:
        _s.setdefaulttimeout(1.0)
        name = _s.gethostbyaddr(ip)[0]
        return name.split(".")[0] if name else ""
    except Exception:
        return ""
    finally:
        _s.setdefaulttimeout(alt)


def scan_netz(timeout: float = SCAN_TIMEOUT, client_ip: str = "") -> list[dict]:
    """Das Netz des Benutzers nach Ollama absuchen.

    Gibt dieselbe Form zurück wie `discover()`: url, models, suggested — dazu
    `name` (Rechnername, wenn ermittelbar) und `gateway` (die Adresse, über
    die dieser Container nach draußen geht; das ist der eigene Wirt und kein
    fremdes Gerät).
    """
    netz = _eigenes_netz(client_ip)
    if netz is None:
        logger.info("Netzsuche: eigene Adresse nicht ermittelbar, "
                    "es wird nicht gescannt")
        return []
    eigene, praefix = netz
    import socket as _s
    from concurrent.futures import ThreadPoolExecutor

    def offen(host: str) -> str | None:
        try:
            with _s.create_connection((host, SCAN_PORT), timeout) as _:
                return host
        except Exception:
            return None

    adressen = [f"{praefix}.{n}" for n in range(1, 255)]
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        treffer = [h for h in pool.map(offen, adressen) if h]

    # 🔑 Ein offener Port ist noch kein Ollama. Erst die Modellliste beweist es
    #    — und sie ist zugleich das, was der Benutzer sehen will.
    gefunden: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(16, max(1, len(treffer)))) as pool:
        def pruefe(host: str) -> dict | None:
            url = f"http://{host}:{SCAN_PORT}"
            modelle = models_at(url, timeout=2.0)
            if modelle is None:
                return None
            return {"url": url, "models": modelle,
                    "suggested": usable_model(modelle),
                    "self": host == eigene,
                    "name": _name_zu(host)}
        for e in pool.map(pruefe, treffer):
            if e:
                gefunden.append(e)
    logger.info("Netzsuche: %d Adresse(n) offen, %d davon mit Ollama",
                len(treffer), len(gefunden))
    return gefunden


def modell_loeschen(base: str, model: str, timeout: float = 30.0) -> tuple[bool, str]:
    """Ein Modell auf einem Ollama löschen.

    🔴 Das gibt mehrere Gigabyte frei und ist nicht rückgängig zu machen —
    der Aufrufer muss nachfragen, bevor er das hier ruft.
    """
    import json as _j
    from urllib import request as _r, error as _e
    wurzel = (base or "").rstrip("/")
    if wurzel.endswith("/v1"):
        wurzel = wurzel[:-3].rstrip("/")
    daten = _j.dumps({"model": model}).encode("utf-8")
    req = _r.Request(wurzel + "/api/delete", data=daten, method="DELETE",
                     headers={"Content-Type": "application/json"})
    try:
        with _r.urlopen(req, timeout=timeout) as r:
            r.read()
        return True, ""
    except _e.HTTPError as exc:
        return False, (exc.read().decode("utf-8", "replace")[:200]
                       or f"HTTP {exc.code}")
    except Exception as exc:  # noqa: BLE001
        return False, type(exc).__name__
