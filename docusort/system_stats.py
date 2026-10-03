"""Was die Maschine gerade tut — CPU, Speicher, Platte, Eingang.

Warum das hier liegt und nicht im Container-Manager: wer DocuSort zuschaut,
will an EINER Stelle sehen, ob gerade gearbeitet wird. Die KI-Kachel zaehlt nur
LLM-Aufrufe; waehrend OCR laeuft steht dort "KI im Leerlauf", obwohl vier Kerne
gluehen. Genau diese Luecke schliesst dieses Modul.

🔴 Wir laufen im Container, aber `/proc/stat` und `/proc/meminfo` zeigen die
   Werte des WIRTS (Synology hat kein lxcfs). Das ist Absicht und auch das,
   was im Container-Manager steht — die Zahlen sind also vergleichbar.
🔴 Nichts hier darf die Anwendung umbringen: jede Messung faellt einzeln auf
   None zurueck, und der Faden stirbt nie an einer Ausnahme.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import threading
import time
import urllib.request
from collections import deque
from typing import Any

TAKT_S = 10          # wie oft gemessen wird
PUNKTE = 180         # 180 × 10 s = 30 Minuten Verlauf

_sperre = threading.Lock()
_verlauf: deque[dict[str, Any]] = deque(maxlen=PUNKTE)
_letzte_cpu: tuple[int, int] | None = None   # (arbeit, gesamt)
_faden: threading.Thread | None = None
_inbox: str = ""
_library: str = ""
_ki_url: str = ""
_ticks_vorher: dict[int, int] = {}
_ticks_zeit: float = 0.0
logger = logging.getLogger("docusort.system_stats")


# ----------------------------------------------------------------- Messungen
def _cpu_prozent() -> float | None:
    """Auslastung aller Kerne zusammen, 0..100, aus zwei /proc/stat-Blicken."""
    global _letzte_cpu
    try:
        with open("/proc/stat", encoding="utf-8") as fh:
            teile = fh.readline().split()
    except OSError:
        return None
    if len(teile) < 5 or teile[0] != "cpu":
        return None
    werte = [int(x) for x in teile[1:8] if x.isdigit()]
    gesamt = sum(werte)
    leerlauf = werte[3] + (werte[4] if len(werte) > 4 else 0)   # idle + iowait
    arbeit = gesamt - leerlauf
    vorher = _letzte_cpu
    _letzte_cpu = (arbeit, gesamt)
    if vorher is None:
        return None            # erster Blick hat keinen Bezug
    d_arbeit = arbeit - vorher[0]
    d_gesamt = gesamt - vorher[1]
    if d_gesamt <= 0:
        return None
    return round(max(0.0, min(100.0, 100.0 * d_arbeit / d_gesamt)), 1)


def _speicher() -> dict[str, Any]:
    try:
        werte = {}
        with open("/proc/meminfo", encoding="utf-8") as fh:
            for z in fh:
                k, _, rest = z.partition(":")
                werte[k] = int(rest.split()[0]) * 1024
        gesamt = werte.get("MemTotal") or 0
        frei = werte.get("MemAvailable")
        if frei is None:
            frei = (werte.get("MemFree") or 0) + (werte.get("Cached") or 0)
        benutzt = max(0, gesamt - frei)
        return {"gesamt": gesamt, "benutzt": benutzt,
                "prozent": round(100.0 * benutzt / gesamt, 1) if gesamt else None}
    except (OSError, ValueError, IndexError):
        return {"gesamt": None, "benutzt": None, "prozent": None}


def _last() -> list[float] | None:
    try:
        return [round(x, 2) for x in os.getloadavg()]
    except (OSError, AttributeError):
        return None


def _eigener_speicher() -> int | None:
    """RSS des DocuSort-Prozesses selbst — der Teil, den WIR verursachen."""
    try:
        with open("/proc/self/status", encoding="utf-8") as fh:
            for z in fh:
                if z.startswith("VmRSS:"):
                    return int(z.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def _platte(pfad: str) -> dict[str, Any]:
    try:
        g, b, f = shutil.disk_usage(pfad)
        return {"gesamt": g, "benutzt": b, "frei": f,
                "prozent": round(100.0 * b / g, 1) if g else None}
    except OSError:
        return {"gesamt": None, "benutzt": None, "frei": None, "prozent": None}


def _eingang(pfad: str) -> dict[str, Any]:
    """Wie viel liegt im Eingang und seit wann — das ist die ehrliche Antwort
    auf „passiert gerade etwas?", auch wenn die KI noch gar nicht dran ist."""
    try:
        namen = [n for n in os.listdir(pfad) if not n.startswith(".")]
    except OSError:
        return {"anzahl": 0, "aeltester_s": None, "aeltester_name": None}
    aeltester_s, aeltester_name = None, None
    jetzt = time.time()
    for n in namen:
        try:
            alter = jetzt - os.path.getmtime(os.path.join(pfad, n))
        except OSError:
            continue
        if aeltester_s is None or alter > aeltester_s:
            aeltester_s, aeltester_name = alter, n
    return {"anzahl": len(namen),
            "aeltester_s": int(aeltester_s) if aeltester_s is not None else None,
            "aeltester_name": aeltester_name}


# ----------------------------------------------------- wer verbraucht was
# Die Teile des OCR-Laufs. ocrmypdf ruft sie als eigene Prozesse auf, also
# tauchen sie im Baum auf und gehoeren der Texterkennung zugerechnet.
_OCR_NAMEN = ("ocrmypdf", "tesseract", "gs", "pngquant", "unpaper", "qpdf",
              "jbig2", "img2pdf")
_UHR = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100


def _eigene_prozesse() -> dict[str, dict[str, Any]]:
    """CPU und Speicher des eigenen Baums, getrennt nach DocuSort und OCR.

    🔴 Im Container zeigt /proc NUR die eigenen Prozesse — Ollama laeuft
       woanders und ist hier unsichtbar. Das ist kein Mangel: die KI wird
       ueber ihren eigenen Endpunkt gefragt, nicht geraten.
    """
    global _ticks_vorher, _ticks_zeit
    jetzt = time.time()
    ticks: dict[int, int] = {}
    gruppen = {"docusort": {"cpu": 0.0, "rss": 0, "anzahl": 0},
               "ocr":      {"cpu": 0.0, "rss": 0, "anzahl": 0}}
    try:
        pids = [int(n) for n in os.listdir("/proc") if n.isdigit()]
    except OSError:
        return gruppen
    for pid in pids:
        try:
            with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
                roh = fh.read()
            # comm steht in Klammern und kann Leerzeichen enthalten
            nach = roh.rsplit(")", 1)
            name = roh.split("(", 1)[1].rsplit(")", 1)[0]
            felder = nach[1].split()
            utime, stime = int(felder[11]), int(felder[12])
            rss_seiten = int(felder[21])
        except (OSError, ValueError, IndexError):
            continue
        g = "ocr" if any(n in name for n in _OCR_NAMEN) else "docusort"
        ticks[pid] = utime + stime
        gruppen[g]["rss"] += rss_seiten * os.sysconf("SC_PAGE_SIZE")
        gruppen[g]["anzahl"] += 1
        vorher = _ticks_vorher.get(pid)
        if vorher is not None and _ticks_zeit and jetzt > _ticks_zeit:
            d = (ticks[pid] - vorher) / _UHR
            gruppen[g]["cpu"] += 100.0 * d / (jetzt - _ticks_zeit)
    _ticks_vorher, _ticks_zeit = ticks, jetzt
    for g in gruppen.values():
        g["cpu"] = round(g["cpu"], 1)
    return gruppen


# 🔴 In einem Container ist der Hostname die CONTAINER-ID — zwölf Zeichen
#    Hexadezimal, die kein Mensch einem Geraet zuordnen kann. Auf der
#    Startseite stand deshalb „DIESER RECHNER · 41d48087aee1", und die Frage
#    des Benutzers lautete zu Recht: warum steht da das?
_HEX12 = re.compile(r"\A[0-9a-f]{12,64}\Z")


def _rechnername() -> str:
    """Der Name DIESES Rechners — so, dass ein Mensch ihn wiedererkennt.

    🔑 Reihenfolge, und sie ist der ganze Punkt:
    1. Der Produktname des WIRTS aus dem DMI (`DS1621+`). Der steht auch im
       Container zur Verfuegung, weil er vom Kernel kommt und nicht aus dem
       Dateisystem — und er benennt das Geraet, das jemand im Regal stehen hat.
    2. Sonst der Hostname — aber nur, wenn er nicht wie eine Container-ID
       aussieht.
    3. Sonst nichts. Ein leeres Feld ist ehrlicher als eine Zahl, die eine
       Auskunft vortaeuscht.
    """
    try:
        from .hardware import _produktname
        produkt = _produktname()
        if produkt:
            return produkt
    except Exception:  # noqa: BLE001
        pass
    try:
        name = os.uname().nodename
    except Exception:  # noqa: BLE001
        return ""
    return "" if _HEX12.match(name or "") else (name or "")


def _ki_lage_unbenutzt() -> dict[str, Any]:
    """Was die KI gerade belegt — gefragt, nicht geschaetzt.

    Ollama laeuft in einem anderen Container; sein /api/ps nennt das geladene
    Modell, seine Groesse und das Kontextfenster. Das ist die einzige ehrliche
    Quelle fuer den Posten KI in der Verbrauchsliste.
    """
    if not _ki_url:
        return {}
    try:
        with urllib.request.urlopen(_ki_url, timeout=3) as r:
            d = json.load(r)
    except Exception:  # noqa: BLE001 — eine unerreichbare KI ist kein Fehler
        return {"erreichbar": False}
    modelle = d.get("models") or []
    if not modelle:
        return {"erreichbar": True, "geladen": False}
    m = modelle[0]
    return {"erreichbar": True, "geladen": True,
            "modell": m.get("name") or m.get("model"),
            "groesse": m.get("size"),
            "parameter": (m.get("details") or {}).get("parameter_size"),
            "quantisierung": (m.get("details") or {}).get("quantization_level"),
            "kontext": m.get("context_length")}


# ------------------------------------------------------------------- Sammler
def _einmal_messen() -> None:
    probe = {
        "t": int(time.time()),
        "cpu": _cpu_prozent(),
        "ram": _speicher().get("prozent"),
        "eingang": _eingang(_inbox).get("anzahl") if _inbox else 0,
    }
    with _sperre:
        _verlauf.append(probe)


def _schleife() -> None:
    _cpu_prozent()                      # Bezugspunkt setzen
    runden = 0
    while True:
        try:
            _einmal_messen()
            runden += 1
            # 🔑 Ein Hintergrund-Arbeiter, der NICHTS ins Protokoll schreibt, ist
            #    nicht ueberpruefbar — man sieht einem stillen Faden nicht an, ob
            #    er lebt oder vor Stunden eingefroren ist. Einmal die Stunde eine
            #    Zeile reicht, um das von aussen zu beantworten.
            if runden == 1 or runden % (3600 // TAKT_S) == 0:
                with _sperre:
                    punkte = len(_verlauf)
                logger.info("Systemwerte: %d Messungen, %d Punkte im Verlauf",
                            runden, punkte)
        except Exception:               # noqa: BLE001 — der Faden stirbt nie
            pass
        time.sleep(TAKT_S)


def start(inbox: str, library: str, ki_base_url: str = "") -> None:
    """Einmal beim Hochfahren rufen. Mehrfachaufrufe sind folgenlos."""
    global _faden, _inbox, _library, _ki_url
    _inbox, _library = str(inbox), str(library)
    if ki_base_url:
        # …/v1 -> …/api/ps  (Ollama spricht beides, die Lage steht unter /api)
        wurzel = ki_base_url.rstrip("/")
        if wurzel.endswith("/v1"):
            wurzel = wurzel[:-3]
        _ki_url = wurzel.rstrip("/") + "/api/ps"
    if _faden is not None and _faden.is_alive():
        return
    _faden = threading.Thread(target=_schleife, name="system-stats", daemon=True)
    _faden.start()
    logger.info("Systemwerte: Sammler gestartet (alle %d s, %d Punkte Verlauf)",
                TAKT_S, PUNKTE)


# ----------------------------------------------------------------- Auskunft
def snapshot() -> dict[str, Any]:
    """Momentaufnahme + Verlauf, fertig fuer die Startseite."""
    with _sperre:
        verlauf = list(_verlauf)
    sp = _speicher()
    eing = _eingang(_inbox) if _inbox else {"anzahl": 0, "aeltester_s": None,
                                            "aeltester_name": None}
    # 🔑 Der jüngste CPU-Wert kommt aus dem Verlauf, nicht aus einer frischen
    #    Messung: zwei Blicke im Abstand von Millisekunden ergeben Rauschen.
    cpu_jetzt = next((p["cpu"] for p in reversed(verlauf) if p["cpu"] is not None), None)
    return {
        "takt_s": TAKT_S,
        "kerne": os.cpu_count(),
        "cpu": cpu_jetzt,
        "ram": sp,
        "last": _last(),
        "eigener_speicher": _eigener_speicher(),
        "platte": _platte(_library or "/"),
        "eingang": eing,
        # Wer verbraucht was AUF DIESEM RECHNER. Das Modell steht
        # moeglicherweise woanders — danach wird der ANBIETER gefragt
        # (Provider.runtime()), denn nur er weiss, wo es wohnt.
        "verbraucher": _eigene_prozesse(),
        "host": _rechnername(),
        # Verlauf als drei schlanke Reihen — die Seite zeichnet daraus Linien.
        "verlauf": {
            "t":       [p["t"] for p in verlauf],
            "cpu":     [p["cpu"] for p in verlauf],
            "ram":     [p["ram"] for p in verlauf],
            "eingang": [p["eingang"] for p in verlauf],
        },
    }
