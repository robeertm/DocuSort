"""Tailscale — von innen, ohne Beiwagen und ohne Kommandozeile.

🔑 WARUM ES DIESES MODUL GIBT
Tailscale gab es hier schon: eine zweite compose-Datei und ein Skript mit
einem Auth-Key. Das setzt eine Kommandozeile voraus, einen Editor und jemanden,
der weiss, in welchem Ordner er steht. Robert wollte es dort, wo es hingehoert:
**in den Einstellungen, ein Feld und ein Knopf.**

🔑 WARUM DAS GEHT — gemessen, nicht gehofft
`tailscaled --tun=userspace-networking` braucht **kein** `NET_ADMIN` und
**kein** `/dev/net/tun`. In einem nackten Container gestartet meldet es
„Logged out." und wartet. Damit kann DocuSort seinen eigenen Tailscale
mitbringen, statt einen zweiten Container zu verlangen.

🔴 DIE ZUSTANDSDATEI GEHOERT DEM NUTZER
Sie liegt unter `<config>/tailscale/` — im eingehaengten Verzeichnis. Nur so
ueberlebt die Anmeldung ein Update: im Abbild waere sie beim naechsten
`docker compose pull` weg, und der Rechner hinge als Karteileiche im Tailnet.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

SOCKET = "/tmp/docusort-tailscaled.sock"
_lock = threading.Lock()
_daemon: subprocess.Popen | None = None


def verfuegbar() -> bool:
    """Sind die Binaerdateien ueberhaupt da? Aeltere Abbilder haben sie nicht."""
    return bool(shutil.which("tailscaled") and shutil.which("tailscale"))


def _zustandsordner(config_dir) -> Path:
    p = Path(config_dir) / "tailscale"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _cli(*args, timeout: float = 20.0) -> tuple[int, str]:
    """`tailscale …` gegen unseren eigenen Socket."""
    try:
        r = subprocess.run(["tailscale", "--socket", SOCKET, *args],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           timeout=timeout)
        return r.returncode, (r.stdout or b"").decode("utf-8", "replace").strip()
    except FileNotFoundError:
        return 127, "tailscale is not in this image"
    except subprocess.TimeoutExpired:
        return 124, "tailscale did not answer in time"


def daemon_laeuft() -> bool:
    return _daemon is not None and _daemon.poll() is None


def daemon_starten(config_dir) -> bool:
    """Den Dienst anwerfen, falls er nicht schon laeuft."""
    global _daemon
    with _lock:
        if daemon_laeuft():
            return True
        if not verfuegbar():
            return False
        ordner = _zustandsordner(config_dir)
        log = open(ordner / "tailscaled.log", "ab", buffering=0)
        _daemon = subprocess.Popen(
            ["tailscaled",
             "--tun=userspace-networking",
             "--state=%s" % (ordner / "tailscaled.state"),
             "--socket=%s" % SOCKET],
            stdout=log, stderr=log, start_new_session=True)
    # 🔑 „gestartet" ist nicht „antwortet". Fragen, nicht annehmen.
    for _ in range(25):
        if _cli("status", "--json", timeout=5)[0] != 124:
            code, _aus = _cli("status", timeout=5)
            if code in (0, 1):            # 1 = laeuft, aber abgemeldet
                return True
        time.sleep(0.4)
    return False


def status(config_dir) -> dict:
    """Was ist der Stand? Immer beantwortbar, auch wenn gar nichts laeuft."""
    if not verfuegbar():
        return {"moeglich": False, "laeuft": False, "verbunden": False,
                "adresse": "", "grund": "this image does not carry Tailscale"}
    if not daemon_laeuft():
        # Eine vorhandene Anmeldung ist Grund genug, ihn anzuwerfen.
        if (Path(config_dir) / "tailscale" / "tailscaled.state").exists():
            daemon_starten(config_dir)
    if not daemon_laeuft():
        return {"moeglich": True, "laeuft": False, "verbunden": False,
                "adresse": "", "grund": ""}
    code, aus = _cli("status", "--json")
    if code != 0 or not aus.startswith("{"):
        return {"moeglich": True, "laeuft": True, "verbunden": False,
                "adresse": "", "grund": aus[:300]}
    try:
        d = json.loads(aus)
    except Exception:
        return {"moeglich": True, "laeuft": True, "verbunden": False,
                "adresse": "", "grund": "could not read the status"}
    selbst = d.get("Self") or {}
    namen = [n for n in (selbst.get("DNSName") or "").split(".") if n]
    adresse = (selbst.get("DNSName") or "").rstrip(".")
    return {
        "moeglich": True,
        "laeuft": True,
        "verbunden": (d.get("BackendState") == "Running"),
        "zustand": d.get("BackendState") or "",
        "adresse": adresse,
        "name": namen[0] if namen else "",
        "grund": "",
    }


def verbinden(config_dir, authkey: str, port: int, hostname: str = "docusort") -> dict:
    """Anmelden und die Oberflaeche ueber HTTPS anbieten.

    🔴 `port` ist der Port, auf dem DocuSort WIRKLICH hoert — nicht die
    Vorgabe. Genau diese Verwechslung hat eine Installation von vor 0.67.0
    hinter Tailscale unerreichbar gemacht: der Name loeste auf, das Zertifikat
    stimmte, und dahinter horchte niemand.
    """
    # 🔑 Erst die EINGABE, dann die Umgebung. Ein leerer Schluessel ist ein
    #    leerer Schluessel — egal, was auf dieser Maschine installiert ist.
    #    Umgekehrt bekaeme jemand ohne Schluessel eine Auskunft ueber das
    #    Abbild, die mit seinem Fehler nichts zu tun hat.
    schluessel = (authkey or "").strip()
    if not schluessel:
        return {"ok": False, "grund": "no auth key given"}
    # 🔴 AUF DER KEYS-SEITE STEHEN ZWEI KNOEPFE
    # „Generate auth key…" (Auth keys) und „Generate access token…" (API access
    # tokens). Gebraucht wird der ERSTE. Der zweite ist ein Schluessel fuer die
    # Tailscale-API und kann einen Rechner nicht anmelden — `tailscale up`
    # scheitert damit mit einer Meldung, die niemandem sagt, dass der Knopf
    # daneben der richtige war. Die beiden sind am Anfang unterscheidbar:
    #   tskey-auth-…   anmelden
    #   tskey-api-…    die API bedienen
    if schluessel.startswith("tskey-api-"):
        return {"ok": False, "grund":
                "that is an API access token, not an auth key. On the Keys "
                "page use the upper button, \u201cGenerate auth key\u2026\u201d "
                "under \u201cAuth keys\u201d \u2014 not \u201cGenerate access "
                "token\u2026\u201d. An auth key starts with tskey-auth-."}
    if not schluessel.startswith("tskey-"):
        return {"ok": False, "grund":
                "that does not look like a Tailscale key \u2014 they start with "
                "tskey-auth-. On the Keys page: \u201cGenerate auth key\u2026\u201d "
                "under \u201cAuth keys\u201d."}
    if not verfuegbar():
        return {"ok": False, "grund": "this image does not carry Tailscale"}
    if not daemon_starten(config_dir):
        return {"ok": False, "grund": "the Tailscale service did not start"}

    code, aus = _cli("up", "--authkey", authkey.strip(),
                     "--hostname", hostname, "--accept-dns=false",
                     timeout=90)
    if code != 0:
        return {"ok": False, "grund": aus[:400] or "tailscale up failed"}

    # 🔑 Erst jetzt anbieten — vorher gibt es kein Zertifikat und keinen Namen.
    code, aus = _cli("serve", "--bg", "--https=443",
                     "http://127.0.0.1:%d" % port, timeout=60)
    if code != 0:
        return {"ok": False, "verbunden": True,
                "grund": "joined, but could not publish the page: %s" % aus[:300]}
    st = status(config_dir)
    return {"ok": True, "adresse": st.get("adresse", ""), "grund": ""}


def trennen(config_dir) -> dict:
    """Abmelden und das Angebot zuruecknehmen — der Rechner verschwindet."""
    if not daemon_laeuft():
        return {"ok": True}
    _cli("serve", "reset", timeout=30)
    code, aus = _cli("down", timeout=30)
    return {"ok": code == 0, "grund": "" if code == 0 else aus[:300]}


def beim_start(config_dir, port: int) -> None:
    """Nach einem Neustart von selbst weitermachen.

    🔴 Ohne das waere die Anmeldung zwar gespeichert, aber nach jedem Update
    muesste jemand den Knopf erneut druecken — und bei stuendlichen Updates
    waere das stuendlich.
    """
    if not verfuegbar():
        return
    if not (Path(config_dir) / "tailscale" / "tailscaled.state").exists():
        return
    if not daemon_starten(config_dir):
        logger.warning("Tailscale: the service did not start")
        return
    code, aus = _cli("serve", "--bg", "--https=443",
                     "http://127.0.0.1:%d" % port, timeout=60)
    if code == 0:
        st = status(config_dir)
        if st.get("adresse"):
            logger.info("Tailscale: reachable at https://%s", st["adresse"])
    else:
        logger.warning("Tailscale: could not publish the page: %s", aus[:200])
