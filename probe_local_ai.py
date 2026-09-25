#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Probes the one-click path to a local model — the parts that fail silently.

    python3 probe_local_ai.py

Runs the whole app in-process against a throwaway config and database. It
never touches a running install, and it presses no button that acts outward.

🔴 What it is really watching for:
  · the three ticket-guarded paths are public, and NOTHING else under
    /api/local-ai/ is — a prefix instead of three exact paths would open the
    search and the apply to anyone who knows the address;
  · a ticket that is missing, wrong or spent is refused;
  · the GENERATED launcher is correct — `%%TEMP%%` in a .bat is taken
    literally, and that is visible only in the generated file;
  · "saved" is reported apart from "works".
"""
from __future__ import annotations

import io
import json
import re as _re2
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

HIER = Path(__file__).resolve().parent
sys.path.insert(0, str(HIER))
fehler: list[str] = []


def probe(name: str, ok: bool, zusatz: str = "") -> None:
    print("  %s %s%s" % ("OK  " if ok else "FEHL", name, ("  — " + zusatz) if zusatz else ""))
    if not ok:
        fehler.append(name)


def baue_app(tmp: Path):
    """A DocuSort with nothing in it, built from a throwaway config."""
    from docusort.config import load_config
    from docusort.db import Database
    from docusort.web.app import create_app

    conf = tmp / "config"
    conf.mkdir(parents=True, exist_ok=True)
    daten = tmp / "data"
    for sub in ("inbox", "library", "library/_Review", "library/_Processed", "logs"):
        (daten / sub).mkdir(parents=True, exist_ok=True)
    (conf / "config.yaml").write_text(json.dumps({
        "paths": {"inbox": str(daten / "inbox"), "library": str(daten / "library"),
                  "review": str(daten / "library/_Review"),
                  "processed": str(daten / "library/_Processed"),
                  "logs": str(daten / "logs"),
                  "db": str(daten / "library/docusort.db")},
        "web": {"host": "127.0.0.1", "port": 9999, "default_language": "en"},
        "ai": {"provider": "openai_compat", "model": "qwen2.5:7b-instruct",
               "base_url": "http://127.0.0.1:11434/v1"},
        "ocr": {}, "sync": {}, "finance": {}, "notifications": {},
    }), encoding="utf-8")
    # Kategorien sind Woerterbuecher mit `name`, keine blossen Zeichenketten —
    # gegen die ECHTE Struktur gebaut, nicht gegen eine vermutete.
    (conf / "categories.yaml").write_text(json.dumps(
        {"categories": [{"name": "Test", "subcategories": [], "tags": []}]}),
        encoding="utf-8")
    settings = load_config(conf)
    db = Database(Path(settings.paths.db))
    return create_app(settings, db), settings, db


def main() -> int:
    from fastapi.testclient import TestClient
    from docusort import local_ai

    tmp = Path(tempfile.mkdtemp(prefix="docusort-probe-"))
    try:
        app, settings, db = baue_app(tmp)
        c = TestClient(app, base_url="http://probe.local")

        print("\n── 1. Welche Wege ohne Anmeldung offenstehen ──")
        # 🔴 Genau drei. Der Einrichter hat keine Sitzung, alles andere schon.
        offen = {"/api/local-ai/setup-script", "/api/local-ai/adopt",
                 "/api/local-ai/finish"}
        zu = {"/api/local-ai/probe", "/api/local-ai/apply", "/api/local-ai/installer"}
        from docusort import auth as _auth
        for p in sorted(offen):
            probe("offen ohne Anmeldung: %s" % p, _auth.is_public_path(p))
        for p in sorted(zu):
            probe("NICHT offen: %s" % p, not _auth.is_public_path(p))
        probe("das Praefix ist nicht offen",
              not _auth.is_public_path("/api/local-ai/"),
              "sonst waeren Suche und Uebernahme fuer jeden erreichbar")

        print("\n── 3. Das Ticket ──")
        r = c.post("/api/local-ai/adopt", json={"url": "http://127.0.0.1:11434",
                                                "model": "x"})
        probe("ohne Ticket abgewiesen", r.status_code == 401, "HTTP %d" % r.status_code)
        r = c.post("/api/local-ai/adopt", json={"ticket": "erfunden",
                                                "url": "http://127.0.0.1:11434",
                                                "model": "x"})
        probe("mit falschem Ticket abgewiesen", r.status_code == 401,
              "HTTP %d" % r.status_code)
        t = local_ai.new_setup_ticket()
        probe("frisches Ticket gilt", local_ai.check_setup_ticket(t))
        local_ai.spend_setup_ticket(t)
        probe("verbrauchtes Ticket gilt nicht mehr",
              not local_ai.check_setup_ticket(t))
        probe("leeres Ticket gilt nicht", not local_ai.check_setup_ticket(""))

        print("\n── 4. Wo ueberhaupt gesucht wird ──")
        # 🔴 Die Adresse des Rechners, der die Seite offen hat, MUSS dabei sein —
        # das ist der ganze Unterschied zur Suche im Browser.
        kand = local_ai._candidates("http://10.0.0.5:11434/v1", "192.168.1.23")
        probe("die eingetragene Adresse wird gefragt",
              "http://10.0.0.5:11434" in kand, "und ohne /v1")
        probe("der eigene Rechner wird gefragt",
              "http://127.0.0.1:11434" in kand)
        probe("der Rechner vor der Seite wird gefragt",
              "http://192.168.1.23:11434" in kand)
        probe("kein Abklappern des Netzes", len(kand) <= 4,
              "%d Adressen" % len(kand))
        probe("127.0.0.1 als Klient wird nicht doppelt gefragt",
              local_ai._candidates("", "127.0.0.1").count("http://127.0.0.1:11434") == 1)

        print("\n── 5. Welches Modell vorgeschlagen wird ──")
        probe("ein Bildmodell wird nicht vorgeschlagen",
              local_ai.usable_model(["llava-llama3:latest", "qwen2.5:7b-instruct"])
              == "qwen2.5:7b-instruct")
        probe("ein Einbettungsmodell wird nicht vorgeschlagen",
              local_ai.usable_model(["nomic-embed-text:latest", "llama3.1:8b"])
              == "llama3.1:8b")
        probe("gibt es nur Untaugliches, wird nichts vorgeschlagen",
              local_ai.usable_model(["moondream:latest"]) == "")
        probe("die Vorliebe gilt", local_ai.usable_model(
              ["mistral:7b", "qwen2.5:7b-instruct"]) == "qwen2.5:7b-instruct")

        # Anmelden, um an den Einrichter zu kommen.
        c.post("/setup/admin", data={"username": "probe", "password": "probe-probe-1",
                                     "password2": "probe-probe-1",
                                     "display_name": "Probe"},
               follow_redirects=True)
        # 🔴 JETZT erst beweist das etwas. Vor der Einrichtung antwortet jeder
        # Pfad mit 503 („noch kein Administrator") — das haette auch ein
        # sperrangelweit offener Endpunkt getan. Geprueft wird mit einem
        # ZWEITEN Browser ohne Sitzungskeks.
        print("\n── 6a. Ohne Anmeldung kommt man nicht an die Suche ──")
        fremd = TestClient(app, base_url="http://probe.local")
        for pfad in ("/api/local-ai/probe", "/api/local-ai/installer?os=mac",
                     "/api/local-ai/apply"):
            m = fremd.post if pfad.endswith("apply") else fremd.get
            r = m(pfad, headers={"Accept": "application/json"},
                  **({"json": {}} if pfad.endswith("apply") else {}))
            probe("ohne Sitzung abgewiesen: %s" % pfad.split("?")[0],
                  r.status_code == 401, "HTTP %d" % r.status_code)
        r = fremd.get("/api/local-ai/setup-script")
        probe("das Einrichter-Skript bleibt ohne Sitzung erreichbar",
              r.status_code == 200, "HTTP %d — der Einrichter hat keine Sitzung"
              % r.status_code)

        # 🔴 Der wichtigste Nachweis vor jedem Ausrollen: die Seite muss sich
        # ZEICHNEN lassen. Ein Jinja-Fehler in der Karte bricht die ganzen
        # Einstellungen — und zwar erst im Browser, nie beim Importieren.
        print("\n── 6b. Die Einstellungsseite zeichnet sich ──")
        for sprache in ("en", "de", "fr", "es", "it"):
            c.post("/api/language/%s" % sprache)
            r = c.get("/settings")
            ok = r.status_code == 200
            probe("Einstellungen (%s) rendern" % sprache, ok,
                  "HTTP %d" % r.status_code)
            if ok:
                probe("Einstellungen (%s): die Karte ist da" % sprache,
                      'x-data="localAI()"' in r.text and "/api/local-ai/installer" in r.text)
                karte = r.text[r.text.index('x-data="localAI()"'):
                               r.text.index("Local AI Bridge")]
                karte = _re2.sub(r"tr\(\s*'[\w.]+'", "tr(", karte)   # Aufrufe sind keine Anzeige
                probe("Einstellungen (%s): kein roher Schluessel" % sprache,
                      "settings.local_ai." not in karte,
                      "ein unuebersetzter Schluessel stuende sonst auf der Karte")
        c.post("/api/language/en")

        print("\n── 6c. Der erzeugte Launcher ──")
        for system, endung in (("mac", ".command"), ("linux", ".sh")):
            r = c.get("/api/local-ai/installer?os=%s" % system)
            probe("%s: Launcher kommt" % system, r.status_code == 200,
                  "HTTP %d" % r.status_code)
            if r.status_code != 200:
                continue
            probe("%s: wird nicht zwischengespeichert" % system,
                  "no-store" in r.headers.get("cache-control", ""))
            z = zipfile.ZipFile(io.BytesIO(r.content))
            eintrag = z.infolist()[0]
            probe("%s: im ZIP, mit Ausfuehrungsrecht" % system,
                  eintrag.filename.endswith(endung)
                  and (eintrag.external_attr >> 16) & 0o111,
                  "ein Browser wirft das Recht sonst weg")
            text = z.read(eintrag).decode()
            probe("%s: raeumt hinter sich auf" % system, "trap 'rm -rf" in text)
            probe("%s: traegt ein Ticket" % system, "--ticket " in text)
            probe("%s: prueft das Zertifikat ZUERST" % system,
                  'curl -fsSL "' in text and "curl -fsSLk" in text,
                  "erst richtig, dann laut zurueckfallen")

        r = c.get("/api/local-ai/installer?os=windows")
        probe("windows: Launcher kommt", r.status_code == 200, "HTTP %d" % r.status_code)
        if r.status_code == 200:
            bat = r.content.decode()
            # 🔴 Der Fund aus der Postwache: `%%TEMP%%` steht in einer .bat
            # WOERTLICH da. Nur in der erzeugten Datei zu sehen.
            probe("windows: EIN Prozentzeichen", "%%" not in bat,
                  "%%TEMP%% waere woertlich" if "%%" in bat else "")
            probe("windows: CRLF", "\r\n" in bat)
            probe("windows: raeumt hinter sich auf", "del " in bat)
            probe("windows: traegt ein Ticket", "--ticket " in bat)

        r = c.get("/api/local-ai/installer?os=amiga")
        probe("unbekanntes System wird abgewiesen", r.status_code == 400,
              "HTTP %d" % r.status_code)

        print("\n── 7. Der Einrichter selbst wird ausgeliefert ──")
        r = c.get("/api/local-ai/setup-script")
        probe("Einrichter-Skript kommt", r.status_code == 200 and
              r.text.lstrip().startswith("#!"), "HTTP %d" % r.status_code)
        probe("und es ist Python, das laeuft",
              "def main()" in r.text and "--docusort" in r.text)

        print("\n── 8. „Gespeichert“ ist nicht „funktioniert“ ──")
        # Eine Adresse, an der garantiert nichts antwortet. Der Eintrag muss
        # trotzdem geschrieben werden — und ehrlich als NICHT geprueft gelten.
        t = local_ai.new_setup_ticket()
        r = c.post("/api/local-ai/adopt",
                   json={"ticket": t, "url": "http://127.0.0.1:1", "model": "x"})
        d = r.json() if r.status_code == 200 else {}
        probe("Uebernahme geht durch", r.status_code == 200, "HTTP %d" % r.status_code)
        probe("gespeichert", bool(d.get("ok")))
        probe("aber NICHT als geprueft gemeldet", d.get("verified") is False,
              "sonst waere jede kaputte Adresse gruen")
        probe("mit Begruendung", bool(d.get("answer")))
        probe("Neustart wird angesagt", d.get("restart_required") is True)
        probe("die Adresse bekommt ihr /v1",
              str(d.get("base_url", "")).endswith("/v1"))

        print("\n── 8a. Keine Uebersetzung in einer JS-Zeichenkette ──")
        # 🔴 `x-text="'{{ t('…') }}'"` sieht harmlos aus und zerreisst das
        # Skript in dem Moment, in dem eine Sprache ein Apostroph enthaelt —
        # „Pas encore d'Ollama". Nur in DIESER Sprache, nur im Browser.
        import re as _re
        html = (HIER / "docusort" / "web" / "templates" / "settings.html").read_text(
            encoding="utf-8")
        block = html[html.index('x-data="localAI()"'):html.index("Local AI Bridge")]
        roh = _re.findall(r"'\{\{[^}]*\}\}'", block)
        probe("keine Uebersetzung in einer JS-Zeichenkette", not roh,
              ", ".join(roh[:2]) if roh else "tr() statt Jinja im Skript")

        print("\n── 9. Alle Sprachen kennen die neuen Texte ──")
        from docusort.i18n import all_translations_for_js
        basis = all_translations_for_js("en")
        neu = [k for k in basis if k.startswith("settings.local_ai.")]
        probe("die Texte gibt es", len(neu) >= 14, "%d Schluessel" % len(neu))
        for code in ("de", "es", "fr", "it"):
            d = json.loads((HIER / "docusort" / "locales" / ("%s.json" % code))
                           .read_text(encoding="utf-8"))
            fehlt = [k for k in neu if k not in d]
            probe("%s vollstaendig" % code, not fehlt, ", ".join(fehlt[:3]))
            schief = [k for k in neu if k in d
                      and set(_platzhalter(basis[k])) != set(_platzhalter(d[k]))]
            probe("%s behaelt die Platzhalter" % code, not schief,
                  ", ".join(schief[:3]))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print("\n%s  %d Fehlschlaege" % ("ALLES GRUEN" if not fehler else "ROT", len(fehler)))
    return 1 if fehler else 0


def _platzhalter(s: str) -> list[str]:
    import re
    return re.findall(r"\{(\w+)\}", s or "")


if __name__ == "__main__":
    sys.exit(main())
