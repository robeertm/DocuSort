#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prueft, dass die Oberflaeche in JEDER Sprache ganz uebersetzt ist — und
vor allem, dass sie in jeder Sprache noch FUNKTIONIERT.

    python3 probe_sprachen.py

Laeuft in einem Wegwerf-Verzeichnis, fasst keine laufende Installation an
und drueckt keinen Knopf, der nach aussen wirkt.

🔴 Worauf er wirklich achtet:

  · Eine Uebersetzung, die in ein JavaScript-Zeichenkettenliteral gerendert
    wird (`x-text="'{{ t('k') }}'"`), zerreisst das Skript in genau der
    Sprache, deren Text ein Apostroph enthaelt — "Aujourd'hui", "Quest'anno",
    "l'IA". Im Deutschen faellt das NIE auf. Steht die Stelle in einem
    <script>-Block, ist die ganze Seite tot: keine einzige Funktion wird
    definiert, und die Seite sieht trotzdem fast normal aus.
  · Sichtbarer Text, der gar nicht erst durch die Uebersetzung laeuft —
    eine englische Karte mitten in einer deutschen Seite.
  · Fehlende Schluessel, Karteileichen und verlorene Platzhalter: ein
    verschwundenes {n} ist eine fehlende Zahl mitten im Satz, und das faellt
    niemandem auf, der die Sprache nicht spricht.

Der Beweis ist nicht, dass er gruen wird — sondern dass er gegen einen
kaputten Stand rot wird.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path

HIER = Path(__file__).resolve().parent
sys.path.insert(0, str(HIER))

VORLAGEN = HIER / "docusort" / "web" / "templates"
SPRACHEN = HIER / "docusort" / "locales"
CODES = ("de", "en", "fr", "es", "it")
LEIT = "en"          # die Sprache, in der die Schluessel benannt sind

fehler: list[str] = []


def probe(name: str, ok: bool, zusatz: str = "") -> None:
    print("  %s %s%s" % ("OK  " if ok else "FEHL", name,
                         ("  — " + zusatz) if zusatz else ""))
    if not ok:
        fehler.append(name)


def leeren(text: str, muster: str) -> str:
    """Loescht Inhalt, behaelt aber die Zeilenzahl — sonst zeigt jeder Befund
    auf eine unbeteiligte Zeile."""
    return re.sub(muster, lambda m: "\n" * m.group(0).count("\n"),
                  text, flags=re.S)


def tabellen() -> dict[str, dict[str, str]]:
    return {c: json.loads((SPRACHEN / f"{c}.json").read_text(encoding="utf-8"))
            for c in CODES}


def skriptzeilen(text: str) -> set[int]:
    """Zeilennummern, die innerhalb eines <script>-Blocks liegen."""
    drin: set[int] = set()
    for m in re.finditer(r"<script\b[^>]*>(.*?)</script>", text, re.S):
        a = text[:m.start(1)].count("\n") + 1
        b = text[:m.end(1)].count("\n") + 1
        drin.update(range(a, b + 1))
    return drin


# ───────────────────────── 1. der Apostroph-Riss ──────────────────────────

# Attribute, deren WERT JavaScript ist (Alpine). Ein title="{{ t('k') }}" ist
# dagegen ein gewoehnliches HTML-Attribut und voellig in Ordnung — der Browser
# dekodiert &#39; dort zurueck zum Apostroph und zeigt ihn an.
JS_ATTR = re.compile(
    r"""(?:x-(?:text|html|show|if|model|init|effect|data)|@[\w.:-]+|:[\w.:-]+)"""
    r"""\s*=\s*"([^"]*)\"""", re.S)
T_AUSDRUCK = re.compile(r"\{\{\s*t\(\s*['\"]([\w.]+)['\"].*?\}\}")


def _im_literal(vorher: str) -> bool:
    """Steht die Stelle innerhalb eines noch offenen JS-Zeichenkettenliterals?"""
    einfach = len(re.findall(r"(?<!\\)'", vorher))
    doppelt = len(re.findall(r'(?<!\\)"', vorher))
    return einfach % 2 == 1 or doppelt % 2 == 1


def _stellen_in_js(text: str):
    """(Zeile, Schluessel, Art) fuer jede Uebersetzung, die in einem
    JavaScript-Literal landet.

    🔴 Die Jinja-Ausdruecke werden ZUERST maskiert. Ein Attributwert wie
        x-text="'{{ t('k') | replace(\"'\", \"x\") }} ' + y"
    enthaelt selbst Anfuehrungszeichen — mitten im Jinja-Teil. Wer die
    Attribute vorher sucht, bricht dort ab und sieht den Rest nie. Genau so
    sind dieser Probe beim ersten Anlauf zehn Stellen entgangen.
    """
    marken: list[str] = []

    def merke(m):
        marken.append(m.group(1))
        # Die Marke ist genau so lang wie das Original, damit Zeilen- und
        # Spaltenzahlen stimmen bleiben.
        return "\x01" * len(m.group(0))

    maskiert = T_AUSDRUCK.sub(merke, text)

    def schluessel_bei(rohtext, pos):
        """Der wievielte maskierte Ausdruck beginnt an dieser Stelle?"""
        return rohtext[:pos].count("\x01\x01")  # nur zur Reihenfolge

    # Reihenfolge der Marken == Reihenfolge der Fundstellen
    stellen = [m.start() for m in re.finditer(r"\x01+", maskiert)]
    zuordnung = dict(zip(stellen, marken))

    im_skript = skriptzeilen(maskiert)
    for pos, k in zuordnung.items():
        zeile = maskiert[:pos].count("\n") + 1
        zeilenanfang = maskiert.rfind("\n", 0, pos) + 1
        if zeile in im_skript:
            if _im_literal(maskiert[zeilenanfang:pos]):
                yield zeile, k, "skript"
    for m in JS_ATTR.finditer(maskiert):
        a, b = m.span(1)
        for pos, k in zuordnung.items():
            if a <= pos < b and _im_literal(maskiert[a:pos]):
                yield maskiert[:pos].count("\n") + 1, k, "attribut"


def abschnitt_literale(T) -> None:
    print("\n── 1. Uebersetzungen in JavaScript-Zeichenketten ──")
    skript, attribut = [], []
    for datei in sorted(VORLAGEN.glob("*.html")):
        text = leeren(datei.read_text(encoding="utf-8"), r"<!--.*?-->")
        for nr, k, art in _stellen_in_js(text):
            betroffen = [c for c in CODES
                         if "'" in T[c].get(k, "") or "\u2019" in T[c].get(k, "")]
            (skript if art == "skript" else attribut).append(
                (datei.name, nr, k, betroffen))

    for d, nr, k, c in [x for x in skript if x[3]]:
        print("       🔴 %s:%d  %s  — im Skript: der Benutzer liest &#39; in %s"
              % (d, nr, k, "/".join(c)))
    for d, nr, k, c in [x for x in attribut if x[3]]:
        print("       🔴 %s:%d  %s  — der Alpine-Ausdruck bricht in %s"
              % (d, nr, k, "/".join(c)))

    probe("keine Uebersetzung steht in einem JS-Literal im Skript",
          not skript, "%d Stellen" % len(skript))
    probe("keine Uebersetzung steht in einem JS-Literal in einem Attribut",
          not attribut, "%d Stellen" % len(attribut))
    # Auch die heute harmlosen sind Zeitbomben: die naechste Uebersetzung mit
    # Apostroph zuendet sie, und zwar nur in DIESER einen Sprache.
    scharf = len([x for x in skript + attribut if x[3]])
    probe("und keine davon ist heute schon scharf", scharf == 0,
          "%d scharf" % scharf)


# ─────────────────── 2. jede Seite parst in jeder Sprache ──────────────────

def abschnitt_render() -> None:
    print("\n── 2. Jede Seite in jeder Sprache — parst das Skript? ──")
    if not shutil.which("node"):
        probe("node vorhanden, um das Skript zu pruefen", False,
              "ohne node beweist dieser Abschnitt nichts")
        return
    try:
        from fastapi.testclient import TestClient
    except Exception as exc:                       # pragma: no cover
        probe("fastapi.testclient vorhanden", False, str(exc))
        return

    tmp = Path(tempfile.mkdtemp(prefix="docusort-sprachprobe-"))
    try:
        app = _app_bauen(tmp)
        c = TestClient(app, base_url="http://probe.local")
        c.post("/setup/admin",
               data={"username": "probe", "password": "probe-probe-1",
                     "password2": "probe-probe-1", "display_name": "Probe"},
               follow_redirects=True)
        seiten = ("/", "/library", "/upload", "/settings", "/analytics",
                  "/finance", "/transactions", "/fixkosten",
                  "/duplicates", "/users", "/konto", "/ausgaben")
        kaputt: list[str] = []
        # 🔴 Eine Seite, die gar nicht erst antwortet, wird NICHT still
        # uebersprungen — sonst meldet dieser Abschnitt "0 kaputt", weil er
        # nichts angesehen hat. Das waere die gefaehrlichste Sorte Gruen.
        nicht_geladen: list[str] = []
        geprueft = 0
        for code in CODES:
            c.cookies.set("lang", code)
            for pfad in seiten:
                r = c.get(pfad, follow_redirects=True)
                if r.status_code != 200:
                    nicht_geladen.append("%s %s → HTTP %d"
                                         % (code, pfad, r.status_code))
                    continue
                # Hat die Sprache ueberhaupt gegriffen? Sonst prueft der
                # Abschnitt fuenfmal dieselbe Sprache.
                if code != "de" and 'lang="%s"' % code not in r.text[:600]:
                    nicht_geladen.append("%s %s → Sprache griff nicht"
                                         % (code, pfad))
                    continue
                for i, m in enumerate(re.finditer(
                        r"<script\b(?![^>]*\bsrc=)[^>]*>(.*?)</script>",
                        r.text, re.S)):
                    js = m.group(1)
                    if not js.strip():
                        continue
                    geprueft += 1
                    d = tmp / ("s_%s_%s_%d.js"
                               % (code, pfad.strip("/") or "start", i))
                    d.write_text(js, encoding="utf-8")
                    lauf = subprocess.run(["node", "--check", str(d)],
                                          capture_output=True, text=True)
                    if lauf.returncode != 0:
                        erst = [z for z in lauf.stderr.split("\n")
                                if "Error" in z]
                        kaputt.append("%s %s: %s"
                                      % (code, pfad,
                                         erst[0][:70] if erst else "?"))
        for k in kaputt:
            print("       🔴 %s" % k)
        for n in nicht_geladen[:12]:
            print("       ?  %s" % n)
        probe("jede Seite laedt in jeder Sprache", not nicht_geladen,
              "%d nicht angesehen" % len(nicht_geladen))
        probe("jedes Skript jeder Seite parst in allen 5 Sprachen",
              not kaputt and geprueft > 0,
              "%d Skripte geprueft, %d kaputt" % (geprueft, len(kaputt)))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _app_bauen(tmp: Path):
    from docusort.config import load_config
    from docusort.db import Database
    from docusort.web.app import create_app
    conf = tmp / "config"
    conf.mkdir(parents=True, exist_ok=True)
    daten = tmp / "data"
    for sub in ("inbox", "library", "library/_Review", "library/_Processed",
                "logs"):
        (daten / sub).mkdir(parents=True, exist_ok=True)
    (conf / "config.yaml").write_text(json.dumps({
        "paths": {"inbox": str(daten / "inbox"),
                  "library": str(daten / "library"),
                  "review": str(daten / "library/_Review"),
                  "processed": str(daten / "library/_Processed"),
                  "logs": str(daten / "logs"),
                  "db": str(daten / "library/docusort.db")},
        "web": {"host": "127.0.0.1", "port": 9999, "default_language": "de"},
        "ai": {"provider": "openai_compat", "model": "qwen2.5:7b-instruct",
               "base_url": "http://127.0.0.1:11434/v1"},
        "ocr": {}, "sync": {}, "finance": {}, "notifications": {},
    }), encoding="utf-8")
    # Kategorien sind Woerterbuecher mit `name`, keine blossen Zeichenketten.
    (conf / "categories.yaml").write_text(json.dumps(
        {"categories": [{"name": "Test", "subcategories": [], "tags": []}]}),
        encoding="utf-8")
    settings = load_config(conf)
    return create_app(settings, Database(Path(settings.paths.db)))


# ──────────────── 3. sichtbarer Text, der nicht uebersetzt wird ────────────

NUR_TEXT_ATTR = ("placeholder", "title", "aria-label", "alt")
AUSDRUCK_ATTR = (":title", ":placeholder", ":aria-label", "x-text", "x-html")
STUMM = {"script", "style", "svg", "path", "circle", "rect", "defs"}
# Eigennamen und Technik, die in keiner Sprache uebersetzt werden.
EIGENNAMEN = re.compile(
    r"^(DocuSort|Anthropic Claude|OpenAI GPT|Google Gemini|Ollama|"
    r"macOS|Windows|Linux|Homebrew|winget|SHA-256|PDF|CSV|IBAN|"
    r"rossmann, dm, müller|https?://\S+)$")


class Sucher(HTMLParser):
    def __init__(self, schluessel: set[str]):
        super().__init__(convert_charrefs=True)
        self.stumm = 0
        self.funde: list[tuple[int, str]] = []
        self.schluessel = schluessel

    def handle_starttag(self, tag, attrs):
        if tag in STUMM:
            self.stumm += 1
            return
        if self.stumm:
            return
        for name, wert in attrs:
            if not wert:
                continue
            if name in NUR_TEXT_ATTR:
                self._melde(wert)
            elif name in AUSDRUCK_ATTR:
                for a, b in re.findall(r"'([^']*)'|\"([^\"]*)\"", wert):
                    self._melde(a or b)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag):
        if tag in STUMM and self.stumm:
            self.stumm -= 1

    def handle_data(self, data):
        if not self.stumm:
            self._melde(data)

    def _melde(self, roh: str) -> None:
        nr = self.getpos()[0]
        for stueck in roh.split("\n"):
            s = stueck.replace("\x00J\x00", " ").strip()
            if not s or s in self.schluessel:      # ein Schluessel ist kein Satz
                continue
            if EIGENNAMEN.match(s):
                continue
            if len(re.findall(r"[A-Za-z][A-Za-z'’-]{2,}", s)) < 2:
                continue
            self.funde.append((nr, s[:90]))


def abschnitt_roh(T) -> None:
    print("\n── 3. Sichtbarer Text, der nicht durch die Uebersetzung laeuft ──")
    schluessel = set(T[LEIT])
    gesamt = 0
    je_datei: Counter = Counter()
    for datei in sorted(VORLAGEN.glob("*.html")):
        roh = datei.read_text(encoding="utf-8")
        roh = leeren(roh, r"\{#.*?#\}")
        roh = leeren(roh, r"<!--.*?-->")
        roh = re.sub(r"\{\{.*?\}\}|\{%.*?%\}",
                     lambda m: "\x00J\x00" + "\n" * m.group(0).count("\n"),
                     roh, flags=re.S)
        p = Sucher(schluessel)
        p.feed(roh)
        for nr, s in p.funde:
            print("       %s:%d: %s" % (datei.name, nr, s))
            je_datei[datei.name] += 1
            gesamt += 1
    probe("kein roher Satz in den Vorlagen", gesamt == 0,
          "%d Stellen in %d Dateien" % (gesamt, len(je_datei)))


# ───────────── 4. rohe Saetze in den Skripten der Vorlagen ────────────────

def abschnitt_roh_js(T) -> None:
    print("\n── 4. Rohe Saetze in den Skripten ──")
    schluessel = set(T[LEIT])
    # Ein englischer Rueckfall HINTER einem T[...]-Nachschlag ist in Ordnung:
    # er greift nur, wenn der Schluessel fehlt.
    gesamt = 0
    for datei in sorted(VORLAGEN.glob("*.html")):
        text = leeren(datei.read_text(encoding="utf-8"), r"<!--.*?-->")
        zeilen = text.split("\n")
        im_skript = skriptzeilen(text)
        for nr, zeile in enumerate(zeilen, 1):
            if nr not in im_skript:
                continue
            ohne = re.sub(r"//.*$", "", zeile)
            # Jinja setzt hier Werte ein, keine Saetze.
            ohne = re.sub(r"\{\{.*?\}\}|\{%.*?%\}", " ", ohne)
            # ${...} in einem Vorlagenliteral ist Rechnung, kein Text.
            ohne = re.sub(r"\$\{[^}]*\}", " ", ohne)
            # console.log/warn/error geht an den Entwickler, nicht an den Benutzer.
            if re.search(r"\bconsole\.\w+\s*\(", ohne):
                continue
            # Zeile und Vorgaengerzeile ansehen: `T['k']\n  || 'fallback'`
            umfeld = (zeilen[nr - 2] if nr >= 2 else "") + " " + ohne
            hat_nachschlag = bool(re.search(r"T\[['\"][\w.]+['\"]\]", umfeld)
                                  or re.search(r"\btr\(", umfeld))
            for a, b in re.findall(r"'([^'\\\n]*)'|\"([^\"\\\n]*)\"", ohne):
                s = (a or b).strip()
                if not s or s in schluessel or hat_nachschlag:
                    continue
                if EIGENNAMEN.match(s) or re.match(r"^[\w./?=&:#-]+$", s):
                    continue
                # CSS-Klassenlisten und Selektoren sind kein Satz
                if re.search(r"(bg-|text-|border-|rounded|hover:|\[name=)", s):
                    continue
                if re.search(r"var\(--|^[\w-]+\s*:\s*[\w(#.]", s):
                    continue        # CSS-Angabe, kein Satz
                # CSS-Erklaerungen sind kein Satz
                if re.search(r"^[\w-]+\s*:\s*\S", s) and ";" in s + ":":
                    if not re.search(r"[.!?]\s|\s[a-z]{3,}\s[a-z]{3,}\s", s):
                        continue
                if len(re.findall(r"[A-Za-z][A-Za-z'’-]{2,}", s)) < 3:
                    continue
                print("       %s:%d: %s" % (datei.name, nr, s[:90]))
                gesamt += 1
    probe("kein roher Satz in den Skripten", gesamt == 0,
          "%d Stellen" % gesamt)


# ──────────────── 5. Vollstaendigkeit und Platzhalter ─────────────────────

def abschnitt_vollstaendig(T) -> None:
    print("\n── 5. Vollstaendigkeit der Sprachdateien ──")
    leit = set(T[LEIT])
    probe("alle Sprachen kennen dieselben Schluessel",
          all(set(T[c]) == leit for c in CODES),
          " · ".join("%s:%d" % (c, len(T[c])) for c in CODES))
    for c in CODES:
        if c == LEIT:
            continue
        fehlt = sorted(leit - set(T[c]))
        ueber = sorted(set(T[c]) - leit)
        if fehlt:
            print("       %s fehlen %d: %s" % (c, len(fehlt), fehlt[:6]))
        if ueber:
            print("       %s hat %d Karteileichen: %s"
                  % (c, len(ueber), ueber[:6]))

    ph = re.compile(r"\{(\w+)\}")
    schief = []
    for k, v in T[LEIT].items():
        soll = set(ph.findall(v))
        for c in CODES:
            if c == LEIT or k not in T[c]:
                continue
            ist = set(ph.findall(T[c][k]))
            if ist != soll:
                schief.append("%s/%s: %s statt %s"
                              % (c, k, sorted(ist) or "—", sorted(soll) or "—"))
    for s in schief[:12]:
        print("       %s" % s)
    probe("die Platzhalter haben die Uebersetzung ueberlebt", not schief,
          "%d abweichend" % len(schief))

    leer = [(c, k) for c in CODES for k, v in T[c].items() if not str(v).strip()]
    probe("kein leerer Text", not leer, "%d leer" % len(leer))


# ──────────────── 6. jeder benutzte Schluessel existiert ──────────────────

BENUTZT = re.compile(r"""(?:\bt\(|\btr\(|T\[)\s*['"]([\w.]+)['"]""")


def abschnitt_benutzt(T) -> None:
    print("\n── 6. Jeder benutzte Schluessel existiert ──")
    leit = set(T[LEIT])
    erfunden = []
    for datei in sorted(VORLAGEN.glob("*.html")):
        text = leeren(datei.read_text(encoding="utf-8"), r"<!--.*?-->")
        for nr, zeile in enumerate(text.split("\n"), 1):
            for k in BENUTZT.findall(zeile):
                if k.endswith("."):        # Praefix, zur Laufzeit ergaenzt
                    continue
                if k not in leit and "." in k:
                    erfunden.append("%s:%d %s" % (datei.name, nr, k))
    for e in erfunden[:15]:
        print("       %s" % e)
    probe("kein erfundener Schluessel in den Vorlagen", not erfunden,
          "%d erfunden" % len(erfunden))

    # …und der Blick zurueck: jeder Schluessel wird auch benutzt.
    text_alles = "\n".join(
        leeren(d.read_text(encoding="utf-8"), r"<!--.*?-->")
        for d in VORLAGEN.glob("*.html"))
    py_alles = "\n".join(
        p.read_text(encoding="utf-8")
        for p in (HIER / "docusort").rglob("*.py"))
    # Auf der Python-Seite heisst der Aufruf translate("schluessel", …) —
    # und oft wird der Schluessel nur ZURUECKGEGEBEN ("auth.err.password_short")
    # und erst beim Anzeigen uebersetzt. Deshalb zaehlt dort jede Zeichenkette,
    # die wie ein Schluessel aussieht, als Fundstelle.
    PY_BENUTZT = re.compile(r"""['"]([a-z][\w]*(?:\.[\w]+)+)['"]""")
    benutzt = (set(BENUTZT.findall(text_alles))
               | set(BENUTZT.findall(py_alles))
               | set(PY_BENUTZT.findall(py_alles)))
    # Schluesselfamilien, die zur Laufzeit zusammengesetzt werden
    dynamisch = ("cat.", "finance.cat.", "auth.role_", "hub.problems.kind.",
                 "dashboard.deadlines.kind.", "ds.stand.", "upload.stage.",
                 "doc.status.", "notif.event.")
    tot = sorted(k for k in leit
                 if k not in benutzt and not k.startswith(dynamisch))
    # Karteileichen sind Unordnung, kein Fehler — sie faerben den Pruefer
    # nicht rot, sonst steht er dauerhaft auf Rot und wird ignoriert. Aber
    # die Zahl wird genannt, damit sie nicht unbemerkt waechst.
    print("       %d Schluessel ohne Fundstelle (Karteileichen, kein Fehler)"
          % len(tot))
    if tot:
        print("       z. B. %s" % ", ".join(tot[:6]))


def main() -> int:
    T = tabellen()
    print("DocuSort — Sprachprobe")
    print("  %d Schluessel je Sprache, %d Sprachen"
          % (len(T[LEIT]), len(CODES)))
    abschnitt_literale(T)
    abschnitt_render()
    abschnitt_roh(T)
    abschnitt_roh_js(T)
    abschnitt_vollstaendig(T)
    abschnitt_benutzt(T)
    print("\n" + "─" * 62)
    if fehler:
        print("ROT — %d Befund(e):" % len(fehler))
        for f in fehler:
            print("   · %s" % f)
        return 1
    print("GRUEN — alle Proben bestanden")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
