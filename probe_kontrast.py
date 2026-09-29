#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prueft, ob die Schrift im hellen Modus auf ihrem Grund ueberhaupt steht.

    python3 probe_kontrast.py

Liest nur Dateien, startet nichts und braucht keine fremden Pakete.

🔴 Der Anlass (29.09.2026): auf /settings stand ein Hinweis in 1,06:1 — hell
auf weiss, schlicht nicht zu sehen. Die Ursache war nicht die Farbe, sondern
die GESTALT ihres Namens. Der helle Modus dreht die Pastelltoene mit einer
Liste von KLASSENNAMEN auf ihr dunkles Geschwister:

    html[data-theme="light"] :is(.text-cyan-100, .text-cyan-200, …)

Tailwind schreibt aber fuer jede Gestalt derselben Farbe einen ANDEREN Namen:

    text-cyan-100            wird getroffen
    text-cyan-100/90         wird NICHT getroffen  (Deckungszusatz)
    hover:text-emerald-300   wird NICHT getroffen  (Zustand)

25 Stellen mit Deckungszusatz und 55 mit Zustand fielen so durch das Raster.
Darum vergleicht diese Probe BEIDE Seiten: was die Vorlagen benutzen, und was
das Stilblatt im hellen Modus wirklich abdeckt.

🔑 Und sie prueft das GEBAUTE Blatt mit: eine Regel, die nur in `input.css`
steht, wirkt auf keiner Seite — nach jeder Aenderung muss
`scripts/build/build-css.sh` gelaufen sein.
"""
import io
import os
import re
import sys

HIER = os.path.dirname(os.path.abspath(__file__))
VORLAGEN = os.path.join(HIER, "docusort", "web", "templates")
QUELLE = os.path.join(HIER, "scripts", "build", "input.css")
GEBAUT = os.path.join(HIER, "docusort", "web", "static", "tailwind.css")

F = []


def pruefe(name, ist, soll=True, hinweis=""):
    F.append((name, ist, soll))
    print(("  OK   " if ist == soll else "  FEHL ") + name
          + (("   — " + hinweis) if hinweis else "")
          + ("" if ist == soll else "   ist=%r soll=%r" % (ist, soll)))


def lies(pfad):
    return io.open(pfad, encoding="utf-8").read()


CSS = lies(QUELLE)
HELL = [z for z in CSS.split("\n") if 'data-theme="light"' in z and "color:" in z]

# Jede Vorlage in einem Rutsch — die Klassen stehen ueber alle Seiten verteilt.
ALLES = ""
for name in sorted(os.listdir(VORLAGEN)):
    if name.endswith(".html"):
        ALLES += lies(os.path.join(VORLAGEN, name)) + "\n"

# Gestalt einer Pastell-Klasse: [Zustand:]text-<Familie>-<Stufe>[/<Deckung>]
MUSTER = re.compile(
    r"(?:(?P<zustand>hover|group-hover|focus|sm|md|lg|xl):)?"
    r"text-(?P<familie>emerald|rose|amber|indigo|sky|cyan|violet)"
    r"-(?P<stufe>[0-9]{3})(?:/(?P<deckung>[0-9]{1,3}))?")

print("\n── 1. Jede benutzte Gestalt hat eine Regel fuer den hellen Modus ───")
benutzt = {}
for m in MUSTER.finditer(ALLES):
    benutzt.setdefault(m.group(0), 0)
    benutzt[m.group(0)] += 1

ohne = []
for klasse, anzahl in sorted(benutzt.items()):
    m = MUSTER.fullmatch(klasse)
    zustand, familie, stufe = m.group("zustand"), m.group("familie"), m.group("stufe")
    kern = "text-%s-%s" % (familie, stufe)
    if zustand in ("hover", "group-hover", "focus"):
        # Ein Zustand braucht eine Regel, die den Zustand MEINT.
        gesucht = '[class*="%s:%s"]' % (zustand, kern)
    elif m.group("deckung"):
        gesucht = '[class*="%s/"]' % kern
    else:
        gesucht = "." + kern
    if not any(gesucht in z for z in HELL):
        ohne.append((klasse, anzahl, gesucht))

pruefe("jede Pastell-Gestalt aus den Vorlagen ist im hellen Modus abgedeckt",
       not ohne,
       hinweis="%d Gestalten benutzt" % len(benutzt))
for klasse, anzahl, gesucht in ohne:
    print("       FEHLT: %-28s (%dx)  — es braucht %s" % (klasse, anzahl, gesucht))

# 🔴 Gegenprobe: die Regeln muessen AUCH die drei Gestalten treffen, die den
#    Fehler ausgeloest haben. Sonst koennte die Liste oben leer sein, weil
#    gerade keine Vorlage so eine Klasse benutzt — und der Schutz waere weg,
#    sobald die naechste kommt.
for gestalt, gesucht in (
        ("Deckungszusatz", '[class*="text-cyan-100/"]'),
        ("Zustand hover",  '[class*="hover:text-emerald-300"]'),
        ("Zustand group-hover", '[class*="group-hover:text-emerald-300"]')):
    pruefe("Regel fuer die Gestalt „%s“ ist vorhanden" % gestalt,
           any(gesucht in z for z in HELL))

print("\n── 2. Das gebaute Blatt ist auf dem Stand ──────────────────────────")
B = lies(GEBAUT)
# 🔑 Nicht das DATUM vergleichen, sondern den INHALT. Ein Zeitstempel wird rot,
#    sobald jemand die Quelle nur anfasst — und ein Pruefer, der grundlos rot
#    wird, wird abgeschaltet. (Gemessen: das Werkzeug schreibt das Blatt gar
#    nicht neu, wenn dasselbe herauskaeme.) Die Frage ist ohnehin eine andere:
#    steht die Regel auch dort, wo der Browser sie liest?
teile = sorted(set(re.findall(r'\[class\*="[^"]+"\]', "\n".join(HELL))))
fehlen = [t for t in teile if t.replace(" ", "") not in B.replace(" ", "")]
pruefe("jede Gestalt-Regel aus der Quelle steht auch im gebauten Blatt",
       not fehlen,
       hinweis="%d Regelteile geprueft" % len(teile))
for t in fehlen[:6]:
    print("       FEHLT im gebauten Blatt:", t)

print("\n── 3. Keine Schrift in einem Ton, der auf Papier verschwindet ──────")
# 🔴 Die Palette wird im hellen Modus gespiegelt, darum ist `ink-600` dort
#    #94a3b8 — 2,56:1 auf Weiss. Und es laesst sich nicht ueber den Farbwert
#    heilen: waere ink-600 dunkel genug, stuende es vor ink-500 und die
#    Blassstufen waeren verdreht. Also ist die blasseste Stufe fuer TEXT
#    ink-500 (4,76:1 auf Weiss). Rahmen und Flaechen duerfen ink-600 bleiben.
text600 = re.findall(r"(?<![-\w])text-ink-600\b", ALLES)
pruefe("keine Vorlage faerbt Text mit text-ink-600",
       not text600,
       hinweis="%d Fundstelle(n)" % len(text600))
pruefe("Rahmen und Flaechen in ink-600 sind unberuehrt geblieben",
       "border-ink-600" in ALLES,
       hinweis="die Haarlinien sollen bleiben, wie sie waren")

schlecht = [n for n, i, s in F if i != s]
print("\n%s  %d Proben, %d Fehlschlaege"
      % ("🔴 ROT" if schlecht else "GRUEN", len(F), len(schlecht)))
for n in schlecht:
    print("   FEHL:", n)
sys.exit(1 if schlecht else 0)
