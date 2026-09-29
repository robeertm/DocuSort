#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prueft die Ordnung im Einstellmenu — und den Tailscale-Weg in den Container.

    python3 probe_einstellungen.py

Liest nur Dateien, startet nichts und fasst keine Installation an.

🔴 Worauf er achtet:

  · **Eine Frage, eine Karte.** Bis 0.62.0 gab es DREI Tueren zu einem lokalen
    Modell: das Auswahlfeld „KI-Anbieter" (mit `openai_compat` und der Adresse
    `localhost:11434`), eine eigene Karte „Ein lokales Modell, in einem Klick"
    und eine eigene Karte „Lokale KI-Bruecke" — die zusaetzlich noch einmal im
    selben Auswahlfeld stand. Wer eine davon benutzte, sah die anderen
    trotzdem weiter. Robert, 29.09.2026: „bei docusort einstellungen gibt es
    zweimal die moeglichkeit die lokale ki zu installieren, raeume das
    einstellmenu von docusort ordentlich auf".

  · **`ports: !reset []`, nicht `ports: []`.** Compose FUEHRT Listen ZUSAMMEN.
    Mit der leeren Liste bleibt der veroeffentlichte Port aus der Hauptdatei
    stehen — und ein Container, der einen Port veroeffentlicht UND im Netz
    eines anderen laeuft, wird von Docker beim Start abgelehnt.
    `docker compose config` nennt die kaputte Fassung **gueltig**; gefunden
    wurde es erst, als die zusammengerechnete Datei wirklich erzeugt wurde.
"""
import io
import json
import os
import re
import sys

HIER = os.path.dirname(os.path.abspath(__file__))
F = []


def pruefe(name, ist, soll=True, hinweis=""):
    F.append((name, ist, soll))
    print(("  OK   " if ist == soll else "  FEHL ") + name
          + (("   — " + hinweis) if hinweis else "")
          + ("" if ist == soll else "   ist=%r soll=%r" % (ist, soll)))


def lies(*teile):
    return io.open(os.path.join(HIER, *teile), encoding="utf-8").read()


print("\n── 1. Eine Frage, eine Karte ───────────────────────────────────────")
E = lies("docusort", "web", "templates", "settings.html")

karten = re.findall(r'<section class="card p-6"[^>]*>\s*(?:<!--.*?-->\s*)?'
                    r'(?:<div[^>]*>\s*)?<h2[^>]*>(.*?)</h2>', E, re.S)
pruefe("es gibt genau eine Karte mit dem KI-Anbieter",
       sum(1 for k in karten if "settings.ai.heading" in k) == 1,
       hinweis="%d Karten insgesamt" % len(karten))

# 🔴 Die beiden lokalen Wege duerfen nicht mehr als EIGENE Karten dastehen.
pruefe("‚Ein lokales Modell‘ ist keine eigene Karte mehr",
       '<section class="card p-6" x-data="localAI()"' not in E)
pruefe("die Bruecke ist keine eigene Karte mehr",
       '<section class="card p-6" id="bridge"' not in E)

# … sondern haengen an der gewaehlten Antwort.
pruefe("der lokale Helfer haengt am Anbieter „openai_compat“",
       "x-if=\"ai.provider === 'openai_compat'\"" in E)
pruefe("die Bruecke haengt am Anbieter „bridge“",
       "x-if=\"ai.provider === 'bridge'\"" in E)

# Und beide sind noch DA — aufgeraeumt heisst nicht weggeworfen.
pruefe("der lokale Helfer ist weiterhin vorhanden", 'x-data="localAI()"' in E)
pruefe("die Bruecke ist weiterhin vorhanden", 'x-data="bridgeSettings()"' in E)
pruefe("die Installationsdateien sind weiterhin erreichbar",
       "/api/local-ai/installer?os=mac" in E
       and "/api/local-ai/installer?os=windows" in E
       and "/api/local-ai/installer?os=linux" in E)

print("\n── 2. Der Tailscale-Weg in den Container ───────────────────────────")
ts_pfad = os.path.join(HIER, "docker-compose.tailscale.yml")
serve_pfad = os.path.join(HIER, "tailscale", "serve.json")
pruefe("es gibt eine Tailscale-Ueberlagerung", os.path.isfile(ts_pfad))
pruefe("und die Serve-Beschreibung daneben", os.path.isfile(serve_pfad))

if os.path.isfile(ts_pfad):
    TS = lies("docker-compose.tailscale.yml")
    pruefe("der Port wird mit !reset entfernt, nicht mit einer leeren Liste",
           "ports: !reset []" in TS)
    pruefe("die App laeuft im Netz des Tailscale-Dienstes",
           "network_mode: service:tailscale" in TS)
    # 🔴 Ohne Schluessel soll der Start ABBRECHEN, statt still ohne Netz zu
    #    kommen — dann waere DocuSort naemlich nirgends erreichbar und niemand
    #    wuesste warum.
    pruefe("ohne TS_AUTHKEY bricht der Start ab", "${TS_AUTHKEY:?" in TS)
    pruefe("der Zustand liegt in einem eigenen Band",
           "./tailscale/state:/var/lib/tailscale" in TS)
    pruefe("die Hauptdatei bleibt unangetastet benutzbar",
           "docker-compose.yml -f docker-compose.tailscale.yml" in TS)

if os.path.isfile(serve_pfad):
    S = json.loads(lies("tailscale", "serve.json"))
    ziel = S["Web"]["${TS_CERT_DOMAIN}:443"]["Handlers"]["/"]["Proxy"]
    haupt = lies("docker-compose.yml")
    # 🔑 Der Port in serve.json muss der sein, auf dem DocuSort wirklich hoert —
    #    sonst steht die Adresse da und dahinter ist nichts.
    pruefe("serve.json zeigt auf den Port, auf dem DocuSort hoert",
           ziel.endswith(":8080") and '"8080:8080"' in haupt, hinweis=ziel)
    pruefe("Tailscale nimmt HTTPS auf 443 an",
           S.get("TCP", {}).get("443", {}).get("HTTPS") is True)

# 🔴 Der Zustandsordner traegt die Kennung dieser Maschine — nie ins Repo.
gi = lies(".gitignore") if os.path.isfile(os.path.join(HIER, ".gitignore")) else ""
pruefe("der Tailscale-Zustand ist vom Repo ausgeschlossen",
       "tailscale/state/" in gi)

schlecht = [n for n, i, s in F if i != s]
print("\n%s  %d Proben, %d Fehlschlaege"
      % ("🔴 ROT" if schlecht else "GRUEN", len(F), len(schlecht)))
for n in schlecht:
    print("   FEHL:", n)
sys.exit(1 if schlecht else 0)
