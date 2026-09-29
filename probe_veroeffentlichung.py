#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prueft, ob diese Version bei den Leuten auch ANKOMMEN kann.

    python3 probe_veroeffentlichung.py

🔴 Warum es diesen Pruefstand gibt (29.09.2026). 0.63.0 war gebaut, getestet
   und auf `main` gepusht — das Abbild in GHCR war aktuell, denn der Workflow
   haengt am Push auf `main`. Trotzdem fragte Robert: „warum ist mein docusort
   nicht aktuell?"

   Der Grund: seine Installation ist eine QUELL-Installation, und der
   eingebaute Aktualisierer fragt GitHub nach `/releases/latest`. Das neueste
   Release war `v0.62.0` — Tag und Release sind HANDARBEIT und blieben liegen.
   Seine Instanz verglich 0.62.0 mit 0.62.0, meldete wahrheitsgemaess
   „aktuell" und konnte 0.63.0 gar nicht sehen. Ein Mangel, den die
   Oberflaeche als Erfolg meldet — die schlimmste Sorte.

   „Veroeffentlicht" besteht darum aus ZWEI Haelften, und nur eine lief von
   selbst:
     · Commit auf `main`  → baut das Abbild        (Workflow, automatisch)
     · Tag + Release      → fuettert den Aktualisierer  (Handgriff — hier)

🔑 Zwei Regeln, die dieser Pruefstand selbst gelernt hat:

   · **Er fragt GitHub, nicht die eigene Kopie.** Weder `git fetch` noch
     `git ls-remote`: beide gehen ueber SSH und HAENGEN in einem abgefangenen
     Unterprozess (25 s Zeitueberschreitung), waehrend sie auf der
     Kommandozeile in 1,4 s durchlaufen. Ueber die API gefragt, misst der
     Pruefstand ausserdem genau die Quelle, aus der auch der Aktualisierer
     draussen liest — und laeuft bei jedem, der das Repo ohne SSH-Schluessel
     klont.

   · **Nicht fragen koennen ist ROT, nicht gruen.** Ein Pruefer, der bei
     fehlender Verbindung „in Ordnung" meldet, prueft nichts.
"""
import base64
import io
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

HIER = os.path.dirname(os.path.abspath(__file__))
REPO = "robeertm/DocuSort"
BESITZER = REPO.split("/")[0]
F = []


def pruefe(name, ist, soll=True, hinweis=""):
    F.append((name, ist, soll))
    print(("  OK   " if ist == soll else "  FEHL ") + name
          + (("   — " + hinweis) if hinweis else ""))


def lies(*teile):
    return io.open(os.path.join(HIER, *teile), encoding="utf-8").read()


def _gh_marke():
    """Die Anmeldemarke von `gh`, falls vorhanden.

    🔴 Die GHCR-Paketliste antwortet unangemeldet mit 401. Ohne Marke waere
    die Abbild-Probe dauerhaft rot — und eine Probe, die immer rot ist, wird
    abgeschaltet und bewacht dann nichts mehr.
    """
    try:
        return subprocess.run(("gh", "auth", "token"), capture_output=True,
                              text=True, timeout=15).stdout.strip()
    except Exception:
        return ""


MARKE = (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
         or _gh_marke())


def github(pfad):
    """Fragt die GitHub-API. Gibt (daten, fehler) zurueck — nie eine Ausnahme."""
    kopf = {"Accept": "application/vnd.github+json",
            "User-Agent": "docusort-probe-veroeffentlichung"}
    if MARKE:
        kopf["Authorization"] = "Bearer " + MARKE
    try:
        r = urllib.request.Request("https://api.github.com" + pfad, headers=kopf)
        with urllib.request.urlopen(r, timeout=25) as a:
            return json.load(a), ""
    except urllib.error.HTTPError as e:
        return None, "HTTP %d" % e.code
    except Exception as e:
        return None, str(e)[:120]


def git(*args):
    """Nur ORTLICHE Auskuenfte — nichts, was ins Netz greift (siehe Kopf)."""
    try:
        return subprocess.run(("git",) + args, cwd=HIER, capture_output=True,
                              text=True, timeout=20).stdout.strip()
    except Exception:
        return ""


# Fuer die Gegenprobe darf die Version ueberschrieben werden: nur so laesst
# sich zeigen, dass dieser Pruefstand ueberhaupt rot werden KANN.
V = os.environ.get("DOCUSORT_PRUEF_VERSION") or re.search(
    r'__version__ = "([^"]+)"', lies("docusort", "__init__.py")).group(1)
TAG = "v" + V
print("\n── Version %s: kommt sie bei den Leuten an? ─────────────────────" % V)

# ── 1. Steht sie ueberhaupt im Changelog? ───────────────────────────────────
pruefe("das Changelog kennt diese Version",
       ("## [%s]" % V) in lies("CHANGELOG.md"))

# ── 2. Ist der Stand draussen? ──────────────────────────────────────────────
pruefe("der Arbeitsbaum ist sauber", git("status", "--porcelain") == "",
       hinweis="sonst ist nicht veroeffentlicht, was hier liegt")
hier = git("rev-parse", "HEAD")
zweig, fehler = github("/repos/%s/commits/main" % REPO)
if zweig is None:
    pruefe("GitHub nach dem Stand von main fragen", False,
           hinweis="ging nicht: %s — ungeprueft ist nicht gruen" % fehler)
else:
    pruefe("HEAD ist auf GitHub angekommen", hier == zweig.get("sha"),
           hinweis="hier %s / dort %s" % (hier[:8], (zweig.get("sha") or "")[:8]))

# ── 3. Gibt es den Tag, und traegt er diese Version? ────────────────────────
ref, fehler = github("/repos/%s/git/ref/tags/%s" % (REPO, TAG))
pruefe("es gibt den Tag %s auf GitHub" % TAG, ref is not None,
       hinweis=(("zeigt auf %s" % (ref["object"]["sha"][:8])) if ref
                else "%s — ohne Tag gibt es kein Archiv zum Herunterladen"
                     % fehler))
if ref is not None:
    inhalt, fehler = github("/repos/%s/contents/docusort/__init__.py?ref=%s"
                            % (REPO, TAG))
    if inhalt is None:
        pruefe("GitHub nach dem Inhalt des Tags fragen", False,
               hinweis="ging nicht: %s" % fehler)
    else:
        text = base64.b64decode(inhalt.get("content") or "").decode(
            "utf-8", "replace")
        pruefe("im Release-Archiv steht wirklich Version %s" % V,
               ('__version__ = "%s"' % V) in text,
               hinweis="genau das packt ein Aktualisierer aus")

    # 🔴 NICHT „der Tag zeigt auf HEAD" pruefen — das laeuft schon beim
    #    naechsten Commit auf main auseinander, ohne dass etwas falsch waere.
    #    Die Frage ist, ob der Tag auf derselben Linie liegt.
    vgl, fehler = github("/repos/%s/compare/%s...main" % (REPO, TAG))
    if vgl is None:
        pruefe("GitHub nach der Lage des Tags fragen", False,
               hinweis="ging nicht: %s" % fehler)
    else:
        pruefe("der Tag liegt auf der Linie von main",
               vgl.get("status") in ("identical", "ahead", "behind"),
               hinweis="main gegenueber dem Tag: %s" % vgl.get("status"))

# ── 4. 🔴 Das Entscheidende: das NEUESTE Release muss diese Version sein ────
#    Genau das liest `updater.version_info()` — und nur das sehen die
#    Quell-Installationen draussen.
rel, fehler = github("/repos/%s/releases/latest" % REPO)
if rel is None:
    pruefe("GitHub nach dem neuesten Release fragen", False,
           hinweis="ging nicht: %s — ungeprueft ist nicht gruen" % fehler)
else:
    neuestes = (rel.get("tag_name") or "").lstrip("v")
    pruefe("das neueste Release auf GitHub ist %s" % V, neuestes == V,
           hinweis="GitHub sagt: %s (veroeffentlicht %s)"
                   % (neuestes or "—", rel.get("published_at") or "—"))
    pruefe("es ist kein Entwurf und keine Vorabfassung",
           not rel.get("draft") and not rel.get("prerelease"))
    pruefe("das Release hat einen Text",
           len((rel.get("body") or "").strip()) > 80,
           hinweis="%d Zeichen" % len((rel.get("body") or "").strip()))

# ── 5. Und das Abbild fuer die Container-Leute ──────────────────────────────
pak, fehler = github("/users/%s/packages/container/docusort/versions" % BESITZER)
if pak is None:
    pruefe("GHCR nach den Abbild-Marken fragen", False,
           hinweis="ging nicht: %s" % fehler)
else:
    marken = set()
    for eintrag in pak[:12]:
        marken.update((eintrag.get("metadata") or {}).get("container", {})
                      .get("tags") or [])
    pruefe("es gibt ein Abbild mit der Marke %s" % V, V in marken,
           hinweis="vorhanden: " + ", ".join(sorted(m for m in marken if m)[:6]))
    pruefe("und „latest“ ist mitgezogen", "latest" in marken)

# 🔴 Der Workflow darf NICHT auf Tags bauen. Er schiebt `:latest` mit, also
#    haette ein Tag auf einer aelteren Fassung jedem Kunden alten Code als
#    `latest` gegeben. Genau das ist bei der Postwache einmal passiert.
wf = lies(".github", "workflows", "docker-publish.yml")
pruefe("der Workflow baut NICHT auf Tags",
       'tags: ["v*"]' not in wf and "tags:" not in wf.split("workflow_dispatch")[0],
       hinweis="sonst schoebe ein alter Tag `:latest` zurueck")

schlecht = [n for n, i, s in F if i != s]
print("\n%s  %d Proben, %d Fehlschlaege"
      % ("🔴 ROT" if schlecht else "GRUEN", len(F), len(schlecht)))
for n in schlecht:
    print("   FEHL:", n)
if schlecht:
    print("\n🔑 Fehlt Tag oder Release, dann so nachziehen:\n"
          "   gh release create %s --target main \\\n"
          "     --title \"DocuSort %s — <Schlagzeile>\" --notes-file <text.md>\n"
          "   Der Text ist ENGLISCH (das Changelog ist deutsch, die Releases\n"
          "   sind die Kundenseite)." % (TAG, V))
sys.exit(1 if schlecht else 0)
