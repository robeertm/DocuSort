# -*- coding: utf-8 -*-
"""Der lokale Verstand im Budgetplaner — Vorschlaege, keine Zahlen.

🔴 DIE REGEL, DIE DIESES MODUL TRAEGT: **die KI rechnet nicht.**

Ein Sprachmodell, das Betraege addiert, addiert irgendwann falsch — und ein
Budget, dessen Summen aus einem Modell stammen, ist wertlos, weil niemand
sie nachrechnet. Darum bekommt das Modell die gemessenen Zahlen GESCHENKT
und darf nur zwei Dinge beisteuern, die eine Formel nicht kann:

  1. **Wo** sich etwas holen laesst, in einer sinnvollen Reihenfolge.
  2. **Wie** — ein Satz, der eine konkrete Handlung nennt, keine Floskel.

Jeder Betrag, den es nennt, wird hier gegen den gemessenen Spielraum des
Topfes gekappt (`0 .. soll-boden`). Ein Vorschlag „spare 900 EUR bei den
Lebensmitteln", wo nur 258 EUR Spielraum sind, wird zu 258 EUR — und die
Summen bildet am Ende `budget.rechnen()`, nicht das Modell.

🔑 Und der Planer funktioniert OHNE dieses Modul vollstaendig. Was hier
   entsteht, kommt zu den gemessenen Vorschlaegen DAZU; es ersetzt sie nie.
   Eine Installation ohne Modell ist kein halber Budgetplaner.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from ..providers import Provider, ProviderError

logger = logging.getLogger("docusort.finance.budget_ai")

SYSTEM_PROMPT = """Du bist ein nüchterner Haushaltsberater. Du bekommst die GEMESSENEN Monatszahlen eines Haushalts und ein Sparziel. Du antwortest mit GENAU EINEM JSON-Array, ohne Prosa, ohne Markdown-Zäune.

Format:
[{"n": 1, "sparen": 120, "wie": "...", "aufwand": "klein"}, ...]
- Ein Objekt je Topf, den du anfassen willst. Nicht jeder Topf muss vorkommen.
- "n" ist die Nummer des Topfes aus der Liste.
- "sparen" ist eine ganze Zahl in Euro pro Monat. Sie darf NIE größer sein als der angegebene Spielraum des Topfes.
- "wie" ist EIN kurzer Satz mit einer konkreten Handlung ("Wocheneinkauf auf eine Fahrt bündeln und mit Liste", "zweite Hausratversicherung kündigen"). Keine Allgemeinplätze wie "weniger ausgeben", kein "könnte man prüfen".
- "aufwand" ist "klein", "mittel" oder "gross": wie sehr es den Alltag ändert.

Regeln:
- Fang bei den Töpfen an, die viel Spielraum und kleinen Aufwand haben.
- Sei realistisch: ein Haushalt hält keine Halbierung durch. Mehr als ein Drittel eines Topfes ist selten haltbar, außer es ist ein einzelner kündbarer Vertrag.
- Töpfe, die als "gebunden" markiert sind, ändert man nur mit Kündigung, Wechsel oder Umzug — sag das dann im Satz.
- Grundbedarf (Wohnen, Strom, Versicherungspflicht, Kredit) nicht auf null. Lebensmittel nie unter die Hälfte.
- Erfinde keine Zahlen, die nicht in der Liste stehen. Rechne keine Summen aus — das macht das Programm.
- Antworte NUR mit dem JSON-Array."""

AUFWAND = ("klein", "mittel", "gross")


def _zeile(nr: int, topf: dict[str, Any], label: str) -> str:
    spielraum = max(0.0, float(topf.get("soll") or 0) - float(topf.get("boden") or 0))
    teile = [
        "%d. %s — heute %.0f EUR/Monat" % (nr, label, topf.get("soll") or 0),
        "Spielraum bis %.0f EUR" % spielraum,
        "Art: %s" % ("gebunden (Vertrag)" if topf.get("art") == "gebunden" else "frei steuerbar"),
    ]
    if topf.get("vertrag"):
        teile.append("davon Verträge %.0f EUR" % topf["vertrag"])
    gelebt = [p["summe"] for p in topf.get("reihe", []) if p.get("summe")]
    if gelebt:
        teile.append("günstigster Monat %.0f EUR" % min(gelebt))
    namen = [p.get("name") for p in (topf.get("posten") or [])[:3] if p.get("name")]
    if namen:
        teile.append("Posten: " + ", ".join(n[:28] for n in namen))
    return " · ".join(teile)


def vorschlagen(provider: Provider, model: str, lg: dict[str, Any],
                rechnung: dict[str, Any], *, labels: dict[str, str] | None = None,
                ziel_text: str = "") -> list[dict[str, Any]]:
    """Fragt das lokale Modell nach Sparvorschlaegen. Gibt eine (moeglicherweise
    leere) Liste zurueck — nie eine Ausnahme nach aussen."""
    labels = labels or {}
    toepfe = [t for t in lg.get("toepfe", [])
              if t.get("art") in ("gebunden", "steuerbar")
              and float(t.get("soll") or 0) - float(t.get("boden") or 0) >= 10.0]
    toepfe = sorted(toepfe, key=lambda t: -(t["soll"] - t["boden"]))[:18]
    if not toepfe:
        return []

    zeilen = [_zeile(i + 1, t, labels.get(t["kategorie"], t["kategorie"]))
              for i, t in enumerate(toepfe)]
    frage = (
        "Einnahmen im gewöhnlichen Monat: %.0f EUR (Median, nur regelmäßige Einkünfte).\n"
        "Laufende Ausgaben: %.0f EUR. Heute bleiben übrig: %.0f EUR.\n"
        "%s\n"
        "Gesucht sind %.0f EUR mehr pro Monat.\n\n"
        "Die Töpfe:\n%s"
        % (lg["einnahmen"]["median"], lg["summen"]["laufend"],
           rechnung.get("ueberschuss_heute") or 0.0,
           (ziel_text.strip() + "\n") if ziel_text.strip() else "",
           max(0.0, float(rechnung.get("luecke") or 0.0)),
           "\n".join(zeilen))
    )

    try:
        resp = provider.classify(
            system_prompt=SYSTEM_PROMPT, user_prompt=frage, model=model,
            # 🔴 Ein Satz je Topf bei bis zu 18 Toepfen — mit dem
            #    Vorgabe-Budget (600) bricht die Antwort mitten im JSON ab,
            #    und abgeschnittenes JSON ist kein halber Vorschlag, sondern
            #    gar keiner.
            max_output_tokens=2000,
        )
    except ProviderError as exc:
        logger.warning("Budget-KI: %s", exc)
        raise
    return _lesen(resp.raw_text, toepfe)


def _lesen(antwort: str, toepfe: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Die Antwort defensiv auspacken und JEDEN Betrag kappen.

    🔴 Hier entscheidet sich, ob ein Modellfehler ein Anzeigefehler bleibt
       oder zu einer falschen Zahl im Plan wird. Alles, was nicht eindeutig
       zu einem gemessenen Topf gehoert, faellt weg.
    """
    text = (antwort or "").strip()
    if "```" in text:
        teile = text.split("```")
        text = max(teile, key=len)
        if text.lstrip().lower().startswith("json"):
            text = text.lstrip()[4:]
    anfang, ende = text.find("["), text.rfind("]")
    if anfang < 0 or ende <= anfang:
        logger.warning("Budget-KI: keine Liste in der Antwort (%d Zeichen)", len(text))
        return []
    try:
        roh = json.loads(text[anfang:ende + 1])
    except ValueError as exc:
        logger.warning("Budget-KI: Antwort nicht lesbar: %s", exc)
        return []
    if not isinstance(roh, list):
        return []

    raus: list[dict[str, Any]] = []
    gesehen: set[str] = set()
    for eintrag in roh:
        if not isinstance(eintrag, dict):
            continue
        try:
            nr = int(eintrag.get("n"))
        except (TypeError, ValueError):
            continue
        if not (1 <= nr <= len(toepfe)):
            continue
        topf = toepfe[nr - 1]
        if topf["kategorie"] in gesehen:
            continue
        try:
            betrag = float(eintrag.get("sparen") or 0.0)
        except (TypeError, ValueError):
            continue
        spielraum = round(max(0.0, float(topf["soll"]) - float(topf["boden"])), 2)
        gekappt = betrag > spielraum
        betrag = round(min(max(0.0, betrag), spielraum), 2)
        if betrag < 1.0:
            continue
        wie = str(eintrag.get("wie") or "").strip()[:220]
        aufwand = str(eintrag.get("aufwand") or "").strip().lower()
        gesehen.add(topf["kategorie"])
        raus.append({
            "quelle": "ki", "art": "idee", "kategorie": topf["kategorie"],
            "spart": betrag, "ziel_betrag": round(topf["soll"] - betrag, 2),
            "wie": wie, "aufwand": aufwand if aufwand in AUFWAND else "mittel",
            "gekappt": gekappt, "spielraum": spielraum,
        })
    raus.sort(key=lambda v: -v["spart"])
    return raus
