"""Wann sind zwei Namen dasselbe, nur anders geschrieben?

Diese Frage stellt sich an zwei Stellen, und sie muss beide Male GLEICH
beantwortet werden:

* bei einer neuen Kategorie — „Versicherungen" neben „Versicherung" ist keine
  zweite Schublade, sondern ein Tippfehler (`kategorien.py`),
* bei Absendern — in einem echten Archiv standen „Ostsächsische Sparkasse
  Dresden" (271 Dokumente), „Ostsaechsische Sparkasse Dresden" (10) und „Ihre
  Ostsächsische Sparkasse Dresden" (3) nebeneinander. Wer nach der ersten
  Schreibweise filtert, bekommt 13 Dokumente seiner Hausbank nicht zu sehen.

🔑 GEMESSEN, NICHT GERATEN. `difflib` gibt eine Zahl, und die Schwelle steht
hier mit den Werten, an denen sie gewaehlt wurde. An Roberts echtem Archiv:

    Versicherungen / Versicherung             0,96   → dasselbe
    Ostsaechsische / Ostsächsische Sparkasse  0,98   → dasselbe
    Tanzschule Lax / Tanzschule LAX           1,00   → dasselbe
    Versicherung   / Vertraege                0,35   → verschieden
    Sparkassen-Vers. Leben / … Allgemeine     0,92   → 🔴 GRENZFALL

Der letzte ist der Grund, warum hier nichts automatisch zusammengelegt wird:
zwei Versicherungssparten desselben Hauses sind ZWEI Absender. Die Schwelle
findet Kandidaten — entscheiden tut ein Mensch.
"""
from __future__ import annotations

import difflib
import unicodedata
from typing import Iterable

# An den Werten oben gewaehlt: ueber 0,86 liegen alle echten Schreibvarianten,
# darunter die, die wirklich verschieden sind.
AEHNLICH_AB = 0.86


def schlicht(name: str) -> str:
    """Zum VERGLEICHEN: ohne Gross-/Kleinschreibung, ohne Akzente, ohne
    doppelte Leerzeichen.

    🔴 Nicht zum Speichern. Gespeichert wird, was dasteht — `ä` zu `a` zu
    machen ist eine Vereinfachung fuers Messen, keine Verbesserung des Namens.
    """
    ohne = unicodedata.normalize("NFKD", name or "")
    ohne = "".join(c for c in ohne if not unicodedata.combining(c))
    return " ".join(ohne.casefold().split())


def gleich(a: str, b: str) -> bool:
    """Dasselbe Wort, nur anders geschrieben (Gross/klein, Akzente, Abstand)."""
    return schlicht(a) == schlicht(b)


def naehe(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, schlicht(a), schlicht(b)).ratio()


def zu_aehnlich(name: str, vorhandene: Iterable[str],
                ab: float = AEHNLICH_AB) -> str:
    """Der vorhandene Name, der dem neuen zu nahe kommt — sonst ''."""
    bester, beste = "", 0.0
    for v in vorhandene:
        wert = naehe(name, v)
        if wert > beste:
            bester, beste = v, wert
    return bester if beste >= ab else ""


def gruppen(namen_mit_anzahl: list[tuple[str, int]],
            ab: float = AEHNLICH_AB) -> list[dict]:
    """Namen, die paarweise zu nah beieinander liegen, zu Gruppen fassen.

    `namen_mit_anzahl` ist [(name, wie oft), …]. Zurueck kommt je Gruppe:

        {"vorschlag": <der haeufigste Name>,
         "mitglieder": [{"name", "anzahl", "naehe"}, …],
         "umzuziehen": <wie viele Dokumente der Vorschlag einsammeln wuerde>}

    🔑 Verbunden, nicht nur paarweise: stehen A~B und B~C, gehoeren alle drei
    in EINE Gruppe, auch wenn A und C sich allein nicht nahe genug waeren.
    Genau so lag es im echten Archiv — die beiden Sparkassen-Schreibweisen
    fanden sich ueber die dritte.

    Der Vorschlag ist der HAEUFIGSTE Name, nicht der laengste oder der
    „richtigste": die Mehrheit der Dokumente soll liegen bleiben, denn jedes
    Umbenennen bewegt eine Datei.
    """
    namen = [(n, int(c)) for n, c in namen_mit_anzahl if (n or "").strip()]
    # Verbundene Komponenten ueber eine einfache Union-Find-Struktur.
    eltern = list(range(len(namen)))

    def wurzel(i: int) -> int:
        while eltern[i] != i:
            eltern[i] = eltern[eltern[i]]
            i = eltern[i]
        return i

    kanten: dict[tuple[int, int], float] = {}
    for i in range(len(namen)):
        for j in range(i + 1, len(namen)):
            w = naehe(namen[i][0], namen[j][0])
            if w >= ab:
                kanten[(i, j)] = w
                a, b = wurzel(i), wurzel(j)
                if a != b:
                    eltern[a] = b

    topf: dict[int, list[int]] = {}
    for i in range(len(namen)):
        topf.setdefault(wurzel(i), []).append(i)

    raus = []
    for teile in topf.values():
        if len(teile) < 2:
            continue
        teile.sort(key=lambda i: (-namen[i][1], schlicht(namen[i][0])))
        kopf = namen[teile[0]][0]
        mitglieder = [
            {"name": namen[i][0], "anzahl": namen[i][1],
             "naehe": round(naehe(kopf, namen[i][0]), 2)}
            for i in teile
        ]
        raus.append({
            "vorschlag": kopf,
            "mitglieder": mitglieder,
            "umzuziehen": sum(m["anzahl"] for m in mitglieder[1:]),
        })
    # Die groesste Wirkung zuerst.
    raus.sort(key=lambda g: (-g["umzuziehen"], g["vorschlag"]))
    return raus
