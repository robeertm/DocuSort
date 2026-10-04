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


# 🔴 ERST UMSCHREIBEN, DANN ENTAKZENTUIEREN. Wer nur die Akzente abzieht,
#    macht aus „ä" ein „a" — und „Ostsächsische" trifft dann NICHT auf
#    „Ostsaechsische", obwohl genau das die haeufigste deutsche
#    Schreibvariante ist. Im echten Archiv waren das 10 Dokumente, die
#    deshalb als „verschieden" gemeldet wurden.
#    `organizer._slug()` schreibt seit jeher so um; hier muss es genauso sein.
_UMSCHRIFT = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}


def schlicht(name: str) -> str:
    """Zum VERGLEICHEN: ohne Gross-/Kleinschreibung, mit deutscher Umschrift,
    ohne uebrige Akzente, ohne doppelte Leerzeichen.

    🔴 Nicht zum Speichern. Gespeichert wird, was dasteht — das hier ist eine
    Vereinfachung fuers Messen, keine Verbesserung des Namens.
    """
    klein = (name or "").casefold()
    for von, nach in _UMSCHRIFT.items():
        klein = klein.replace(von, nach)
    ohne = unicodedata.normalize("NFKD", klein)
    ohne = "".join(c for c in ohne if not unicodedata.combining(c))
    return " ".join(ohne.split())


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


# 🔴 DIE SCHWELLE ALLEIN GENUEGT NICHT, und das wurde an echten Daten
#    gelernt. Zwei Paare, beide bei 0,92:
#
#        Verti Versicherung AG            / Verti Versicherung
#            → dieselbe Firma, nur die Rechtsform fehlt
#        Sparkassen-Vers. Sachsen Leben…  / … Allgemeine Versicherung…
#            → ZWEI Gesellschaften desselben Hauses
#
#    Keine Zahl trennt die beiden Faelle, denn der Unterschied liegt nicht im
#    Abstand, sondern darin, WAS sich unterscheidet. Eine Rechtsform ist kein
#    anderer Absender; ein anderer Geschaeftszweig schon.
#
# 🔑 Also wird genau das gemessen: bleibt nach Abzug von Gross/Klein, Akzenten,
#    Zeichensetzung und Rechtsform nichts uebrig, ist es dieselbe Stelle, nur
#    anders geschrieben. Bleibt ein Wort uebrig, muss ein Mensch hinsehen.
_RECHTSFORMEN = {
    "ag", "gmbh", "mbh", "kg", "ohg", "gbr", "eg", "se", "kgaa", "ug",
    "ev", "e v", "aoer", "koer", "ag & co kg", "gmbh & co kg",
    "inc", "ltd", "llc", "plc", "sa", "nv", "bv", "co", "und", "&",
}


def _kern(name: str) -> str:
    """Der Name ohne Rechtsform und Zeichensetzung — das, was uebrig bleibt,
    wenn man nur die Schreibweise abzieht."""
    roh = schlicht(name)
    roh = "".join(c if (c.isalnum() or c.isspace()) else " " for c in roh)
    woerter = [w for w in roh.split() if w not in _RECHTSFORMEN]
    return " ".join(woerter)


def nur_schreibweise(a: str, b: str) -> bool:
    """Unterscheiden sich die beiden NUR in der Schreibweise?

    Gross/klein, Akzente, Zeichensetzung, Abstaende und die Rechtsform
    zaehlen nicht. Alles andere schon — ein zusaetzliches Wort ist eine
    Aussage.
    """
    return _kern(a) == _kern(b)


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
        # 🔑 „sicher" heisst: ALLE Mitglieder unterscheiden sich vom Kopf nur
        #    in der Schreibweise. Sobald eines ein eigenes Wort mitbringt,
        #    gilt die ganze Gruppe als anzusehen — lieber einmal zu oft
        #    gefragt als einmal zwei Firmen verschmolzen.
        sicher = all(nur_schreibweise(kopf, m["name"]) for m in mitglieder[1:])
        for m in mitglieder[1:]:
            m["nur_schreibweise"] = nur_schreibweise(kopf, m["name"])
            m["unterschied"] = _unterschied(kopf, m["name"])
        mitglieder[0]["nur_schreibweise"] = True
        mitglieder[0]["unterschied"] = ""
        raus.append({
            "vorschlag": kopf,
            "mitglieder": mitglieder,
            "sicher": sicher,
            "umzuziehen": sum(m["anzahl"] for m in mitglieder[1:]),
        })
    # Die eindeutigen zuerst, dann nach Wirkung — wer die Liste von oben
    # abarbeitet, trifft die leichten Entscheidungen zuerst.
    raus.sort(key=lambda g: (not g["sicher"], -g["umzuziehen"], g["vorschlag"]))
    return raus


def _unterschied(a: str, b: str) -> str:
    """Die Woerter, die nur in EINEM der beiden stehen — klein geschrieben.

    Das ist, was ein Mensch sehen muss, um zu entscheiden: steht dort „ag",
    ist es dieselbe Firma; steht dort „allgemeine versicherung", sind es
    zwei."""
    wa, wb = set(_kern(a).split()), set(_kern(b).split())
    nur = sorted((wa - wb) | (wb - wa))
    return " ".join(nur)
