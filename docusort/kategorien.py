"""Kategorien von Hand — und die Regel, die Wildwuchs verhindert.

Bis 0.87.3 standen die Kategorien ausschliesslich in `categories.yaml`. Wer
eine neue brauchte, musste die Datei im Container bearbeiten und neu starten.

Gefordert war zweierlei: Kategorien von Hand anlegen zu koennen, und zwar an
jeder Stelle, die eine Auswahl anbietet — und dass auch das Modell neue
beitragen kann.

Dieses Modul beantwortet beides an EINER Stelle:

* `zusammen()` legt die eigenen Kategorien aus der Datenbank ueber die
  eingebauten aus der Datei. Alles, was eine Kategorienliste braucht — die
  Oberflaeche, die Pruefung beim Speichern, der Ablagepfad UND der Systemtext
  des Modells — liest dieses eine Ergebnis.

* `pruefe_namen()` sagt nein, bevor etwas entsteht, das man nicht mehr
  loswird.

🔑 WARUM DIE KI NUR VORSCHLAEGT

Ein Modell, das sich selbst neue Schubladen aufmachen darf, hat nach einer
Woche „Versicherung", „Versicherungen" und „Police" — und jedes Dokument liegt
in einer davon. Die Kategorie ist hier kein Etikett: sie ist der ORDNERNAME auf
der Platte (`library/<Jahr>/<Kategorie>/`). Ein Tippfehler des Modells waere
ein Ordner.

Darum schlaegt die KI vor und ein Mensch bestaetigt einmal. Danach ist die
Kategorie eine ganz normale — das Modell darf sie ab dann von sich aus
benutzen. Damit kann die KI Kategorien beitragen, nur ohne den Haufen.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

from .aehnlichkeit import AEHNLICH_AB, gleich as namen_gleich, schlicht as _schlicht, zu_aehnlich

# Mehr als das passt in keine Auswahlliste und in keinen Dateinamen mehr.
MAX_NAME = 40
MAX_BESCHREIBUNG = 400

# 🔴 Eine Kategorie wird zu einem Ordnernamen. Was einen Pfad zerlegen oder
#    aus ihm heraus zeigen kann, darf gar nicht erst entstehen.
_VERBOTEN = re.compile(r'[/\\:*?"<>|\x00-\x1f]')

# 🔑 Wann zwei Namen dasselbe sind, beantwortet `aehnlichkeit.py` — fuer
#    Kategorien UND fuer Absender. Zwei Antworten auf dieselbe Frage waeren
#    zwei Schwellen, die irgendwann auseinanderlaufen.


class NameFehler(ValueError):
    """Der Name taugt nicht — die Meldung ist fuer Menschen gedacht."""


def pruefe_namen(name: str, vorhandene: Iterable[str]) -> str:
    """Der geputzte Name — oder `NameFehler` mit einem Satz, der erklaert warum.

    `vorhandene` sind die Namen, mit denen er sich nicht beissen darf: bei
    einer Kategorie alle Kategorien, bei einer Unterkategorie die
    Geschwister unter demselben Dach.
    """
    sauber = " ".join((name or "").split())
    if not sauber:
        raise NameFehler("Bitte einen Namen angeben.")
    if len(sauber) > MAX_NAME:
        raise NameFehler("Höchstens %d Zeichen — das wird ein Ordnername."
                         % MAX_NAME)
    if _VERBOTEN.search(sauber):
        raise NameFehler("Diese Zeichen gehen nicht: / \\ : * ? \" < > | — "
                         "der Name wird ein Ordner auf der Platte.")
    if sauber.startswith(".") or sauber in (".", ".."):
        raise NameFehler("Ein Name darf nicht mit einem Punkt beginnen.")
    # 🔴 Windows kennt diese Namen als Geraete, nicht als Ordner. Ein Archiv,
    #    das einmal auf einen Windows-Rechner kopiert wird, waere dort kaputt.
    if sauber.upper().split(".")[0] in {
        "CON", "PRN", "AUX", "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }:
        raise NameFehler("„%s“ ist auf Windows ein Gerätename und taugt nicht "
                         "als Ordner." % sauber)
    liste = list(vorhandene)
    for v in liste:
        if namen_gleich(sauber, v):
            raise NameFehler("„%s“ gibt es schon." % v)
    nah = zu_aehnlich(sauber, liste)
    if nah:
        raise NameFehler("Das ist kaum zu unterscheiden von „%s“. Nimm die, "
                         "oder wähle einen deutlicher anderen Namen." % nah)
    return sauber


def pruefe_beschreibung(text: str) -> str:
    """Die Beschreibung ist das, was das MODELL liest. Sie darf leer sein —
    dann entscheidet der Name allein."""
    sauber = " ".join((text or "").split())
    return sauber[:MAX_BESCHREIBUNG]


def zusammen(basis: list[dict[str, Any]],
             eigene: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Die eingebauten Kategorien plus die selbst angelegten.

    `basis` ist `settings.categories` aus `categories.yaml`.
    `eigene` sind Zeilen aus `doc_categories` — je mit `name`, `parent`
    (leer = eigene Kategorie) und `description`.

    🔑 Diese Funktion ist die EINZIGE Stelle, an der die beiden Quellen
    zusammenkommen. Wer eine zweite baut, bekommt eine Oberflaeche, die eine
    Kategorie anbietet, die der Klassifizierer nicht kennt — oder umgekehrt.

    Die Liste kommt alphabetisch zurueck (`sortiert`) — eine neu angelegte
    Kategorie steht damit dort, wo man sie sucht, und nicht hinten dran.
    """
    aus = [
        {**c, "subcategories": list(c.get("subcategories") or [])}
        for c in basis
    ]
    nach_namen = {_schlicht(c["name"]): c for c in aus}

    eigene = list(eigene)
    # Erst die eigenen Kategorien, dann die Unterkategorien — sonst findet
    # eine Unterkategorie ihr frisch angelegtes Dach nicht.
    for zeile in eigene:
        if (zeile.get("parent") or "").strip():
            continue
        name = (zeile.get("name") or "").strip()
        if not name or _schlicht(name) in nach_namen:
            continue
        neu = {"name": name,
               "description": zeile.get("description") or "",
               "subcategories": []}
        aus.append(neu)
        nach_namen[_schlicht(name)] = neu

    for zeile in eigene:
        dach = (zeile.get("parent") or "").strip()
        if not dach:
            continue
        ziel = nach_namen.get(_schlicht(dach))
        if ziel is None:
            # 🔴 Nie werfen. Eine Unterkategorie, deren Dach aus
            #    `categories.yaml` verschwunden ist, darf nicht die ganze
            #    Liste mitreissen — sie faellt einfach weg.
            continue
        name = (zeile.get("name") or "").strip()
        if name and not any(namen_gleich(name, s) for s in ziel["subcategories"]):
            ziel["subcategories"].append(name)
    # 🔑 Die Reihenfolge wird hier festgelegt, nicht beim Anzeigen: so steht
    #    sie in der Oberflaeche, im Systemtext des Modells und in der
    #    Pruefung gleich. Wer eine Sprache hat, sortiert danach noch einmal
    #    nach der Beschriftung — siehe `sortiert`.
    return sortiert(aus)


def namen(kategorien: list[dict[str, Any]]) -> list[str]:
    return [c["name"] for c in kategorien]


def unterkategorien(kategorien: list[dict[str, Any]]) -> dict[str, list[str]]:
    return {c["name"]: list(c.get("subcategories") or []) for c in kategorien}


def sortiert(kategorien: list[dict[str, Any]],
             beschriftung=None,
             unterbeschriftung=None) -> list[dict[str, Any]]:
    """Alphabetisch — nach dem, was in der Auswahlliste STEHT.

    🔴 Der gespeicherte Name und der gezeigte sind nicht dasselbe. In
    `categories.yaml` heisst die Kategorie `Behoerde`, die Auswahl zeigt
    „Behörde"; auf Englisch zeigt sie „Authorities", und `Auto` steht dort
    als „Vehicle". Wer die NAMEN sortiert, bekommt auf Deutsch zufaellig das
    Richtige und auf Englisch eine Liste, die mit Vehicle, Banking,
    Authorities beginnt — also gar keine Sortierung.

    Darum nimmt diese Funktion die Beschriftung entgegen: `beschriftung(name)`
    und `unterbeschriftung(dach, name)`. Ohne sie wird nach dem Namen
    sortiert — das ist die richtige Antwort ueberall dort, wo es keine
    Sprache gibt (Systemtext des Modells, Pruefung beim Speichern).

    Umlaute zaehlen wie ihre Umschrift (`aehnlichkeit.schlicht`): „Behörde"
    liegt zwischen „Bank" und „Bildung", nicht hinter „Z".
    """
    def _kopf(c: dict[str, Any]) -> tuple[str, str]:
        name = c.get("name") or ""
        gezeigt = beschriftung(name) if beschriftung else name
        return (_schlicht(gezeigt), _schlicht(name))

    def _unter(dach: str, s: str) -> tuple[str, str]:
        gezeigt = unterbeschriftung(dach, s) if unterbeschriftung else s
        return (_schlicht(gezeigt), _schlicht(s))

    aus = []
    for c in sorted(kategorien, key=_kopf):
        # Eine Kopie — die Eingabe gehoert dem Aufrufer.
        neu = dict(c)
        if "subcategories" in c:
            dach = c.get("name") or ""
            neu["subcategories"] = sorted(
                list(c.get("subcategories") or []),
                key=lambda s, d=dach: _unter(d, s))
        aus.append(neu)
    return aus
