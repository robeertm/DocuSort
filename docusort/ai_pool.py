"""Wer rechnet dieses Dokument — und wer das naechste.

Der Auftrag, woertlich:

    „docusort sollte auch umschalten koennen, wenn der mac nicht da ist wird
     auf dem nas gerechnet ist der mac da wieder dort oder auf beiden
     jenachdem wie die last an dokumenten ist"

Drei Forderungen in einem Satz, und sie haben EINE gemeinsame Antwort.

🔑 DIE REGEL: jedes Dokument geht dorthin, wo es am FRUEHESTEN FERTIG ist.
Nicht zum schnellsten Rechner — zum fruehesten Ende. Der Unterschied ist der
ganze Punkt:

    Gemessen in dieser Installation: Mac 11 s je Dokument, NAS 835 s. Liegt
    EIN Dokument an, ist der Mac richtig. Liegt ein zweites an, waehrend der
    Mac rechnet, ist der Mac IMMER NOCH richtig — 11 s warten plus 11 s
    rechnen schlaegt 835 s auf der NAS um Laengen. „Auf beide verteilen"
    waere hier die LANGSAMERE Antwort, und genau das macht eine naive
    Lastverteilung, die nur freie Rechner zaehlt.

    Erst wenn die Schlange vor dem Mac laenger wird als eine NAS-Runde — bei
    diesen Zahlen ab rund 76 wartenden Dokumenten — lohnt die NAS sich, und
    dann schaltet sie sich von selbst dazu.

Damit beantwortet dieselbe Regel alle drei Faelle aus dem Auftrag: faellt der
Mac aus, ist er nicht `bereit` und bekommt nichts; kommt er zurueck, gewinnt
er beim naechsten Dokument wieder; und bei genug Dokumenten rechnen beide.

🔴 GESCHAETZT WIRD NUR, WAS GEMESSEN WURDE. Die Dauer je Rechenort kommt aus
den echten Einordnungen DIESER Installation — nicht aus Kernen, nicht aus RAM,
nicht aus einer Tabelle. Das ist nicht Bequemlichkeit, sondern Erfahrung: die
NAS hat 32 GB und 8 Kerne und ist trotzdem 76-mal langsamer als ein Laptop mit
weniger von beidem. Wer dieses Repo frisch klont, hat keine Messung und faengt
mit einer neutralen Annahme an, die sich nach dem ersten Dokument korrigiert.

🔴 DIESES MODUL KENNT KEINE ADRESSE UND KEINEN RECHNERNAMEN. Es bekommt die
Ziele von `ai_targets` und die Zustaende vom Waechter. Wer das Repo mit einer
ganz anderen Aufstellung klont, bekommt dieselbe Mechanik.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Any, Callable

logger = logging.getLogger("docusort.ai_pool")

# Wie viele Messungen je Rechenort im Gedaechtnis bleiben. Genug, damit ein
# einzelner Ausreisser (ein besonders langes Dokument) den Median nicht
# verschiebt; wenig genug, damit eine echte Aenderung — Modellwechsel, anderer
# Rechner am selben Schluessel — binnen weniger Dokumente ankommt.
PROBEN = 15

# Was ein noch nie gemessener Rechenort kostet, in Sekunden. 🔑 Die Zahl ist
# bewusst niedrig: ein unbekanntes Ziel soll EINMAL drankommen, damit es eine
# echte Messung bekommt. Nach dem ersten Dokument spielt sie keine Rolle mehr.
ANNAHME_S = 45.0

_schloss = threading.RLock()
# key -> deque[(zeitpunkt, sekunden)]
_dauern: dict[str, deque[tuple[float, float]]] = {}
# key -> Anzahl Faeden, die gerade auf diesem Ziel rechnen oder darauf warten
_belegt: dict[str, int] = {}
# key -> (start_monotonic, was) des laufenden Aufrufs
_laeuft: dict[str, tuple[float, str]] = {}
# key -> Tor, das genau EINE Einordnung gleichzeitig durchlaesst.
#
# 🔴 WARUM NICHT MEHRERE. Ollama nimmt zwar mehrere Anfragen an, rechnet sie
#    aber auf derselben Hardware — zwei gleichzeitige Einordnungen sind beide
#    etwa doppelt so langsam, nicht etwa schneller. Schlimmer: sie machen die
#    Zeitmessung unbrauchbar, und auf der die ganze Verteilung beruht. Ein
#    Rechenort, ein Dokument; die Parallelitaet entsteht zwischen den
#    Rechenorten, nicht in einem.
_tore: dict[str, threading.Semaphore] = {}
# key -> Zustand aus ai_targets (bereit/ohne_modell/tot/offen)
_zustaende: dict[str, str] = {}
_zustaende_zeit: float = 0.0


# --------------------------------------------------------------- Messungen
def merke_dauer(key: str, sekunden: float) -> None:
    """Eine echte Einordnung ist zu Ende. Das ist die einzige Quelle fuer die
    Schaetzung — gemessen wird die ganze Anfrage, so wie sie ein Dokument
    erlebt, samt Netzweg und kaltem Modell."""
    if sekunden <= 0:
        return
    with _schloss:
        ring = _dauern.setdefault(key, deque(maxlen=PROBEN))
        ring.append((time.time(), round(float(sekunden), 2)))


def dauer_schaetzung(key: str) -> tuple[float, bool]:
    """(Sekunden je Dokument, ob gemessen). Der MEDIAN, nicht der Mittelwert:
    ein einzelnes Dokument, das in eine Zeitgrenze lief, soll die Wahl nicht
    fuer die naechsten fuenfzehn verderben."""
    with _schloss:
        werte = sorted(s for _, s in _dauern.get(key, ()))
    if not werte:
        return ANNAHME_S, False
    return werte[len(werte) // 2], True


def verlauf(key: str) -> list[float]:
    with _schloss:
        return [s for _, s in _dauern.get(key, ())]


# ----------------------------------------------------------- Zustandspflege
def setze_zustaende(d: dict[str, Any]) -> None:
    """Der Waechter misst ohnehin alle zwei Minuten jeden Rechenort. Seine
    Messung hier abzulegen kostet nichts und erspart dem Verteiler eine eigene
    Anfrage vor JEDEM Dokument — bei einem toten Rechner waeren das 2,5 s
    Wartezeit je Dokument, nur um festzustellen, was der Waechter schon weiss."""
    global _zustaende_zeit
    with _schloss:
        for key, z in (d or {}).items():
            _zustaende[key] = str((z or {}).get("zustand") or "")
        _zustaende_zeit = time.time()


def setze_zustand(key: str, zustand: str) -> None:
    with _schloss:
        _zustaende[key] = zustand


def zustand_von(key: str) -> str:
    with _schloss:
        return _zustaende.get(key, "")


# ------------------------------------------------------------- Die Auswahl
def _fertig_in(key: str) -> float:
    """Wann waere ein JETZT abgegebenes Dokument auf diesem Rechenort fertig?

    Drei Summanden, und jeder steht fuer etwas Beobachtbares:
      * was gerade laeuft, abzueglich der Zeit, die es schon laeuft,
      * was davor noch in der Schlange steht,
      * die eigene Rechenzeit.
    """
    je_dok, _ = dauer_schaetzung(key)
    with _schloss:
        wartend = max(0, _belegt.get(key, 0))
        laufend = _laeuft.get(key)
    rest = 0.0
    if laufend:
        vergangen = time.monotonic() - laufend[0]
        # 🔴 Nie negativ, und nie null, nur weil ein Aufruf laenger braucht als
        #    der Median. Wer ueberzieht, hat mindestens noch einen Takt vor
        #    sich — sonst sieht ein haengender Rechner „sofort frei" aus und
        #    bekommt alles.
        rest = max(je_dok - vergangen, je_dok * 0.25)
        wartend = max(0, wartend - 1)
    return rest + wartend * je_dok + je_dok


def waehle(kandidaten: list[str]) -> str:
    """Der Rechenort mit dem fruehesten Ende. Leere Liste -> leerer Schluessel."""
    bereit = [k for k in kandidaten if zustand_von(k) in ("", "bereit")]
    if not bereit:
        return ""
    return min(bereit, key=_fertig_in)


# ------------------------------------------------------------- Der Verteiler
class Verteiler:
    """Haelt je Rechenort einen Klassifizierer und entscheidet je Dokument.

    🔴 Er BAUT die Klassifizierer traege und behaelt sie: einen pro Ziel, nicht
    einen pro Dokument. Ein Aufbau liest Schluessel und Kategorien von der
    Platte; das je Dokument zu tun waere Arbeit ohne Ertrag.
    """

    def __init__(self, baue: Callable[[Any], Any]) -> None:
        self._baue = baue
        self._gebaut: dict[str, Any] = {}
        self._schloss = threading.RLock()

    def hole(self, t: Any) -> Any:
        with self._schloss:
            vorhanden = self._gebaut.get(t.key)
            if vorhanden is not None:
                return vorhanden
        # 🔴 Der Bau laeuft AUSSERHALB des Schlosses: er kann Dateien lesen,
        #    und ein zweiter Faden soll solange nicht auf einem anderen Ziel
        #    blockieren.
        neu = self._baue(t)
        with self._schloss:
            return self._gebaut.setdefault(t.key, neu)

    def vergiss(self, key: str) -> None:
        """Nach einem Zielwechsel in der Konfiguration: der alte Klassifizierer
        zeigt auf die alte Adresse."""
        with self._schloss:
            self._gebaut.pop(key, None)

    def leeren(self) -> None:
        with self._schloss:
            self._gebaut.clear()


class _Platz:
    """Ein belegter Rechenort, der sich beim Verlassen selbst freigibt — und
    dabei die gemessene Dauer ablegt."""

    def __init__(self, key: str, was: str) -> None:
        self.key = key
        self._was = was
        self._t0 = 0.0
        self._hat_tor = False

    def __enter__(self) -> "_Platz":
        # 🔑 DIE REIHENFOLGE IST DIE AUSSAGE. Erst zaehlen, dann warten, dann
        #    die Uhr starten:
        #      * gezaehlt wird SOFORT, damit ein gleichzeitig entscheidender
        #        Faden die Schlange sieht und gegebenenfalls woanders hingeht;
        #      * gewartet wird am Tor, weil der Rechenort nur eines auf einmal
        #        kann;
        #      * gemessen wird ERST DANACH. 🔴 Wer die Wartezeit mitmisst,
        #        verdirbt genau die Zahl, auf der die Verteilung beruht: der
        #        beliebteste Rechner saehe nach jeder Schlange langsamer aus,
        #        bekaeme weniger Arbeit, saehe wieder schneller aus — ein
        #        Pendel statt einer Messung.
        with _schloss:
            _belegt[self.key] = _belegt.get(self.key, 0) + 1
            tor = _tore.get(self.key)
            if tor is None:
                tor = _tore[self.key] = threading.Semaphore(1)
        tor.acquire()
        self._hat_tor = True
        self._t0 = time.monotonic()
        with _schloss:
            _laeuft[self.key] = (self._t0, self._was)
        return self

    def __exit__(self, art, wert, spur) -> None:
        dauer = time.monotonic() - self._t0
        with _schloss:
            _belegt[self.key] = max(0, _belegt.get(self.key, 1) - 1)
            vorhanden = _laeuft.get(self.key)
            if vorhanden and vorhanden[0] == self._t0:
                _laeuft.pop(self.key, None)
            tor = _tore.get(self.key)
        if self._hat_tor and tor is not None:
            self._hat_tor = False
            tor.release()
        # 🔴 Nur ERFOLGE gehen in die Schaetzung. Eine Anfrage, die nach drei
        #    Sekunden an einer abgelehnten Verbindung scheitert, wuerde den
        #    toten Rechner sonst als den schnellsten ausweisen.
        if art is None:
            merke_dauer(self.key, dauer)


def platz(key: str, was: str = "") -> _Platz:
    return _Platz(key, was)


# ---------------------------------------------------------------- Auskunft
def stand(ziele: list[Any] | None = None) -> dict[str, Any]:
    """Was die Startseite ueber die Rechenorte zeigt — gemessen, nicht geraten."""
    schluessel = [t.key for t in (ziele or [])] or list(set(
        list(_dauern) + list(_belegt) + list(_zustaende)))
    raus: dict[str, Any] = {}
    jetzt = time.monotonic()
    for k in schluessel:
        je_dok, gemessen = dauer_schaetzung(k)
        with _schloss:
            laufend = _laeuft.get(k)
            wartend = max(0, _belegt.get(k, 0) - (1 if laufend else 0))
        raus[k] = {
            "s_pro_dokument": round(je_dok, 1) if gemessen else None,
            "gemessen": gemessen,
            "proben": len(verlauf(k)),
            "verlauf": verlauf(k),
            "wartend": wartend,
            "rechnet_seit_s": round(jetzt - laufend[0], 1) if laufend else None,
            "rechnet_an": laufend[1] if laufend else "",
            "zustand": zustand_von(k),
            "fertig_in_s": round(_fertig_in(k), 1),
        }
    return raus


def zuruecksetzen() -> None:
    """Nur fuer Pruefstaende."""
    with _schloss:
        _dauern.clear()
        _belegt.clear()
        _laeuft.clear()
        _tore.clear()
        _zustaende.clear()
