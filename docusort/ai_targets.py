"""Benannte Rechenorte fuer die Einordnung — und ein Wechsel OHNE Neustart.

Der Auftrag, woertlich:

    „gib auch eine umschaltmoeglichkeit zwischen den rechnern zu wechseln, zum
    beispiel ich brauche schnell das dokument in docusort dann will ich den mac
    waehlen koennen, ollama ist ja drauf und aktiv und wenn ich zeit habe und er
    die nacht zeit hat soll es auf dem nas rechnen"

Und der zweite, genauso wichtige:

    „denke immer dran es muss auch fuer fremde klappen die das repo ueber git
    holen und dort eine ganz andere konstelation fahren!!"

🔴 DESHALB STEHT HIER KEINE ADRESSE, KEIN MODELLNAME UND KEIN RECHNERNAME.
Wer dieses Repo klont, hat keinen Mac im Netz und keine Synology. Die Ziele
kommen aus `ai.targets` in der config.yaml. Ist dort nichts eingetragen, wird
genau das abgeleitet, was *messbar* vorhanden ist — der eingerichtete Anbieter,
und eine Bruecke nur dann, wenn tatsaechlich einer dranhaengt. Gibt es am Ende
nur ein Ziel, zeigt die Oberflaeche keine Auswahl, weil es nichts zu waehlen
gibt. Das ist dieselbe Regel wie auf der Einstellungsseite: nur anbieten, was
diese Installation wirklich kann.

Beispiel fuer `config.yaml` — rein illustrativ, jeder tragt seine eigenen ein:

    ai:
      provider: openai_compat          # das bleibt der Rueckfall
      model: qwen2.5:7b-instruct
      base_url: http://10.0.0.5:11434/v1
      active_target: schnell
      targets:
        - key: schnell
          label: Arbeitsrechner
          provider: openai_compat
          model: qwen2.5:7b-instruct
          base_url: http://10.0.0.7:11434/v1
        - key: nachts
          label: Server
          provider: openai_compat
          model: qwen2.5:7b-instruct
          base_url: http://10.0.0.5:11434/v1

🔑 WARUM EIN HALTER UND NICHT EIN NEUSTART. Bisher schrieb die
Einstellungsseite den Anbieter in die Datei und meldete `restart_required`.
Der laufende Klassifizierer behielt seinen alten Anbieter, bis jemand den
Container neu startet — und ein Neustart wirft jede laufende Texterkennung weg
(gemessen: drei Auslieferungen hielten dasselbe Dokument 47 min im Eingang).
Zum Umschalten „mal schnell auf den anderen Rechner" ist das untauglich.

`ClassifierHandle` sieht fuer jeden Aufrufer aus wie ein `Classifier` — es
reicht `classify()` und jedes andere Attribut durch. Innen haelt es den
wirklichen Klassifizierer und tauscht ihn beim Wechsel aus. Damit greift der
Wechsel in jedem Faden, ohne dass eine einzige Aufrufstelle davon weiss.

🔴 EIN WECHSEL ZERREISST KEINE LAUFENDE EINORDNUNG. `classify()` holt sich den
aktiven Klassifizierer EINMAL in eine lokale Variable und arbeitet damit zu
Ende. Ein Tausch mitten in einer Anfrage betrifft erst das naechste Dokument.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, replace
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Schluessel, unter dem das in der config.yaml eingerichtete Ziel laeuft, wenn
# der Benutzer keine eigenen Ziele benannt hat. Stabil, damit ein gemerkter
# Wechsel einen Neustart uebersteht.
KEY_CONFIGURED = "configured"
KEY_BRIDGE = "bridge"


@dataclass(frozen=True)
class Target:
    """Ein benannter Ort, an dem gerechnet werden kann."""
    key: str
    label: str
    provider: str
    model: str
    base_url: str = ""
    note: str = ""
    # True, wenn dieses Ziel nicht aus der config.yaml stammt, sondern aus dem
    # abgeleitet wurde, was vorhanden ist. Die Oberflaeche darf das zeigen,
    # damit niemand eine Einstellung sucht, die es nicht gibt.
    derived: bool = False

    def as_dict(self) -> dict[str, Any]:
        """Fuer die Oberflaeche. 🔴 `base_url` kann einen Rechnernamen oder
        eine Adresse aus einem fremden Netz tragen — das ist fuer den
        Besitzer der Installation bestimmt, aber es gehoert nicht in ein
        Protokoll und nicht in eine Fehlermeldung. Hier steht es, weil die
        Seite hinter der Anmeldung liegt und der Benutzer seine eigenen
        Adressen sehen darf."""
        return {
            "key": self.key, "label": self.label, "provider": self.provider,
            "model": self.model, "base_url": self.base_url,
            "note": self.note, "derived": self.derived,
        }


def _label_fuer(provider: str, model: str) -> str:
    """Ein lesbarer Name, wenn der Benutzer keinen vergeben hat. Nennt das
    Modell, nicht den Rechner — den Rechner kennt nur der Anbieter selbst
    (Provider.runtime()), und erraten wird hier nichts."""
    model = (model or "").strip()
    provider = (provider or "").strip() or "?"
    return f"{provider} · {model}" if model else provider


def _aus_eintrag(roh: Any) -> Target | None:
    """Ein Eintrag aus `ai.targets`. Unbrauchbares wird uebersprungen und
    gemeldet — 🔴 nie geworfen: eine krumme Zeile in der Konfiguration darf
    nicht das ganze Programm am Starten hindern."""
    if not isinstance(roh, dict):
        logger.warning("ai.targets: Eintrag ist kein Abschnitt, uebersprungen")
        return None
    provider = str(roh.get("provider") or "").strip()
    if not provider:
        logger.warning("ai.targets: Eintrag ohne `provider`, uebersprungen")
        return None
    key = str(roh.get("key") or "").strip()
    model = str(roh.get("model") or "").strip()
    if not key:
        # Ein Schluessel muss stabil sein, damit ein gemerkter Wechsel einen
        # Neustart uebersteht. Ohne eigenen bauen wir einen aus dem Inhalt.
        key = f"{provider}-{model}".strip("-").replace(" ", "_") or provider
    return Target(
        key=key,
        label=str(roh.get("label") or "").strip() or _label_fuer(provider, model),
        provider=provider,
        model=model,
        base_url=str(roh.get("base_url") or "").strip(),
        note=str(roh.get("note") or "").strip(),
    )


def ziele(settings: Any, *, bridge_verbunden: bool | None = None) -> list[Target]:
    """Alle waehlbaren Rechenorte dieser Installation.

    Reihenfolge: erst was der Benutzer in `ai.targets` benannt hat (so wie er
    es hingeschrieben hat), dann das abgeleitete. Doppelte Schluessel gewinnt
    der Benutzer.
    """
    ai = settings.ai
    raus: list[Target] = []
    gesehen: set[str] = set()

    for roh in (getattr(ai, "targets", None) or []):
        t = _aus_eintrag(roh)
        if t is None or t.key in gesehen:
            continue
        gesehen.add(t.key)
        raus.append(t)

    # Das eingerichtete Ziel gehoert immer dazu — sonst kann sich eine
    # Installation mit einem kaputten `targets`-Block selbst aussperren.
    # 🔑 Es wird nur dann als eigener Eintrag gezeigt, wenn kein benanntes Ziel
    #    schon genau dasselbe beschreibt; zwei Zeilen fuer denselben Rechner
    #    sind keine Auswahl, sondern eine Verwirrung.
    eingerichtet = Target(
        key=KEY_CONFIGURED,
        label=_label_fuer(getattr(ai, "provider", ""), getattr(ai, "model", "")),
        provider=str(getattr(ai, "provider", "") or ""),
        model=str(getattr(ai, "model", "") or ""),
        base_url=str(getattr(ai, "base_url", "") or ""),
        derived=True,
    )
    # 🔴 DER VERGLEICH LAEUFT UEBER ANBIETER UND ADRESSE, NICHT UEBER DAS
    #    MODELL. Ein Rechenort ist ein RECHNER; welches Modell dort geladen
    #    ist, ist eine Einstellung darauf. Erst stand hier auch `model` im
    #    Vergleich — dann erschien das eingerichtete Ollama als DRITTER Knopf
    #    neben den zwei benannten Rechnern, nur weil dort ein anderer
    #    Modellname eingetragen war. Der Pruefstand hat das gefunden, nicht
    #    ich. Bei einem Anbieter in der Wolke ist `base_url` leer und der
    #    Anbieter entscheidet allein — auch richtig: „die Wolke" ist ein Ort.
    schon_da = any(
        t.provider == eingerichtet.provider
        and t.base_url.rstrip("/") == eingerichtet.base_url.rstrip("/")
        for t in raus
    )
    if eingerichtet.provider and not schon_da and KEY_CONFIGURED not in gesehen:
        raus.insert(0, eingerichtet)
        gesehen.add(KEY_CONFIGURED)

    # Eine Bruecke nur, wenn wirklich eine dranhaengt. 🔴 Das ist eine
    # Messung, keine Vermutung: `bridge_verbunden` kommt von der Bruecke
    # selbst. Ohne Klienten waere es ein Ziel, das beim Anwaehlen scheitert.
    if (bridge_verbunden
            and eingerichtet.provider != "bridge"
            and KEY_BRIDGE not in gesehen
            and not any(t.provider == "bridge" for t in raus)):
        raus.append(Target(
            key=KEY_BRIDGE, label="", provider="bridge",
            model=str(getattr(ai, "model", "") or ""), derived=True,
        ))
    return raus


def aktiver_schluessel(settings: Any, vorhandene: list[Target]) -> str:
    """Welches Ziel laeuft gerade. Steht in `ai.active_target`; ist der
    Eintrag leer oder zeigt er auf ein Ziel, das es nicht mehr gibt, gilt das
    erste — 🔴 nie None: ohne aktives Ziel koennte nichts einordnen."""
    gemerkt = str(getattr(settings.ai, "active_target", "") or "").strip()
    schluessel = [t.key for t in vorhandene]
    if gemerkt and gemerkt in schluessel:
        return gemerkt
    if gemerkt:
        logger.warning("ai.active_target=%r gibt es nicht (mehr) — nehme %r",
                       gemerkt, schluessel[0] if schluessel else "")
    return schluessel[0] if schluessel else ""


def ai_settings_fuer(ai: Any, t: Target) -> Any:
    """Die AI-Einstellungen, aber mit Anbieter/Modell/Adresse dieses Ziels.
    Alles andere — Zeitgrenze, Textgrenze, Mindestsicherheit — bleibt wie
    eingestellt: 🔑 das sind Entscheidungen des Benutzers ueber die ARBEIT,
    nicht ueber den Rechner."""
    return replace(ai, provider=t.provider, model=t.model, base_url=t.base_url)


class ClassifierHandle:
    """Sieht fuer jeden Aufrufer aus wie ein `Classifier`, haelt innen aber
    einen austauschbaren.

    `baue(target) -> Classifier` wird von aussen gegeben, damit dieses Modul
    weder den Klassifizierer noch die Schluesselverwaltung kennen muss.
    """

    def __init__(self, settings: Any, baue: Callable[[Target], Any],
                 *, start: Target, aktiv: Any) -> None:
        self._settings = settings
        self._baue = baue
        self._ziel = start
        self._aktiv = aktiv
        self._schloss = threading.RLock()

    # ------------------------------------------------------------- Auskunft
    @property
    def ziel(self) -> Target:
        return self._ziel

    @property
    def inner(self) -> Any:
        """Der wirkliche Klassifizierer. Fuer Pruefstaende und fuer Code, der
        ausdruecklich das echte Objekt braucht."""
        return self._aktiv

    # -------------------------------------------------------------- Arbeiten
    def classify(self, text: str) -> Any:
        # 🔴 EINMAL holen, dann damit zu Ende arbeiten. Wer hier
        #    `self._aktiv.classify(...)` schreibt, laesst einen Wechsel
        #    mitten in die laufende Anfrage greifen.
        aktiv = self._aktiv
        return aktiv.classify(text)

    # -------------------------------------------------------------- Wechseln
    def wechsle(self, key: str) -> Target:
        """Auf ein anderes Ziel umstellen. Wirft `KeyError`, wenn es den
        Schluessel nicht gibt, und laesst bei einem Baufehler das ALTE Ziel
        stehen — 🔴 ein fehlgeschlagener Wechsel darf die Installation nicht
        ohne Klassifizierer zuruecklassen."""
        with self._schloss:
            vorhandene = ziele(self._settings,
                               bridge_verbunden=_bridge_verbunden())
            passend = [t for t in vorhandene if t.key == key]
            if not passend:
                raise KeyError(key)
            neu_ziel = passend[0]
            if neu_ziel.key == self._ziel.key:
                return self._ziel
            neu = self._baue(neu_ziel)      # wirft bei fehlendem Schluessel
            alt = self._ziel
            self._aktiv = neu
            self._ziel = neu_ziel
            logger.info("KI-Ziel gewechselt: %s -> %s (Anbieter %s, Modell %s)",
                        alt.key, neu_ziel.key, neu_ziel.provider,
                        neu_ziel.model or "?")
            return neu_ziel

    # ------------------------------------------- alles andere durchreichen
    def __getattr__(self, name: str) -> Any:
        # Wird nur gerufen, wenn das Attribut hier NICHT existiert — also fuer
        # `provider`, `settings`, `categories`, `holder_names` und jedes
        # andere, das eine Aufrufstelle vom Klassifizierer erwartet.
        return getattr(self._aktiv, name)

    def __repr__(self) -> str:  # pragma: no cover
        return f"<ClassifierHandle ziel={self._ziel.key!r}>"


def _bridge_verbunden() -> bool:
    """Haengt ein Bruecken-Klient dran? 🔴 Nie werfen — ohne Bruecken-Modul
    ist die Antwort einfach nein."""
    try:
        from .bridge.server import get_bridge
        return bool((get_bridge().info() or {}).get("connected"))
    except Exception:  # noqa: BLE001
        return False


def baue_handle(settings: Any, baue_classifier: Callable[[Any, Target], Any]):
    """Den Halter aufsetzen: Ziele bestimmen, das aktive bauen.

    `baue_classifier(ai_settings, target) -> Classifier`.

    Gibt `(handle, ziele, aktiver_schluessel)` zurueck. 🔴 Schlaegt der Bau des
    aktiven Ziels fehl, wird die Ausnahme durchgelassen — der Aufrufer behandelt
    das genauso wie bisher einen gescheiterten Klassifizierer-Aufbau (Warnung
    im Protokoll, Einrichtungsmodus), statt hier still ein anderes Ziel zu
    nehmen, das der Benutzer nie gewaehlt hat.
    """
    vorhandene = ziele(settings, bridge_verbunden=_bridge_verbunden())
    key = aktiver_schluessel(settings, vorhandene)
    start = next((t for t in vorhandene if t.key == key), None)
    if start is None:
        raise ValueError("Kein KI-Ziel vorhanden")
    aktiv = baue_classifier(ai_settings_fuer(settings.ai, start), start)
    handle = ClassifierHandle(
        settings,
        lambda t: baue_classifier(ai_settings_fuer(settings.ai, t), t),
        start=start, aktiv=aktiv,
    )
    return handle, vorhandene, key
