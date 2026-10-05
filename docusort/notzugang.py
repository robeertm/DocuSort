# -*- coding: utf-8 -*-
"""Wieder hineinkommen, wenn das Passwort nicht mehr geht.

🔴 WARUM ES DAS GIBT (05.10.2026)
Ein Benutzer kam nicht mehr in seine Installation: das Passwort wurde nicht
angenommen, und es gab **keinen Weg zurueck** — kein Befehl, keine Stelle in
der Oberflaeche, nichts. Die Anmeldung ist die einzige Tuer, und sie hatte
kein zweites Schloss. Die Dokumente lagen unversehrt auf der Platte und waren
trotzdem unerreichbar.

🔑 DIE EIGENSCHAFT, DIE HIER GEBAUT WIRD
Wer am Rechner steht, auf dem DocuSort laeuft, kommt immer wieder hinein.
Das ist keine Schwaechung: wer `docker exec` auf dem Wirt ausfuehren kann,
hat ohnehin Zugriff auf die Datenbank und auf jede abgelegte Datei. Ein
Riegel, der NUR den rechtmaessigen Besitzer aussperrt, schuetzt niemanden.

🔴 UND DARUM KEIN FESTES NOTPASSWORT. Ein eingebautes Kennwort waere auf
jeder Installation dasselbe und stuende im oeffentlichen Quelltext. Hier wird
jedesmal ein neues gewuerfelt, einmal angezeigt und nirgends gespeichert.
"""
from __future__ import annotations

import secrets
import string
from typing import Any

from . import auth

# Keine Zeichen, die man beim Abtippen verwechselt (0/O, 1/l/I).
_ALPHABET = "".join(c for c in (string.ascii_letters + string.digits)
                    if c not in "0O1lI")


def neues_passwort(laenge: int = 20) -> str:
    """Ein zufaelliges Passwort — jedesmal ein anderes, nirgends gespeichert."""
    return "".join(secrets.choice(_ALPHABET) for _ in range(laenge))


def benutzer_auflisten(db) -> list[dict[str, Any]]:
    return db.user_list()


def passwort_setzen(db, benutzername: str = "", passwort: str = "",
                    *, muss_wechseln: bool = True) -> tuple[str, str]:
    """Setzt das Passwort eines Kontos neu und gibt (Benutzername, Passwort).

    Ohne `benutzername` wird der **erste Verwalter** genommen — bei einer
    Installation mit einem einzigen Konto ist das die einzige sinnvolle Wahl,
    und wer ausgesperrt ist, soll nicht erst Namen raten muessen.

    🔴 Das Konto wird dabei auch wieder **aktiv** gesetzt und auf die Rolle
    `admin` gehoben, falls es das verloren hatte: ein Zugang, der zwar ein
    Passwort hat, aber nichts darf, hilft dem Ausgesperrten nicht.
    """
    leute = db.user_list()
    if not leute:
        raise LookupError("keine Konten")
    if benutzername:
        treffer = [u for u in leute if u["username"] == benutzername]
        if not treffer:
            raise LookupError(benutzername)
        ziel = treffer[0]
    else:
        verwalter = [u for u in leute if u.get("role") == "admin"]
        ziel = (verwalter or leute)[0]
    passwort = passwort or neues_passwort()
    # 🔴 DIE ROLLE NICHT BLIND AUF `admin` HEBEN.
    #    Beim namenlosen Aufruf ist das richtig: wer sich selbst aussperrt,
    #    soll danach auch verwalten koennen. Wird aber ein NAME genannt, kann
    #    das ein Maschinenkonto sein (die Postwache hat `deliver`) — es zum
    #    Verwalter zu machen gaebe ihm Rechte, die es nie brauchte, und waere
    #    beim naechsten Start ohnehin wieder zurueckgezogen.
    felder = {"password_hash": auth.hash_password(passwort), "is_active": 1,
              "must_change_password": 1 if muss_wechseln else 0}
    ohne_verwalter = not any(u.get("role") == "admin" and u.get("is_active")
                             for u in leute)
    if not benutzername or ziel.get("role") == "admin" or ohne_verwalter:
        felder["role"] = "admin"
    db.user_update(ziel["id"], **felder)
    return ziel["username"], passwort


def notkonto_anlegen(db, benutzername: str = "notzugang",
                     passwort: str = "") -> tuple[str, str]:
    """Legt ein zusaetzliches Verwalterkonto an — ohne ein bestehendes anzufassen.

    🔑 Fuer den Fall, dass jemand den EIGENEN Zugang behalten moechte (etwa
    weil ein zweiter Mensch ihn benutzt) und nur selbst wieder hineinwill.
    """
    passwort = passwort or neues_passwort()
    name = benutzername
    vorhanden = {u["username"] for u in db.user_list()}
    i = 2
    while name in vorhanden:
        name = "%s%d" % (benutzername, i)
        i += 1
    db.user_create(name, auth.hash_password(passwort), "admin",
                   "Notzugang", must_change=True)
    return name, passwort


# ---------------------------------------------------------------------------
# Einmalpasswort ueber den Benachrichtigungsweg (Telegram / E-Mail)
#
# 🔑 WARUM DAS DER BESSERE WEG IST
# Der Befehl auf der Konsole hilft nur, wer an der Maschine sitzt und sich
# damit auskennt. Wer DocuSort auf seiner NAS laufen laesst und vom Handy
# darauf schaut, hat beides nicht. Er hat aber schon einen Kanal eingerichtet,
# ueber den DocuSort ihm Dinge meldet — und dieser Kanal gehoert nachweislich
# ihm. Darueber darf er sich eine Einmal-Anmeldung schicken lassen.
#
# 🔴 WAS DABEI NICHT PASSIERT: das Konto wird NICHT angefasst. Wer den Knopf
#    drueckt, aendert gar nichts — das Passwort bleibt, bis jemand den Code
#    WIRKLICH benutzt. Ein Fremder, der den Knopf drueckt, sperrt den Besitzer
#    also nicht aus; er schickt ihm nur eine Nachricht.
#
# 🔴 UND ES WIRD BEGRENZT. Sonst waere der Knopf ein Weg, jemandem beliebig
#    viele Telegram-Nachrichten zu schicken.

import json as _json
import time as _time

_SCHLUESSEL = "auth.einmalpasswort"
_GUELTIG_S = 15 * 60          # 15 Minuten
_SPERRE_S = 120               # fruehestens alle 2 Minuten ein neuer
_MAX_VERSUCHE = 5             # danach ist der Code verbrannt


def _lesen(db) -> dict:
    try:
        v = _json.loads(db.meta_get(_SCHLUESSEL) or "null")
    except ValueError:
        v = None
    return v if isinstance(v, dict) else {}


def _schreiben(db, wert: dict | None) -> None:
    db.meta_set(_SCHLUESSEL, _json.dumps(wert) if wert else "")


def einmalpasswort_moeglich(db) -> bool:
    """Gibt es ueberhaupt einen Kanal, ueber den etwas ankommen kann?"""
    try:
        from . import notifier
        return bool(notifier.get_dispatcher().channels_summary())
    except Exception:          # noqa: BLE001
        return False


DATEINAME = "passwort-zuruecksetzen.txt"


def _in_datei_legen(ordner, benutzer: str, code: str, bis: int) -> str:
    """Das Einmalpasswort in den Konfigordner legen — der ZWEITE Weg.

    🔑 WARUM DAS DER WICHTIGSTE RUECKFALL IST
    Der Konsolenbefehl hilft nur, wer sich mit SSH auskennt. Ein Kanal
    (Telegram, E-Mail) ist bei vielen gar nicht eingerichtet. Was JEDER hat,
    der DocuSort auf einer NAS betreibt: Zugriff auf den Ordner, in dem es
    liegt — ueber die Dateiverwaltung im Browser, eine Netzwerkfreigabe oder
    den Dateimanager. Genau dort liegt die Datei.

    🔴 Das ist keine Schwaechung. In demselben Ordner liegt die Datenbank mit
    allen Dokumenten. Wer die Datei lesen kann, koennte ohnehin alles lesen —
    ein Riegel, der nur den Besitzer aussperrt, schuetzt niemanden.

    🔴 Die Datei wird beim Einloesen GELOESCHT und gilt nur bis `bis`.
    """
    import datetime as _dt
    import os as _os
    ziel = _pfad(ordner) / DATEINAME
    text = (
        "DocuSort — Einmalpasswort\n"
        "=========================\n\n"
        "  Benutzer:  %s\n"
        "  Passwort:  %s\n\n"
        "Gueltig bis %s, genau einmal benutzbar.\n"
        "Nach dem Anmelden fragt DocuSort sofort nach einem neuen Passwort,\n"
        "und diese Datei verschwindet von selbst.\n\n"
        "Hat das niemand angefordert? Dann loesche die Datei einfach —\n"
        "es wurde nichts geaendert, das bisherige Passwort gilt weiter.\n"
        % (benutzer, code,
           _dt.datetime.fromtimestamp(bis).strftime("%d.%m.%Y %H:%M")))
    ziel.write_text(text, encoding="utf-8")
    try:
        # Nur der Besitzer des Dienstes soll sie lesen muessen.
        _os.chmod(ziel, 0o600)
    except OSError:
        pass
    return str(ziel)


def _pfad(ordner):
    from pathlib import Path as _P
    return _P(ordner)


def _datei_weg(ordner) -> None:
    try:
        (_pfad(ordner) / DATEINAME).unlink()
    except OSError:
        pass


def einmalpasswort_anfordern(db, config_dir=None) -> dict:
    """Wuerfelt ein Einmalpasswort, legt es dem Besitzer hin, aendert NICHTS.

    Zuerst ueber einen eingerichteten Kanal (Telegram, E-Mail). Gibt es
    keinen — oder scheitert der Versand —, wird die Datei im Konfigordner
    geschrieben. 🔴 Ohne BEIDES waere der Knopf fuer die meisten Leute tot.

    Rueckgabe: {"ok": True, "weg": "kanal"|"datei", ...} oder
               {"ok": False, "grund": "..."}
    """
    from . import notifier

    alt = _lesen(db)
    jetzt = int(_time.time())
    if alt and jetzt - int(alt.get("erzeugt") or 0) < _SPERRE_S:
        return {"ok": False, "grund": "zu_frueh",
                "warten_s": _SPERRE_S - (jetzt - int(alt["erzeugt"]))}

    leute = db.user_list()
    verwalter = [u for u in leute if u.get("role") == "admin" and u.get("is_active")]
    if not verwalter:
        return {"ok": False, "grund": "kein_konto"}
    ziel = verwalter[0]

    code = neues_passwort(10)
    # 🔑 Gespeichert wird nur der HASH. Wer die Datenbank liest, bekommt den
    #    Code nicht — und wer die Datenbank liest, braucht ihn ohnehin nicht.
    _schreiben(db, {"benutzer": ziel["username"],
                    "hash": auth.hash_password(code),
                    "erzeugt": jetzt, "ablauf": jetzt + _GUELTIG_S,
                    "versuche": 0})

    try:
        disp = notifier.get_dispatcher()
        kanaele = disp.channels_summary()
    except Exception:          # noqa: BLE001
        kanaele = []
    ergebnis = [] if not kanaele else disp.send_now(notifier.NotificationEvent(
        kind="password_reset",
        title="DocuSort — Einmalpasswort",
        body=("Jemand hat auf der Anmeldeseite ein Einmalpasswort angefordert.\n\n"
              "Benutzer:  %s\n"
              "Passwort:  %s\n\n"
              "Gueltig 15 Minuten, genau einmal benutzbar. Nach dem Anmelden "
              "fragt DocuSort sofort nach einem neuen Passwort.\n\n"
              "Warst du das nicht? Dann ignoriere die Nachricht — es wurde "
              "nichts geaendert, dein bisheriges Passwort gilt weiter."
              % (ziel["username"], code))))
    geschafft = [r["channel"] for r in ergebnis if r.get("ok")]
    if geschafft:
        return {"ok": True, "weg": "kanal", "kanaele": geschafft,
                "benutzer": ziel["username"], "gueltig_min": _GUELTIG_S // 60}

    # 🔴 Kein Kanal, oder der Versand ist gescheitert. Dann der zweite Weg —
    #    und NICHT einfach aufgeben: genau hier stand der Benutzer, der nicht
    #    mehr hineinkam.
    fehler = "; ".join("%s: %s" % (r.get("channel"), r.get("error"))
                       for r in ergebnis if not r.get("ok"))
    if config_dir is None:
        _schreiben(db, None)
        return {"ok": False, "grund": "versand", "fehler": fehler[:200]}
    try:
        ort = _in_datei_legen(config_dir, ziel["username"], code,
                              jetzt + _GUELTIG_S)
    except OSError as exc:
        _schreiben(db, None)
        return {"ok": False, "grund": "datei", "fehler": str(exc)[:200]}
    return {"ok": True, "weg": "datei", "datei": ort, "dateiname": DATEINAME,
            "benutzer": ziel["username"], "gueltig_min": _GUELTIG_S // 60,
            "kanal_fehler": fehler[:200]}


def einmalpasswort_einloesen(db, benutzername: str, passwort: str,
                             config_dir=None) -> dict | None:
    """Passt dieses Passwort zum laufenden Einmalcode? Dann einloesen.

    Gibt die Benutzerzeile zurueck (und verbrennt den Code) oder None.
    🔴 Der Code gilt genau einmal und nur fuer das Konto, fuer das er
       erzeugt wurde.
    """
    stand = _lesen(db)
    if not stand:
        return None
    jetzt = int(_time.time())
    if jetzt > int(stand.get("ablauf") or 0):
        _schreiben(db, None)
        if config_dir:
            _datei_weg(config_dir)
        return None
    if (benutzername or "").strip() != stand.get("benutzer"):
        return None
    if not auth.verify_password(passwort or "", stand.get("hash") or ""):
        stand["versuche"] = int(stand.get("versuche") or 0) + 1
        # Zu oft daneben: der Code ist verbrannt, nicht nur dieser Versuch.
        verbrannt = stand["versuche"] >= _MAX_VERSUCHE
        _schreiben(db, None if verbrannt else stand)
        if verbrannt and config_dir:
            _datei_weg(config_dir)
        return None
    _schreiben(db, None)                       # genau einmal
    if config_dir:
        _datei_weg(config_dir)
    reihe = db.user_by_name(stand["benutzer"])
    if not reihe:
        return None
    # Erst JETZT wird etwas geaendert: der Zugang wird einsatzfaehig gemacht
    # und ein neues Passwort verlangt.
    db.user_update(int(reihe["id"]), is_active=1, must_change_password=1)
    return db.user_by_name(stand["benutzer"])


# ---------------------------------------------------------------------------
# Die Postwache
#
# 🔴 WARUM SIE EINEN EIGENEN WEG BRAUCHT
# Das Postwache-Konto bekommt sein Passwort NICHT aus der Datenbank, sondern
# aus einer Geheimdatei im Konfigordner — und `postwache.konto_sichern()`
# setzt es bei JEDEM Start wieder durch. Wer nur den Hash in der Datenbank
# aendert, hat das Passwort bis zum naechsten Neustart geaendert und danach
# nicht mehr. Zurueckgesetzt werden muss also die QUELLE.
#
# 🔑 Und das Ergebnis muss man SEHEN: die Postwache auf der anderen Seite
# traegt dasselbe Wort, sonst kommt sie nicht mehr herein.

def postwache_zuruecksetzen(db, config_dir) -> tuple[str, str]:
    """Wuerfelt das Kopplungswort neu und zieht das Konto nach.

    Gibt (Benutzername, neues Wort). Das Wort muss danach auch in der
    Postwache eingetragen werden — es ist dasselbe auf beiden Seiten.
    """
    import os
    import secrets as _s
    from . import postwache as _pw

    if (os.environ.get(_pw.UMGEBUNG) or "").strip():
        # 🔴 Steht es in der Umgebung, gewinnt die Umgebung bei jedem Start.
        #    Ein neues Wort hier waere nach dem naechsten Neustart weg — also
        #    lieber ehrlich abbrechen als stillschweigend nichts tun.
        raise RuntimeError(
            "Das Kopplungswort kommt aus der Umgebungsvariable %s. "
            "Dort aendern (z. B. in der .env) und neu starten." % _pw.UMGEBUNG)
    neu = _s.token_urlsafe(24)
    _pw._merken(_pw._pfad(config_dir), neu)
    _pw.konto_sichern(db, config_dir)
    return _pw.BENUTZER, neu
