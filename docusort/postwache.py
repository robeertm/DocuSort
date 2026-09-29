# -*- coding: utf-8 -*-
"""Die Kopplung mit der Postwache — ohne dass der Kunde etwas tun muss.

🔑 Die Vorgabe: wer beide Programme installiert hat, findet die Verbindung
zwischen ihnen bereits gesetzt — hoechstens ein Klick, und ohne dass jemand
etwas einrichten muss.

Die Postwache uebergibt Anhaenge aus Mails durch `POST /upload` — dieselbe
Vordertuer, die auch der Browser benutzt. Dafuer braucht sie ein Konto. Bisher
musste ein Mensch es anlegen: Benutzer erfinden, Passwort erzeugen, beides auf
der Postwache-Seite eintragen. Genau das faellt hier weg.

Es gibt zwei Wege, und sie unterscheiden sich nur darin, ob beide Programme
zusammen installiert wurden:

  · **Zusammen installiert** (`docker-compose.both.yml` oder der Installer):
    beide bekommen dasselbe Kopplungswort aus der gemeinsamen `.env`. DocuSort
    legt das Konto damit an, die Postwache traegt es damit ein. **Null Klicks.**

  · **Getrennt installiert:** DocuSort zeigt in den Einstellungen eine Zeile zum
    Kopieren, die Postwache hat ein Feld zum Einfuegen. **Ein Klick je Seite.**

🔑 Das Konto bekommt den Rang `deliver` — es darf `POST /upload` und
`GET /api/status/<name>`, und sonst NICHTS. Ein `user` haette auch die
Bibliothek und die Finanzen lesen duerfen; das waere bei einer Kopplung, die
von selbst passiert, eine Entscheidung, die niemand getroffen hat.

🔴 Das Kopplungswort ist ein Geheimnis. Es liegt in der Konfiguration mit 0600
und wird in Meldungen nie ausgeschrieben.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import secrets
from pathlib import Path

from . import auth as _auth

logger = logging.getLogger("docusort.postwache")

BENUTZER = "Postwache"
DATEI = "postwache_password"          # 0600, neben den anderen Geheimnissen
UMGEBUNG = "DOCUSORT_POSTWACHE_PASSWORD"
MARKE = "pw1."                        # damit die Postwache die Zeile erkennt


def _pfad(config_dir: str | Path) -> Path:
    return Path(config_dir) / DATEI


def kennwort(config_dir: str | Path) -> str:
    """Das Kopplungswort: aus der Umgebung, sonst gemerkt, sonst neu erzeugt.

    Die Umgebung hat Vorrang — so kann eine gemeinsame `.env` beide Seiten
    setzen, und ein Wechsel dort wirkt beim naechsten Start.
    """
    aus_umgebung = (os.environ.get(UMGEBUNG) or "").strip()
    p = _pfad(config_dir)
    if aus_umgebung:
        if _gemerkt(p) != aus_umgebung:
            _merken(p, aus_umgebung)
        return aus_umgebung
    gemerkt = _gemerkt(p)
    if gemerkt:
        return gemerkt
    neu = secrets.token_urlsafe(24)
    _merken(p, neu)
    return neu


def _gemerkt(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""
    except OSError as exc:
        logger.warning("Kopplungswort nicht lesbar (%s): %s", p, exc)
        return ""


def _merken(p: Path, wort: str) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(wort, encoding="utf-8")
        try:
            os.chmod(p, 0o600)
        except OSError:
            pass
    except OSError as exc:
        logger.warning("Kopplungswort nicht speicherbar (%s): %s", p, exc)


def konto_sichern(db, config_dir: str | Path) -> str:
    """Legt das Postwache-Konto an oder zieht es nach. Gibt das Wort zurueck.

    Wird bei JEDEM Start gerufen und darf darum nichts umsonst tun:

    🔑 Ob das Passwort noch stimmt, wird GEPRUEFT, nicht neu gesetzt. Ein
    blindes Neusetzen bei jedem Start waere nicht falsch, aber es wuerfe die
    laufende Sitzung der Postwache jedes Mal weg — sie muesste sich nach jedem
    Neustart neu anmelden, und in den Meldungen stuende ein Fehler, der keiner
    ist.
    """
    wort = kennwort(config_dir)
    reihe = db.user_by_name(BENUTZER)
    if reihe is None:
        db.user_create(BENUTZER, _auth.hash_password(wort), _auth.ROLE_DELIVER,
                       display_name=BENUTZER, must_change=False)
        logger.info("Postwache-Konto angelegt (Rang %s)", _auth.ROLE_DELIVER)
        return wort

    aenderungen: dict = {}
    if not _auth.verify_password(wort, reihe.get("password_hash") or ""):
        aenderungen["password_hash"] = _auth.hash_password(wort)
    # 🔴 Ein Konto, das es schon als `user` gab (von Hand angelegt, vor 0.64.0),
    #    wird auf den schmalen Rang heruntergezogen. Das ist Absicht: es soll
    #    nicht mehr koennen, als es braucht.
    if reihe.get("role") != _auth.ROLE_DELIVER:
        aenderungen["role"] = _auth.ROLE_DELIVER
    if not reihe.get("is_active"):
        aenderungen["is_active"] = 1
    if reihe.get("must_change_password"):
        aenderungen["must_change_password"] = 0
    if aenderungen:
        db.user_update(int(reihe["id"]), **aenderungen)
        logger.info("Postwache-Konto nachgezogen: %s",
                    ", ".join(sorted(aenderungen)))
    return wort


def kopplung(db, config_dir: str | Path, basis: str) -> dict:
    """Was die Postwache braucht — als Zeile zum Kopieren.

    `basis` ist die Adresse, unter der DIESE Anfrage hereinkam. Das ist die
    beste Schaetzung, die DocuSort ueber sich selbst machen kann: mehr weiss es
    nicht, denn es sieht nur, wie man es gerade erreicht hat. Stimmt sie fuer
    die Postwache nicht (anderes Netz, eigener Container), sucht die Postwache
    selbst weiter — die Zeile traegt dann trotzdem Benutzer und Wort.
    """
    wort = konto_sichern(db, config_dir)
    inhalt = {"url": basis.rstrip("/"), "benutzer": BENUTZER, "passwort": wort}
    roh = json.dumps(inhalt, separators=(",", ":")).encode("utf-8")
    zeile = MARKE + base64.urlsafe_b64encode(roh).decode("ascii").rstrip("=")
    return {"benutzer": BENUTZER, "url": inhalt["url"], "zeile": zeile}
