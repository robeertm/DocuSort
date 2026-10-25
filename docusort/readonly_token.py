# -*- coding: utf-8 -*-
"""Das Lese-Token fuer die nur lesende Schnittstelle `/api/readonly/documents`.

    python3 -m docusort.readonly_token new       # neues Token, EINMAL auf stdout
    python3 -m docusort.readonly_token status    # an / aus — nie das Token
    python3 -m docusort.readonly_token off       # Schnittstelle abschalten

🔑 WAS HIER GESPEICHERT WIRD — UND WAS NICHT
Gespeichert wird nur der SHA-256 des Tokens, in einer Datei mit den Rechten
0600 im Konfigurationsordner. Das Token selbst steht nirgends: nicht auf der
Platte, nicht im Protokoll, nicht in einer Umgebungsvariable. Wer die Datei
liest, kann damit keine Anfrage stellen.

🔑 WARUM KEINE OBERFLAECHE
Wer `docker exec` auf dem Wirt ausfuehren darf, hat ohnehin Zugriff auf jede
Datei. Ein Knopf in den Einstellungen waere ein zweiter Weg zum selben Ziel —
und einer, der ueber das Netz erreichbar ist.

🔴 `new` schreibt AUF STDOUT NUR DAS TOKEN. Der gedachte Aufruf leitet stdout
   direkt in eine Datei auf einer anderen Maschine weiter; jedes weitere
   Zeichen dort waere Teil des Tokens und die Anmeldung schluege fehl.
   Erklaerungen gehen nach stderr.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sys
from datetime import datetime
from pathlib import Path

FILENAME = "readonly_token.json"


def token_path(config_dir: Path | str | None = None) -> Path:
    if config_dir is None:
        from .config import DEFAULT_CONFIG_DIR
        config_dir = DEFAULT_CONFIG_DIR
    return Path(config_dir) / FILENAME


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def stored_hash(config_dir: Path | str | None = None) -> str:
    """Der gespeicherte Hash, oder "" wenn die Schnittstelle aus ist.

    🔴 Bei JEDER Anfrage frisch gelesen, nie gemerkt: sonst wirkte `off` erst
       nach einem Neustart, und ein abgeschaltetes Token liefe weiter.
       Eine unlesbare oder kaputte Datei zaehlt als „aus" — lieber eine
       verschlossene Tuer als eine, deren Schloss niemand pruefen kann.
    """
    try:
        daten = json.loads(token_path(config_dir).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    wert = str(daten.get("sha256", "")) if isinstance(daten, dict) else ""
    if len(wert) != 64 or any(c not in "0123456789abcdef" for c in wert):
        return ""
    return wert


def is_enabled(config_dir: Path | str | None = None) -> bool:
    return bool(stored_hash(config_dir))


def verify(token: str, config_dir: Path | str | None = None) -> bool:
    """Passt das Token? Vergleich in konstanter Zeit."""
    gespeichert = stored_hash(config_dir)
    if not gespeichert or not token:
        return False
    return hmac.compare_digest(_hash(token), gespeichert)


def created_at(config_dir: Path | str | None = None) -> str:
    try:
        daten = json.loads(token_path(config_dir).read_text(encoding="utf-8"))
        return str(daten.get("created", "")) if isinstance(daten, dict) else ""
    except (OSError, ValueError):
        return ""


def new_token(config_dir: Path | str | None = None) -> str:
    """Wuerfelt ein neues Token, speichert seinen Hash und gibt es zurueck.

    Ein vorhandenes Token wird damit ungueltig.
    """
    token = secrets.token_urlsafe(32)          # 256 Bit
    ziel = token_path(config_dir)
    ziel.parent.mkdir(parents=True, exist_ok=True)
    inhalt = json.dumps({"sha256": _hash(token),
                         "created": datetime.now().isoformat(timespec="seconds")})
    # 🔑 Erst eine Nebendatei mit 0600 anlegen, dann austauschen. Die Rechte
    #    stehen VOR dem ersten Byte fest — es gibt keinen Moment, in dem die
    #    Datei fuer andere lesbar ist, und kein halb geschriebenes Ziel.
    neben = ziel.with_name(ziel.name + ".neu")
    try:
        neben.unlink()
    except FileNotFoundError:
        pass
    fd = os.open(str(neben), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, inhalt.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(str(neben), 0o600)
    os.replace(str(neben), str(ziel))
    return token


def disable(config_dir: Path | str | None = None) -> bool:
    """Schaltet die Schnittstelle ab. True, wenn vorher ein Token da war."""
    try:
        token_path(config_dir).unlink()
        return True
    except FileNotFoundError:
        return False


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(
        prog="python3 -m docusort.readonly_token",
        description="Manage the token of the read-only JSON API "
                    "(GET /api/readonly/documents).")
    parser.add_argument("action", choices=("new", "status", "off"),
                        help="new: create a token and print it once (replaces "
                             "the old one) · status: on/off · off: disable the API")
    parser.add_argument("--config-dir", default=None,
                        help="configuration directory (default: "
                             "$DOCUSORT_CONFIG_DIR or /app/config)")
    args = parser.parse_args(argv)

    if args.action == "new":
        try:
            token = new_token(args.config_dir)
        except OSError as exc:
            print("cannot write the token file: %s" % exc, file=sys.stderr)
            return 2
        # 🔴 stdout traegt NUR das Token (siehe oben).
        sys.stdout.write(token + "\n")
        sys.stdout.flush()
        print("read-only API: on — the token above is shown only once; "
              "any previous token no longer works", file=sys.stderr)
        return 0
    if args.action == "off":
        war = disable(args.config_dir)
        print("read-only API: off" + ("" if war else " (it was already off)"))
        return 0
    if is_enabled(args.config_dir):
        seit = created_at(args.config_dir)
        print("read-only API: on" + (" (token created %s)" % seit if seit else ""))
    else:
        print("read-only API: off")
    return 0


if __name__ == "__main__":
    sys.exit(main())
