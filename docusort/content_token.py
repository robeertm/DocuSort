# -*- coding: utf-8 -*-
"""Das Inhalts-Token fuer die nur lesende Inhalts-Tuer `/api/content/…` (1.7.0).

    python3 -m docusort.content_token new       # neues Token, EINMAL auf stdout
    python3 -m docusort.content_token status    # an / aus — nie das Token
    python3 -m docusort.content_token off       # Inhalts-Tuer abschalten

🔑 WARUM EIN EIGENES TOKEN
Die Inhalts-Tuer gibt den Text eines Dokuments heraus — viel mehr als die
Liste der Lese-Tuer (`/api/readonly/…`), die nur Eckdaten zeigt. Darum ein
zweites, getrenntes Token in einer eigenen Datei: das eine oeffnet die andere
Tuer nie, und jede laesst sich fuer sich abschalten.

Gespeichert wird wie beim Lese-Token nur der SHA-256 (0600, Konfigurations-
ordner); die Mechanik steht in `readonly_token.py`.

🔴 `new` schreibt AUF STDOUT NUR DAS TOKEN (es wird in eine Datei auf einer
   anderen Maschine umgeleitet). Erklaerungen gehen nach stderr.
"""
from __future__ import annotations

import sys
from pathlib import Path

from . import readonly_token as _rt

FILENAME = "content_token.json"


def token_path(config_dir: Path | str | None = None) -> Path:
    return _rt.token_path(config_dir, FILENAME)


def is_enabled(config_dir: Path | str | None = None) -> bool:
    return _rt.is_enabled(config_dir, FILENAME)


def verify(token: str, config_dir: Path | str | None = None) -> bool:
    return _rt.verify(token, config_dir, FILENAME)


def new_token(config_dir: Path | str | None = None) -> str:
    return _rt.new_token(config_dir, FILENAME)


def disable(config_dir: Path | str | None = None) -> bool:
    return _rt.disable(config_dir, FILENAME)


def created_at(config_dir: Path | str | None = None) -> str:
    return _rt.created_at(config_dir, FILENAME)


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(
        prog="python3 -m docusort.content_token",
        description="Manage the token of the read-only content API "
                    "(GET /api/content/text, GET /api/content/search).")
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
        # 🔴 stdout traegt NUR das Token.
        sys.stdout.write(token + "\n")
        sys.stdout.flush()
        print("content API: on — the token above is shown only once; "
              "any previous content token no longer works", file=sys.stderr)
        return 0
    if args.action == "off":
        war = disable(args.config_dir)
        print("content API: off" + ("" if war else " (it was already off)"))
        return 0
    if is_enabled(args.config_dir):
        seit = created_at(args.config_dir)
        print("content API: on" + (" (token created %s)" % seit if seit else ""))
    else:
        print("content API: off")
    return 0


if __name__ == "__main__":
    sys.exit(main())
