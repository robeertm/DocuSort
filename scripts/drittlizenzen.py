#!/usr/bin/env python3
"""Writes THIRD-PARTY-NOTICES.md: every third-party component with its licence.

Three parts:
1. the Python packages of the hash-locked requirements.txt - licence and
   licence texts are read from the installed package metadata;
2. the front-end libraries that ship inside the app (htmx, Alpine.js, the
   pre-compiled Tailwind stylesheet) - curated below, texts live next to the
   files in docusort/web/static/vendor/;
3. the software the Docker image adds on top (base image, Debian packages,
   Tailscale) - curated below.

Usage (never install into a shared environment - use a throwaway venv):

    python3 -m venv /tmp/ds-notices
    # requirements.txt carries hashes; for the metadata read it is enough to
    # install the exact versions without them:
    python3 -I scripts/drittlizenzen.py --pins > /tmp/ds-pins.txt
    /tmp/ds-notices/bin/pip install --no-deps -r /tmp/ds-pins.txt
    /tmp/ds-notices/bin/python -I scripts/drittlizenzen.py > THIRD-PARTY-NOTICES.md

tests/test_third_party_notices.py fails when a package of requirements.txt, or
a vendored library of base.html, is missing from THIRD-PARTY-NOTICES.md.
"""
from __future__ import annotations

import hashlib
import importlib.metadata as md
import importlib.util
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENDOR = ROOT / "docusort" / "web" / "static" / "vendor"

# Packages whose metadata carries free text instead of an SPDX expression.
LIZENZ_UEBERSCHREIBEN = {
    "distro": "Apache-2.0",
    "google-auth": "Apache-2.0",
    "jinja2": "BSD-3-Clause",
    "pyasn1-modules": "BSD-2-Clause",
    "pytesseract": "Apache-2.0",
    "reportlab": "BSD-3-Clause",
    "tenacity": "Apache-2.0",
    "uvloop": "MIT OR Apache-2.0",
}
# Free-text licence names -> SPDX.
NORMALISIERT = {
    "apache license, version 2.0": "Apache-2.0",
    "apache 2.0": "Apache-2.0",
    "apache license 2.0": "Apache-2.0",
    "mit license": "MIT",
    "bsd": "BSD-3-Clause",
}

VENDORED = [
    # (name, version, SPDX, copyright, what, licence file)
    ("htmx", "2.0.3", "0BSD", "Big Sky Software and contributors",
     "partial page updates; served as `/static/vendor/htmx-2.0.3.min.js`",
     "htmx-LICENSE.txt"),
    ("Alpine.js", "3.14.3", "MIT", "Caleb Porzio and contributors",
     "small interactive widgets; served as `/static/vendor/alpine-3.14.3.min.js`",
     "alpine-LICENSE.txt"),
]

TAILWIND = (
    "**Tailwind CSS** (MIT, Copyright (c) Tailwind Labs, Inc.) - the stylesheet "
    "`docusort/web/static/tailwind.css` is compiled from `scripts/build/input.css` "
    "with the Tailwind CSS standalone command-line tool (a build-time tool; it is "
    "not part of the app or the image). The compiled file contains Tailwind's base "
    "reset, which is derived from modern-normalize (MIT, Copyright (c) Sindre "
    "Sorhus). No web fonts are shipped; the interface uses the fonts of the "
    "user's system."
)

IMAGE = [
    ("python:3.12-slim (Docker base image)",
     "Debian GNU/Linux and CPython; Python Software Foundation License 2.0 and the "
     "licences of the Debian packages inside"),
    ("Tesseract OCR (`tesseract-ocr`, `-deu`, `-eng`)", "Apache-2.0"),
    ("OCRmyPDF (`ocrmypdf`)", "MPL-2.0"),
    ("Ghostscript (`ghostscript`)", "AGPL-3.0-or-later"),
    ("Poppler (`poppler-utils`)", "GPL-2.0-or-later"),
    ("qpdf (`qpdf`)", "Apache-2.0"),
    ("pngquant (`pngquant`)", "GPL-3.0-or-later"),
    ("unpaper (`unpaper`)", "GPL-2.0-or-later"),
    ("Tailscale (`tailscale`, `tailscaled`)", "BSD-3-Clause, Copyright (c) Tailscale Inc. "
     "and contributors"),
]


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _check_pins():
    spec = importlib.util.spec_from_file_location("check_pins", ROOT / "scripts" / "check_pins.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def pins() -> dict[str, str]:
    entries, problems = _check_pins().lock_entries((ROOT / "requirements.txt").read_text(encoding="utf-8"))
    if problems:
        sys.exit("requirements.txt is not a clean lock: " + "; ".join(problems))
    return entries


def spdx(name: str, dist: md.Distribution) -> str:
    if name in LIZENZ_UEBERSCHREIBEN:
        return LIZENZ_UEBERSCHREIBEN[name]
    meta = dist.metadata
    raw = (meta.get("License-Expression") or "").strip()
    if not raw:
        raw = " ".join((meta.get("License") or "").split())
    if not raw:
        sys.exit(f"no licence in the metadata of {name} - add it to LIZENZ_UEBERSCHREIBEN")
    return NORMALISIERT.get(raw.lower(), raw)


def lizenzdateien(dist: md.Distribution) -> list[tuple[str, str]]:
    out = []
    for f in dist.files or []:
        if re.search(r"(LICEN[CS]E|COPYING|NOTICE|AUTHORS)", f.name, re.I) and "/tests/" not in str(f):
            try:
                text = f.read_binary().decode("utf-8", errors="replace").strip()
            except OSError:
                continue
            if text:
                out.append((f.name, text))
    return out


def main() -> int:
    lock = pins()
    if "--pins" in sys.argv:
        for name, version in sorted(lock.items()):
            print(f"{name}=={version}")
        return 0

    zeilen, texte = [], {}
    for name, version in sorted(lock.items()):
        try:
            dist = md.distribution(name)
        except md.PackageNotFoundError:
            sys.exit(f"{name} is not installed in this environment - see the usage note at the top")
        if dist.version != version:
            sys.exit(f"{name}: installed {dist.version}, locked {version}")
        meta = dist.metadata
        url = meta.get("Home-page") or next(
            (u.split(",", 1)[1].strip() for u in meta.get_all("Project-URL") or []
             if u.split(",", 1)[0].strip().lower() in ("homepage", "source", "repository", "home")), "")
        zeilen.append(f"- `{name}` {version} - {spdx(name, dist)}" + (f" - <{url}>" if url else ""))
        for fname, text in lizenzdateien(dist):
            h = hashlib.sha256(text.encode()).hexdigest()
            texte.setdefault(h, (text, []))[1].append(f"{name} {version}")

    out = ["""# Third-party notices

DocuSort is built on the work of others. This file lists every third-party
component that is part of DocuSort - the Python packages, the front-end
libraries delivered with the app and the software added by the Docker image -
with its licence. DocuSort's own licence is in [LICENSE](LICENSE); it does not
limit the rights these licences grant for their components.

The file is generated by `scripts/drittlizenzen.py` from the hash-locked
`requirements.txt` and is checked by `tests/test_third_party_notices.py`.

## Python packages

Exact versions as locked in `requirements.txt`. Each entry gives the licence
declared by the package. The licence texts follow below. Packages under the
MPL-2.0 (`certifi`, `tqdm`) are used unmodified; their source code is available
from the project pages and from PyPI.

""", "\n".join(zeilen), "\n\n## Front-end libraries delivered with the app\n\n"
           "These files are part of the app and are served locally; the browser "
           "does not load any script or stylesheet from a third-party host.\n\n"]
    for name, version, lic, holder, what, fname in VENDORED:
        out.append(f"- **{name}** {version} - {lic}, Copyright (c) {holder} - {what}. "
                   f"Licence text: [docusort/web/static/vendor/{fname}](docusort/web/static/vendor/{fname})\n")
    out.append("\n" + TAILWIND + "\n\n## Docker image\n\n"
               "The Docker image installs the following software on top of the DocuSort "
               "code. It is installed from the Debian archive (or, for Tailscale, from "
               "the publisher's release archive) and keeps its own licence; the licence "
               "texts are in `/usr/share/doc/<package>/copyright` inside the image. "
               "They are called as separate programs and are not linked into DocuSort.\n\n")
    for name, lic in IMAGE:
        out.append(f"- {name} - {lic}\n")
    out.append("\n## Licence texts\n")
    for ort in sorted(VENDORED):
        out.append(f"\n---\n\nUsed by: {ort[0]} {ort[1]}\n\n```text\n"
                   f"{(VENDOR / ort[5]).read_text(encoding='utf-8').strip()}\n```\n")
    for text, wer in sorted(texte.values(), key=lambda t: (sorted(t[1])[0], t[0])):
        out.append("\n---\n\nUsed by: " + ", ".join(sorted(set(wer))) + "\n\n```text\n" + text + "\n```\n")
    sys.stdout.write("".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
