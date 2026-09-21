"""Seitenbilder für die Vorschau — eine PDF-Seite als PNG.

Warum das sein muss: Die Vorschau lag als `<iframe src="…file#view=Fit">`
auf der Seite. Auf dem Schreibtisch zeigt Chrome darin seinen eigenen
PDF-Betrachter und passt die Seite ein; **iOS-Safari ignoriert den
Fragment-Teil** und stellt das Dokument in Originalgröße dar — sichtbar
war die linke obere Ecke, der Rest abgeschnitten (Wunsch: „PDF Vorschau
nicht passend im Vorschaufenster").

Ein Bild kennt dieses Problem nicht: `width: 100%` passt immer.

Gerendert wird mit **pdftoppm** aus Poppler — dasselbe Paket, das für OCR
ohnehin installiert sein muss (`poppler-utils`). Fehlt es, liefert
`render_page` `None` und die Oberfläche fällt auf den iframe zurück.

Die Bilder liegen neben der Datenbank in `preview-cache/` und tragen den
Änderungszeitpunkt der Quelldatei im Namen: wird ein Dokument ersetzt,
entsteht ein neuer Name und das alte Bild wird beim nächsten Aufräumen
gelöscht, statt veraltet weiterzuleben.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Breite des gerenderten Bildes. 1 400 px reichen für ein Telefon mit
# dreifacher Pixeldichte (420 pt × 3) und bleiben unter 300 KB je Seite.
DEFAULT_WIDTH = 1400
MAX_WIDTH = 2400
MAX_PAGES = 30          # mehr rendert niemand auf einem Telefon
RENDER_TIMEOUT = 25     # Sekunden je Seite
CACHE_DIRNAME = "preview-cache"
CACHE_MAX_AGE = 60 * 60 * 24 * 30   # 30 Tage ohne Zugriff → weg


def available() -> bool:
    """Liegt Poppler auf dem PATH?"""
    return shutil.which("pdftoppm") is not None


def _cache_dir(db_path: Path) -> Path | None:
    """Verzeichnis neben der Datenbank — oder None, wenn dort nicht
    geschrieben werden darf.

    🔴 Ein nicht beschreibbarer Pfad (schreibgeschützt eingehängtes
    `/data`, falsch gesetzter `paths.db`) hat die Anfrage mit einem
    500er beendet. Eine Vorschau ist Beiwerk: schlägt sie fehl, fällt
    die Seite auf den eingebetteten Betrachter zurück.
    """
    try:
        d = db_path.parent / CACHE_DIRNAME
        d.mkdir(parents=True, exist_ok=True)
        return d
    except OSError as exc:
        logger.info("preview: kein Zwischenspeicher unter %s (%s)", db_path.parent, exc)
        return None


def _cache_name(src: Path, page: int, width: int) -> str:
    """Name aus Pfad, Änderungszeit, Seite und Breite.

    Die Änderungszeit gehört in den Schlüssel: sonst zeigt die Vorschau
    nach einem erneuten OCR-Lauf weiter die alte Seite.
    """
    try:
        stamp = int(src.stat().st_mtime)
    except OSError:
        stamp = 0
    key = f"{src}|{stamp}|{page}|{width}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32] + ".png"


def render_page(src: Path, db_path: Path, page: int = 1,
                width: int = DEFAULT_WIDTH) -> Path | None:
    """Rendert eine Seite und gibt den Pfad zum PNG zurück (oder None).

    Ein zweiter Aufruf für dieselbe Seite liefert das zwischengespeicherte
    Bild, ohne Poppler zu starten.
    """
    if not src.is_file() or src.suffix.lower() != ".pdf":
        return None
    page = max(1, min(int(page), MAX_PAGES))
    width = max(200, min(int(width), MAX_WIDTH))

    cache = _cache_dir(db_path)
    if cache is None:
        return None
    out = cache / _cache_name(src, page, width)
    if out.is_file() and out.stat().st_size > 0:
        # Zugriffszeit anfassen, damit das Aufräumen weiß, was in Gebrauch ist.
        try:
            out.touch()
        except OSError:
            pass
        return out

    if not available():
        logger.info("preview: pdftoppm fehlt — Vorschaubild nicht möglich")
        return None

    # pdftoppm hängt die Endung selbst an, deshalb der Stamm ohne „.png".
    stem = out.with_suffix("")
    cmd = [
        "pdftoppm", "-png", "-singlefile",
        "-f", str(page), "-l", str(page),
        "-scale-to-x", str(width), "-scale-to-y", "-1",   # -1 = Seitenverhältnis halten
        str(src), str(stem),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=RENDER_TIMEOUT)
    except subprocess.TimeoutExpired:
        logger.warning("preview: pdftoppm lief zu lange für %s (Seite %d)", src.name, page)
        return None
    except subprocess.CalledProcessError as exc:
        # Verschlüsselte PDFs und kaputte Dateien landen hier — die
        # Oberfläche fällt dann auf den iframe zurück.
        err = (exc.stderr or b"").decode("utf-8", "replace").strip()[:200]
        logger.info("preview: pdftoppm konnte %s nicht rendern: %s", src.name, err)
        return None
    except OSError as exc:
        logger.warning("preview: pdftoppm nicht startbar: %s", exc)
        return None

    return out if out.is_file() else None


def cleanup(db_path: Path, max_age: int = CACHE_MAX_AGE) -> int:
    """Löscht Vorschaubilder, die lange niemand mehr geöffnet hat."""
    d = db_path.parent / CACHE_DIRNAME
    if not d.is_dir():
        return 0
    cutoff = time.time() - max_age
    n = 0
    for f in d.glob("*.png"):
        try:
            if f.stat().st_atime < cutoff:
                f.unlink()
                n += 1
        except OSError:
            continue
    return n
