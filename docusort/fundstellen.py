"""Wo im PDF steht das gesuchte Wort — Seite und Rechteck.

Die Suche findet ein Dokument an seinem INHALT. Danach steht man vor einem
zwanzigseitigen Bescheid und sucht die Stelle von Hand. Dieses Modul sagt, wo
sie ist; die Seite malt ein gelbes Feld darueber.

🔑 WARUM POPPLER UND NICHT DER GESPEICHERTE TEXT
`documents.extracted_text` weiss, DASS das Wort vorkommt — nicht WO. Die
Koordinaten kennt nur, wer das PDF selbst liest. `pdftotext -bbox-layout`
liefert je Seite ihre Masse und je Wort ein Rechteck; es steckt in
`poppler-utils`, demselben Paket, aus dem schon die Seitenbilder kommen
(`preview.py`). Es kommt also keine Abhaengigkeit dazu.

🔴 MASSE RELATIV, NICHT IN PUNKTEN. Das Seitenbild wird in einer anderen
Breite gerendert als die PDF-Seite misst, und die Seite skaliert es noch
einmal auf ihre Spalte. Ein Rechteck in Punkten laege ueberall, nur nicht auf
dem Wort. Darum alles als Anteil der Seite (0..1) — das ueberlebt jede
Skalierung.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from .aehnlichkeit import schlicht as _schlicht

logger = logging.getLogger("docusort.fundstellen")

# Mehr als das braucht niemand, und es haelt die Antwort klein.
MAX_TREFFER = 60
# Ein PDF mit hunderten Seiten darf die Seite nicht aufhalten.
ZEITGRENZE_S = 20


def verfuegbar() -> bool:
    return shutil.which("pdftotext") is not None


def _woerter(roh: str) -> list[str]:
    """Dieselbe Zerlegung wie die Suche: alles, was ein Wort sein kann."""
    import re
    return [w for w in re.findall(r"[0-9A-Za-zÀ-ÿ_]+", roh or "") if w]


def finde(pdf: Path, frage: str, *, max_treffer: int = MAX_TREFFER) -> dict[str, Any]:
    """Alle Stellen, an denen eines der gesuchten Woerter steht.

    Rueckgabe:
        {"verfuegbar": bool, "seiten": int,
         "treffer": [{"seite": 1, "x": .09, "y": .07, "b": .10, "h": .02, "wort": "…"}]}

    🔑 Dieselbe Trefferregel wie die Suchleiter: erst am Wortanfang, dann
    irgendwo im Wort. Sonst faende die Liste ein Dokument, und die Seite
    faende darin nichts — zwei Antworten auf dieselbe Frage.
    """
    gesucht = [_schlicht(w) for w in _woerter(frage)]
    gesucht = [w for w in gesucht if w]
    leer: dict[str, Any] = {"verfuegbar": verfuegbar(), "seiten": 0, "treffer": []}
    if not gesucht or not verfuegbar() or not pdf.exists():
        return leer
    try:
        roh = subprocess.run(
            ["pdftotext", "-bbox-layout", str(pdf), "-"],
            capture_output=True, timeout=ZEITGRENZE_S, check=False)
    except (subprocess.TimeoutExpired, OSError) as exc:
        logger.info("fundstellen: pdftotext %s: %s", pdf.name, exc)
        return leer
    if roh.returncode != 0 or not roh.stdout:
        return leer
    try:
        # 🔴 Die Ausgabe ist XHTML MIT Namensraum — ohne das findet
        # `iter("page")` nichts und die Antwort waere stillschweigend leer.
        baum = ET.fromstring(roh.stdout)
    except ET.ParseError as exc:
        logger.info("fundstellen: Ausgabe von pdftotext unlesbar (%s): %s", pdf.name, exc)
        return leer

    def ohne_raum(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    treffer: list[dict[str, Any]] = []
    seiten = 0
    for nr, seite in enumerate(
            (e for e in baum.iter() if ohne_raum(e.tag) == "page"), start=1):
        seiten = nr
        try:
            breite = float(seite.get("width") or 0)
            hoehe = float(seite.get("height") or 0)
        except ValueError:
            continue
        if breite <= 0 or hoehe <= 0:
            continue
        for wort in (e for e in seite.iter() if ohne_raum(e.tag) == "word"):
            text = (wort.text or "").strip()
            if not text:
                continue
            s = _schlicht(text)
            if not s:
                continue
            # vorn > drin, wie in der Suche
            if not any(s.startswith(g) or g in s for g in gesucht):
                continue
            try:
                x0 = float(wort.get("xMin") or 0); y0 = float(wort.get("yMin") or 0)
                x1 = float(wort.get("xMax") or 0); y1 = float(wort.get("yMax") or 0)
            except ValueError:
                continue
            if x1 <= x0 or y1 <= y0:
                continue
            treffer.append({
                "seite": nr,
                # 🔑 yMin zaehlt bei pdftotext von OBEN — dieselbe Richtung,
                # in der CSS `top` zaehlt. Kein Umdrehen noetig (nachgemessen).
                "x": round(x0 / breite, 5), "y": round(y0 / hoehe, 5),
                "b": round((x1 - x0) / breite, 5), "h": round((y1 - y0) / hoehe, 5),
                "wort": text[:60],
            })
            if len(treffer) >= max_treffer:
                return {"verfuegbar": True, "seiten": seiten, "treffer": treffer}
    return {"verfuegbar": True, "seiten": seiten, "treffer": treffer}
