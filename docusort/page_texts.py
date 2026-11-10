# -*- coding: utf-8 -*-
"""Seitentexte vorab ablegen (1.8.0) — die Inhalts-Tuer liest nur noch die DB.

🔴 WARUM ES DAS GIBT
Die Inhalts-Tuer (`content_api.py`, 1.7.0) las die Textebene eines PDFs erst,
wenn jemand danach fragte — mit pypdf, Seite fuer Seite, in der Anfrage. Ein
16-MB-Scan brauchte so beim ersten Aufruf 9,5 s, eine Suche, deren Treffer
noch nie gelesen waren, 6,5 s. Danach war es schnell, aber nur bis zum
naechsten Neustart: der Zwischenspeicher lag im Arbeitsspeicher und hielt
32 Dateien.

🔑 WAS JETZT PASSIERT
  · Je Dokument liegen die Seitentexte in `document_pages`, dazu in
    `document_pages_src` ein Schluessel der Datei (Groesse + Aenderungszeit
    in ns) und die volle Seitenzahl.
  · Passt der Schluessel nicht mehr zur Datei auf der Platte (neu
    zusammengefuegt, ersetzt, neu erkannt), gilt der Eintrag als veraltet —
    die Tuer liest dann wie frueher selbst und legt das Ergebnis ab. Ein
    reines Umbenennen/Verschieben behaelt Groesse und Aenderungszeit und
    braucht darum nichts neu.
  · Ein Nachlauf im Hintergrund fuellt den Bestand und alles Neue: beim
    Einsortieren wird er angestossen, sonst schaut er alle paar Minuten nach.

🔴 LASTDECKEL
Der Nachlauf liest jedes PDF in einem EIGENEN Prozess mit niedrigster
Prioritaet (nice 19) und einer Zeitgrenze: pypdf ist reines Python, im
Webprozess wuerde es ueber das GIL jede Anfrage bremsen, und eine kaputte
Datei koennte haengen. Nach jedem Dokument wartet er mindestens so lange,
wie das Lesen gedauert hat (hoechstens die Haelfte eines Kerns).

🔑 Er laeuft nur, wenn die Inhalts-Tuer eingeschaltet ist — wer sie nicht
benutzt, bezahlt nichts dafuer. Abschalten: `DOCUSORT_PAGE_TEXTS=0`.

🔴 Die Texte sind dieselben, die schon in `documents.extracted_text` stehen;
maskiert wird wie bisher erst an der Tuer. Hier wird nichts protokolliert
ausser Zahlen und Dateinamen, nie ein Pfad oder ein Text.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("docusort.page_texts")

PAGES_MAX = 60                  # so viele Seiten werden gelesen und abgelegt
PAGE_STORE_MAX = 50_000         # Zeichen je abgelegter Seite (die Tuer gibt ohnehin hoechstens 12 000 heraus)
FAILED = -1                     # Seitenzahl-Marke: Datei liess sich nicht lesen

START_DELAY_S = 60.0            # erst nach dem Hochfahren anfangen
ROUND_S = 300.0                 # so oft schaut der Nachlauf nach Neuem
EXTRACT_TIMEOUT_S = 180.0       # Zeitgrenze je Datei
PAUSE_MIN_S = 1.0               # Pause nach jedem Dokument …
PAUSE_FACTOR = 1.0              # … mindestens so lang wie das Lesen dauerte
ENV_SWITCH = "DOCUSORT_PAGE_TEXTS"

_VISIBLE = ("d.deleted_at IS NULL AND d.category != '_csv_container' "
            "AND d.status != 'duplicate'")


# ── Tabellen ────────────────────────────────────────────────────────────────
def ensure_schema(conn) -> None:
    """Idempotent; wird aus `Database._migrate` gerufen."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS document_pages (
               doc_id INTEGER NOT NULL,
               seite  INTEGER NOT NULL,
               text   TEXT NOT NULL,
               PRIMARY KEY (doc_id, seite)
           )""")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS document_pages_src (
               doc_id      INTEGER PRIMARY KEY,
               src_key     TEXT NOT NULL,
               pages_total INTEGER NOT NULL,
               stored_at   TEXT NOT NULL
           )""")
    # 🔑 Ein endgueltig geloeschtes Dokument nimmt seine Seiten mit.
    conn.execute(
        """CREATE TRIGGER IF NOT EXISTS documents_pages_ad AFTER DELETE ON documents BEGIN
               DELETE FROM document_pages WHERE doc_id = old.id;
               DELETE FROM document_pages_src WHERE doc_id = old.id;
           END""")


def src_key(src: Path | None) -> str | None:
    """Groesse + Aenderungszeit (ns) der Datei — aendert sich der Inhalt, aendert sich der Schluessel."""
    if src is None:
        return None
    try:
        st = src.stat()
    except OSError:
        return None
    return "%d:%d" % (st.st_size, st.st_mtime_ns)


# ── Lesen der Textebene ─────────────────────────────────────────────────────
def extract(src: Path, max_pages: int = PAGES_MAX) -> tuple[int, list[str]]:
    """(volle Seitenzahl, Text der ersten `max_pages` Seiten). Wirft bei einer unlesbaren Datei."""
    from pypdf import PdfReader
    reader = PdfReader(str(src))
    if reader.is_encrypted:
        try:
            reader.decrypt("")
        except Exception:                               # noqa: BLE001
            pass
    gesamt = len(reader.pages)
    seiten = []
    for i, seite in enumerate(reader.pages):
        if i >= max_pages:
            break
        seiten.append((seite.extract_text() or "").strip())
    return gesamt, seiten


def extract_isolated(src: Path, timeout: float = EXTRACT_TIMEOUT_S) -> tuple[int, list[str]]:
    """Wie `extract`, aber in einem eigenen Prozess mit niedrigster Prioritaet
    und Zeitgrenze. Wirft bei Fehler oder Zeitueberschreitung."""
    wurzel = str(Path(__file__).resolve().parent.parent)
    env = dict(os.environ)
    env["PYTHONPATH"] = wurzel + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    lauf = subprocess.run(
        [sys.executable, "-m", "docusort.page_texts", "--extract", str(src)],
        capture_output=True, timeout=timeout, cwd=wurzel, env=env)
    if lauf.returncode != 0:
        raise RuntimeError("extractor exited with %d" % lauf.returncode)
    daten = json.loads(lauf.stdout.decode("utf-8"))
    return int(daten["total"]), [str(t) for t in daten["pages"]]


# ── Ablage ──────────────────────────────────────────────────────────────────
def store(db, doc_id: int, key: str, total: int, pages: list[str]) -> None:
    jetzt = datetime.now().isoformat(timespec="seconds")
    with db._lock:
        conn = db._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM document_pages WHERE doc_id = ?", (int(doc_id),))
            conn.executemany(
                "INSERT INTO document_pages (doc_id, seite, text) VALUES (?, ?, ?)",
                [(int(doc_id), i, (t or "")[:PAGE_STORE_MAX]) for i, t in enumerate(pages, start=1)])
            conn.execute(
                "INSERT OR REPLACE INTO document_pages_src (doc_id, src_key, pages_total, stored_at) "
                "VALUES (?, ?, ?, ?)", (int(doc_id), key, int(total), jetzt))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def mark_failed(db, doc_id: int, key: str) -> None:
    """Merken, dass DIESE Fassung der Datei nicht lesbar war — erst eine
    geaenderte Datei wird wieder versucht."""
    store(db, doc_id, key, FAILED, [])


def load(db, doc_id: int, key: str | None) -> tuple[int, list[str]] | None:
    """(Seitenzahl, Seiten) fuer genau diese Fassung der Datei, sonst None.
    Seitenzahl FAILED = diese Fassung war nicht lesbar."""
    if not key:
        return None
    with db._lock:
        kopf = db._conn.execute(
            "SELECT src_key, pages_total FROM document_pages_src WHERE doc_id = ?",
            (int(doc_id),)).fetchone()
        if kopf is None or kopf["src_key"] != key:
            return None
        reihen = db._conn.execute(
            "SELECT text FROM document_pages WHERE doc_id = ? ORDER BY seite",
            (int(doc_id),)).fetchall()
    return int(kopf["pages_total"]), [r["text"] for r in reihen]


def pending(db) -> list[tuple[int, Path, str]]:
    """Sichtbare PDFs ohne passenden Eintrag, neueste zuerst: [(id, Datei, Schluessel)]."""
    with db._lock:
        reihen = db._conn.execute(
            f"SELECT d.id, d.library_path, s.src_key FROM documents d "
            f"LEFT JOIN document_pages_src s ON s.doc_id = d.id "
            f"WHERE {_VISIBLE} AND lower(COALESCE(d.library_path, '')) LIKE '%.pdf' "
            f"ORDER BY d.created_at DESC, d.id DESC").fetchall()
    offen = []
    for r in reihen:
        src = Path(r["library_path"])
        key = src_key(src)
        if key is not None and key != r["src_key"]:
            offen.append((int(r["id"]), src, key))
    return offen


def counts(db) -> dict[str, int]:
    with db._lock:
        r = db._conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(pages_total = ?), 0) AS failed "
            "FROM document_pages_src", (FAILED,)).fetchone()
    return {"stored": int(r["n"]) - int(r["failed"]), "failed": int(r["failed"])}


def fill_one(db, doc_id: int, src: Path, key: str, *, isolated: bool = True) -> bool:
    """Eine Datei lesen und ablegen. True = abgelegt, False = als unlesbar gemerkt."""
    try:
        total, seiten = extract_isolated(src) if isolated else extract(src)
    except Exception as exc:                            # noqa: BLE001
        # 🔴 Nur der Name, nie der volle Pfad.
        logger.info("page texts: cannot read text layer of %s: %s", src.name, type(exc).__name__)
        mark_failed(db, doc_id, key)
        return False
    # Hat sich die Datei waehrend des Lesens geaendert, gilt das Ergebnis nicht.
    if src_key(src) != key:
        return False
    store(db, doc_id, key, total, seiten)
    return True


# ── Nachlauf ────────────────────────────────────────────────────────────────
def switched_off() -> bool:
    return os.environ.get(ENV_SWITCH, "").strip().lower() in ("0", "off", "false", "no", "aus")


class Backfill:
    """Ein Hintergrund-Faden, der fehlende und veraltete Seitentexte nachlegt."""

    def __init__(self, db, enabled=lambda: True, *, start_delay: float = START_DELAY_S,
                 round_s: float = ROUND_S, isolated: bool = True,
                 pause_min: float = PAUSE_MIN_S, pause_factor: float = PAUSE_FACTOR):
        self.db = db
        self.enabled = enabled
        self.start_delay = start_delay
        self.round_s = round_s
        self.isolated = isolated
        self.pause_min = pause_min
        self.pause_factor = pause_factor
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.done = 0

    def start(self) -> "Backfill":
        self._thread = threading.Thread(target=self._run, name="page-texts", daemon=True)
        self._thread.start()
        return self

    def wake(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def run_round(self) -> int:
        """Eine Runde: alles Offene der Reihe nach, mit Pause. Gibt die Zahl der gelesenen Dateien zurueck."""
        if switched_off() or not self.enabled():
            return 0
        try:
            offen = pending(self.db)
        except Exception as exc:                        # noqa: BLE001
            logger.warning("page texts: cannot list documents: %s", type(exc).__name__)
            return 0
        if offen:
            logger.info("page texts: %d document(s) to read", len(offen))
        n = 0
        for doc_id, src, key in offen:
            if self._stop.is_set() or switched_off() or not self.enabled():
                break
            t0 = time.monotonic()
            try:
                fill_one(self.db, doc_id, src, key, isolated=self.isolated)
            except Exception as exc:                    # noqa: BLE001
                logger.warning("page texts: storing document %d failed: %s", doc_id, type(exc).__name__)
            n += 1
            self.done += 1
            dauer = time.monotonic() - t0
            self._stop.wait(max(self.pause_min, dauer * self.pause_factor))
        if n:
            c = counts(self.db)
            logger.info("page texts: round done (%d read, %d stored, %d unreadable)",
                        n, c["stored"], c["failed"])
        return n

    def _run(self) -> None:
        if self._stop.wait(self.start_delay):
            return
        while not self._stop.is_set():
            try:
                self.run_round()
            except Exception:                           # noqa: BLE001 — der Faden darf nie sterben
                logger.exception("page texts: round failed")
            self._wake.wait(self.round_s)
            self._wake.clear()


_laufend: Backfill | None = None


def start_backfill(db, config_dir) -> Backfill | None:
    """Beim Programmstart: den Nachlauf starten (nur einer je Prozess)."""
    global _laufend
    if switched_off():
        logger.info("page texts: backfill switched off (%s)", ENV_SWITCH)
        return None
    if _laufend is None:
        from . import content_token
        _laufend = Backfill(db, enabled=lambda: content_token.is_enabled(config_dir)).start()
    return _laufend


def nudge() -> None:
    """Nach dem Einsortieren: den Nachlauf sofort nachsehen lassen. Tut nie weh."""
    if _laufend is not None:
        _laufend.wake()


def _main(argv: list[str]) -> int:                      # pragma: no cover — Kindprozess
    if len(argv) == 2 and argv[0] == "--extract":
        # 🔑 Niedrigste Prioritaet setzt der Kindprozess selbst — `preexec_fn`
        #    ist neben Faeden im Elternprozess nicht sicher.
        if hasattr(os, "nice"):
            try:
                os.nice(19)
            except OSError:
                pass
        total, seiten = extract(Path(argv[1]))
        sys.stdout.write(json.dumps({"total": total, "pages": seiten}, ensure_ascii=False))
        return 0
    sys.stderr.write("usage: python -m docusort.page_texts --extract FILE\n")
    return 2


if __name__ == "__main__":                              # pragma: no cover
    sys.exit(_main(sys.argv[1:]))
