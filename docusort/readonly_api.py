# -*- coding: utf-8 -*-
"""Die nur lesende JSON-Schnittstelle: `GET /api/readonly/documents`.

Fuer einen Helfer, der ohne Anmeldung die neuesten Eingaenge und offenen
Fristen abfragen will — und der dafuer genau EINEN Weg bekommt, mit eigenem
Token, und nichts sonst.

🔑 DER SCHUTZ IST DAS TOKEN — NICHT DIE ABSENDERADRESSE
In einem gewoehnlichen Docker-Aufbau ersetzt das Port-Mapping die Adresse des
Absenders: der Container sieht fuer JEDE Anfrage die Adresse der Docker-Bruecke
(z. B. 172.17.0.1). Eine Sperre nach Adresse waere dort wirkungslos und wuerde
trotzdem nach Sicherheit aussehen. Darum:

  · Ohne eingerichtetes Token antwortet der Weg mit 404 — es gibt ihn nicht.
  · Ohne oder mit falschem Token: 401, und die Fehlversuche werden gebremst.
  · Die Erlaubnisliste nach Netz (`DOCUSORT_READONLY_ALLOW`) ist eine
    ZUSAETZLICHE Huerde, die nur dort etwas abweist, wo die echte Adresse
    ankommt (Quell-Installation, Host-Netz). Sie ersetzt das Token nie.

🔴 WAS NIE HINAUSGEHT
Kein `extracted_text` (die Suche darf ihn benutzen, die Antwort traegt ihn
nie), keine Datei, kein Pfad (`library_path`, `processed_path`), keine Kosten,
keine Tokenzahlen des Modells. Die Antwort wird aus einer festen Feldliste
GEBAUT, nicht aus der Zeile gefiltert — ein neues Feld in der Tabelle kann
deshalb nicht versehentlich mitlaufen.

Since 1.5.0 two more exact paths share the same token and gate:
`GET /api/readonly/document?id=N` (one document's metadata plus its preview
page count) and `GET /api/readonly/preview?id=N&seite=P` (one page rendered
as PNG). The same rule holds there: no file, no path — only a rendered image.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

logger = logging.getLogger("docusort.readonly")

LIMIT_DEFAULT, LIMIT_MAX = 10, 50
DAYS_MAX = 365
Q_MAX = 80
SUMMARY_MAX = 300

# ── Erlaubnisliste nach Netz ────────────────────────────────────────────────
ALLOW_ENV = "DOCUSORT_READONLY_ALLOW"
# Vorgabe: private Netze, das CGNAT-Netz (100.64.0.0/10, darin liegen die
# Adressen eines Tailnets) und Loopback. Oeffentliche Absender werden
# abgewiesen — sofern ihre Adresse ueberhaupt bis hierher durchkommt.
DEFAULT_NETS = (
    "127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
    "100.64.0.0/10", "169.254.0.0/16",
    "::1/128", "fc00::/7", "fe80::/10",
)


def allowed_networks() -> list | None:
    """Die Netze, aus denen gefragt werden darf — None heisst „jeder".

    `DOCUSORT_READONLY_ALLOW` leer/ungesetzt → Vorgabe (private Netze).
    `any`, `off` oder `*` → keine Pruefung nach Netz.
    Sonst eine Komma-Liste von Netzen, z. B. `192.168.1.0/24,100.64.0.0/10`.
    """
    roh = os.environ.get(ALLOW_ENV, "").strip()
    if roh.lower() in ("any", "off", "*", "none", "disabled"):
        return None
    teile = [t.strip() for t in roh.split(",") if t.strip()] if roh else list(DEFAULT_NETS)
    netze = []
    for t in teile:
        try:
            netze.append(ipaddress.ip_network(t, strict=False))
        except ValueError:
            logger.warning("%s: ignoring invalid network %r", ALLOW_ENV, t)
    if not netze:
        # 🔴 Eine Liste, die nur aus Tippfehlern besteht, darf die Tuer NICHT
        #    fuer jeden oeffnen. Dann gilt die Vorgabe.
        netze = [ipaddress.ip_network(t) for t in DEFAULT_NETS]
    return netze


def client_allowed(host: str | None) -> bool:
    netze = allowed_networks()
    if netze is None:
        return True
    try:
        ip = ipaddress.ip_address((host or "").split("%", 1)[0])
    except ValueError:
        return False
    if getattr(ip, "ipv4_mapped", None):
        ip = ip.ipv4_mapped
    return any(ip.version == n.version and ip in n for n in netze)


# ── Fehlversuchsbremse ──────────────────────────────────────────────────────
class FailureBrake:
    """Hoechstens `max_fails` Fehlversuche je Absender und Fenster.

    🔴 Hinter einem Docker-Port-Mapping haben ALLE Anfragen denselben Absender.
       Die Bremse wirkt dort fuer alle gemeinsam — das ist gewollt: sie soll
       Raten verlangsamen, nicht Einzelne unterscheiden. Ein 256-Bit-Token
       raet ohnehin niemand; die Bremse haelt das Protokoll und die Maschine
       ruhig, wenn es jemand trotzdem versucht.
    """

    def __init__(self, max_fails: int = 10, window: float = 60.0):
        self.max_fails = max_fails
        self.window = window
        self._hits: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def _frisch(self, key: str, jetzt: float) -> list[float]:
        treffer = [t for t in self._hits.get(key, ()) if jetzt - t < self.window]
        if treffer:
            self._hits[key] = treffer
        else:
            self._hits.pop(key, None)
        return treffer

    def retry_after(self, key: str) -> int:
        """0 = darf fragen, sonst Sekunden bis zum naechsten Versuch."""
        jetzt = time.monotonic()
        with self._lock:
            treffer = self._frisch(key, jetzt)
            if len(treffer) < self.max_fails:
                return 0
            return max(1, int(self.window - (jetzt - treffer[0])) + 1)

    def fail(self, key: str) -> None:
        jetzt = time.monotonic()
        with self._lock:
            # Speicher begrenzen: wer viele Adressen durchprobiert, soll den
            # Prozess nicht nebenbei aufblaehen.
            if len(self._hits) > 4096:
                for k in list(self._hits):
                    self._frisch(k, jetzt)
            self._hits.setdefault(key, []).append(jetzt)


def bearer_token(authorization: str | None) -> str:
    teile = (authorization or "").strip().split(None, 1)
    if len(teile) == 2 and teile[0].lower() == "bearer":
        return teile[1].strip()
    return ""


# ── Parameter ───────────────────────────────────────────────────────────────
class BadParameter(ValueError):
    pass


def parse_params(limit: str | None, days: str | None, q: str | None) -> tuple[int, int | None, str]:
    """Pruefen erst NACH dem Token: wer nicht berechtigt ist, erfaehrt auch
    nichts ueber die erlaubten Werte."""
    def _zahl(name: str, wert: str | None) -> int | None:
        if wert is None or str(wert).strip() == "":
            return None
        try:
            return int(str(wert).strip())
        except ValueError:
            raise BadParameter("%s must be a whole number" % name) from None

    n = _zahl("limit", limit)
    n = LIMIT_DEFAULT if n is None else max(1, min(LIMIT_MAX, n))
    d = _zahl("days", days)
    if d is not None:
        d = max(1, min(DAYS_MAX, d))
    such = (q or "").strip()
    if len(such) > Q_MAX:
        raise BadParameter("q must be at most %d characters" % Q_MAX)
    return n, d, such


# ── Abfrage ─────────────────────────────────────────────────────────────────
# Alles, was gelesen wird — und nichts darueber hinaus. 🔴 Kein `d.*`.
_SPALTEN = (
    "d.id, d.filename, d.subject, d.doc_date, d.created_at, d.category, "
    "d.subcategory, d.sender, d.due_amount, d.due_date, d.deadline_done_at, "
    "d.paid_tx_id, d.paid_at, d.reasoning, "
    "r.total_amount AS receipt_amount, r.currency AS receipt_currency"
)
_GRUND = ("d.deleted_at IS NULL AND d.category != '_csv_container' "
          "AND d.status != 'duplicate'")


def _like(s: str) -> str:
    return "%" + s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def _volltext_ids(db, q: str, zusatz: str, zusatz_p: list[Any]) -> set[int]:
    """Dieselbe Suche wie die Bibliothek (Wortanfang, dann Teilwort).

    🔑 Die lockere Stufe der Bibliothek (ODER ueber alle Woerter) bleibt hier
       weg: wer nach Absender UND Art fragt, will keine Liste aller
       Dokumente zurueckbekommen, die nur eines der beiden Woerter tragen. Der Volltext dient nur der Auswahl — er
       steht in keiner Antwort.
    """
    try:
        stufen = [s for s in db.such_stufen(q) if s[0] != "locker"]
    except Exception:                                   # noqa: BLE001
        return set()
    for _name, tabelle, ausdruck in stufen:
        sql = (f"SELECT d.id FROM documents d JOIN {tabelle} f ON f.rowid = d.id "
               f"WHERE {tabelle} MATCH ? AND {_GRUND}")
        p: list[Any] = [ausdruck]
        if zusatz:
            sql += " AND " + zusatz
            p += zusatz_p
        try:
            with db._lock:
                reihen = db._conn.execute(sql, p).fetchall()
        except Exception as exc:                        # noqa: BLE001
            logger.warning("read-only search stage failed: %s", exc)
            continue
        if reihen:
            return {int(r["id"]) for r in reihen}
    return set()


def _titel(subject: str | None, filename: str | None) -> str:
    if subject and subject.strip():
        return subject.strip()
    name = filename or ""
    # Beide Trenner abschneiden — der Name kann von jedem System stammen.
    return PureWindowsPath(PurePosixPath(name).name).name


def _kurz(text: str | None) -> str:
    t = " ".join((text or "").split())
    if len(t) <= SUMMARY_MAX:
        return t
    return t[:SUMMARY_MAX - 1].rstrip() + "…"


def _als_dokument(r: Any) -> dict[str, Any]:
    if r["due_amount"] is not None:
        betrag, waehrung = r["due_amount"], "EUR"
    elif r["receipt_amount"] is not None:
        betrag, waehrung = r["receipt_amount"], (r["receipt_currency"] or "EUR")
    else:
        betrag, waehrung = None, "EUR"
    return {
        "id": int(r["id"]),
        "title": _titel(r["subject"], r["filename"]),
        "date": r["doc_date"] or None,
        "added": r["created_at"] or None,
        "category": r["category"] or None,
        "subcategory": r["subcategory"] or None,
        "sender": r["sender"] or None,
        "amount": float(betrag) if betrag is not None else None,
        "currency": waehrung or "EUR",
        "due_date": r["due_date"] or None,
        "due_done": bool(r["deadline_done_at"]),
        "paid": bool(r["paid_tx_id"]) or bool(r["paid_at"]),
        "summary": _kurz(r["reasoning"]),
    }


def list_documents(db, *, limit: int = LIMIT_DEFAULT, days: int | None = None,
                   q: str = "") -> list[dict[str, Any]]:
    """Die neuesten Dokumente (nach `created_at`), wahlweise eingegrenzt."""
    zusatz, zusatz_p = "", []
    if days:
        grenze = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        zusatz, zusatz_p = "d.created_at >= ?", [grenze]

    sql = (f"SELECT {_SPALTEN} FROM documents d "
           "LEFT JOIN receipts r ON r.doc_id = d.id "
           f"WHERE {_GRUND}")
    p: list[Any] = []
    if zusatz:
        sql += " AND " + zusatz
        p += zusatz_p
    if q:
        muster = _like(q)
        felder = ("d.subject", "d.sender", "d.category", "d.subcategory", "d.filename")
        bedingung = " OR ".join(f"COALESCE({f}, '') LIKE ? ESCAPE '\\'" for f in felder)
        ids = _volltext_ids(db, q, zusatz, zusatz_p)
        if ids:
            bedingung += " OR d.id IN (SELECT value FROM json_each(?))"
        sql += f" AND ({bedingung})"
        p += [muster] * len(felder)
        if ids:
            p.append(json.dumps(sorted(ids)))
    sql += " ORDER BY d.created_at DESC, d.id DESC LIMIT ?"
    p.append(int(limit))
    with db._lock:
        reihen = db._conn.execute(sql, p).fetchall()
    return [_als_dokument(r) for r in reihen]


# ── One document: metadata and preview image (1.5.0) ────────────────────────
# 🔑 Opening ONE document a trusted reader named. The answer is the same
#    fixed field list as above, plus how many pages can be shown as an image.
#    🔴 The source path is looked up here and stays here: it is handed to the
#    renderer and never placed in any answer. The only bytes that leave are a
#    rendered PNG — never the original file.

def parse_id(raw: str | None) -> int:
    """A positive whole number, or BadParameter."""
    try:
        doc_id = int(str(raw if raw is not None else "").strip())
    except ValueError:
        raise BadParameter("id must be a whole number") from None
    if doc_id < 1:
        raise BadParameter("id must be a positive number")
    return doc_id


def parse_page(raw: str | None) -> int:
    """The requested page (1 when not given). Capped later to what exists."""
    if raw is None or str(raw).strip() == "":
        return 1
    try:
        return int(str(raw).strip())
    except ValueError:
        raise BadParameter("seite must be a whole number") from None


def document_visible(db, doc_id: int) -> bool:
    """Does this id name a document the listing could show?"""
    with db._lock:
        r = db._conn.execute(
            f"SELECT 1 FROM documents d WHERE d.id = ? AND {_GRUND}",
            (int(doc_id),)).fetchone()
    return r is not None


def _source_pdf(db, doc_id: int) -> Path | None:
    """The document's PDF on disk — for internal use only, never returned.

    Same visibility rule as the listing (`_GRUND`): a deleted document, a
    duplicate note or a CSV container cannot be opened through this door
    either. Anything that is not an existing PDF counts as "no preview".
    """
    with db._lock:
        r = db._conn.execute(
            f"SELECT d.library_path FROM documents d WHERE d.id = ? AND {_GRUND}",
            (int(doc_id),)).fetchone()
    raw = (r["library_path"] if r else "") or ""
    if not raw:
        return None
    src = Path(raw)
    try:
        if src.suffix.lower() != ".pdf" or not src.is_file():
            return None
    except OSError:
        return None
    return src


def preview_pages(src: Path | None) -> int:
    """How many pages can be rendered as an image (0 = no preview).

    0 when there is no PDF, the PDF cannot be read, or pdftoppm is missing.
    Capped at what the renderer itself is willing to render.
    """
    from . import preview
    if src is None or not preview.available():
        return 0
    try:
        from pypdf import PdfReader
        n = len(PdfReader(str(src)).pages)
    except Exception as exc:                            # noqa: BLE001
        # 🔴 Log the name only — the full path stays out of every message.
        logger.info("read-only preview: cannot count pages of %s: %s",
                    src.name, type(exc).__name__)
        return 0
    return max(0, min(n, preview.MAX_PAGES))


def get_document(db, doc_id: int) -> dict[str, Any] | None:
    """One document's metadata — the same fields `list_documents` returns,
    plus `seiten` and `vorschau_verfuegbar`. None if the id is not visible."""
    sql = (f"SELECT {_SPALTEN} FROM documents d "
           "LEFT JOIN receipts r ON r.doc_id = d.id "
           f"WHERE d.id = ? AND {_GRUND}")
    with db._lock:
        r = db._conn.execute(sql, (int(doc_id),)).fetchone()
    if r is None:
        return None
    dok = _als_dokument(r)
    seiten = preview_pages(_source_pdf(db, doc_id))
    dok["seiten"] = seiten
    dok["vorschau_verfuegbar"] = seiten > 0
    return dok


def render_preview(db, doc_id: int, db_path: Path, page: int = 1) -> bytes | None:
    """PNG bytes of one page, or None when the document has no preview.

    The page is clamped to 1..pages, so asking for page 99 of a three-page
    letter shows page 3.
    """
    from . import preview
    src = _source_pdf(db, doc_id)
    seiten = preview_pages(src)
    if not seiten or src is None:
        return None
    seite = max(1, min(int(page), seiten))
    img = preview.render_page(src, db_path, page=seite, width=preview.DEFAULT_WIDTH)
    if img is None:
        return None
    try:
        daten = img.read_bytes()
    except OSError:
        return None
    # 🔴 Only a real PNG leaves — never whatever else might sit at that place.
    if not daten.startswith(b"\x89PNG\r\n\x1a\n"):
        return None
    return daten
