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
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import PurePosixPath, PureWindowsPath
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
