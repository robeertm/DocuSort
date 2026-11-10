# -*- coding: utf-8 -*-
"""Die nur lesende Inhalts-Tuer (1.7.0): Text eines Dokuments und Suche im Inhalt.

    GET /api/content/text?id=N[&seite=P]    Text eines Dokuments, Seite fuer Seite
    GET /api/content/search?q=…[&limit=N]   Volltextsuche mit Trefferausschnitten

Fuer einen vertrauten Helfer (einen Sprachassistenten), der Fragen wie „was
steht in der letzten Stromrechnung als Abschlag?“ beantworten soll — und dafuer
den Text braucht, nicht nur die Eckdaten der Lese-Tuer (`readonly_api.py`).

🔑 EIGENES TOKEN, EIGENE TUER
Die Inhalts-Tuer hat ein eigenes Token (`content_token.py`). Das Token der
Lese-Tuer oeffnet sie nicht und umgekehrt. Ohne eingerichtetes Token gibt es
die Pfade nicht (404). Dieselbe Netz-Huerde und Fehlversuchsbremse wie die
Lese-Tuer, dazu eine Mengenbremse fuer GUELTIGE Anfragen: ein Helfer, der in
einer Schleife haengt, liest so nicht die ganze Bibliothek leer.

🔑 SEITEN AUS DER TEXTEBENE DES PDFS
`documents.extracted_text` kennt keine Seitengrenzen. Die Seiten kommen darum
aus der Textebene des PDFs in der Bibliothek (nach einer Texterkennung hat
auch ein Scan eine). Hat das PDF keine lesbare Textebene, gilt der
gespeicherte Text als EIN Block ohne Seitenzahl (`seite: null`).
Seit 1.8.0 liegen die Seiten vorab in der Datenbank (`page_texts.py`); die
Textebene liest die Tuer nur noch, wenn fuer diese Fassung der Datei nichts
abgelegt ist.

🔴 WAS NIE HINAUSGEHT
Keine Datei, kein Pfad, keine Kosten. Kontonummern (IBAN) und Kartennummern
werden im Text und in den Ausschnitten maskiert (nur die letzten vier Zeichen
bleiben), Kfz-Kennzeichen ganz und Nummern hinter Stichworten (Vertrag,
Kunde, Konto, Zaehler …) bis auf die letzten drei (1.7.1), ehe etwas die Tuer
verlaesst. Geloeschte Dokumente, Doppelgaenger-
Notizen und CSV-Huellen bleiben unsichtbar — dieselbe Regel wie die Lese-Tuer.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from pathlib import Path
from typing import Any

from . import page_texts as _pt
from . import readonly_api as _ro

logger = logging.getLogger("docusort.content")

TEXT_MAX_CHARS = 60_000         # so viel Text verlaesst die Tuer je Anfrage hoechstens
PAGE_MAX_CHARS = 12_000         # je Seite
PAGES_MAX = 60                  # mehr Seiten werden nicht gelesen
SEARCH_LIMIT_DEFAULT, SEARCH_LIMIT_MAX = 8, 20
Q_MAX = 120
PAGE_HINT_DOCS = 5              # fuer so viele Treffer wird die Seite gesucht
RATE_MAX, RATE_WINDOW = 120, 60.0


# ── Mengenbremse fuer gueltige Anfragen ─────────────────────────────────────
class RateLimit:
    """Hoechstens `max_requests` gueltige Anfragen je Fenster — fuer alle
    gemeinsam (hinter einem Docker-Port-Mapping hat ohnehin jeder dieselbe
    Absenderadresse)."""

    def __init__(self, max_requests: int = RATE_MAX, window: float = RATE_WINDOW):
        self.max_requests = max_requests
        self.window = window
        self._hits: list[float] = []
        self._lock = threading.Lock()

    def take(self) -> int:
        """0 = darf, sonst Sekunden bis zur naechsten Anfrage."""
        jetzt = time.monotonic()
        with self._lock:
            self._hits = [t for t in self._hits if jetzt - t < self.window]
            if len(self._hits) >= self.max_requests:
                return max(1, int(self.window - (jetzt - self._hits[0])) + 1)
            self._hits.append(jetzt)
            return 0


# ── Maskieren ───────────────────────────────────────────────────────────────
# IBAN: Laenderkennung, zwei Pruefziffern, 11–30 weitere Zeichen, gern in
# Vierergruppen; Gross/klein egal, auch doppelte Leerzeichen und Umbrueche.
_IBAN = re.compile(r"\b([A-Za-z]{2})\s{0,2}(\d{2})((?:\s{0,2}[A-Za-z0-9]){11,30})\b")
# 🔴 1.7.1: eine IBAN zaehlt nur mit gueltigem Laendercode, passender Laenge und
#    bestandener mod-97-Pruefsumme — sonst fraesse das grobe Muster normalen
#    Text wie „Mo 12 Uhr bis Fr 18 Uhr“. Gross/klein und Umbrueche bleiben erlaubt.
IBAN_LAENGE = {"AD": 24, "AT": 20, "BE": 16, "BG": 22, "CH": 21, "CY": 28, "CZ": 24, "DE": 22, "DK": 18, "EE": 20,
               "ES": 24, "FI": 18, "FR": 27, "GB": 22, "GR": 27, "HR": 21, "HU": 28, "IE": 22, "IS": 26, "IT": 27,
               "LI": 21, "LT": 20, "LU": 20, "LV": 21, "MC": 27, "MT": 31, "NL": 18, "NO": 15, "PL": 28, "PT": 25,
               "RO": 24, "SE": 24, "SI": 19, "SK": 24, "SM": 27}


def iban_gueltig(s: str) -> bool:
    s = s.upper()
    if len(s) != IBAN_LAENGE.get(s[:2], -1) or not s.isalnum():
        return False
    try:
        return int("".join(str(int(c, 36)) for c in s[4:] + s[:4])) % 97 == 1
    except ValueError:
        return False


def iban_ersetzen(ganz: str, rest_zeichen: int = 4) -> str | None:
    """Treffer des groben Musters → verdeckte IBAN + unveraenderter Rest, oder None (keine gueltige IBAN)."""
    laenge = IBAN_LAENGE.get(ganz[:2].upper())
    if not laenge:
        return None
    pos = [i for i, c in enumerate(ganz) if c.isalnum()]
    if len(pos) < laenge:
        return None
    kandidat = "".join(ganz[i] for i in pos[:laenge]).upper()
    if not iban_gueltig(kandidat):
        return None
    return kandidat[:2] + "•• •••• •••• " + kandidat[-rest_zeichen:] + ganz[pos[laenge - 1] + 1:]
# Kartennummern: 13–19 Ziffern, wahlweise in Gruppen; nur mit gueltiger
# Luhn-Pruefziffer (sonst waere jede lange Kundennummer betroffen).
_KARTE = re.compile(r"(?<![\d])(\d(?:[ -]?\d){12,18})(?![\d])")
# Kfz-Kennzeichen: Ort (1–3 Buchstaben), Trenner, 1–2 Buchstaben, 1–4 Ziffern
# (1.7.1: kam vorher ungeschwaerzt heraus).
_KENNZEICHEN = re.compile(r"(?<![A-ZÄÖÜ0-9])[A-ZÄÖÜ]{1,3}[- ][A-Z]{1,2} ?\d{1,4}[EH]?(?![A-Za-z0-9])")
# Stichwort + Nummer (1.7.1): Vertrags-, Kunden-, Versicherungs-, Konto-,
# Zaehlernummer (nie der Zaehlerstand), Steuer-, Rechnungsnummer … Der Wert besteht aus Grossbuchstaben und
# Ziffern; weitere Gruppen nur, wenn sie mit einer Ziffer beginnen. Auch kurze
# alphanumerische Werte („AB-123-XYZ“) werden verdeckt, Daten nicht.
_STICHWORT = re.compile(
    r"(?i:(?P<wort>\b(vertrag\w*|kunde\w*|versicherung\w*|versicherte\w*|police\w*|konto\w*|mitglied\w*|"
    r"z[äa]hler(?:nummer|nr\.?)|z[äa]hler(?=\s*[-‐]\s*(?:nummer|nr))|steuer\w*|identifikations\w*|personal\w*|referenz\w*|mandats\w*|gl[äa]ubiger\w*|kassenzeichen|"
    r"aktenzeichen|rechnungs\w*|abonnenten\w*|vorgangs\w*|fahrgestell\w*|depot\w*|iban|bic|kennzeichen|"
    r"kfz-kennzeichen)(\s*[-‐]?\s*(nummer|nr\.?|id))?\s*[:.#]?\s*))"
    r"(?P<nr>[A-Z0-9](?:[A-Z0-9\-/.]*[A-Z0-9])?(?: \d(?:[\d\-/.]*\d)?(?![\d]|,\d)){0,5})(?![A-Za-zÄÖÜäöüß\d]|,\d)")
_DATUM = re.compile(r"^(\d{1,2}\.\d{1,2}\.(\d{2,4})?|\d{1,2}[./]\d{2,4}|(19|20)\d{2}([/-]\d{2,4})?)$")


def _luhn(ziffern: str) -> bool:
    summe, doppelt = 0, False
    for c in reversed(ziffern):
        n = ord(c) - 48
        if doppelt:
            n *= 2
            if n > 9:
                n -= 9
        summe += n
        doppelt = not doppelt
    return summe % 10 == 0


def mask_text(text: str) -> tuple[str, dict[str, int]]:
    """IBAN und Kartennummern bis auf die letzten vier Zeichen verdecken,
    Kfz-Kennzeichen ganz, Nummern hinter Stichworten bis auf die letzten drei."""
    zahl = {"iban": 0, "card": 0, "plate": 0, "number": 0}

    def _iban(m: re.Match) -> str:
        neu = iban_ersetzen(m.group(0))
        if neu is None:
            return m.group(0)
        zahl["iban"] += 1
        return neu

    def _karte(m: re.Match) -> str:
        ziffern = re.sub(r"\D", "", m.group(1))
        if not (13 <= len(ziffern) <= 19) or not _luhn(ziffern):
            return m.group(0)
        zahl["card"] += 1
        return "•••• •••• •••• " + ziffern[-4:]

    def _kennzeichen(m: re.Match) -> str:
        zahl["plate"] += 1
        return "•••"

    def _stichwort(m: re.Match) -> str:
        nr = m.group("nr")
        rein = re.sub(r"[\s\-/.]", "", nr)
        if "•" in nr or not any(c.isdigit() for c in nr) or _DATUM.match(nr) or len(rein) < 3:
            return m.group(0)
        zahl["number"] += 1
        return m.group("wort") + "•••" + rein[-3:]

    aus = _IBAN.sub(_iban, text or "")
    aus = _KARTE.sub(_karte, aus)
    aus = _KENNZEICHEN.sub(_kennzeichen, aus)
    aus = _STICHWORT.sub(_stichwort, aus)
    return aus, zahl


# ── Seiten ──────────────────────────────────────────────────────────────────
# 🔑 1.8.0: Die Seiten liegen vorab in der Datenbank (`page_texts.py`). Nur wenn
#    fuer GENAU diese Fassung der Datei noch nichts abgelegt ist, liest die Tuer
#    die Textebene selbst — und legt das Ergebnis fuer das naechste Mal ab.


def _pdf_pages(db, doc_id: int, src: Path | None) -> tuple[list[str], int | None]:
    """(Text je Seite, volle Seitenzahl oder None). Hoechstens PAGES_MAX Seiten."""
    if src is None:
        return [], None
    key = _pt.src_key(src)
    if key is None:
        return [], None
    try:
        abgelegt = _pt.load(db, doc_id, key)
    except Exception as exc:                            # noqa: BLE001 — Ablage fehlt: lesen wie frueher
        logger.info("content API: page store unavailable: %s", type(exc).__name__)
        abgelegt = None
    if abgelegt is not None and abgelegt[0] != _pt.FAILED:
        return abgelegt[1][:PAGES_MAX], abgelegt[0]
    try:
        gesamt, seiten = _pt.extract(src, PAGES_MAX)
    except Exception as exc:                            # noqa: BLE001
        # 🔴 Nur der Name, nie der volle Pfad.
        logger.info("content API: cannot read text layer of %s: %s",
                    src.name, type(exc).__name__)
        return [], None
    if abgelegt is None:
        try:
            _pt.store(db, doc_id, key, gesamt, seiten)
        except Exception as exc:                        # noqa: BLE001 — Antwort geht trotzdem raus
            logger.info("content API: cannot store page texts: %s", type(exc).__name__)
    return seiten, gesamt


def _stored_text(db, doc_id: int) -> str:
    with db._lock:
        r = db._conn.execute(
            f"SELECT d.extracted_text FROM documents d WHERE d.id = ? AND {_ro._GRUND}",
            (int(doc_id),)).fetchone()
    return (r["extracted_text"] if r else "") or ""


def document_pages(db, doc_id: int) -> tuple[list[dict[str, Any]], str]:
    """[(seite, text)] und woher der Text stammt ("pdf" oder "stored")."""
    seiten, herkunft, _ = _pages_and_total(db, doc_id)
    return seiten, herkunft


def _pages_and_total(db, doc_id: int) -> tuple[list[dict[str, Any]], str, int | None]:
    src = _ro._source_pdf(db, doc_id)
    seiten, gesamt = _pdf_pages(db, doc_id, src)
    if any(s.strip() for s in seiten):
        return [{"seite": i, "text": t} for i, t in enumerate(seiten, start=1)], "pdf", gesamt
    text = _stored_text(db, doc_id).strip()
    return ([{"seite": None, "text": text}] if text else []), "stored", gesamt


def _known_pages(db, doc_id: int) -> int | None:
    """Abgelegte volle Seitenzahl dieser Dateifassung — erspart das Zaehlen per pypdf."""
    try:
        src = _ro._source_pdf(db, doc_id)
        abgelegt = _pt.load(db, doc_id, _pt.src_key(src))
    except Exception:                                   # noqa: BLE001
        return None
    if abgelegt is None or abgelegt[0] == _pt.FAILED:
        return None
    return abgelegt[0]


def get_text(db, doc_id: int, page: int | None = None) -> dict[str, Any] | None:
    """Eckdaten + Text Seite fuer Seite (maskiert, gekuerzt). None = unsichtbar."""
    if not _ro.document_visible(db, doc_id):
        return None
    seiten, herkunft, gesamt_pdf = _pages_and_total(db, doc_id)
    meta = _ro.get_document(db, doc_id, known_pages=gesamt_pdf)
    if meta is None:
        return None
    gesamt = len(seiten)
    if page is not None and herkunft == "pdf" and seiten:
        nr = max(1, min(int(page), gesamt))
        seiten = [seiten[nr - 1]]
    rest, gekuerzt = TEXT_MAX_CHARS, False
    aus, maskiert = [], {"iban": 0, "card": 0, "plate": 0, "number": 0}
    for s in seiten:
        if rest <= 0:
            gekuerzt = True
            break
        text = s["text"]
        if len(text) > PAGE_MAX_CHARS:
            text, gekuerzt = text[:PAGE_MAX_CHARS], True
        if len(text) > rest:
            text, gekuerzt = text[:rest], True
        text, z = mask_text(text)
        for k in maskiert:
            maskiert[k] += z[k]
        rest -= len(text)
        aus.append({"seite": s["seite"], "text": text})
    meta.pop("vorschau_verfuegbar", None)
    return {"document": meta, "source": herkunft, "pages_total": gesamt,
            "pages": aus, "truncated": gekuerzt, "masked": maskiert}


# ── Suche ───────────────────────────────────────────────────────────────────
SNIPPET_CHARS = 140             # Zeichen links und rechts der Fundstelle


def _varianten(wort: str) -> list[str]:
    """Ein Suchwort und seine deutsche Schreibvariante (ae ↔ ä …)."""
    w = wort.lower()
    v = {w}
    for a, b in (("ae", "ä"), ("oe", "ö"), ("ue", "ü"), ("ss", "ß")):
        v |= {x.replace(a, b) for x in v} | {x.replace(b, a) for x in v}
    return sorted(v, key=len, reverse=True)


def snippet(text: str, q: str) -> str:
    """Ausschnitt um die erste Fundstelle, die Fundstelle in [ ].

    🔴 ERST maskieren, DANN schneiden. Die eingebaute FTS-Funktion `snippet()`
       schneidet zuerst — eine abgeschnittene IBAN („DE89 3704…“) erkennt
       danach kein Muster mehr und ginge halb im Klartext hinaus.
    """
    t, _ = mask_text(" ".join((text or "").split()))
    if not t:
        return ""
    klein = t.lower()
    beste = None
    for roh in re.findall(r"[0-9A-Za-zÀ-ÿ_]+", q or ""):
        if len(roh) < 2:
            continue
        for v in _varianten(roh):
            i = klein.find(v)
            if i >= 0 and (beste is None or i < beste[0]):
                beste = (i, len(v))
                break
    if beste is None:
        aus = t[:2 * SNIPPET_CHARS]
        return aus + ("…" if len(t) > len(aus) else "")
    i, n = beste
    a, e = max(0, i - SNIPPET_CHARS), min(len(t), i + n + SNIPPET_CHARS)
    return (("…" if a else "") + t[a:i] + "[" + t[i:i + n] + "]" + t[i + n:e]
            + ("…" if e < len(t) else ""))


def parse_search(q: str | None, limit: str | None) -> tuple[str, int]:
    such = " ".join((q or "").split())
    if not such:
        raise _ro.BadParameter("q is required")
    if len(such) > Q_MAX:
        raise _ro.BadParameter("q must be at most %d characters" % Q_MAX)
    if limit is None or str(limit).strip() == "":
        n = SEARCH_LIMIT_DEFAULT
    else:
        try:
            n = int(str(limit).strip())
        except ValueError:
            raise _ro.BadParameter("limit must be a whole number") from None
    return such, max(1, min(SEARCH_LIMIT_MAX, n))


def parse_optional_page(raw: str | None) -> int | None:
    if raw is None or str(raw).strip() == "":
        return None
    return _ro.parse_page(raw)


def _woerter(q: str) -> list[str]:
    from .aehnlichkeit import schlicht
    return [w for w in (schlicht(x) for x in re.findall(r"[0-9A-Za-zÀ-ÿ_]+", q)) if len(w) >= 2]


def _seite_mit(db, doc_id: int, woerter: list[str]) -> int | None:
    """Die erste Seite, auf der die meisten gesuchten Woerter stehen."""
    if not woerter:
        return None
    from .aehnlichkeit import schlicht
    src = _ro._source_pdf(db, doc_id)
    seiten, _ = _pdf_pages(db, doc_id, src)
    beste, punkte = None, 0
    for i, t in enumerate(seiten, start=1):
        s = schlicht(t)
        n = sum(1 for w in woerter if w in s)
        if n > punkte:
            beste, punkte = i, n
    return beste


def search(db, q: str, limit: int = SEARCH_LIMIT_DEFAULT) -> dict[str, Any]:
    """Dieselbe Suchleiter wie die Bibliothek (genau → Teilwort → locker),
    je Treffer die Eckdaten, ein maskierter Ausschnitt und — fuer die ersten
    Treffer — die Seite, auf der die Woerter stehen."""
    try:
        stufen = db.such_stufen(q)
    except Exception:                                   # noqa: BLE001
        stufen = []
    reihen, stufe = [], None
    for name, tabelle, ausdruck in stufen:
        sql = (f"SELECT d.id, d.extracted_text FROM documents d JOIN {tabelle} ON {tabelle}.rowid = d.id "
               f"WHERE {tabelle} MATCH ? AND {_ro._GRUND} "
               f"ORDER BY bm25({tabelle}), d.created_at DESC LIMIT ?")
        try:
            with db._lock:
                reihen = db._conn.execute(sql, (ausdruck, int(limit))).fetchall()
        except Exception as exc:                        # noqa: BLE001
            logger.warning("content search stage %s failed: %s", name, type(exc).__name__)
            reihen = []
        if reihen:
            stufe = name
            break
    woerter = _woerter(q)
    treffer = []
    for i, r in enumerate(reihen):
        meta = _ro.get_document(db, int(r["id"]), known_pages=_known_pages(db, int(r["id"])))
        if meta is None:
            continue
        meta.pop("vorschau_verfuegbar", None)
        treffer.append({**meta, "snippet": snippet(r["extracted_text"] or "", q),
                        "page": _seite_mit(db, int(r["id"]), woerter) if i < PAGE_HINT_DOCS else None})
    return {"query": q, "stage": stufe, "count": len(treffer), "hits": treffer}
