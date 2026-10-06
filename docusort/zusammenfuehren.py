# -*- coding: utf-8 -*-
"""Zwei DocuSort-Installationen zu einer machen.

🔴 WARUM ES DAS GIBT (06.10.2026)

Der Installer legte alles in `$PWD/docusort` ab — also dorthin, wo jemand
gerade stand. Wer den Einzeiler ein zweites Mal aus einem anderen Ordner
startete, bekam ein zweites, leeres Verzeichnis; der Container hiess beide Male
`docusort` und wurde mit den neuen Pfaden neu gebaut. Ergebnis: eine leere
Bibliothek, waehrend die alte Datenbank unversehrt nebenan lag. Ein Nutzer hat
daraufhin alle Dokumente neu hochgeladen — und hatte danach ZWEI Archive, jedes
mit einem Teil seiner Post.

Der Installer kann das seit 0.98.1 nicht mehr anrichten. Dieses Werkzeug ist
fuer alle, bei denen es schon passiert ist.

🔑 DIE REGELN, NACH DENEN HIER GEARBEITET WIRD

  * **Es wird nichts geloescht.** Die Quelle wird nur gelesen (`mode=ro`), das
    Ziel bekommt vorher eine Sicherung. Schlaegt irgendetwas fehl, liegt die
    Sicherung daneben und beide Archive sind noch da.
  * **Trockenlauf ist die Vorgabe.** Erst zaehlen und zeigen, dann — auf
    ausdrueckliche Ansage — schreiben.
  * **Dubletten werden am INHALT erkannt**, nicht am Namen: Dokumente ueber
    `content_hash` (SHA256 der Datei), Buchungen ueber `tx_hash`, Konten ueber
    `iban_hash`, Kontoauszuege ueber `file_hash`. Dasselbe Dokument in beiden
    Archiven wird einmal gezaehlt, nicht zweimal abgelegt.
  * **Die Datei gehoert zum Datensatz.** Eine Zeile ohne ihr PDF waere ein
    Eintrag, der ins Leere zeigt. Jede uebernommene Zeile bringt ihre Datei
    mit; laesst sie sich nicht kopieren, wird die Zeile NICHT uebernommen und
    das gesagt.
  * **Konten und Einstellungen bleiben, wie sie sind.** Benutzer, Sitzungen und
    `meta` der Quelle werden nicht angefasst — ein Zusammenfuehren darf keine
    Anmeldung aendern.
"""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from typing import Any, Callable

# Die Reihenfolge ist die der Abhaengigkeiten: erst das Dokument, dann was
# daran haengt.
WISSENSTABELLEN = ("category_rules", "merchant_buckets", "custom_categories",
                   "doc_categories", "transaction_category_overrides")


def _spalten(conn, tabelle: str) -> list[str]:
    try:
        return [r[1] for r in conn.execute("PRAGMA table_info(%s)" % tabelle)]
    except sqlite3.Error:
        return []


def _hat(conn, tabelle: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (tabelle,)).fetchone())


def _wert(reihe, name, vorgabe=None):
    try:
        return reihe[name]
    except (IndexError, KeyError):
        return vorgabe


def _dateiweg(container_pfad: str, daten: Path) -> Path | None:
    """Aus einem Pfad IM CONTAINER den Pfad auf dieser Platte machen.

    🔴 In der Datenbank stehen Container-Pfade (`/data/library/...`). Ein
    `Path(...).exists()` vom Wirt aus meldet fuer alles „fehlt" — derselbe
    Fehler ist mir beim Lesen der NAS-Datenbank schon einmal passiert.
    """
    p = str(container_pfad or "").strip()
    if not p:
        return None
    for marke in ("/data/", "data/"):
        if p.startswith(marke):
            return daten / p[len(marke):]
    if p.startswith("/"):
        # Ein Pfad, den wir nicht deuten koennen — lieber nichts behaupten.
        return None
    return daten / p


def zusammenfuehren(
    ziel_daten: Path, quelle_daten: Path, *,
    trocken: bool = True,
    sagen: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Alles aus `quelle_daten` in `ziel_daten` uebernehmen, was dort fehlt.

    Beide Angaben sind DATENverzeichnisse (die mit `library/` darin).
    Gibt einen Bericht zurueck; schreibt nur, wenn `trocken=False`.
    """
    ziel_daten = Path(ziel_daten).resolve()
    quelle_daten = Path(quelle_daten).resolve()
    ziel_db = ziel_daten / "library" / "docusort.db"
    quelle_db = quelle_daten / "library" / "docusort.db"
    bericht: dict[str, Any] = {"trocken": trocken, "ziel": str(ziel_db),
                               "quelle": str(quelle_db), "sicherung": "",
                               "dokumente_neu": 0, "dokumente_bekannt": 0,
                               "dateien_kopiert": 0, "ohne_datei": 0,
                               "buchungen_neu": 0, "buchungen_ohne_auszug": 0,
                               "konten_neu": 0,
                               "auszuege_neu": 0, "kassenzettel_neu": 0,
                               "regeln_neu": 0, "fehler": []}

    if not quelle_db.exists():
        bericht["fehler"].append("keine Datenbank in %s" % quelle_db)
        return bericht
    if not ziel_db.exists():
        bericht["fehler"].append("keine Datenbank in %s" % ziel_db)
        return bericht
    if ziel_db.resolve() == quelle_db.resolve():
        bericht["fehler"].append("Ziel und Quelle sind dieselbe Datenbank")
        return bericht

    # 🔴 ERST SICHERN. `Connection.backup` statt `cp`: neben der Datei liegen
    #    `-wal` und `-shm`, und eine Kopie ohne sie ist eine halbe Datenbank.
    if not trocken:
        from datetime import datetime
        marke = datetime.now().strftime("%Y%m%d-%H%M%S")
        sicherung = ziel_db.with_name("docusort.db.vor-zusammenfuehren-%s" % marke)
        with sqlite3.connect(ziel_db) as q, sqlite3.connect(sicherung) as z:
            q.backup(z)
        bericht["sicherung"] = str(sicherung)
        sagen("Sicherung: %s" % sicherung)

    # Das Ziel ueber die App oeffnen, damit die Wanderungen laufen.
    from .db import Database
    ziel = Database(ziel_db)
    zc = ziel._conn

    # 🔴 DIE QUELLE WIRD KOPIERT, NICHT DIREKT GEOEFFNET.
    #    Gemessen in der Simulation: `mode=ro` auf einem schreibgeschuetzten
    #    Mount (`-v …:/fremd:ro`) scheitert mit „unable to open database file".
    #    Grund ist WAL — SQLite will dafuer `-shm`/`-wal` anlegen duerfen, und
    #    auf einem :ro-Mount darf es das nicht. Genau so wird ein fremdes
    #    Archiv aber eingehaengt, und richtigerweise.
    #
    # 🔑 Eine Kopie loest beides auf einmal: sie laesst sich normal oeffnen
    #    (das Journal wird dabei nachgezogen), und sie macht ganz sicher, dass
    #    an der Quelle nichts angefasst wird — auch nicht versehentlich.
    import tempfile
    _kopie_ordner = tempfile.mkdtemp(prefix="ds-quelle-")
    _kopie = Path(_kopie_ordner) / "quelle.db"
    shutil.copy2(quelle_db, _kopie)
    for _zusatz in ("-wal", "-shm"):
        _neben = Path(str(quelle_db) + _zusatz)
        if _neben.exists():
            shutil.copy2(_neben, str(_kopie) + _zusatz)
    qc = sqlite3.connect(_kopie)
    qc.row_factory = sqlite3.Row

    # 🔴 DER TROCKENLAUF MUSS DIESELBE ZAHL NENNEN WIE DER ECHTE LAUF.
    #    Beim ersten Entwurf blieben die Zuordnungstabellen im Trockenlauf
    #    leer — es wurde ja nichts eingefuegt. Damit fand jede Buchung ihren
    #    Kontoauszug nicht mehr und wurde als „ohne Auszug" uebersprungen:
    #    gemeldet 0 neue Buchungen, geschrieben haette er 1. Eine Zahl, auf
    #    die ein Mensch „ja" sagt, muss die Zahl sein, die er bekommt.
    #    Also bekommt der Trockenlauf Platzhalter-Nummern.
    _platz = [0]

    def neue_nummer(cursor):
        if trocken:
            _platz[0] -= 1
            return _platz[0]
        return int(cursor.lastrowid)

    try:
        # ── 1. Dokumente ────────────────────────────────────────────────
        zc.row_factory = sqlite3.Row
        bekannt_hash = {r[0] for r in zc.execute(
            "SELECT content_hash FROM documents WHERE COALESCE(content_hash,'') <> ''")}
        bekannt_name = {(r[0], r[1]) for r in zc.execute(
            "SELECT original_name, COALESCE(file_size,0) FROM documents")}
        z_spalten = _spalten(zc, "documents")
        q_spalten = _spalten(qc, "documents")
        gemeinsam = [s for s in q_spalten if s in z_spalten and s != "id"]

        doc_karte: dict[int, int] = {}
        for r in qc.execute("SELECT * FROM documents"):
            h = _wert(r, "content_hash") or ""
            schluessel = (_wert(r, "original_name") or "",
                          _wert(r, "file_size") or 0)
            if (h and h in bekannt_hash) or (not h and schluessel in bekannt_name):
                bericht["dokumente_bekannt"] += 1
                treffer = zc.execute(
                    "SELECT id FROM documents WHERE (COALESCE(content_hash,'')=? "
                    "AND ?<>'') OR (original_name=? AND COALESCE(file_size,0)=?)",
                    (h, h, schluessel[0], schluessel[1])).fetchone()
                if treffer:
                    doc_karte[int(r["id"])] = int(treffer[0])
                continue

            # 🔑 Die Datei zuerst. Ohne sie waere die Zeile ein Zeiger ins Leere.
            quelle_datei = _dateiweg(_wert(r, "library_path") or "", quelle_daten)
            ziel_datei = None
            if quelle_datei is not None and quelle_datei.exists():
                rel = quelle_datei.relative_to(quelle_daten)
                ziel_datei = ziel_daten / rel
                n = 1
                while ziel_datei.exists():
                    ziel_datei = ziel_datei.with_name(
                        "%s (%d)%s" % (ziel_datei.stem, n, ziel_datei.suffix))
                    n += 1
                if not trocken:
                    ziel_datei.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(quelle_datei, ziel_datei)
                bericht["dateien_kopiert"] += 1
            else:
                bericht["ohne_datei"] += 1
                bericht["fehler"].append(
                    "Datei fehlt, Zeile uebersprungen: %s"
                    % (_wert(r, "original_name") or _wert(r, "filename") or "?"))
                continue

            werte = []
            for s in gemeinsam:
                v = _wert(r, s)
                if s == "library_path" and ziel_datei is not None:
                    v = "/data/" + str(ziel_datei.relative_to(ziel_daten))
                werte.append(v)
            bericht["dokumente_neu"] += 1
            cur = None
            if not trocken:
                cur = zc.execute(
                    "INSERT INTO documents (%s) VALUES (%s)"
                    % (",".join(gemeinsam), ",".join("?" * len(gemeinsam))),
                    werte)
                if h:
                    bekannt_hash.add(h)
                bekannt_name.add(schluessel)
            doc_karte[int(r["id"])] = neue_nummer(cur)

        # ── 2. Konten ───────────────────────────────────────────────────
        konto_karte: dict[int, int] = {}
        if _hat(qc, "accounts"):
            vorhanden = {r[0]: r[1] for r in zc.execute(
                "SELECT COALESCE(iban_hash,''), id FROM accounts")}
            gem = [s for s in _spalten(qc, "accounts")
                   if s in _spalten(zc, "accounts") and s != "id"]
            for r in qc.execute("SELECT * FROM accounts"):
                h = _wert(r, "iban_hash") or ""
                if h and h in vorhanden:
                    konto_karte[int(r["id"])] = int(vorhanden[h])
                    continue
                bericht["konten_neu"] += 1
                cur = None
                if not trocken:
                    cur = zc.execute(
                        "INSERT INTO accounts (%s) VALUES (%s)"
                        % (",".join(gem), ",".join("?" * len(gem))),
                        [_wert(r, s) for s in gem])
                    if h:
                        vorhanden[h] = cur.lastrowid
                konto_karte[int(r["id"])] = neue_nummer(cur)

        # ── 3. Kontoauszuege ────────────────────────────────────────────
        auszug_karte: dict[int, int] = {}
        if _hat(qc, "statements"):
            da = {r[0] for r in zc.execute(
                "SELECT COALESCE(file_hash,'') FROM statements")}
            gem = [s for s in _spalten(qc, "statements")
                   if s in _spalten(zc, "statements") and s != "id"]
            for r in qc.execute("SELECT * FROM statements"):
                fh = _wert(r, "file_hash") or ""
                if fh and fh in da:
                    continue
                neu_doc = doc_karte.get(int(_wert(r, "doc_id") or 0))
                if neu_doc is None:
                    continue
                bericht["auszuege_neu"] += 1
                cur = None
                if not trocken:
                    werte = []
                    for s in gem:
                        v = _wert(r, s)
                        if s == "doc_id":
                            v = neu_doc
                        elif s == "account_id":
                            v = konto_karte.get(int(v or 0), v)
                        werte.append(v)
                    cur = zc.execute(
                        "INSERT INTO statements (%s) VALUES (%s)"
                        % (",".join(gem), ",".join("?" * len(gem))), werte)
                    if fh:
                        da.add(fh)
                auszug_karte[int(r["id"])] = neue_nummer(cur)

        # ── 4. Buchungen ────────────────────────────────────────────────
        if _hat(qc, "transactions"):
            da = {r[0] for r in zc.execute(
                "SELECT COALESCE(tx_hash,'') FROM transactions")}
            gem = [s for s in _spalten(qc, "transactions")
                   if s in _spalten(zc, "transactions") and s != "id"]
            for r in qc.execute("SELECT * FROM transactions"):
                th = _wert(r, "tx_hash") or ""
                if th and th in da:
                    continue
                # 🔴 `statement_id` ist NOT NULL: eine Buchung ohne ihren
                #    Kontoauszug laesst sich gar nicht anlegen — und sie waere
                #    auch inhaltlich eine Waise. Kam der Auszug nicht mit
                #    (etwa weil sein Dokument schon im Ziel lag), bleibt sie
                #    draussen, und das wird gesagt statt verschwiegen.
                neu_auszug = auszug_karte.get(int(_wert(r, "statement_id") or 0))
                if neu_auszug is None:
                    bericht["buchungen_ohne_auszug"] = \
                        bericht.get("buchungen_ohne_auszug", 0) + 1
                    continue
                bericht["buchungen_neu"] += 1
                if not trocken:
                    werte = []
                    for s in gem:
                        v = _wert(r, s)
                        if s == "statement_id":
                            v = neu_auszug
                        elif s == "account_id":
                            v = konto_karte.get(int(v or 0), v)
                        werte.append(v)
                    try:
                        zc.execute(
                            "INSERT INTO transactions (%s) VALUES (%s)"
                            % (",".join(gem), ",".join("?" * len(gem))), werte)
                        if th:
                            da.add(th)
                    except sqlite3.IntegrityError:
                        bericht["buchungen_neu"] -= 1

        # ── 5. Kassenzettel ─────────────────────────────────────────────
        if _hat(qc, "receipts"):
            gem = [s for s in _spalten(qc, "receipts")
                   if s in _spalten(zc, "receipts") and s != "id"]
            gem_p = [s for s in _spalten(qc, "receipt_items")
                     if s in _spalten(zc, "receipt_items") and s != "id"]
            for r in qc.execute("SELECT * FROM receipts"):
                neu_doc = doc_karte.get(int(_wert(r, "doc_id") or 0))
                if neu_doc is None:
                    continue
                if zc.execute("SELECT 1 FROM receipts WHERE doc_id=?",
                              (neu_doc,)).fetchone():
                    continue
                bericht["kassenzettel_neu"] += 1
                if not trocken:
                    werte = [neu_doc if s == "doc_id" else _wert(r, s) for s in gem]
                    cur = zc.execute(
                        "INSERT INTO receipts (%s) VALUES (%s)"
                        % (",".join(gem), ",".join("?" * len(gem))), werte)
                    neu_beleg = int(cur.lastrowid)
                    for p in qc.execute(
                            "SELECT * FROM receipt_items WHERE receipt_id=?",
                            (int(r["id"]),)):
                        zc.execute(
                            "INSERT INTO receipt_items (%s) VALUES (%s)"
                            % (",".join(gem_p), ",".join("?" * len(gem_p))),
                            [neu_beleg if s == "receipt_id" else _wert(p, s)
                             for s in gem_p])

        # ── 6. Gelerntes ────────────────────────────────────────────────
        # 🔑 Regeln und Schubladen sind Arbeit, die ein Mensch hineingesteckt
        #    hat. Was im Ziel fehlt, kommt dazu; was da ist, bleibt wie es ist.
        for tab in WISSENSTABELLEN:
            if not (_hat(qc, tab) and _hat(zc, tab)):
                continue
            gem = [s for s in _spalten(qc, tab)
                   if s in _spalten(zc, tab) and s != "id"]
            if not gem:
                continue
            for r in qc.execute("SELECT * FROM %s" % tab):
                werte = [_wert(r, s) for s in gem]
                bedingung = " AND ".join("COALESCE(%s,'')=COALESCE(?,'')" % s
                                         for s in gem)
                if zc.execute("SELECT 1 FROM %s WHERE %s" % (tab, bedingung),
                              werte).fetchone():
                    continue
                bericht["regeln_neu"] += 1
                if not trocken:
                    try:
                        zc.execute(
                            "INSERT INTO %s (%s) VALUES (%s)"
                            % (tab, ",".join(gem), ",".join("?" * len(gem))),
                            werte)
                    except sqlite3.IntegrityError:
                        bericht["regeln_neu"] -= 1

        if not trocken:
            zc.commit()
    finally:
        qc.close()
        shutil.rmtree(_kopie_ordner, ignore_errors=True)

    return bericht


def bericht_zeigen(b: dict, sagen: Callable[[str], None] = print) -> None:
    sagen("")
    sagen("  %-26s %s" % ("aus", b["quelle"]))
    sagen("  %-26s %s" % ("nach", b["ziel"]))
    if b.get("sicherung"):
        sagen("  %-26s %s" % ("Sicherung vorher", b["sicherung"]))
    sagen("")
    for feld, text in (("dokumente_neu", "Dokumente kommen dazu"),
                       ("dokumente_bekannt", "schon vorhanden (Inhalt gleich)"),
                       ("dateien_kopiert", "Dateien werden kopiert"),
                       ("ohne_datei", "Zeilen OHNE Datei (uebersprungen)"),
                       ("konten_neu", "Konten"),
                       ("auszuege_neu", "Kontoauszuege"),
                       ("buchungen_neu", "Buchungen"),
                       ("buchungen_ohne_auszug", "Buchungen ohne Auszug (uebersprungen)"),
                       ("kassenzettel_neu", "Kassenzettel"),
                       ("regeln_neu", "gelernte Regeln")):
        sagen("  %-26s %d" % (text, b.get(feld, 0)))
    if b["fehler"]:
        sagen("")
        sagen("  Hinweise:")
        for f in b["fehler"][:10]:
            sagen("    - %s" % f)
        if len(b["fehler"]) > 10:
            sagen("    ... und %d weitere" % (len(b["fehler"]) - 10))
    sagen("")
    if b["trocken"]:
        sagen("  TROCKENLAUF — es wurde nichts geschrieben.")
