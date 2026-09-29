#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prueft „Faellig demnaechst": was als bezahlt gilt und was auf der Karte steht.

    python3 probe_fristen.py

Baut eine eigene, leere Datenbank in einem Wegwerf-Ordner. Ruehrt keine
Installation und keine echten Daten an.

🔴 Drei Befunde vom 29.09.2026, an der Live-Datenbank gemessen:

  ① **Zwei Arztrechnungen blieben offen, obwohl sie bezahlt waren.** Betrag auf
    den Cent, Zeitfenster richtig — gescheitert ist es am EMPFAENGER: die
    Rechnung kommt vom Arzt, ueberwiesen wird an seine Verrechnungsstelle. Kein
    gemeinsames Wort. 🔑 Aber die RECHNUNGSNUMMER steht auf beiden Seiten, im
    Dokument und im Verwendungszweck. Also gibt es einen zweiten Weg, den
    Empfaenger zu belegen — und er ist strenger als der Name, nicht lockerer:
    acht Ziffern muessen gleich sein.

  ② **Eine Gutschrift stand als Forderung da.** „Rechnungsbetrag (Guthaben)
    -113,05 €" — Geld, das zurueckkommt, kann man nicht ueberweisen.

  ③ **Bezahltes blieb ewig stehen.** Es soll ein paar Tage gruen sichtbar sein
    und dann von selbst verschwinden.

Jede dieser Proben hat ihre Gegenprobe: die Falle aus 0.49.0 (eine Buchung ueber
denselben Betrag von einem FREMDEN Empfaenger) muss weiterhin abgelehnt werden.
"""
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from docusort.db import Database                                    # noqa: E402

F = []
HEUTE = date.today()


def pruefe(name, ist, soll=True, hinweis=""):
    F.append((name, ist, soll))
    print(("  OK   " if ist == soll else "  FEHL ") + name
          + (("   — " + hinweis) if hinweis else "")
          + ("" if ist == soll else "   ist=%r soll=%r" % (ist, soll)))


def tag(versatz):
    return (HEUTE + timedelta(days=versatz)).isoformat()


ORDNER = Path(tempfile.mkdtemp(prefix="ds-fristen-"))
db = Database(ORDNER / "probe.db")
roh = sqlite3.connect(ORDNER / "probe.db")


def dokument(did, sender, betrag, faellig, text, kategorie="Rechnungen"):
    roh.execute(
        "INSERT INTO documents (id, filename, original_name, category, library_path,"
        " status, created_at, sender, subject, doc_date, due_date, due_kind,"
        " due_amount, due_amount_src, extracted_text) "
        "VALUES (?,?,?,?,?,'filed',?,?,?,?,?,'zahlung',?,?,?)",
        (did, "d%d.pdf" % did, "d%d.pdf" % did, kategorie, "/x/d%d.pdf" % did,
         HEUTE.isoformat(), sender, "Rechnung %d" % did, tag(-20), faellig,
         betrag, "Betrag", text))


def buchung(tid, betrag, datum, empfaenger, zweck):
    roh.execute(
        "INSERT INTO transactions (id, statement_id, amount, booking_date,"
        " counterparty, purpose) VALUES (?,1,?,?,?,?)",
        (tid, betrag, datum, empfaenger, zweck))


print("\n── 1. Rechnung und Zahlung finden sich ueber die Belegnummer ───────")
# Die echte Gestalt: Arzt stellt die Rechnung, die Verrechnungsstelle kassiert.
dokument(1, "Dr. med. dent. Uwe Weber, Gemeinschaftspraxis", 123.24, tag(2),
         "Rechnungsnummer : 01-1425-137975 Offener Re.-Betrag: 123,24 EUR")
buchung(11, -123.24, tag(-1), "Privataerztliche Verrechnungsstelle Sachsen GmbH",
        "ONLINE-UEBERWEISUNG TERM. 01-1425-137975 DATUM 15.09.2026, 19.25 UHR")

# 🔴 Die Gegenprobe, die es seit 0.49.0 gibt: derselbe Betrag, dasselbe Fenster,
#    ein voellig fremder Empfaenger — und KEINE gemeinsame Nummer.
dokument(2, "Telekom Deutschland GmbH", 54.95, tag(3),
         "Rechnungsbetrag 54,95 € Buchungskonto 563 422 1440")
buchung(12, -54.95, tag(-1), "AMAZON PAYMENTS EUROPE S.C.A.",
        "AMZN Mktp DE 302-8812345-1122334")

# Und die dritte Gestalt: dasselbe Datum steht in beiden — ein Datum ist KEINE
# Belegnummer, sonst passt jedes Dokument auf jede Buchung desselben Tages.
dokument(3, "Stadtwerke Musterstadt", 88.10, tag(4),
         "Rechnungsdatum 15.09.2026 Rechnungsbetrag 88,10 €")
buchung(13, -88.10, tag(-1), "Fremde Firma XY",
        "UEBERWEISUNG 15.09.2026 Danke")
roh.commit()

s = db.finance_match_due_payments()
stand = dict(roh.execute("SELECT id, paid_tx_id FROM documents").fetchall())
pruefe("die Arztrechnung findet ihre Ueberweisung ueber die Rechnungsnummer",
       stand.get(1) == 11, hinweis="Empfaenger heissen voellig verschieden")
pruefe("die fremde Buchung ueber denselben Betrag wird NICHT genommen",
       stand.get(2) is None,
       hinweis="Amazon zahlt keine Telefonrechnung")
pruefe("ein gemeinsames DATUM reicht nicht als Beleg",
       stand.get(3) is None)
pruefe("insgesamt genau eine Zuordnung", s["matched"] == 1,
       hinweis="matched=%d" % s["matched"])

print("\n── 2. Die Belegnummer selbst ───────────────────────────────────────")
n = Database._belegnummern
pruefe("eine Rechnungsnummer wird gelesen",
       "011425137975" in n("Rechnungsnummer : 01-1425-137975"))
pruefe("ein Vertragskonto mit Leerzeichen auch",
       "5634221440" in n("Buchungskonto 563 422 1440"))
pruefe("ein Datum ist keine Belegnummer", n("Rechnungsdatum 15.09.2026") == set())
pruefe("eine kurze Nummer ist keine Belegnummer", n("Kundennr. 12345") == set())
pruefe("leerer Text gibt nichts", n("") == set() and n(None) == set())

print("\n── 3. Eine Gutschrift ist keine Forderung ──────────────────────────")
dokument(4, "Telekom Deutschland GmbH", -113.05, tag(5),
         "Rechnungsbetrag (Guthaben) -113,05 € schreiben wir gut")
roh.commit()
karte = {z["id"] for z in db.upcoming_deadlines()}
pruefe("die Gutschrift steht NICHT auf der Karte", 4 not in karte)
mahnung = {z["id"] for z in db.deadlines_needing_notice()}
pruefe("und sie wird auch nicht angemahnt", 4 not in mahnung)
pruefe("eine echte offene Forderung steht sehr wohl da", 2 in karte)

print("\n── 4. Bezahltes verschwindet nach ein paar Tagen ───────────────────")
pruefe("die bezahlte Rechnung ist gleich noch zu sehen", 1 in karte,
       hinweis="man will sehen, DASS die Zahlung erkannt wurde")
# Dieselbe Zeile, nur die Zahlung liegt laenger zurueck.
roh.execute("UPDATE documents SET paid_at = ? WHERE id = 1",
            (tag(-(Database.BEZAHLT_SICHTBAR_TAGE + 1)),))
roh.commit()
spaeter = {z["id"] for z in db.upcoming_deadlines()}
pruefe("nach der Frist ist sie von selbst weg", 1 not in spaeter,
       hinweis="Frist: %d Tage" % Database.BEZAHLT_SICHTBAR_TAGE)
pruefe("die offene Forderung steht weiterhin da", 2 in spaeter)
# Am Rand der Frist muss sie noch stehen — sonst ist die Grenze um einen Tag falsch.
roh.execute("UPDATE documents SET paid_at = ? WHERE id = 1",
            (tag(-Database.BEZAHLT_SICHTBAR_TAGE),))
roh.commit()
pruefe("am letzten Tag der Frist steht sie noch",
       1 in {z["id"] for z in db.upcoming_deadlines()})

roh.close()
shutil.rmtree(ORDNER, ignore_errors=True)

schlecht = [k for k, i, sl in F if i != sl]
print("\n%s  %d Proben, %d Fehlschlaege"
      % ("🔴 ROT" if schlecht else "GRUEN", len(F), len(schlecht)))
for k in schlecht:
    print("   FEHL:", k)
sys.exit(1 if schlecht else 0)
