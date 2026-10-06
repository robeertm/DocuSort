"""Die Rechnungsauswertung: filtern, gruppieren, summieren.

🔑 WARUM DAS NICHT IN DER ROUTE STEHT
Hier liegt die ganze Rechenarbeit der Seite — und genau die muss ein
Pruefstand fahren koennen, ohne einen Webserver zu starten. Die Route
beschafft die Zeilen und reicht sie durch; entschieden wird alles hier.

🔴 ZWEI ZUSTAENDE, DIE MAN NICHT ZUSAMMENWERFEN DARF
Ein Haken von Hand und eine Buchung aus einem Kontoauszug sind beide
„erledigt", aber nicht gleich viel wert:

  * `offen`     — nichts spricht dafuer, dass sie bezahlt ist
  * `abgehakt`  — jemand hat gesagt, sie sei erledigt. UNBESTAETIGT: dahinter
                  steht kein Geldfluss, nur eine Behauptung.
  * `bezahlt`   — an eine Buchung gebunden. BESTAETIGT, und die Herkunft der
                  Buchung wird mitgenannt (Kontoauszug oder CSV-Einfuhr),
                  damit man sie nachsehen kann.

Die Seite zeigt sie darum in drei Spalten und summiert sie getrennt. Eine
Zahl, die „bezahlt" und „abgehakt" addiert, beantwortet keine Frage, die
jemand wirklich hat.
"""
from __future__ import annotations

from .invoice_kind import einordnen

# Die Arten, die in einer Forderungssumme ueberhaupt etwas zu suchen haben.
# Eine Gutschrift mindert sie (sie gehoert zur Rechnung); ein Hinweis — eine
# Rente, ein Vertragsguthaben — gehoert nicht hinein und wird nur gezeigt,
# wenn jemand ausdruecklich danach fragt.
ZAEHLENDE_ARTEN = ("forderung", "gutschrift")

GRUPPEN = ("kategorie", "absender", "jahr", "monat", "zustand", "art")


def _leer(v) -> bool:
    return v is None or str(v).strip() == ""


def zustand(z: dict) -> str:
    """offen | abgehakt | bezahlt — in dieser Rangfolge.

    🔴 Die Buchung schlaegt den Haken. Wer von Hand abhakt und spaeter wird
    die Buchung gefunden, soll den BELEG sehen, nicht seinen eigenen Haken.
    """
    if not _leer(z.get("paid_tx_id")):
        return "bezahlt"
    if not _leer(z.get("deadline_done_at")):
        return "abgehakt"
    return "offen"


def herkunft(z: dict) -> str:
    """Woher die Buchung stammt, die diese Rechnung beglichen hat."""
    if _leer(z.get("paid_tx_id")):
        return ""
    # Der Traeger einer CSV-Einfuhr ist genau an dieser Kategorie erkennbar.
    if (z.get("beleg_traeger") or "") == "_csv_container":
        return "csv"
    if not _leer(z.get("beleg_traeger")):
        return "kontoauszug"
    # Gebunden, aber die Herkunft laesst sich nicht mehr nachsehen. Das ist
    # eine eigene Auskunft und nicht dasselbe wie „Kontoauszug".
    return "unbekannt"


def anreichern(zeilen: list[dict]) -> list[dict]:
    """Jede Zeile um das ergaenzen, was die Seite zum Urteilen braucht."""
    aus = []
    for z in zeilen:
        e = einordnen(z.get("due_amount_src"), z.get("due_amount"),
                      z.get("category"))
        d = dict(z)
        d["art"] = e["art"]
        d["vorbehalt"] = e["vorbehalt"]
        d["art_grund"] = e["grund"]
        d["zustand"] = zustand(z)
        d["herkunft"] = herkunft(z)
        d["jahr"] = (z.get("doc_date") or "")[:4]
        d["monat"] = (z.get("doc_date") or "")[:7]
        aus.append(d)
    return aus


def _passt(z: dict, *, von, bis, kategorien, zustaende, arten, suche,
           ids=None) -> bool:
    datum = z.get("doc_date") or ""
    # 🔴 Ein Dokument OHNE Datum faellt aus einem Zeitraum heraus — es
    #    stillschweigend mitzuzaehlen hiesse, eine Spanne zu behaupten, die
    #    niemand geprueft hat.
    if von and (not datum or datum < von):
        return False
    if bis and (not datum or datum > bis):
        return False
    if kategorien and (z.get("category") or "") not in kategorien:
        return False
    if zustaende and z["zustand"] not in zustaende:
        return False
    if arten and z["art"] not in arten:
        return False
    if suche:
        # 🔑 ZWEI WEGE, UND BEIDE WERDEN GEBRAUCHT.
        #
        # `ids` ist die Antwort der Suchleiter — dieselbe, die die Bibliothek
        # benutzt: Wort fuer Wort, im ganzen Text eines Dokuments, Teilwoerter
        # und ein falsches Wort verzeihend. Sie findet „telekom maerz" ueber
        # zwei Felder hinweg und „kundennummer" im Inhalt eines Scans.
        #
        # Daneben bleibt der Vergleich mit den Feldern DIESER Seite, denn zwei
        # davon stehen in keinem Volltextindex: der gelesene Betrag
        # (`due_amount_src`, „Rechnungsbetrag 54,95 €") und der Dateiname. Wer
        # „54,95" tippt, meint den Betrag.
        n = suche.lower()
        felder = (z.get("sender"), z.get("subject"), z.get("category"),
                  z.get("subcategory"), z.get("original_name"),
                  z.get("due_amount_src"))
        im_feld = any(n in (f or "").lower() for f in felder)
        im_text = ids is not None and int(z.get("id") or 0) in ids
        if not (im_feld or im_text):
            return False
    return True


def _gruppenname(z: dict, gruppe: str) -> str:
    if gruppe == "absender":
        return (z.get("sender") or "").strip() or "—"
    if gruppe == "jahr":
        return z.get("jahr") or "—"
    if gruppe == "monat":
        return z.get("monat") or "—"
    if gruppe == "zustand":
        return z["zustand"]
    if gruppe == "art":
        return z["art"]
    return (z.get("category") or "").strip() or "—"


def auswerten(zeilen: list[dict], *, von: str = "", bis: str = "",
              kategorien=(), zustaende=(), arten=(), suche: str = "",
              gruppe: str = "kategorie", ids=None, stufe: str = "",
              nur_bibliothek: int = 0) -> dict:
    """Die ganze Seite in einem Aufruf.

    `arten` leer heisst: nur was zaehlt (Forderungen und Gutschriften). Wer
    die Hinweise sehen will, muss sie ausdruecklich anfordern — sonst waere
    eine Rente stillschweigend Teil der offenen Summe.
    """
    if gruppe not in GRUPPEN:
        gruppe = "kategorie"
    alle = anreichern(zeilen)
    gewaehlt = tuple(arten) if arten else ZAEHLENDE_ARTEN

    def passt(z, arten_):
        return _passt(z, von=von, bis=bis, kategorien=tuple(kategorien),
                      zustaende=tuple(zustaende), arten=arten_, suche=suche,
                      ids=ids)

    treffer = [z for z in alle if passt(z, gewaehlt)]
    # 🔴 WIE VIELE WERDEN HIER WIRKLICH VERSTECKT. Vorher zaehlte diese Zahl
    #    ueber den GESAMTEN Bestand — bei einer Suche stand also „(60)" neben
    #    dem Schalter, waehrend von den eigenen Treffern gar keiner ausgeblendet
    #    war. Oder umgekehrt: drei Treffer lagen unter dem Schalter und die Zahl
    #    sprach von sechzig. Gezaehlt wird jetzt, was dieselbe Frage OHNE den
    #    Artenfilter ergibt.
    ohne_artenfilter = [z for z in alle if passt(z, ())]

    summen = {"anzahl": len(treffer), "gesamt": 0.0,
              "offen": 0.0, "abgehakt": 0.0, "bezahlt": 0.0,
              "anzahl_offen": 0, "anzahl_abgehakt": 0, "anzahl_bezahlt": 0,
              "netto_vorbehalt": 0}
    gruppen: dict[str, dict] = {}
    for z in treffer:
        betrag = float(z.get("due_amount") or 0.0)
        summen["gesamt"] += betrag
        summen[z["zustand"]] += betrag
        summen["anzahl_" + z["zustand"]] += 1
        if z["vorbehalt"] == "netto":
            summen["netto_vorbehalt"] += 1
        g = gruppen.setdefault(_gruppenname(z, gruppe), {
            "name": _gruppenname(z, gruppe), "anzahl": 0, "summe": 0.0,
            "offen": 0.0, "abgehakt": 0.0, "bezahlt": 0.0})
        g["anzahl"] += 1
        g["summe"] += betrag
        g[z["zustand"]] += betrag

    for k in ("gesamt", "offen", "abgehakt", "bezahlt"):
        summen[k] = round(summen[k], 2)
    liste = sorted(gruppen.values(), key=lambda g: (-g["summe"], g["name"]))
    for g in liste:
        for k in ("summe", "offen", "abgehakt", "bezahlt"):
            g[k] = round(g[k], 2)

    # Die Auswahlmoeglichkeiten kommen aus den DATEN, nicht aus einer Liste im
    # Quelltext — eine eigene Kategorie des Nutzers soll im Filter auftauchen,
    # ohne dass jemand sie hier nachtraegt.
    daten = [z.get("doc_date") for z in alle if z.get("doc_date")]
    return {
        "zeilen": treffer,
        "summen": summen,
        "gruppen": liste,
        "gruppe": gruppe,
        "kategorien": sorted({(z.get("category") or "").strip()
                              for z in alle if (z.get("category") or "").strip()}),
        "arten_vorhanden": sorted({z["art"] for z in alle}),
        "zeitraum": {"von": min(daten) if daten else "",
                     "bis": max(daten) if daten else ""},
        "ausgeblendet": len(ohne_artenfilter) - len(treffer),
        # Welche Stufe der Suchleiter geantwortet hat — eine weite Antwort, die
        # wie eine genaue aussieht, waere schlimmer als keine.
        # 🔴 Nur wenn die Leiter WIRKLICH gelockert hat und daraus ein
        #    sichtbarer Treffer wurde. „54,95" findet seine zwei Rechnungen
        #    ueber den gelesenen Betrag, nicht ueber den Volltext — ein Schild
        #    „ungenauer Treffer" waere dort eine Falschaussage.
        "stufe": stufe if (suche and stufe not in ("", "genau") and ids
                           and any(int(z.get("id") or 0) in ids
                                   for z in treffer)) else "",
        # 🔑 Treffer, die es gibt, die aber keine Rechnung sind: ein Dokument
        #    ohne gelesenen Betrag steht auf dieser Seite nicht. Das stumm zu
        #    uebergehen heisst, jemanden suchen zu lassen, was man selbst schon
        #    gefunden hat.
        "nur_bibliothek": nur_bibliothek if suche else 0,
    }
