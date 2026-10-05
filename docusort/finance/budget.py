# -*- coding: utf-8 -*-
"""Der Budgetplaner — wieviel ist wirklich frei, und wo kommt mehr her.

🔑 DIE DREI ENTSCHEIDUNGEN, DIE DIESES MODUL TRAEGT

1. **Gerechnet wird mit dem MEDIAN, nie mit dem Mittel.** In echten
   Kontodaten liegen zwischen beiden Welten: in Roberts Archiv steht einem
   Median von 9 692 EUR Monatsausgaben ein Monat mit 60 562 EUR gegenueber
   (ein Hausbau-Abschlag aus einem Erbe). Ein Mittelwert haette daraus
   „normal sind 14 000 EUR" gemacht und jede Aussage des Planers wertlos.
   Der Median sagt, wie ein GEWOEHNLICHER Monat aussieht — und genau den
   plant man.

2. **Nicht jeder Eingang ist Einkommen.** Erbschaft, Erstattungen,
   aufgeloeste Sparbuecher, Steuerrueckzahlungen: alles echtes Geld, aber
   nichts, worauf man eine monatliche Rate baut. Verplant wird nur, was
   regelmaessig kommt (Gehalt, staatliche Leistungen); der Rest wird
   ausgewiesen, aber nicht eingerechnet. Ein Planer, der 90 000 EUR Erbe
   durch zwoelf teilt, verspricht 7 500 EUR im Monat, die es nicht gibt.

3. **Wie fest ein Posten ist, wird GEMESSEN, nicht geraten.** Ob
   „lebensmittel" ein fester Posten ist, haengt nicht am Namen — es haengt
   daran, wieviel der Ausgaben dieser Kategorie von wiederkehrenden
   Vertraegen getragen wird. Dafuer gibt es `finance_fixed_costs`: deckt es
   den groessten Teil der Kategorie ab, ist sie gebunden (Kuendigung noetig);
   sonst ist sie steuerbar (Verhalten reicht). Das funktioniert auch fuer
   eigene Kategorien, die dieses Modul nie gesehen hat.

🔴 UND: dieses Modul rechnet. Es entscheidet nichts. Jede Zahl, die aus der
   KI kommt, ist ein VORSCHLAG und wird hier nachgerechnet und gedeckelt —
   `plan()` nimmt nur Reglerstellungen entgegen und rechnet daraus die
   Summen. Siehe `budget_ai.py`.
"""

from __future__ import annotations

import json as _json
from datetime import date as _date
from statistics import median
from typing import Any

# Eingaenge, auf die man eine monatliche Rate bauen kann. Alles andere ist
# Geld, aber kein Einkommen. 🔑 Eigene Kategorien des Nutzers landen
# automatisch bei „einmalig" — das ist die sichere Seite.
VERLAESSLICH = ("gehalt", "rente-zuschuss")

# 🔴 KATEGORIE-SCHLUESSEL SIND NICHT UEBERALL DEUTSCH. DocuSorts eingebaute
#    heissen `miete`/`nebenkosten`, eine englische Installation legt
#    `rent-housing`/`utilities` an, und jeder darf eigene erfinden. Eine
#    feste Namensliste greift dort still daneben — in der Demo-Welt stand
#    deshalb „Wohnkosten heute: 0 EUR" neben einem Mietposten von 1 180 EUR.
#    Darum WORTTEILE in mehreren Sprachen statt ganzer Schluessel.
#
# 🔑 Und das ist nur ein BODEN. Die eigentliche Arbeit macht die Messung
#    (`vertragsanteil >= GEBUNDEN_AB`); diese Worte fangen den Fall, dass
#    gerade wenig gebucht wurde.
def _passt(schluessel: str, worte: tuple[str, ...]) -> bool:
    k = (schluessel or "").lower()
    return any(w in k for w in worte)


WOHNEN_WORTE = ("miete", "wohn", "rent", "housing", "loyer", "alquiler",
                "affitto", "nebenkosten", "utilit", "strom", "electric",
                "energie", "energy", "heiz", "heating")

GEBUNDEN_WORTE = WOHNEN_WORTE + (
    "kredit", "darlehen", "loan", "mortgage", "hypothek", "prestito",
    "prestamo", "pret", "tilgung",
    "versicher", "insur", "assuran", "seguro", "assicuraz",
    "abo", "subscri", "abbonam", "suscrip",
    "telefon", "phone", "internet", "mobilfunk", "telecom",
    "steuer", "tax", "impot", "impuesto", "imposta",
    "gebuehr", "gebuhr", "fee", "frais", "comision", "commission",
)

# Was gar nicht erst in die Monatsrechnung gehoert: Umbuchungen und Geld,
# das nur den Platz wechselt.
NIE = ("uebertrag",)

# Ab diesem Anteil gilt eine Kategorie als gebunden: so viel von ihren
# Ausgaben wird von echten Vertraegen getragen.
GEBUNDEN_AB = 0.60

# 🔴 VERTRAG IST NICHT GEWOHNHEIT, und der Unterschied entscheidet alles.
#    `finance_fixed_costs` fasst beides als „wiederkehrend" zusammen: die
#    Miete und den woechentlichen Supermarkt. Fuer einen Planer sind das
#    zwei Welten — das eine aendert man nur mit einer Kuendigung, das andere
#    mit einer Entscheidung.
#    Gemessen trennt sie `exact_share`: zahlt ein Posten immer DENSELBEN
#    Betrag, ist es ein Vertrag (Miete 1,00 · Autokredit 1,00 · Versicherung
#    1,00); schwankt er, ist es Verhalten (Lebensmittel 0,00 bei 175
#    Buchungen). Dazu der Rhythmus: „Ø monatlich" heisst „im Schnitt", also
#    gerade kein Termin.
VERTRAG_AB = 0.50


def _ist_vertrag(posten: dict[str, Any]) -> bool:
    """Echter Vertrag — oder nur eine regelmaessige Gewohnheit?"""
    rhythmus = str(posten.get("rhythm") or "")
    if rhythmus.startswith("\u00d8"):          # „Ø monatlich" = ein Schnitt, kein Termin
        return False
    return float(posten.get("exact_share") or 0.0) >= VERTRAG_AB

ARTEN = ("gebunden", "steuerbar", "projekt", "sparen")

# 🔴 Diese Vertraege gehoeren NICHT in die Vorschlagsliste. Die eigene
#    Miete zu kuendigen ist keine Sparmassnahme, sondern ein Umzug — und den
#    bildet das Ziel selbst ab (`wohnen_heute` → `wohnen_kuenftig`). Ein
#    Planer, dessen bester Vorschlag ein Auszug ist, wird nicht gelesen.
#    Sichtbar bleiben sie trotzdem: in der Vertragsliste ihres Topfes.
GRUNDBEDARF_WORTE = WOHNEN_WORTE + ("kredit", "darlehen", "loan", "mortgage",
                                    "hypothek", "prestito", "prestamo", "pret",
                                    "tilgung")

# Toepfe unter dieser Monatssumme bekommen keinen Regler. Ein Regler fuer
# 2,10 EUR kostet mehr Aufmerksamkeit, als er je einbringt.
KLEINKRAM = 5.0

SCHLUESSEL = "finance.budgetplan"


def _monatsliste(bis: _date, anzahl: int) -> list[str]:
    """Die letzten `anzahl` VOLLEN Kalendermonate, aeltester zuerst.

    🔴 Der laufende Monat ist nie dabei. Er ist halb gelebt und wuerde den
       Median nach unten ziehen — am dritten Tag des Monats haette der
       Planer verkuendet, man gebe nur noch ein Zehntel aus.
    """
    jahr, monat = bis.year, bis.month
    raus: list[str] = []
    for _ in range(anzahl):
        monat -= 1
        if monat == 0:
            monat, jahr = 12, jahr - 1
        raus.append("%04d-%02d" % (jahr, monat))
    return sorted(raus)


def _median0(werte: list[float], n: int) -> float:
    """Median ueber n Monate — fehlende Monate zaehlen als 0.

    🔑 Ohne die Nullen waere ein Posten, der in zwei von zwoelf Monaten
       auftaucht, so teuer wie einer, der jeden Monat kommt.
    """
    voll = list(werte) + [0.0] * max(0, n - len(werte))
    return round(median(voll), 2) if voll else 0.0


def lage(db, *, monate: int = 12, account_ids: list[int] | None = None,
         heute: _date | None = None) -> dict[str, Any]:
    """Die gemessene Ausgangslage: was kommt, was geht, was ist davon fest.

    Alles hier ist Messung. Kein Vorschlag, keine Schaetzung, keine KI.
    """
    heute = heute or _date.today()
    schluessel = _monatsliste(heute, max(3, min(36, monate)))
    von, bis = schluessel[0] + "-01", schluessel[-1] + "-31"
    n = len(schluessel)

    acc_sql, acc_args = "", ()
    if account_ids:
        acc_sql = " AND t.account_id IN (" + ",".join("?" * len(account_ids)) + ")"
        acc_args = tuple(account_ids)

    nie_sql = ",".join("?" * len(NIE))
    with db._lock:
        zeilen = [dict(r) for r in db._conn.execute(
            "SELECT substr(t.booking_date,1,7) AS m, COALESCE(t.category,'') AS k, "
            "       SUM(t.amount) AS summe, COUNT(*) AS anzahl "
            "FROM transactions t "
            "JOIN statements s ON s.id = t.statement_id "
            "JOIN documents  d ON d.id = s.doc_id "
            "LEFT JOIN accounts a ON a.id = t.account_id "
            "WHERE d.deleted_at IS NULL AND COALESCE(a.is_savings, 0) = 0 "
            "  AND COALESCE(t.category,'') NOT IN (" + nie_sql + ") "
            "  AND t.booking_date >= ? AND t.booking_date <= ?" + acc_sql + " "
            "GROUP BY 1, 2",
            (*NIE, von, bis, *acc_args)).fetchall()]

    ein: dict[str, dict[str, float]] = {}       # Monat -> Kategorie -> Summe
    aus: dict[str, dict[str, float]] = {}
    for z in zeilen:
        s = float(z["summe"] or 0.0)
        topf = ein if s > 0 else aus
        topf.setdefault(z["m"], {})[z["k"] or "sonstiges"] = abs(round(s, 2))

    # ---------------------------------------------------------------- Einnahmen
    verlaesslich_reihe = [
        round(sum(v for k, v in ein.get(m, {}).items() if k in VERLAESSLICH), 2)
        for m in schluessel
    ]
    einmalig: dict[str, float] = {}
    for m in schluessel:
        for k, v in ein.get(m, {}).items():
            if k not in VERLAESSLICH:
                einmalig[k] = round(einmalig.get(k, 0.0) + v, 2)

    einnahmen = {
        "median": _median0(verlaesslich_reihe, n),
        "mittel": round(sum(verlaesslich_reihe) / n, 2) if n else 0.0,
        "min": round(min(verlaesslich_reihe), 2) if verlaesslich_reihe else 0.0,
        "max": round(max(verlaesslich_reihe), 2) if verlaesslich_reihe else 0.0,
        "reihe": [{"monat": m, "summe": s} for m, s in zip(schluessel, verlaesslich_reihe)],
        "kategorien": list(VERLAESSLICH),
        # 🔑 Ausgewiesen, aber NICHT eingerechnet — und zwar sichtbar, damit
        #    niemand denkt, der Planer habe sie uebersehen.
        "einmalig": [{"kategorie": k, "summe": v, "je_monat": round(v / n, 2)}
                     for k, v in sorted(einmalig.items(), key=lambda kv: -kv[1])
                     if v > 0],
        "einmalig_summe": round(sum(einmalig.values()), 2),
    }

    # ------------------------------------------------------- Vertraege je Kategorie
    fix = db.finance_fixed_costs(months_back=max(12, monate * 2), account_ids=account_ids)
    fix_je_kat: dict[str, float] = {}        # nur ECHTE Vertraege
    gewohnheit_je_kat: dict[str, float] = {}  # regelmaessig, aber frei
    posten_je_kat: dict[str, list[dict[str, Any]]] = {}
    for p in fix.get("items", []):
        k = p.get("category") or "sonstiges"
        betrag = float(p.get("monthly") or 0.0)
        vertrag = _ist_vertrag(p)
        if vertrag:
            fix_je_kat[k] = round(fix_je_kat.get(k, 0.0) + betrag, 2)
        else:
            gewohnheit_je_kat[k] = round(gewohnheit_je_kat.get(k, 0.0) + betrag, 2)
        posten_je_kat.setdefault(k, []).append({
            "uid": p.get("uid"), "name": p.get("name") or "", "monatlich": p.get("monthly"),
            "betrag": p.get("amount"), "rhythmus": p.get("rhythm"),
            "letzte": p.get("last"), "naechste": p.get("next_due"),
            "vertrag": vertrag, "genauigkeit": p.get("exact_share"),
        })

    spar_kat = set(db.finance_saving_categories())
    projekt_kat = set(db.finance_special_categories())

    # ------------------------------------------------------------------ Toepfe
    toepfe: list[dict[str, Any]] = []
    for k in sorted({k for m in schluessel for k in aus.get(m, {})}):
        reihe = [aus.get(m, {}).get(k, 0.0) for m in schluessel]
        med = _median0([x for x in reihe if x], n)
        mit = round(sum(reihe) / n, 2) if n else 0.0
        vertrag = fix_je_kat.get(k, 0.0)
        # 🔑 Was man im Monat EINPLANEN muss. Eine Jahrespolice taucht in elf
        #    von zwoelf Monaten nicht auf — ihr Median waere null, und der
        #    Planer haette sie verschwiegen. Darum der groessere der beiden.
        soll = round(max(med, vertrag), 2)
        anteil = round(min(1.0, vertrag / soll), 2) if soll > 0 else 0.0
        if k in projekt_kat:
            art = "projekt"
        elif k in spar_kat:
            art = "sparen"
        elif _passt(k, GEBUNDEN_WORTE) or anteil >= GEBUNDEN_AB:
            art = "gebunden"
        else:
            art = "steuerbar"
        toepfe.append({
            "kategorie": k, "median": med, "mittel": mit, "soll": soll,
            # 🔴 Der Boden eines Reglers: unter seine laufenden Vertraege
            #    kommt ein Topf nicht, ohne dass einer davon gekuendigt wird.
            "boden": vertrag,
            "gewohnheit": gewohnheit_je_kat.get(k, 0.0),
            "hoechster": round(max(reihe), 2) if reihe else 0.0,
            "monate_mit": sum(1 for x in reihe if x > 0),
            "vertrag": vertrag, "vertragsanteil": anteil, "art": art,
            "posten": sorted(posten_je_kat.get(k, []),
                             key=lambda p: -(p["monatlich"] or 0))[:12],
            "reihe": [{"monat": m, "summe": s} for m, s in zip(schluessel, reihe)],
        })
    # 🔴 Was unter die Wahrnehmungsschwelle faellt, bekommt keinen Regler.
    #    Ohne das stehen „Erbe 0,00 EUR" und „Zinsen 0,00 EUR" als Toepfe da
    #    (eine einzelne Rueckbuchung in zwoelf Monaten) und machen die Liste
    #    unbrauchbar, lange bevor der erste echte Topf erreicht ist.
    toepfe = [t for t in toepfe if t["soll"] >= KLEINKRAM]
    toepfe.sort(key=lambda t: -t["soll"])

    def _summe(art: str) -> float:
        return round(sum(t["soll"] for t in toepfe if t["art"] == art), 2)

    laufend = round(_summe("gebunden") + _summe("steuerbar"), 2)
    ueberschuss = round(einnahmen["median"] - laufend - _summe("sparen"), 2)

    # Die Monatssummen selbst — damit die Seite zeigen kann, wie weit die
    # einzelnen Monate auseinanderliegen. Ein Median ohne Streuung ist eine
    # Behauptung.
    aus_reihe = [round(sum(aus.get(m, {}).values()), 2) for m in schluessel]

    return {
        "monate": schluessel,
        "von": von, "bis": bis, "anzahl_monate": n,
        "einnahmen": einnahmen,
        "toepfe": toepfe,
        "summen": {
            "gebunden": _summe("gebunden"),
            "steuerbar": _summe("steuerbar"),
            "projekt": _summe("projekt"),
            "sparen": _summe("sparen"),
            "laufend": laufend,
            "ueberschuss": ueberschuss,
        },
        "ausgaben_reihe": [{"monat": m, "summe": s} for m, s in zip(schluessel, aus_reihe)],
        "ausgaben_median": _median0(aus_reihe, n),
        "accounts": fix.get("accounts", []),
        "accounts_filtered": fix.get("accounts_filtered", False),
    }


# ---------------------------------------------------------------------------
# Das Ziel und die Rechnung
# ---------------------------------------------------------------------------

def wohnkosten_heute(lg: dict[str, Any]) -> float:
    """Was Wohnen heute im Monat kostet — Miete plus Nebenkosten, gemessen."""
    return round(sum(t["soll"] for t in lg["toepfe"]
                     if _passt(t["kategorie"], WOHNEN_WORTE)), 2)


def ziel_lesen(roh: dict[str, Any] | None, lg: dict[str, Any]) -> dict[str, Any]:
    """Das Ziel — generisch, mit gemessenen Vorgaben wo nichts gesagt wurde.

    🔑 Ein Budgetplaner darf nicht EIN Vorhaben kennen. Drei Bausteine
       decken alles ab, was Leute wirklich vorhaben, und sie addieren sich:

       · `mehr_monatlich` — ein fester Betrag, der monatlich uebrig bleiben soll
       · `wohnen_heute` → `wohnen_kuenftig` — eine wiederkehrende Ausgabe
         steigt bekanntermassen (Umzug, Hauskredit statt Miete, Pflegeheim)
       · `kapital_ziel` bis `monate_bis` — eine Summe muss bis zu einem
         Zeitpunkt da sein (Eigenkapital, Auto, Weltreise, Ruecklage)

       Dazu ein `puffer`, den man nicht verplant. Wer nur eines davon
       braucht, laesst die anderen auf null; die Rechnung bleibt dieselbe.
    """
    roh = roh or {}

    def _z(name: str, vorgabe: float = 0.0) -> float:
        try:
            w = float(roh.get(name))
        except (TypeError, ValueError):
            return round(vorgabe, 2)
        return round(max(0.0, w), 2)

    return {
        "titel": str(roh.get("titel") or "")[:120],
        "mehr_monatlich": _z("mehr_monatlich"),
        "wohnen_heute": _z("wohnen_heute", wohnkosten_heute(lg)),
        "wohnen_kuenftig": _z("wohnen_kuenftig"),
        "kapital_ziel": _z("kapital_ziel"),
        "kapital_da": _z("kapital_da"),
        "monate_bis": int(max(0, min(600, int(_z("monate_bis"))))),
        "puffer": _z("puffer"),
    }


def rechnen(lg: dict[str, Any], ziel: dict[str, Any],
            regler: dict[str, float] | None = None,
            gekuendigt: list[str] | None = None) -> dict[str, Any]:
    """Was der Plan ergibt. `regler` = {kategorie: neuer Monatsbetrag}.

    `gekuendigt` sind uids von Vertraegen, die der Nutzer aufgibt — sie
    senken den BODEN ihres Topfes, denn genau darum geht es: ohne Kuendigung
    kommt ein Topf nicht unter seine laufenden Vertraege.

    🔴 Hier wird jede Zahl neu gerechnet, auch die, die aus einem
       KI-Vorschlag stammt. Ein Regler kann nie unter den Boden und nie
       ueber das Doppelte des heutigen Betrags — ein Plan, der eine
       Kategorie verzehnfacht, ist kein Plan, sondern ein Tippfehler.
    """
    regler = regler or {}
    weg = set(gekuendigt or [])
    zeilen: list[dict[str, Any]] = []
    neu_laufend = 0.0
    gekuendigt_summe = 0.0

    for t in lg["toepfe"]:
        if t["art"] == "projekt":
            continue
        heute = t["soll"]
        boden = t["boden"]
        for posten in t.get("posten", []):
            if posten.get("uid") in weg and posten.get("vertrag"):
                betrag = float(posten.get("monatlich") or 0.0)
                boden = round(max(0.0, boden - betrag), 2)
                gekuendigt_summe += betrag
        boden = round(min(boden, heute), 2)

        ziel_betrag = heute
        if t["kategorie"] in regler:
            try:
                ziel_betrag = float(regler[t["kategorie"]])
            except (TypeError, ValueError):
                ziel_betrag = heute
        ziel_betrag = round(min(max(ziel_betrag, boden), heute * 2 + 50), 2)

        if t["art"] != "sparen":
            neu_laufend += ziel_betrag
        zeilen.append({
            "kategorie": t["kategorie"], "art": t["art"],
            "heute": heute, "ziel": ziel_betrag,
            "spart": round(heute - ziel_betrag, 2),
            "boden": boden, "boden_heute": t["boden"],
            "vertragsanteil": t["vertragsanteil"],
        })

    neu_laufend = round(neu_laufend, 2)
    sparen = round(sum(z["ziel"] for z in zeilen if z["art"] == "sparen"), 2)
    ueberschuss = round(lg["einnahmen"]["median"] - neu_laufend - sparen, 2)

    # Die drei Bausteine des Ziels addieren sich zum Monatsbedarf.
    mehr_wohnen = round(max(0.0, ziel["wohnen_kuenftig"] - ziel["wohnen_heute"]), 2)
    fehlt_kapital = round(max(0.0, ziel["kapital_ziel"] - ziel["kapital_da"]), 2)
    kapital_rate = round(fehlt_kapital / ziel["monate_bis"], 2) if ziel["monate_bis"] else 0.0
    bedarf = round(ziel["mehr_monatlich"] + mehr_wohnen + kapital_rate + ziel["puffer"], 2)
    luecke = round(bedarf - ueberschuss, 2)

    return {
        "zeilen": zeilen,
        "gefunden": round(sum(z["spart"] for z in zeilen), 2),
        "gekuendigt_summe": round(gekuendigt_summe, 2),
        "laufend_neu": neu_laufend,
        "sparen_neu": sparen,
        "ueberschuss_neu": ueberschuss,
        "ueberschuss_heute": lg["summen"]["ueberschuss"],
        "mehr_wohnen": mehr_wohnen,
        "mehr_monatlich": ziel["mehr_monatlich"],
        "kapital_rate": kapital_rate,
        "kapital_fehlt": fehlt_kapital,
        "bedarf": bedarf,
        "luecke": luecke,
        "geschafft": luecke <= 0,
    }


# ---------------------------------------------------------------------------
# Vorschlaege, die aus der MESSUNG kommen — nicht aus einem Modell
# ---------------------------------------------------------------------------

def vorschlaege(lg: dict[str, Any]) -> list[dict[str, Any]]:
    """Was die Zahlen selbst hergeben. Jeder Eintrag nennt seinen Beleg.

    🔑 Diese Liste braucht keine KI und ist auch ohne sie da — eine
       Installation ohne Modell ist kein halber Planer. Sie sagt nur Dinge,
       die in den Buchungen stehen, und sie sagt zu jedem Betrag, WORAUS er
       stammt. Keine Lebensberatung, keine Pauschalen, keine Prozentregeln
       aus dem Internet.
    """
    raus: list[dict[str, Any]] = []
    for t in lg["toepfe"]:
        if t["art"] in ("projekt", "sparen") or t["soll"] <= 0:
            continue
        kat, med = t["kategorie"], t["soll"]
        gelebt = [p["summe"] for p in t["reihe"] if p["summe"] > 0]

        # 1. Der Topf schwankt, und es gab gelebte Monate, die deutlich
        #    guenstiger waren. Was schon einmal gereicht hat, ist kein
        #    Verzicht, sondern eine Rueckkehr.
        if t["art"] == "steuerbar" and len(gelebt) >= 4 and med >= 30:
            niedrig = min(gelebt)
            if niedrig < med * 0.75:
                raus.append({
                    "quelle": "gemessen", "art": "schwankung", "kategorie": kat,
                    "spart": round(med - niedrig, 2), "ziel_betrag": round(niedrig, 2),
                    "beleg_zahlen": {"guenstigster_monat": round(niedrig, 2),
                                     "gewoehnlich": med, "monate": len(gelebt)},
                })

        # 2. Jeder einzelne Vertrag eines gebundenen Topfes — mit seinem
        #    vollen Monatsbetrag. Gekuendigt kostet er null; ob das geht,
        #    entscheidet der Mensch, nicht der Rechner.
        if t["art"] == "gebunden" and not _passt(kat, GRUNDBEDARF_WORTE):
            genommen = 0
            for p in sorted(t["posten"], key=lambda x: -(x.get("monatlich") or 0)):
                betrag = float(p.get("monatlich") or 0.0)
                if genommen >= 2:
                    break
                if betrag >= 5.0 and p.get("vertrag"):
                    genommen += 1
                    raus.append({
                        "quelle": "gemessen", "art": "vertrag", "kategorie": kat,
                        "posten": p.get("uid"), "name": p.get("name"),
                        "spart": round(betrag, 2), "ziel_betrag": round(med - betrag, 2),
                        "beleg_zahlen": {"betrag": p.get("betrag"),
                                         "rhythmus": p.get("rhythmus"),
                                         "naechste": p.get("naechste") or ""},
                    })

    # 🔑 Verhalten zuerst: was man durch eine Entscheidung aendert, steht
    #    vor dem, wofuer man kuendigen und warten muss.
    raus.sort(key=lambda v: (0 if v["art"] == "schwankung" else 1, -v["spart"]))
    return raus[:16]


# ---------------------------------------------------------------------------
# Speichern
# ---------------------------------------------------------------------------

def plan_lesen(db) -> dict[str, Any]:
    try:
        d = _json.loads(db.meta_get(SCHLUESSEL) or "{}")
    except ValueError:
        d = {}
    return d if isinstance(d, dict) else {}


def plan_schreiben(db, ziel: dict[str, Any], regler: dict[str, float],
                   notizen: str = "", gekuendigt: list[str] | None = None) -> dict[str, Any]:
    d = {"ziel": ziel,
         "regler": {str(k): round(float(v), 2) for k, v in (regler or {}).items()},
         "gekuendigt": [str(x)[:200] for x in (gekuendigt or [])][:200],
         "notizen": str(notizen or "")[:2000],
         "stand": _date.today().isoformat()}
    db.meta_set(SCHLUESSEL, _json.dumps(d, ensure_ascii=False))
    return d
