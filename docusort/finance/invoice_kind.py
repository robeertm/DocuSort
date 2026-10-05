"""Ist dieser Betrag eine FORDERUNG — oder nur eine Zahl auf einem Papier?

🔴 WARUM ES DIESES MODUL GIBT (02.10.2026)

`invoice_amount.find_invoice_amount()` liest den Betrag aus dem Text und legt
den gefundenen Schnipsel als Beleg daneben. Das ist richtig so — aber der
Schnipsel verraet auch, dass der Betrag oft gar keine Rechnung ist. Aus Roberts
echtem Bestand, woertlich aus `due_amount_src`:

    Gesamtes Vertragsguthaben 21.357,37 EUR    ← ein GUTHABEN
    Gesamtrente 117,81 EUR                     ← EINKOMMEN
    Gesamtersparnis: 0,45 €                    ← eine Ersparnis
    Rechnungsbetrag (Guthaben) -113,05 €       ← eine Gutschrift
    Summe Netto 6.501,11 €                     ← eine Rechnung, aber OHNE Steuer

Summiert man das stumpf, kommt „offene Summe 142.390,91 €" heraus — eine Zahl,
die niemand nachrechnen kann und die nichts bedeutet. Eine Tabelle, die falsch
summiert, ist schlimmer als keine Tabelle: man glaubt ihr.

🔑 DIE REGEL IST VORSICHTIG, NICHT KLUG. Im Zweifel `hinweis` statt
`forderung` — eine Rechnung, die in der Summe fehlt, faellt beim Durchsehen auf;
eine Rente, die als offene Forderung mitgezaehlt wird, verfaelscht still jede
Zahl auf der Seite.

🔑 ES WIRD NICHTS GELOESCHT UND NICHTS UEBERSCHRIEBEN. Diese Einordnung wird bei
der Anzeige berechnet, nicht in die Datenbank geschrieben: sie ist eine Meinung
ueber vorhandene Daten, und eine Meinung, die sich verbessert, soll nicht erst
eine Wanderung durch den Bestand brauchen.
"""
from __future__ import annotations
from ..kategorien import ist as _kat_ist

import re

# Eine Zahl, die man NICHT schuldet. Reihenfolge egal — es reicht EIN Treffer.
_HINWEIS = (
    r"vertragsguthaben",
    r"\bguthaben\b",
    r"ersparnis",
    r"\brente\b",
    r"gesamtrente",
    r"versicherungssumme",
    r"\bauszahlung\b",
    r"\berstattung\b",
    r"\bgutschrift\b",
    r"\bsparbeitrag\b",
    r"\buebertrag\b",
    r"\bübertrag\b",
)

# Ein Betrag OHNE Steuer. Das ist eine echte Forderung, aber nicht die Zahl,
# die ueberwiesen wird — die Tabelle muss das danebenschreiben, statt sie
# stillschweigend mitzusummieren.
_NETTO = (r"\bnetto\b", r"zzgl\.?\s*(?:ges\.?\s*)?(?:mwst|ust)", r"ohne\s+mwst")

# Kategorien, in denen ein Betrag grundsaetzlich kein offener Posten ist.
# 🔴 Rollen, keine Woerter: dieselbe Kategorie heisst englisch „Payroll"
# bzw. „Bank statement" (kategorien.ROLLEN).
_KEINE_RECHNUNG_ROLLEN = ("gehalt", "kontoauszug")

_RE_HINWEIS = re.compile("|".join(_HINWEIS), re.I)
_RE_NETTO = re.compile("|".join(_NETTO), re.I)


def einordnen(beleg: str | None, betrag: float | None,
              kategorie: str | None = None) -> dict:
    """Was ist das fuer ein Betrag?

    Rueckgabe:
      art       — "forderung" | "gutschrift" | "hinweis"
      vorbehalt — "netto" oder "" — etwas, das die Tabelle dazusagen muss
      grund     — warum, in einem Wort; die Oberflaeche zeigt es als Hilfe an

    🔴 `betrag is None` ist KEINE Forderung ueber 0 €. Ein Dokument ohne
    erkannten Betrag gehoert nicht in eine Summe, sondern in die Spalte „nicht
    erkannt" — sonst zieht jede Rechnung, deren Text schlecht gelesen wurde,
    die Summe lautlos nach unten.
    """
    if betrag is None:
        return {"art": "unbekannt", "vorbehalt": "", "grund": "kein Betrag erkannt"}

    text = (beleg or "").strip()

    if any(_kat_ist(kategorie, r) for r in _KEINE_RECHNUNG_ROLLEN):
        return {"art": "hinweis", "vorbehalt": "",
                "grund": "Kategorie %s" % kategorie}

    treffer = _RE_HINWEIS.search(text)
    if treffer:
        # 🔑 Eine GUTSCHRIFT ist etwas anderes als eine Rente: sie gehoert zu
        #    einer Rechnung und darf die Summe mindern. Erkennbar am Vorzeichen
        #    — der Leser hat das Minus mitgelesen.
        if betrag < 0:
            return {"art": "gutschrift", "vorbehalt": "",
                    "grund": treffer.group(0)}
        return {"art": "hinweis", "vorbehalt": "", "grund": treffer.group(0)}

    if betrag < 0:
        return {"art": "gutschrift", "vorbehalt": "", "grund": "negativer Betrag"}

    return {"art": "forderung",
            "vorbehalt": "netto" if _RE_NETTO.search(text) else "",
            "grund": ""}
