# -*- coding: utf-8 -*-
"""Der gewaehlte Zeitraum als EIN Blatt — /ausgaben.pdf.

🔑 WARUM ES DAS GIBT (05.10.2026)
Die Seite /ausgaben beantwortet „wofuer ist das Geld gegangen". Diese
Antwort will man auch abheften, dem Steuerberater schicken oder neben die
Kontoauszuege legen — und dafuer taugt ein Bildschirmfoto nicht.

🔴 EIN BLATT HEISST EIN BLATT.
Ein Bericht, der auf Seite 2 weiterlaeuft, ist genau das, was hier NICHT
bestellt war. Darum wird hier nichts „geflowt": die Hoehe jeder Zeile wird
aus dem verbleibenden Platz GERECHNET, und was nicht mehr hineinpasst,
wird zu einer Zeile „Weitere (n)" zusammengefasst. Es gibt genau einen
`showPage()`.

🔴 UND NUR ZEICHEN, DIE DIE SCHRIFT KENNT.
Helvetica wird hier in WinAnsi gesetzt. Haekchen, Dreiecke und Rauten
(✓ ▲ ▼ ◆) stehen dort NICHT drin und kaemen als schwarze Kaesten heraus —
auf einem Blatt, das niemand mehr nachsieht, bevor er es verschickt. Es
werden deshalb ausschliesslich Zeichen benutzt, die WinAnsi fuehrt
(Umlaute, €, –, •, ·).
"""
from __future__ import annotations

import io
from typing import Any, Callable

# A4 in Punkten.
BREITE, HOEHE = 595.276, 841.89
RAND = 42.0

# Dieselbe Reihe wie das Ringdiagramm auf der Seite — ein Bericht, der
# andere Farben benutzt als die Ansicht, sieht aus wie ein anderer Bericht.
FARBEN = [
    (0.06, 0.72, 0.51), (0.23, 0.51, 0.96), (0.96, 0.62, 0.07),
    (0.94, 0.27, 0.27), (0.66, 0.33, 0.97), (0.02, 0.71, 0.83),
    (0.93, 0.28, 0.60), (0.52, 0.80, 0.09), (0.98, 0.45, 0.09),
    (0.39, 0.40, 0.95), (0.08, 0.64, 0.72), (0.85, 0.47, 0.02),
]

# Zeichen, fuer die es eine BESSERE Ersetzung gibt als Weglassen. Alles
# andere wird unten gegen den Zeichenvorrat der SCHRIFT geprueft.
_ERSATZ = {
    "\u2713": "+", "\u2717": "x", "\u25b2": "+", "\u25bc": "-",
    "\u25c6": "*", "\u2192": "-", "\u21e2": "-", "\u2082": "2",
    "\u00a0": " ", "\u2011": "-", "\u2060": "",
}

NORMAL, FETT = "Helvetica", "Helvetica-Bold"
_VORRAT: "set[int] | None" = None


def _schrift_laden() -> tuple[str, str]:
    """Bitstream Vera einbetten — und zurueckgeben, womit gesetzt wird.

    🔴 WARUM NICHT HELVETICA (05.10.2026)
    Die 14 eingebauten PDF-Schriften werden NICHT eingebettet; der
    Betrachter setzt etwas Aehnliches ein. Und das Euro-Zeichen gibt es in
    der originalen Helvetica gar nicht — eingesetzt wird ein fremdes
    Zeichen, das BREITER ist als die 556 Einheiten, die die Metrik angibt.
    Auf dem fertigen Blatt stand deshalb „9.000,00 EURaus Sondertoepfen":
    das Leerzeichen dahinter WAR gezeichnet und wurde ueberdeckt.
    🔑 Gefunden nur, weil das Blatt angesehen wurde — gemessen hatte die
    Breitenrechnung alles richtig.

    Vera liegt reportlab bei, ist frei lizenziert (Bitstream), fuehrt Euro,
    Umlaute und typografische Anfuehrungszeichen — und wird EINGEBETTET.
    Damit sieht das Blatt ueberall gleich aus, auch in zehn Jahren im
    Ordner. Fehlt sie wider Erwarten, bleibt Helvetica der Rueckfall.
    """
    global _VORRAT
    import os
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    import reportlab
    ordner = os.path.join(os.path.dirname(reportlab.__file__), "fonts")
    try:
        if "DocuSort" not in pdfmetrics.getRegisteredFontNames():
            f = TTFont("DocuSort", os.path.join(ordner, "Vera.ttf"))
            pdfmetrics.registerFont(f)
            pdfmetrics.registerFont(TTFont("DocuSort-Bold",
                                           os.path.join(ordner, "VeraBd.ttf")))
            _VORRAT = set(f.face.charToGlyph)
        elif _VORRAT is None:
            _VORRAT = set(pdfmetrics.getFont("DocuSort").face.charToGlyph)
        return "DocuSort", "DocuSort-Bold"
    except Exception:          # noqa: BLE001 — lieber Helvetica als kein Blatt
        _VORRAT = None
        return NORMAL, FETT


def _sicher(text) -> str:
    """Nur Zeichen, die die Schrift wirklich hat.

    🔑 Es wird die SCHRIFT gefragt, nicht eine Liste gepflegt. Hier kommt
    Nutzertext durch (Kategorie- und Empfaengernamen) — eine gepflegte
    Liste waere beim ersten unerwarteten Zeichen still falsch, und auf
    einem Blatt, das niemand mehr nachsieht, faellt ein schwarzer Kasten
    erst dem Empfaenger auf.
    """
    s = "" if text is None else str(text)
    for a, b in _ERSATZ.items():
        s = s.replace(a, b)
    if _VORRAT is None:
        return s.encode("cp1252", "replace").decode("cp1252").replace("?", "")
    return "".join(ch for ch in s if ord(ch) in _VORRAT)


class Blatt:
    """Ein duenner Mantel um den Zeichner — kuerzt jeden Text auf die
    Breite, die er bekommen hat, damit nichts ueber den Rand laeuft."""

    def __init__(self, c, normal=NORMAL, fett=FETT):
        self.c = c
        self.normal_name, self.fett_name = normal, fett

    def text(self, x, y, s, *, groesse=9.0, fett=False, farbe=(0.1, 0.12, 0.16),
             breite=None, rechts=False):
        s = _sicher(s)
        font = self.fett_name if fett else self.normal_name
        if breite:
            s = self.kuerze(s, font, groesse, breite)
        self.c.setFont(font, groesse)
        self.c.setFillColorRGB(*farbe)
        (self.c.drawRightString if rechts else self.c.drawString)(x, y, s)
        return self.c.stringWidth(s, font, groesse)

    def kuerze(self, s, font, groesse, breite):
        if self.c.stringWidth(s, font, groesse) <= breite:
            return s
        while s and self.c.stringWidth(s + "...", font, groesse) > breite:
            s = s[:-1]
        return s + "..."

    def linie(self, x1, y, x2, farbe=(0.85, 0.87, 0.9), dicke=0.6):
        self.c.setStrokeColorRGB(*farbe)
        self.c.setLineWidth(dicke)
        self.c.line(x1, y, x2, y)

    def balken(self, x, y, b, h, farbe, radius=1.5):
        self.c.setFillColorRGB(*farbe)
        self.c.roundRect(x, y, max(b, 0.8), h, radius, stroke=0, fill=1)


def spending_pdf(data: dict[str, Any], *, t: Callable[[str], str],
                 label: Callable[[str], str], geld: Callable[[float], str],
                 titel: str = "DocuSort", erzeugt: str = "",
                 konten: str = "") -> bytes:
    """Zeichnet den Zeitraum aus `data` (wie `/api/ausgaben/data` ihn
    liefert) auf genau eine A4-Seite und gibt die Bytes zurueck.

    `t` uebersetzt einen Schluessel, `label` einen Kategorienamen, `geld`
    formatiert einen Betrag — alle drei kommen von aussen, damit das Blatt
    in derselben Sprache und Schreibweise steht wie die Seite.
    """
    from reportlab.pdfgen import canvas

    normal, fett = _schrift_laden()
    puffer = io.BytesIO()
    c = canvas.Canvas(puffer, pagesize=(BREITE, HOEHE))
    c.setTitle(_sicher("%s - %s" % (t("spending.title"), data.get("month_label") or "")))
    b = Blatt(c, normal, fett)
    # 🔴 Ein eigener weisser Grund. Eine PDF-Seite OHNE gezeichneten
    #    Hintergrund ist durchsichtig — gedruckt faellt das nicht auf, aber
    #    jeder Betrachter mit dunklem Grund (und jedes Werkzeug, das die
    #    Seite in ein Bild wandelt) zeigt dann weisse Schrift auf schwarz,
    #    bzw. hier dunkle Schrift auf schwarz: unlesbar. Beim ersten
    #    Rastern genau so passiert.
    c.setFillColorRGB(1, 1, 1)
    c.rect(0, 0, BREITE, HOEHE, stroke=0, fill=1)
    links, rechts = RAND, BREITE - RAND
    innen = rechts - links
    y = HOEHE - RAND

    # ---------------- Kopf ----------------
    b.text(links, y - 4, t("spending.title"), groesse=18, fett=True)
    b.text(rechts, y - 4, titel, groesse=9, farbe=(0.55, 0.58, 0.63), rechts=True)
    y -= 22
    b.text(links, y, data.get("month_label") or "", groesse=11, fett=True,
           farbe=(0.25, 0.28, 0.33), breite=innen * 0.7)
    if erzeugt:
        b.text(rechts, y, erzeugt, groesse=8, farbe=(0.6, 0.63, 0.68), rechts=True)
    y -= 13
    unterzeile = data.get("range_label") or ""
    if konten:
        unterzeile = (unterzeile + "  ·  " + konten) if unterzeile else konten
    if unterzeile:
        b.text(links, y, unterzeile, groesse=8.5, farbe=(0.55, 0.58, 0.63),
               breite=innen)
        y -= 11
    y -= 6
    b.linie(links, y, rechts, farbe=(0.2, 0.22, 0.26), dicke=1.0)
    y -= 26

    # ---------------- Die drei Zahlen ----------------
    aus = float(data.get("total") or 0.0)
    ein = float(data.get("income_total") or 0.0)
    netto = float(data.get("net") or 0.0)
    spalten = [
        (t("spending.income_total"), geld(ein), (0.02, 0.55, 0.38)),
        (t("spending.total_spent"), geld(aus), (0.80, 0.16, 0.24)),
        (t("finance.net"), ("+" if netto >= 0 else "") + geld(netto),
         (0.02, 0.55, 0.38) if netto >= 0 else (0.80, 0.16, 0.24)),
    ]
    sb = innen / 3.0
    for i, (kopf, wert, farbe) in enumerate(spalten):
        x = links + i * sb
        b.text(x, y, kopf.upper(), groesse=7.5, farbe=(0.55, 0.58, 0.63),
               breite=sb - 10)
        b.text(x, y - 20, wert, groesse=17, fett=True, farbe=farbe,
               breite=sb - 10)
    y -= 38

    # Vorperiode — eine Zahl ohne Vergleich ist eine halbe Aussage.
    if data.get("prev_month"):
        vor = float(data.get("prev_total") or 0.0)
        d = aus - vor
        richtung = t("spending.pdf_more") if d > 0 else t("spending.pdf_less")
        b.text(links, y, "%s %s  (%s: %s)"
               % (geld(abs(d)), richtung, data.get("prev_month_label") or "",
                  geld(vor)),
               groesse=8.5, farbe=(0.45, 0.48, 0.53), breite=innen)
        y -= 14

    # Was bewusst draussen steht (oder bewusst drin).
    sonder = float(data.get("special_total") or 0.0)
    if sonder > 0:
        wort = (t("spend.special_counted") if data.get("special_included")
                else t("spend.special_sum"))
        b.text(links, y, "* %s %s" % (geld(sonder), wort), groesse=8.5,
               farbe=(0.65, 0.45, 0.05), breite=innen)
        y -= 14
    y -= 6
    b.linie(links, y, rechts)
    y -= 20

    # ---------------- Kategorien ----------------
    b.text(links, y, t("spending.by_category").upper(), groesse=8,
           fett=True, farbe=(0.4, 0.43, 0.48))
    y -= 16

    kats = [k for k in (data.get("categories") or []) if float(k.get("total") or 0) > 0]

    # 🔴 HIER WIRD AUS „passt hoffentlich" EINE RECHNUNG.
    #    Unten muss die Fusszeile Platz haben; was darueber bleibt, wird
    #    durch die Zeilenhoehe geteilt. Mehr Kategorien als Zeilen gibt es
    #    oft — die uebrigen werden zu EINER Zeile zusammengefasst, damit die
    #    Summe der Seite trotzdem aufgeht.
    fuss_hoehe = 34.0
    zeile = 15.0
    platz = y - (RAND + fuss_hoehe)
    max_zeilen = max(3, int(platz // zeile))
    rest = []
    if len(kats) > max_zeilen:
        rest = kats[max_zeilen - 1:]
        kats = kats[:max_zeilen - 1]

    groesste = max([float(k["total"]) for k in kats] or [1.0])
    # Spalten: Punkt | Name | Balken | Betrag | Anteil
    x_punkt = links
    x_name = links + 10
    w_name = 150.0
    x_balken = x_name + w_name + 6
    w_betrag, w_anteil = 62.0, 38.0
    w_balken = rechts - x_balken - w_betrag - w_anteil - 12

    for i, k in enumerate(kats):
        wert = float(k["total"])
        farbe = FARBEN[i % len(FARBEN)]
        b.balken(x_punkt, y + 1.5, 5, 5, farbe, radius=2.5)
        b.text(x_name, y, label(k["category"]), groesse=9, breite=w_name)
        b.balken(x_balken, y - 1, w_balken * (wert / groesste), 7.5, farbe)
        b.text(rechts - w_anteil - 8, y, geld(wert), groesse=9, fett=True,
               rechts=True)
        b.text(rechts, y, "%s %%" % ("%.1f" % float(k.get("share") or 0.0)).replace(".", ","),
               groesse=8.5, farbe=(0.55, 0.58, 0.63), rechts=True)
        y -= zeile

    if rest:
        summe = sum(float(k["total"]) for k in rest)
        anteil = sum(float(k.get("share") or 0.0) for k in rest)
        b.balken(x_punkt, y + 1.5, 5, 5, (0.62, 0.65, 0.70), radius=2.5)
        b.text(x_name, y, t("spending.pdf_more_cats").replace("{n}", str(len(rest))),
               groesse=9, farbe=(0.45, 0.48, 0.53), breite=w_name)
        b.balken(x_balken, y - 1, w_balken * (summe / groesste), 7.5,
                 (0.62, 0.65, 0.70))
        b.text(rechts - w_anteil - 8, y, geld(summe), groesse=9, fett=True, rechts=True)
        b.text(rechts, y, "%s %%" % ("%.1f" % anteil).replace(".", ","),
               groesse=8.5, farbe=(0.55, 0.58, 0.63), rechts=True)
        y -= zeile

    # ---------------- Einnahmen, wenn der Platz sicher reicht ----------
    # 🔴 „Wenn noch Platz ist" wird GERECHNET, nicht geschaetzt. Der Block
    #    erscheint nur, wenn er VOLLSTAENDIG unter die Ausgaben passt —
    #    sonst bliebe er halb stehen oder schoebe die Seite auf zwei.
    einnahmen = [k for k in (data.get("income_categories") or [])
                 if float(k.get("total") or 0) > 0]
    rest_platz = y - (RAND + fuss_hoehe)
    if einnahmen and (20 + len(einnahmen) * zeile) <= rest_platz:
        y -= 8
        b.linie(links, y, rechts)
        y -= 18
        b.text(links, y, t("finance.pie.income_title").upper(), groesse=8,
               fett=True, farbe=(0.4, 0.43, 0.48))
        y -= 16
        groesste_e = max(float(k["total"]) for k in einnahmen)
        for i, k in enumerate(einnahmen):
            wert = float(k["total"])
            farbe = (0.06, 0.60, 0.44)
            b.balken(x_punkt, y + 1.5, 5, 5, farbe, radius=2.5)
            b.text(x_name, y, label(k["category"]), groesse=9, breite=w_name)
            b.balken(x_balken, y - 1, w_balken * (wert / groesste_e), 7.5, farbe)
            b.text(rechts - w_anteil - 8, y, geld(wert), groesse=9, fett=True,
                   rechts=True)
            b.text(rechts, y, "%s %%" % ("%.1f" % float(k.get("share") or 0.0)).replace(".", ","),
                   groesse=8.5, farbe=(0.55, 0.58, 0.63), rechts=True)
            y -= zeile

    # ---------------- Fuss ----------------
    y = RAND + fuss_hoehe - 12
    b.linie(links, y + 10, rechts)
    fest = float(data.get("fixed_total") or 0.0)
    veraenderlich = float(data.get("variable_total") or 0.0)
    b.text(links, y - 2, "%s %s  ·  %s %s"
           % (t("spending.fixed_pill"), geld(fest),
              t("spending.variable_part"), geld(veraenderlich)),
           groesse=8.5, farbe=(0.45, 0.48, 0.53), breite=innen * 0.7)
    b.text(rechts, y - 2, t("spending.pdf_footer"), groesse=7.5,
           farbe=(0.65, 0.68, 0.72), rechts=True)

    c.showPage()
    c.save()
    return puffer.getvalue()
