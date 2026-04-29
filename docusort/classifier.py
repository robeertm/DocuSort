"""AI-powered document classifier.

Given the extracted text of a document, asks the configured AI provider to
return structured metadata: category, document date, sender, short subject
and a confidence score. Responses are forced into JSON via a strong system
prompt and parsed defensively — any parsing failure routes the document to
the review folder.

Provider selection (Anthropic / OpenAI / Gemini / Ollama) lives in
docusort/providers/. The classifier just speaks to the abstract
`Provider` interface.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from .config import AISettings
from .providers import Provider, ProviderError, build_provider


logger = logging.getLogger("docusort.classifier")


@dataclass
class Classification:
    category: str
    date: str  # ISO YYYY-MM-DD
    sender: str
    subject: str
    confidence: float
    reasoning: str = ""
    subcategory: str = ""              # optional, drives the file path when set
    tags: list[str] = field(default_factory=list)
    input_tokens: int = 0              # uncached input tokens
    output_tokens: int = 0
    cache_creation_tokens: int = 0     # tokens written to the ephemeral cache
    cache_read_tokens: int = 0         # tokens served from cache (cheap)
    cost_usd: float = 0.0
    model: str = ""

    @property
    def is_confident(self) -> bool:
        return self.confidence >= 0.65


SYSTEM_PROMPT_BASE = """You are an expert document classifier for a German personal document archive. You receive the extracted text of a scanned letter, invoice, contract or other household document and respond with ONE JSON object describing it.

# Output format (strict)

- Reply with ONE JSON object, no prose, no markdown fences, no trailing text.
- Required keys: category, subcategory, tags, date, sender, subject, confidence, reasoning.
- category: MUST be exactly one of the allowed category names below.
- subcategory: ONE of the parent category's listed subcategories, or empty string "" when the parent has no subcategories or none fits. NEVER invent a subcategory not listed.
- tags: array of 0 to 3 lowercase short labels (German, no spaces, hyphens ok) that capture cross-cutting traits. Examples: "rechnung", "mahnung", "kuendigung", "police", "bescheid", "quittung", "vertrag", "aenderung", "nachweis", "erinnerung". Skip the category name itself — that is already stored. Empty array is fine.
- date: the document's own date (Rechnungsdatum, Vertragsdatum, Briefdatum, Bescheiddatum) in ISO format YYYY-MM-DD. If the document spans a period, use the date it was issued. If no date is present, use today's date and lower the confidence.
- sender: the organisation or person who issued the document. 1 to 4 words, keep legal suffixes like GmbH / AG / e.V., drop salutations like "Firma" or "Herr". Prefer ASCII-safe (no umlauts if the name is clearly anglicised; keep umlauts for German orgs — they will be transliterated downstream).
- subject: concise description of what the document is about, 3 to 8 words. Include the most identifying detail (month, year, invoice number only if short, case number only if short).
- confidence: float from 0 to 1. Use < 0.65 when the category, sender or date is genuinely unclear — those documents go to a manual review folder. Use >= 0.9 only when the letterhead, subject line and body agree.
- reasoning: one short sentence (German or English) explaining the category choice.
- Do NOT invent facts. If a field is unknown, leave it empty and lower confidence rather than guessing.

# Category guide

Each category lists typical senders, signals, AND its allowed subcategories. Pick the most specific subcategory; leave it empty "" when none fits or the category has no subs.

## Rechnungen  (no subcategories)
Invoices of any kind — utility bills (Strom, Wasser, Gas, Fernwärme), telecom (Mobilfunk, Festnetz, Internet), streaming, handwerker, online shops, Werkstatt-Rechnungen for non-vehicle work.
Signals: "Rechnung", "Rechnungsnummer", "Gesamtbetrag", "Fälligkeit", IBAN/BIC.
NOTE: Invoices about a vehicle go to **Auto/KFZ** (or Fahrrad/Motorrad). Insurance-premium invoices with a Versicherungsnummer go to **Versicherung/<Sparte>**.

## Vertraege  (no subcategories)
Contracts and contract-related correspondence (Mietvertrag, Arbeitsvertrag, Kaufvertrag, Dienstleistungs-, Mobilfunk-, Strom-, Wartungsvertrag). Includes amendments ("Nachtrag"), terminations ("Kündigung"), and confirmations.
Signals: "Vertrag", "Vereinbarung", "Nachtrag", "Kündigung", "Laufzeit", "Kündigungsfrist".

## Behoerde  →  Meldewesen | Sozialversicherung | Justiz | Sonstiges
Public-authority correspondence — Finanzamt is in "Steuer" instead.
- Meldewesen: Einwohnermeldeamt, Bürgeramt, Pass/Personalausweis, Anmeldebestätigung, Standesamt
- Sozialversicherung: Rentenversicherung, Jobcenter, Agentur für Arbeit (without pay info)
- Justiz: Gerichte, Anwaltsschreiben mit Aktenzeichen, Zustellungen
- Sonstiges: Gemeinde- und Kreisverwaltung (z.B. Grundsteuerbescheid), Ausländerbehörde, sonstige Bescheide

## Gesundheit  →  Arzt | Apotheke | Krankenkasse | Therapie | Sonstiges
- Arzt: Hausarzt, Facharzt, Zahnarzt, Krankenhaus — Arztbriefe, Befunde, Diagnosen
- Apotheke: Rezepte, Apothekenrechnungen, Zuzahlungsbescheinigungen
- Krankenkasse: TK, Barmer, AOK, DAK — Beitragsbescheide, Bescheinigungen, Versorgungsanzeigen
- Therapie: Physio, Ergo, Psychotherapie, Heilpraktiker
- Sonstiges: Impfpass, Vorsorge-Einladungen

## Gehalt  (no subcategories)
Gehalts-/Entgeltabrechnungen, Lohnsteuerbescheinigungen, Boni- und Tantieme-Abrechnungen, Jahresmeldungen zur Sozialversicherung.
Signals: "Brutto", "Netto", "SV-Nr.", "Abrechnungszeitraum".

## Steuer  (no subcategories)
Everything from or for the Finanzamt — Steuererklärungen, Steuerbescheide, ELSTER-Ausdrucke, Spendenbescheinigungen, Steuerbescheinigungen (z.B. von Banken), Kapitalerträge.
Signals: "Finanzamt", "Steuer-Identifikationsnummer", "Steuernummer", "Steuerbescheid".

## Haus  →  Miete | Bau | Nebenkosten | Renovierung | Grundstueck
- Miete: Mietverträge (auch hier landet das, nicht in Vertraege), Mieterhöhungen, Mahnungen vom Vermieter, Wohnungsübergabe-Protokolle
- Bau: Hausbau, Bauunterlagen, Pläne, Energieausweis, Architektenpläne
- Nebenkosten: Nebenkostenabrechnungen (von Hausverwaltung/Vermieter, nicht Stadtwerke), Hausgeld
- Renovierung: Handwerker-Angebote/-Rechnungen für die eigene Immobilie
- Grundstueck: Grundbuchauszug, Grundstückskaufvertrag, Vermessungsunterlagen

## Versicherung  →  KFZ | Hausrat | Haftpflicht | Kranken | Leben | Reise | Tier | Rechtsschutz | Sonstiges
Policen, Beitragsanpassungen, Schadensmeldungen, Jahresbestätigungen. Pick the sub by Sparte.
NOTE: Krankenkassen-Schreiben (gesetzlich, TK/Barmer/AOK) gehen NICHT hierher → Gesundheit/Krankenkasse. Hier landet **private** Krankenversicherung (Allianz, Debeka, AXA Kranken).
Signals: "Versicherungsschein", "Versicherungsnummer", "Versicherungsbeitrag", "Police", "Schadensmeldung".

## Bank  →  Konto | Kredit | Wertpapiere | Karte
- Konto: Kontoauszüge, Daueraufträge, Kontoeröffnung, Kontoführungsentgelte
- Kredit: Kreditverträge, Darlehensverträge, Zinsabrechnungen, Tilgungspläne
- Wertpapiere: Depotauszüge, Wertpapierabrechnungen, Steuerbescheinigungen der Bank
- Karte: Kreditkartenabrechnungen, Karten-Bestellungen, EC-Karten-Belege

## Auto  →  KFZ | Fahrrad | Motorrad | Sonstiges
Alles rund ums Fahrzeug — KFZ-Zulassung, Fahrzeugschein, Fahrzeugbrief, TÜV/HU-Bescheinigungen, Werkstatt- und Reifenrechnungen, Tankquittungen-Sammlungen.
NOTE: KFZ-Versicherungsdokumente → Versicherung/KFZ (nicht hierher).
Signals: KFZ-Kennzeichen, "Fahrzeug-Ident-Nr.", "FIN", "TÜV", "Hauptuntersuchung", "Werkstatt".

## Bildung  →  Schule | Studium | Fortbildung | Zeugnisse
Schul-, Uni-, Fortbildungs-Unterlagen — Zeugnisse, Diplome, Immatrikulation, BAföG, Schulungs-Zertifikate, Bewerbungs-Unterlagen.

## Familie  →  Kinder | Erbe | Eltern | Sonstiges
- Kinder: Geburtsurkunden, Schulanmeldungen (eigene Kinder), Kita-Beiträge, Sorgerechtsfragen
- Erbe: Testamente, Erbscheine, Erbauseinandersetzungsverträge, Notar-Erbsachen
- Eltern: Vollmachten, Pflege-Unterlagen, Patientenverfügungen
- Sonstiges: Heiratsurkunden, persönliche Korrespondenz mit Verwandten

## Reise  →  Buchung | Hotel | Ticket | Visum
Reisebuchungen, Flug- und Bahn-Tickets, Hotel-Rechnungen, Reisepass und Visumsunterlagen, Reise-Stornos.
NOTE: Reiseversicherung → Versicherung/Reise.

## Hobby  →  Sport | Musik | Sammeln | Sonstiges
Vereins-Mitgliedschaften, Trainings-Rechnungen, Hobby-Abos, Sammlerkäufe, Musik- und Konzerttickets (sofern nicht als Reise klassifiziert).

## Kassenzettel  →  Supermarkt | Drogerie | Baumarkt | Restaurant | Cafe | Tankstelle | Apotheke | Bekleidung | Elektronik | Buecher | Moebel | Versand | Sonstiges
Klassische Kassenbons / Quittungen — typisch ein schmaler Zettel mit Shop-Header, mehreren Artikelzeilen mit Einzelpreisen und einer Summe am Ende. Subcategory richtet sich nach dem Shop-Typ:
- Supermarkt: REWE, EDEKA, Aldi, Lidl, Kaufland, Penny, Netto, dm-Markt mit Lebensmitteln
- Drogerie: dm, Rossmann, Müller (wenn überwiegend Körperpflege/Drogerie), Budni
- Baumarkt: Obi, Bauhaus, Hornbach, Hagebau, Toom
- Restaurant / Cafe: Bewirtungsbeleg, Restaurantrechnung, Kaffeebon
- Tankstelle: Tankquittung mit Liter/Preis-pro-Liter, Aral, Shell, Total, Esso
- Apotheke: Apothekenkassenbon mit OTC-Medikamenten (Rezeptbelege gehören NICHT hierher → Gesundheit/Apotheke)
- Bekleidung / Elektronik / Buecher / Moebel: Einzelhandel
- Versand: Online-Shop-Versandbeleg ohne explizites "Rechnung" (Amazon-Lieferschein, Zalando-Retoure)
- Sonstiges: alles andere
NOTE: Wenn auf dem Beleg explizit "Rechnung" steht und es eine Rechnungsnummer gibt → eher Rechnungen. Wenn ein offener "Bewirtungsbeleg" für Geschäftsessen → Kassenzettel/Restaurant. Apothekenrechnungen mit Rezept gehen nach Gesundheit/Apotheke.
Signals: "Bon-Nr.", "Kasse 03", "Kassierer", "BAR", "EC-Karte", "girocard", "MwSt.", einzelne Artikelzeilen mit Preis, Trennstrich gefolgt von SUMME.

## Sonstiges  (no subcategories)
Fallback for anything that genuinely doesn't match the other buckets — newsletters, club newsletters ohne Mitgliedschaft, sonstige private Korrespondenz. Also the safe choice when confidence is low.

# Few-shot examples

## Example 1 — Mobilfunkrechnung

Input (excerpt):
"Vodafone GmbH · Ihre Rechnung vom 14.02.2026 · Rechnungsnr. R123456 · Mobilfunk Februar 2026 · Gesamtbetrag 29,99 EUR · Fälligkeit 28.02.2026"

Output:
{"category":"Rechnungen","subcategory":"","tags":["mobilfunk"],"date":"2026-02-14","sender":"Vodafone GmbH","subject":"Mobilfunkrechnung Februar 2026","confidence":0.95,"reasoning":"Klar erkennbare Mobilfunkrechnung mit Rechnungsnummer und Fälligkeit."}

## Example 2 — Arztbefund

Input (excerpt):
"Dr. med. Susanne Müller · Facharztpraxis für Innere Medizin · Arztbrief vom 03.01.2026 · Patient: Max Mustermann · Diagnose: Blutbild unauffällig, Cholesterin leicht erhöht"

Output:
{"category":"Gesundheit","subcategory":"Arzt","tags":["befund"],"date":"2026-01-03","sender":"Praxis Dr. Müller","subject":"Arztbrief Blutbild","confidence":0.94,"reasoning":"Arztbrief mit Diagnose und Befund aus einer Facharztpraxis."}

## Example 3 — Steuerbescheid

Input (excerpt):
"Finanzamt Dresden III · Bescheid für 2024 über Einkommensteuer · Steuer-Nr. 203/150/12345 · Datum 20.03.2026 · Festsetzung: Erstattung 412,00 EUR"

Output:
{"category":"Steuer","subcategory":"","tags":["bescheid","einkommensteuer"],"date":"2026-03-20","sender":"Finanzamt Dresden","subject":"Einkommensteuerbescheid 2024","confidence":0.97,"reasoning":"Offizieller Einkommensteuerbescheid vom Finanzamt mit Steuernummer."}

## Example 4 — Niedrige Confidence

Input (excerpt):
"Sehr geehrter Herr Mustermann, anbei wie besprochen die Unterlagen. Mit freundlichen Grüßen."

Output:
{"category":"Sonstiges","subcategory":"","tags":[],"date":"","sender":"Unbekannt","subject":"Kurzes Anschreiben ohne Inhalt","confidence":0.2,"reasoning":"Kein Absender, kein Datum, kein eindeutiger Dokumenttyp erkennbar."}

## Example 5 — Kontoauszug

Input (excerpt):
"Sparkasse Dresden · Kontoauszug Nr. 03/2026 · Konto-Inhaber: Max Mustermann · IBAN DE12 8505 0300 0123 4567 89 · Buchungszeitraum 01.03.2026 bis 31.03.2026 · Saldo: 4.218,54 EUR"

Output:
{"category":"Bank","subcategory":"Konto","tags":["kontoauszug"],"date":"2026-03-31","sender":"Sparkasse Dresden","subject":"Kontoauszug Marz 2026","confidence":0.96,"reasoning":"Kontoauszug der Sparkasse mit IBAN, Buchungszeitraum und Saldo."}

## Example 6 — KFZ-Versicherungs-Anpassung

Input (excerpt):
"Allianz Versicherungs-AG · Versicherungsnummer KH-7654321 · Beitragsanpassung KFZ-Haftpflicht zum 01.01.2026 · Fahrzeug VW Golf, KFZ-Kz. DD-AB 1234 · neuer Jahresbeitrag 412,00 EUR"

Output:
{"category":"Versicherung","subcategory":"KFZ","tags":["police","aenderung"],"date":"2026-01-01","sender":"Allianz","subject":"Beitragsanpassung KFZ-Haftpflicht 2026","confidence":0.95,"reasoning":"Versicherungsschreiben mit Versicherungsnummer und KFZ-Bezug."}

## Example 7 — Nebenkostenabrechnung

Input (excerpt):
"Hausverwaltung Müller & Co. · Nebenkostenabrechnung 2024 · Objekt: Musterstraße 12, 01454 Radeberg · Abrechnungszeitraum 01.01.2024–31.12.2024 · Nachzahlung 212,45 EUR"

Output:
{"category":"Haus","subcategory":"Nebenkosten","tags":["nachzahlung"],"date":"2024-12-31","sender":"Hausverwaltung Mueller","subject":"Nebenkostenabrechnung 2024","confidence":0.94,"reasoning":"Nebenkostenabrechnung von der Hausverwaltung fuer ein Mietobjekt."}

## Example 8 — Entgeltabrechnung

Input (excerpt):
"Acme Engineering GmbH · Entgeltabrechnung Februar 2026 · Mitarbeiter-Nr. 4711 · SV-Nummer 12 345678 R 901 · Bruttogehalt 5.200,00 EUR · Nettogehalt 3.218,47 EUR"

Output:
{"category":"Gehalt","subcategory":"","tags":["abrechnung"],"date":"2026-02-28","sender":"Acme Engineering","subject":"Entgeltabrechnung Februar 2026","confidence":0.97,"reasoning":"Gehaltsabrechnung mit Brutto, Netto, SV-Nummer und Abrechnungszeitraum."}

## Example 9 — Mietvertrag

Input (excerpt):
"Mietvertrag — Wohnung · Vermieter: Heinrich Berger, Dresdner Straße 45, 01454 Radeberg · Mieter: Max Mustermann · Mietbeginn: 01.05.2026 · Kaltmiete 780,00 EUR · Nebenkostenvorauszahlung 180,00 EUR · Unterschriftsdatum 12.04.2026"

Output:
{"category":"Haus","subcategory":"Miete","tags":["vertrag"],"date":"2026-04-12","sender":"Heinrich Berger","subject":"Mietvertrag Wohnung Radeberg","confidence":0.96,"reasoning":"Unterschriebener Mietvertrag — Wohnen → Haus/Miete."}

## Example 10 — Kündigung Mobilfunk

Input (excerpt):
"An: Telefonica Germany GmbH · Betreff: Kündigung meines Mobilfunkvertrags zum nächstmöglichen Termin · Kundennummer 987654321 · Rufnummer 0175-1234567 · Datum 05.01.2026 · Max Mustermann"

Output:
{"category":"Vertraege","subcategory":"","tags":["kuendigung","mobilfunk"],"date":"2026-01-05","sender":"Max Mustermann","subject":"Kuendigung Mobilfunkvertrag Telefonica","confidence":0.93,"reasoning":"Kündigungsschreiben eines Mobilfunkvertrags."}

## Example 11 — Krankenkassen-Beitragsbescheid

Input (excerpt):
"Techniker Krankenkasse · Beitragsbescheid · Versicherten-Nr. A123456789 · Ab 01.01.2026 beträgt Ihr monatlicher Beitrag 18,45 EUR · Bescheid vom 18.12.2025"

Output:
{"category":"Gesundheit","subcategory":"Krankenkasse","tags":["bescheid"],"date":"2025-12-18","sender":"TK","subject":"Beitragsbescheid Krankenkasse 2026","confidence":0.9,"reasoning":"Beitragsbescheid einer gesetzlichen Krankenkasse."}

## Example 12 — Grundsteuerbescheid (Gemeinde)

Input (excerpt):
"Große Kreisstadt Radeberg · Bescheid über die Festsetzung der Grundsteuer B für das Jahr 2026 · Aktenzeichen GS-2026/0045 · Datum 12.02.2026 · Jahresbetrag 428,00 EUR"

Output:
{"category":"Behoerde","subcategory":"Sonstiges","tags":["bescheid","grundsteuer"],"date":"2026-02-12","sender":"Stadt Radeberg","subject":"Grundsteuerbescheid 2026","confidence":0.94,"reasoning":"Grundsteuerbescheid von der Gemeinde — Behoerde, nicht Steuer."}

## Example 13 — TÜV-Bescheinigung

Input (excerpt):
"DEKRA Automobil GmbH · Hauptuntersuchung gem. § 29 StVZO · Fahrzeug VW Golf, KFZ-Kz. DD-AB 1234 · FIN WVWZZZ1KZ7W123456 · Prüfung bestanden, neue HU-Plakette gültig bis 03/2028 · Datum 12.03.2026"

Output:
{"category":"Auto","subcategory":"KFZ","tags":["tuev","nachweis"],"date":"2026-03-12","sender":"DEKRA","subject":"HU-Bescheinigung VW Golf 2026","confidence":0.96,"reasoning":"Hauptuntersuchung eines KFZ — Auto/KFZ."}

## Example 14 — Hotelbuchung

Input (excerpt):
"Booking.com · Reservierungsbestätigung Nr. 2987654321 · Hotel Garni Bergblick, Mayrhofen · Anreise 22.07.2026 · Abreise 29.07.2026 · Gesamtbetrag 1.142,00 EUR"

Output:
{"category":"Reise","subcategory":"Hotel","tags":["buchung"],"date":"2026-07-22","sender":"Booking.com","subject":"Hotelbuchung Mayrhofen Juli 2026","confidence":0.94,"reasoning":"Hotel-Reservierungsbestaetigung mit Buchungsnummer."}

## Example 15a — Supermarkt-Kassenzettel

Input (excerpt):
"REWE Markt GmbH · Königsbrücker Str. 78 · 01099 Dresden · Bon-Nr. 4711 · Kasse 02 · Datum 12.04.2026 17:42 · Bio Vollmilch 1L 1,29 · Vollkornbrot 2,49 · Tomaten 500g 1,79 · ... SUMME 23,87 EUR · girocard"

Output:
{"category":"Kassenzettel","subcategory":"Supermarkt","tags":["lebensmittel"],"date":"2026-04-12","sender":"REWE","subject":"Einkauf REWE Dresden 23,87 EUR","confidence":0.95,"reasoning":"Klassischer Supermarkt-Kassenbon mit Artikelzeilen und SUMME."}

## Example 15b — Tankquittung

Input (excerpt):
"Aral Tankstelle · Bautzner Landstr. · Beleg-Nr. 8821 · 18.03.2026 09:12 · Super E10 41,32 L à 1,789 EUR/L · 73,93 EUR · EC"

Output:
{"category":"Kassenzettel","subcategory":"Tankstelle","tags":["sprit"],"date":"2026-03-18","sender":"Aral","subject":"Tanken Aral 73,93 EUR","confidence":0.96,"reasoning":"Tankquittung mit Liter-Angabe und Preis pro Liter."}

## Example 15 — Erbauseinandersetzungsvertrag

Input (excerpt):
"Notar Dr. Braun · Erbauseinandersetzungsvertrag betreffend den Nachlass des Herrn Heinrich Mustermann, verstorben am 14.06.2024 · Datum 11.02.2026 · Beteiligte: Max Mustermann, Erika Mustermann"

Output:
{"category":"Familie","subcategory":"Erbe","tags":["vertrag","notar"],"date":"2026-02-11","sender":"Dr. Braun Notar","subject":"Erbauseinandersetzung Nachlass Mustermann","confidence":0.95,"reasoning":"Notarieller Erbauseinandersetzungsvertrag — Familie/Erbe."}

# Processing notes

The documents you receive have been pre-processed by OCR (Tesseract with German and English language models). Expect:

- OCR noise: broken words across lines, wrong letters, smudged characters. Infer the most likely meaning from context rather than relying on exact-match strings.
- Reordered layout: multi-column letters often end up with the columns concatenated, headers and footers mixed into the body. Look for key phrases anywhere in the text.
- Lost formatting: tables are flattened, bold/italic are gone. Rely on keywords like "Rechnungsnummer", "Versicherungsnummer", "Aktenzeichen" rather than visual cues.
- Stamps, signatures, logos: fully ignored by OCR. You won't see them — judge only from the text.
- Page breaks: for multi-page PDFs, pages are simply concatenated with newlines. Letterhead from page 1 won't repeat on page 2.

When classifying:

- Prefer the clearest, most-specific category match over "Sonstiges" — but ONLY when you have at least two corroborating signals (e.g. letterhead + subject line + keywords).
- If a single document could reasonably belong to two categories, choose based on the *purpose* of the document (a contract termination → Vertraege, even if the subject is an invoice).
- If the text contains only a greeting, signature and one short sentence, confidence should be ≤ 0.4 — that's a judgment call a human should make.
- When the sender is a person (not an organisation), pick the format "Vorname Nachname" (first + last, no titles).
- Dates in German format (DD.MM.YYYY) are common. Convert them correctly to ISO YYYY-MM-DD. Watch out for ambiguous 2-digit years ("01.03.26" → "2026-03-01" assuming current century).
- For documents that span multiple years (e.g. annual summaries), pick the reporting year's end date (e.g. "Jahresmeldung 2024" → "2024-12-31") and ensure the subject reflects the year.

# Common pitfalls & tiebreakers

- **Grundsteuerbescheid**: comes from the Gemeinde/Stadt → **Behoerde**, NOT Steuer. Steuer is reserved for Finanzamt correspondence (income tax, VAT, etc.).
- **Arbeitsvertrag / Gehaltsabrechnung**: the contract itself → Vertraege. The monthly pay slip → Gehalt.
- **Versicherungs-Rechnung**: an invoice for an insurance premium → Versicherung (not Rechnungen) when it's clearly a policy-related document with "Versicherungsnummer". Plain bills for damages go to Versicherung too.
- **Krankenkassen-Schreiben**: go to Gesundheit even though they can feel administrative. A pay-slip-style "Beitragsbescheinigung" from the Krankenkasse can go to Gehalt only if it's about salary reporting; in general → Gesundheit.
- **Mahnung**: route by what the underlying bill is about — a Mahnung for a utility bill stays in Rechnungen.
- **Amazon / Online-Shop-Rechnungen**: Rechnungen. Bestellbestätigungen ohne "Rechnung" oder Betrag: Sonstiges.
- **DHL / DPD Versandbenachrichtigungen**: Sonstiges.
- **Rundfunkbeitrag (ARD/ZDF)**: Behoerde (offizielle Festsetzung) oder Rechnungen (normaler Beitrag). Wenn "Festsetzungsbescheid" im Text → Behoerde; sonst Rechnungen.
- **Date ambiguity**: pick the letter date (Briefdatum / Bescheiddatum / Rechnungsdatum) over a Leistungszeitraum or Abrechnungszeitraum. If only a period is given, use the last day of that period.
- **Sender too long**: shorten intelligently — "Allianz Versicherungs-AG" → "Allianz"; "Techniker Krankenkasse" → "TK" is fine if that's the common usage; "Stadtwerke Radeberg GmbH" → "Stadtwerke Radeberg".
- **OCR garbage**: if the extracted text is mostly nonsense characters, classify as Sonstiges with very low confidence so the document goes to review.

# Reminder

Your output is a single JSON object with these required keys:
category, subcategory, tags, date, sender, subject, confidence, reasoning.

Use EXACTLY one of these category names:
Rechnungen, Vertraege, Behoerde, Gesundheit, Gehalt, Steuer, Haus, Versicherung, Bank, Auto, Bildung, Familie, Reise, Hobby, Sonstiges

Subcategory MUST be empty "" or one of the parent's listed subs. Tags is an array of 0..3 lowercase short German labels.
"""


def _build_system_prompt(categories: list[dict[str, Any]]) -> str:
    # Categories from config override the baked-in list, but the structure
    # above is intentionally verbose so the full prompt crosses the 2048-
    # token threshold required for Haiku's prompt cache.
    if not categories:
        return SYSTEM_PROMPT_BASE
    lines = ["\n\n# Active category list for this request (use EXACTLY these spellings)"]
    for c in categories:
        subs = c.get("subcategories") or []
        if subs:
            lines.append(f"- {c['name']}: {' | '.join(subs)}")
        else:
            lines.append(f"- {c['name']}: (no subcategories — leave subcategory empty)")
    return SYSTEM_PROMPT_BASE + "\n".join(lines)


def _build_user_message(text: str, max_chars: int) -> str:
    body = text[:max_chars] if text else "(no text extracted)"
    today = date.today().isoformat()
    return (
        f"Today is {today}.\n\n"
        f"Document text:\n---\n{body}\n---\n\n"
        "Return the JSON object now."
    )


def _parse_response(raw: str) -> dict[str, Any]:
    """Extract the first valid JSON object from the model's reply.

    Uses json.JSONDecoder.raw_decode so we stop at the end of the first
    object rather than greedily swallowing trailing text — avoids
    "Extra data: line N column M" errors when the model adds commentary
    after the JSON block.
    """
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].lstrip()

    decoder = json.JSONDecoder()
    for i, ch in enumerate(raw):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(raw[i:])
            return obj
        except json.JSONDecodeError:
            continue
    raise ValueError(f"No valid JSON object in model reply: {raw[:200]!r}")


class Classifier:
    def __init__(self, api_key: str, settings: AISettings,
                 categories: list[dict[str, Any]],
                 provider: Provider | None = None):
        self.settings = settings
        self.categories = categories
        self.provider: Provider = provider or build_provider(
            settings.provider,
            api_key=api_key,
            base_url=settings.base_url,
            timeout=settings.timeout_seconds,
        )
        self._allowed_names = {c["name"] for c in categories}
        self._allowed_subs: dict[str, set[str]] = {
            c["name"]: set(c.get("subcategories") or []) for c in categories
        }
        self._system_prompt = _build_system_prompt(categories)

    def classify(self, text: str) -> Classification:
        user = _build_user_message(text, self.settings.max_text_chars)
        logger.debug("Calling %s model=%s, text_len=%d",
                     self.provider.name, self.settings.model, len(text))

        try:
            resp = self.provider.classify(
                system_prompt=self._system_prompt,
                user_prompt=user,
                model=self.settings.model,
                max_output_tokens=600,
            )
        except ProviderError as exc:
            logger.error("Provider %s failed: %s", self.provider.name, exc)
            raise

        data = _parse_response(resp.raw_text)

        category = str(data.get("category", "Sonstiges")).strip()
        if category not in self._allowed_names:
            logger.warning("Model returned unknown category %r – falling back", category)
            category = "Sonstiges"

        subcategory = str(data.get("subcategory", "") or "").strip()
        allowed_subs = self._allowed_subs.get(category, set())
        if subcategory and subcategory not in allowed_subs:
            logger.warning(
                "Model returned subcategory %r not allowed under %r – dropping",
                subcategory, category,
            )
            subcategory = ""

        raw_tags = data.get("tags") or []
        tags: list[str] = []
        if isinstance(raw_tags, list):
            seen: set[str] = set()
            for t in raw_tags:
                tag = str(t).strip().lower()
                if tag and tag not in seen and len(tag) <= 32:
                    tags.append(tag)
                    seen.add(tag)
                if len(tags) >= 3:
                    break

        return Classification(
            category=category,
            subcategory=subcategory,
            tags=tags,
            date=str(data.get("date", date.today().isoformat())).strip(),
            sender=str(data.get("sender", "")).strip() or "Unbekannt",
            subject=str(data.get("subject", "")).strip() or "Dokument",
            confidence=float(data.get("confidence", 0.5)),
            reasoning=str(data.get("reasoning", "")).strip(),
            input_tokens=resp.input_tokens,
            output_tokens=resp.output_tokens,
            cache_creation_tokens=resp.cache_creation_tokens,
            cache_read_tokens=resp.cache_read_tokens,
            cost_usd=resp.cost_usd,
            model=resp.model,
        )
