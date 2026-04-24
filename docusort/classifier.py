"""Claude-powered document classifier.

Given the extracted text of a document, asks Claude to return structured
metadata:  category, document date, sender, short subject and a confidence
score. Responses are forced into JSON via a strong system prompt and parsed
defensively – any parsing failure routes the document to the review folder.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from anthropic import Anthropic

from .config import ClaudeSettings
from .db import calculate_cost


logger = logging.getLogger("docusort.classifier")


@dataclass
class Classification:
    category: str
    date: str  # ISO YYYY-MM-DD
    sender: str
    subject: str
    confidence: float
    reasoning: str = ""
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
- Required keys: category, date, sender, subject, confidence, reasoning.
- category: MUST be exactly one of the allowed category names below.
- date: the document's own date (Rechnungsdatum, Vertragsdatum, Briefdatum, Bescheiddatum) in ISO format YYYY-MM-DD. If the document spans a period, use the date it was issued. If no date is present, use today's date and lower the confidence.
- sender: the organisation or person who issued the document. 1 to 4 words, keep legal suffixes like GmbH / AG / e.V., drop salutations like "Firma" or "Herr". Prefer ASCII-safe (no umlauts if the name is clearly anglicised; keep umlauts for German orgs — they will be transliterated downstream).
- subject: concise description of what the document is about, 3 to 8 words. Include the most identifying detail (month, year, invoice number only if short, case number only if short).
- confidence: float from 0 to 1. Use < 0.65 when the category, sender or date is genuinely unclear — those documents go to a manual review folder. Use >= 0.9 only when the letterhead, subject line and body agree.
- reasoning: one short sentence (German or English) explaining the category choice.
- Do NOT invent facts. If a field is unknown, leave it empty and lower confidence rather than guessing.

# Category guide

Each category lists typical senders, subject patterns, and the signals that reliably identify it.

## Rechnungen
Invoices of any kind — utility bills (Strom, Wasser, Gas, Fernwärme), telecom (Mobilfunk, Festnetz, Internet), streaming (Netflix, Spotify), handwerker, online shops, Werkstatt.
Signals: "Rechnung", "Rechnungsnummer", "Gesamtbetrag", "zu zahlender Betrag", "Fälligkeit", IBAN/BIC for payment.
Typical senders: Vodafone, Telekom, 1&1, Stadtwerke, ENBW, E.ON, Amazon, IKEA.

## Vertraege
Contracts and contract-related correspondence (Mietvertrag, Arbeitsvertrag, Kaufvertrag, Dienstleistungs-, Mobilfunk-, Strom-, Wartungsvertrag). Includes amendments ("Nachtrag"), terminations ("Kündigung"), and confirmations ("Vertragsbestätigung").
Signals: "Vertrag", "Vereinbarung", "Nachtrag", "Kündigung", "Laufzeit", "Kündigungsfrist".

## Behoerde
Official documents from public authorities — Finanzamt is in "Steuer" instead. Here: Einwohnermeldeamt, Bürgeramt, KFZ-Zulassung, Gemeinde- und Kreisverwaltung, Rentenversicherung (without pay info), Ausländerbehörde, Standesamt, Jobcenter.
Signals: Stadt-/Landkreis-/Gemeindewappen, "Bescheid", "Aktenzeichen", "Az.:", "Behörde".
Examples: Grundsteuerbescheid (when issued by the Gemeinde, NOT the Finanzamt), Anmeldebestätigung, Fahrzeugschein-Kopie, Pass-Verlängerung.

## Gesundheit
Arzt, Zahnarzt, Krankenhaus, Therapie, Apotheken, Krankenkassen-Schreiben. Befunde, Arztbriefe, Rezepte, Atteste, Impfnachweise, Heilmittelverordnungen, Zuzahlungsbescheinigungen, Vorsorge-Einladungen.
Signals: "Diagnose", "ICD", "Befund", "Rezept", "AU-Bescheinigung", "Krankenkasse".
Typical senders: Hausarzt, Fachärzte, Krankenhäuser, TK, Barmer, AOK, DAK.

## Gehalt
Gehaltsabrechnungen / Entgeltabrechnungen, Lohnzettel, Boni-/Tantieme-Abrechnungen, Jahresmeldungen zur Sozialversicherung, Lohnsteuerbescheinigungen, Sozialversicherungsnachweis.
Signals: "Brutto", "Netto", "Sozialversicherung", "Lohnsteuer", "SV-Nr.", "Abrechnungszeitraum".

## Steuer
Everything from or for the Finanzamt — Steuererklärungen, Steuerbescheide, ELSTER-Ausdrucke, Spendenbescheinigungen, Steuerbescheinigungen (z.B. von Banken), Umsatzsteuer-Voranmeldungen, Kapitalerträge.
Signals: "Finanzamt", "Steuer-Identifikationsnummer", "Steuernummer", "Steuerbescheid", "Lohnsteuer" (when NOT part of a payslip).
Note: Grundsteuerbescheide come from the Gemeinde → Behoerde. Income-tax assessments from the Finanzamt → Steuer.

## Haus
Hausbau, Renovierung, Grundstück, Grundbuchauszug, Bauunterlagen, Pläne, Energieausweis, Nebenkostenabrechnungen (from Hausverwaltung / Vermieter, not utility companies), Hausverwaltungs-Rundschreiben, Protokolle der Eigentümerversammlung.
Signals: "Wohnungseigentümergemeinschaft", "Hausgeld", "Nebenkostenabrechnung", "Hausverwaltung", "Grundbuch".

## Versicherung
Versicherungspolicen, Beitragsanpassungen, Schadensmeldungen, Schadensregulierungen, Jahresbestätigungen. Sparten: Haftpflicht, Hausrat, KFZ, Leben, Rente, Unfall, Rechtsschutz, Reise, Tier, Gebäude.
Signals: "Versicherungsschein", "Versicherungsnummer", "Versicherungsbeitrag", "Schadensmeldung", "Police".

## Bank
Kontoauszüge, Depotauszüge, Kreditverträge, Darlehen, Zinsabrechnungen, Wertpapierabrechnungen, Kreditkartenabrechnungen, Bankbescheinigungen.
Signals: IBAN, "Kontoauszug", "Buchungsdatum", "Wertpapierabrechnung", "Dispositionskredit", bank letterheads.

## Sonstiges
Fallback for anything that genuinely doesn't match the other buckets — newsletters, club memberships, hobbies, private correspondence. Also the safe choice when confidence is low.

# Few-shot examples

## Example 1 — Mobilfunkrechnung

Input (excerpt):
"Vodafone GmbH · Ihre Rechnung vom 14.02.2026 · Rechnungsnr. R123456 · Mobilfunk Februar 2026 · Gesamtbetrag 29,99 EUR · Fälligkeit 28.02.2026"

Output:
{"category":"Rechnungen","date":"2026-02-14","sender":"Vodafone GmbH","subject":"Mobilfunkrechnung Februar 2026","confidence":0.95,"reasoning":"Klar erkennbare Mobilfunkrechnung mit Rechnungsnummer und Fälligkeit."}

## Example 2 — Arztbefund

Input (excerpt):
"Dr. med. Susanne Müller · Facharztpraxis für Innere Medizin · Arztbrief vom 03.01.2026 · Patient: Robert Manuwald · Diagnose: Blutbild unauffällig, Cholesterin leicht erhöht"

Output:
{"category":"Gesundheit","date":"2026-01-03","sender":"Praxis Dr. Müller","subject":"Arztbrief Blutbild","confidence":0.94,"reasoning":"Arztbrief mit Diagnose und Befund aus einer Facharztpraxis."}

## Example 3 — Steuerbescheid

Input (excerpt):
"Finanzamt Dresden III · Bescheid für 2024 über Einkommensteuer · Steuer-Nr. 203/150/12345 · Datum 20.03.2026 · Festsetzung: Erstattung 412,00 EUR"

Output:
{"category":"Steuer","date":"2026-03-20","sender":"Finanzamt Dresden","subject":"Einkommensteuerbescheid 2024","confidence":0.97,"reasoning":"Offizieller Einkommensteuerbescheid vom Finanzamt mit Steuernummer."}

## Example 4 — Niedrige Confidence

Input (excerpt):
"Sehr geehrter Herr Manuwald, anbei wie besprochen die Unterlagen. Mit freundlichen Grüßen."

Output:
{"category":"Sonstiges","date":"","sender":"Unbekannt","subject":"Kurzes Anschreiben ohne Inhalt","confidence":0.2,"reasoning":"Kein Absender, kein Datum, kein eindeutiger Dokumenttyp erkennbar."}

## Example 5 — Kontoauszug

Input (excerpt):
"Sparkasse Dresden · Kontoauszug Nr. 03/2026 · Konto-Inhaber: Robert Manuwald · IBAN DE12 8505 0300 0123 4567 89 · Buchungszeitraum 01.03.2026 bis 31.03.2026 · Saldo: 4.218,54 EUR"

Output:
{"category":"Bank","date":"2026-03-31","sender":"Sparkasse Dresden","subject":"Kontoauszug Marz 2026","confidence":0.96,"reasoning":"Kontoauszug der Sparkasse mit IBAN, Buchungszeitraum und Saldo."}

## Example 6 — Versicherungsschreiben

Input (excerpt):
"Allianz Versicherungs-AG · Versicherungsnummer HV-7654321 · Beitragsanpassung zum 01.01.2026 · Haftpflichtversicherung · neuer Jahresbeitrag 89,40 EUR"

Output:
{"category":"Versicherung","date":"2026-01-01","sender":"Allianz","subject":"Beitragsanpassung Haftpflicht 2026","confidence":0.95,"reasoning":"Versicherungsschreiben mit Versicherungsnummer und Beitragsanpassung."}

## Example 7 — Nebenkostenabrechnung

Input (excerpt):
"Hausverwaltung Müller & Co. · Nebenkostenabrechnung 2024 · Objekt: Musterstraße 12, 01454 Radeberg · Abrechnungszeitraum 01.01.2024–31.12.2024 · Nachzahlung 212,45 EUR"

Output:
{"category":"Haus","date":"2024-12-31","sender":"Hausverwaltung Mueller","subject":"Nebenkostenabrechnung 2024","confidence":0.94,"reasoning":"Nebenkostenabrechnung von der Hausverwaltung fuer ein Mietobjekt."}

## Example 8 — Entgeltabrechnung

Input (excerpt):
"Acme Engineering GmbH · Entgeltabrechnung Februar 2026 · Mitarbeiter-Nr. 4711 · SV-Nummer 12 345678 R 901 · Bruttogehalt 5.200,00 EUR · Nettogehalt 3.218,47 EUR"

Output:
{"category":"Gehalt","date":"2026-02-28","sender":"Acme Engineering","subject":"Entgeltabrechnung Februar 2026","confidence":0.97,"reasoning":"Gehaltsabrechnung mit Brutto, Netto, SV-Nummer und Abrechnungszeitraum."}

## Example 9 — Mietvertrag

Input (excerpt):
"Mietvertrag — Wohnung · Vermieter: Heinrich Berger, Dresdner Straße 45, 01454 Radeberg · Mieter: Robert Manuwald · Mietbeginn: 01.05.2026 · Kaltmiete 780,00 EUR · Nebenkostenvorauszahlung 180,00 EUR · Unterschriftsdatum 12.04.2026"

Output:
{"category":"Vertraege","date":"2026-04-12","sender":"Heinrich Berger","subject":"Mietvertrag Wohnung Radeberg","confidence":0.96,"reasoning":"Unterschriebener Mietvertrag mit Mietbeginn, Kaltmiete und Vermieter-Angaben."}

## Example 10 — Kündigung Mobilfunk

Input (excerpt):
"An: Telefonica Germany GmbH · Betreff: Kündigung meines Mobilfunkvertrags zum nächstmöglichen Termin · Kundennummer 987654321 · Rufnummer 0175-1234567 · Datum 05.01.2026 · Robert Manuwald"

Output:
{"category":"Vertraege","date":"2026-01-05","sender":"Robert Manuwald","subject":"Kuendigung Mobilfunkvertrag Telefonica","confidence":0.93,"reasoning":"Kündigungsschreiben eines Mobilfunkvertrags — gehört zu den Verträgen."}

## Example 11 — Krankenkassen-Beitragsbescheid

Input (excerpt):
"Techniker Krankenkasse · Beitragsbescheid · Versicherten-Nr. A123456789 · Ab 01.01.2026 beträgt Ihr monatlicher Beitrag 18,45 EUR · Bescheid vom 18.12.2025"

Output:
{"category":"Gesundheit","date":"2025-12-18","sender":"TK","subject":"Beitragsbescheid Krankenkasse 2026","confidence":0.9,"reasoning":"Beitragsbescheid der Krankenkasse — Gesundheit-Kategorie, da keine Lohn-/Gehaltsinformationen."}

## Example 12 — Grundsteuerbescheid (Gemeinde)

Input (excerpt):
"Große Kreisstadt Radeberg · Bescheid über die Festsetzung der Grundsteuer B für das Jahr 2026 · Aktenzeichen GS-2026/0045 · Datum 12.02.2026 · Jahresbetrag 428,00 EUR"

Output:
{"category":"Behoerde","date":"2026-02-12","sender":"Stadt Radeberg","subject":"Grundsteuerbescheid 2026","confidence":0.94,"reasoning":"Grundsteuerbescheid von der Gemeinde — Behoerde, nicht Steuer."}

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

Your output is a single JSON object. Use EXACTLY one of these category names:
Rechnungen, Vertraege, Behoerde, Gesundheit, Gehalt, Steuer, Haus, Versicherung, Bank, Sonstiges
"""


def _build_system_prompt(categories: list[dict[str, Any]]) -> str:
    # Categories from config override the baked-in list, but the structure
    # above is intentionally verbose so the full prompt crosses the 2048-
    # token threshold required for Haiku's prompt cache.
    names = [c["name"] for c in categories]
    if names:
        override = "\n\n# Active category list for this request\n" + ", ".join(names)
    else:
        override = ""
    return SYSTEM_PROMPT_BASE + override


def _build_user_message(text: str, max_chars: int) -> str:
    body = text[:max_chars] if text else "(no text extracted)"
    today = date.today().isoformat()
    return (
        f"Today is {today}.\n\n"
        f"Document text:\n---\n{body}\n---\n\n"
        "Return the JSON object now."
    )


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _parse_response(raw: str) -> dict[str, Any]:
    """Extract the first JSON object from the model's reply."""
    raw = raw.strip()
    # Strip common markdown fences just in case.
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].lstrip()
    match = _JSON_RE.search(raw)
    if not match:
        raise ValueError(f"No JSON object in model reply: {raw[:200]!r}")
    return json.loads(match.group(0))


class Classifier:
    def __init__(self, api_key: str, settings: ClaudeSettings,
                 categories: list[dict[str, Any]]):
        self.client = Anthropic(api_key=api_key, timeout=settings.timeout_seconds)
        self.settings = settings
        self.categories = categories
        self._allowed_names = {c["name"] for c in categories}
        self._system_prompt = _build_system_prompt(categories)

    def classify(self, text: str) -> Classification:
        user = _build_user_message(text, self.settings.max_text_chars)
        logger.debug("Calling Claude model=%s, text_len=%d",
                     self.settings.model, len(text))

        # Mark the system prompt as cacheable. Haiku requires ~2048 tokens to
        # cache; our beefed-up prompt sits just above that, so after the first
        # call the prompt is billed at 0.1x instead of 1.0x.
        resp = self.client.messages.create(
            model=self.settings.model,
            max_tokens=600,
            system=[{
                "type": "text",
                "text": self._system_prompt,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": user}],
        )
        raw = "".join(
            block.text for block in resp.content if getattr(block, "type", "") == "text"
        )
        data = _parse_response(raw)

        category = str(data.get("category", "Sonstiges")).strip()
        if category not in self._allowed_names:
            logger.warning("Model returned unknown category %r – falling back", category)
            category = "Sonstiges"

        u = resp.usage
        in_tok = int(getattr(u, "input_tokens", 0) or 0)
        out_tok = int(getattr(u, "output_tokens", 0) or 0)
        cache_write = int(getattr(u, "cache_creation_input_tokens", 0) or 0)
        cache_read  = int(getattr(u, "cache_read_input_tokens", 0) or 0)

        cost = calculate_cost(
            self.settings.model, in_tok, out_tok,
            cache_write=cache_write, cache_read=cache_read,
        )

        return Classification(
            category=category,
            date=str(data.get("date", date.today().isoformat())).strip(),
            sender=str(data.get("sender", "")).strip() or "Unbekannt",
            subject=str(data.get("subject", "")).strip() or "Dokument",
            confidence=float(data.get("confidence", 0.5)),
            reasoning=str(data.get("reasoning", "")).strip(),
            input_tokens=in_tok,
            output_tokens=out_tok,
            cache_creation_tokens=cache_write,
            cache_read_tokens=cache_read,
            cost_usd=cost,
            model=self.settings.model,
        )
