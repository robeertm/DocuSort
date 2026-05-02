"""Bank statement (Kontoauszug) line-item extractor.

Same shape as `docusort.receipts.ReceiptExtractor` — a second-pass LLM
call invoked when `category == "Kontoauszug"`. The novelty is the
pseudonymisation step: before any cloud-hosted provider sees the OCR
text, IBANs / emails / addresses / account-holder names get masked to
stable tokens. After the LLM returns JSON, tokens are restored locally
and we compute SHA256 hashes of the IBANs for account dedup.

Two routes through this module:

* **Cloud + pseudonymise** (default): the OCR text is rewritten,
  the LLM sees `IBAN_001 / NAME_001 / ADDR_002`, and the response is
  un-pseudonymised before storage.
* **Local-only** (paranoia mode): raw text goes directly to a local
  provider (Ollama via openai-compat). No tokens, no third-party
  network round-trip.

The caller (the pipeline in main.py) picks the route based on the
`finance.local_only` setting + the active provider.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import OCRSettings

from ..providers import Provider, ProviderError
from .categories import TX_CATEGORIES, TX_TYPES, BANK_NAMES
from .pseudonymizer import Pseudonymizer, iban_hash


logger = logging.getLogger("docusort.finance.extractor")


# A pseudonym token that survived the restore step is a sign the LLM
# hallucinated a reference (= emitted "NAME_001" without us ever having
# masked anything as NAME_001). Storing such a value would mean the
# user sees `NAME_001` as the account holder, which is worse than
# admitting we don't know.
_RESIDUAL_TOKEN_RE = re.compile(r"^(IBAN|NAME|ADDR|EMAIL)_\d+$")


def _scrub_residual(value: str) -> str:
    if isinstance(value, str) and _RESIDUAL_TOKEN_RE.match(value.strip()):
        return ""
    return value


@dataclass
class Transaction:
    booking_date: str = ""
    value_date: str = ""
    amount: float = 0.0
    currency: str = "EUR"
    counterparty: str = ""
    counterparty_iban: str = ""
    purpose: str = ""
    tx_type: str = ""
    category: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "booking_date": self.booking_date,
            "value_date": self.value_date,
            "amount": self.amount,
            "currency": self.currency,
            "counterparty": self.counterparty,
            "counterparty_iban": self.counterparty_iban,
            "purpose": self.purpose,
            "tx_type": self.tx_type,
            "category": self.category,
        }


@dataclass
class Statement:
    bank_name: str = ""
    iban: str = ""
    iban_hash: str = ""
    iban_last4: str = ""
    account_holder: str = ""
    period_start: str = ""
    period_end: str = ""
    statement_no: str = ""
    opening_balance: float | None = None
    closing_balance: float | None = None
    currency: str = "EUR"
    transactions: list[Transaction] = field(default_factory=list)
    raw_response: str = ""
    privacy_mode: str = ""    # 'pseudonymize' | 'local' | 'plain'
    # Diagnostic flag set by the extractor when the result smells wrong
    # but isn't a hard error — e.g. opening / closing balances differ
    # but the model returned zero transactions. Callers use this to
    # tag the statement as "needs retry" instead of accepting it as
    # legitimately empty.
    extraction_warning: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "bank_name": self.bank_name,
            "iban": self.iban,
            "iban_hash": self.iban_hash,
            "iban_last4": self.iban_last4,
            "account_holder": self.account_holder,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "statement_no": self.statement_no,
            "opening_balance": self.opening_balance,
            "closing_balance": self.closing_balance,
            "currency": self.currency,
            "privacy_mode": self.privacy_mode,
            "transactions": [t.as_dict() for t in self.transactions],
        }


SYSTEM_PROMPT = f"""You are a bank-statement (Kontoauszug) parser for a German personal-finance app. You receive OCR text and reply with ONE JSON object describing it.

# Privacy notice (critical)

The user's IBAN, address, account-holder name, and email may have been replaced with stable tokens before you see the text:

  * `IBAN_001`, `IBAN_002`, …  for IBAN-shaped numbers
  * `NAME_001`, `NAME_002`, …  for the account holder
  * `ADDR_001`, `ADDR_002`, …  for street / zip-city blocks
  * `EMAIL_001`, …             for email addresses

When you see these tokens, **keep them verbatim** in your output. Do
NOT invent real-looking IBANs. Do NOT speculate about who the holder
is. The tokens get substituted back to real values locally before
storage.

# Output format (strict)

Reply with ONE JSON object, no prose, no markdown fences, no trailing text.

Schema:
{{
  "bank_name": string,            // canonical bank name (e.g. "Sparkasse", "DKB", "ING", "PayPal", "Commerzbank")
  "account_iban_token": string,   // the IBAN_xxx token belonging to the user (the account this statement is FOR), or "" if absent
  "account_holder_token": string, // NAME_xxx token of the account holder, or ""
  "period_start": string,         // ISO YYYY-MM-DD — earliest BOOKING DATE inside the table (NOT the document's issue / generation date at the top of the page!)
  "period_end": string,           // ISO YYYY-MM-DD — latest booking date inside the table; ALWAYS >= period_start
  "statement_no": string,         // bank-assigned number / "Auszug-Nr.", or ""
  "opening_balance": number | null,  // "Anfangssaldo" / "alter Saldo" / "Übertrag"
  "closing_balance": number | null,  // "Endsaldo" / "neuer Saldo"
  "currency": string,             // 3-letter ISO; default "EUR"
  "transactions": [
    {{
      "booking_date": string,     // ISO YYYY-MM-DD
      "value_date": string,       // ISO YYYY-MM-DD; usually same as booking; "" if absent
      "amount": number,           // SIGNED — positive = incoming, NEGATIVE = outgoing
      "counterparty": string,     // recipient (outgoing) or sender (incoming) name as printed
      "counterparty_iban_token": string, // IBAN_xxx token of the counterparty, or "" if not shown
      "purpose": string,          // "Verwendungszweck" / Mandatsreferenz / line description
      "tx_type": string,          // EXACTLY one of: {", ".join(TX_TYPES)}
      "category": string          // EXACTLY one of: {", ".join(TX_CATEGORIES)}
    }}
  ]
}}

# Rules

- Numbers use a dot as decimal separator: "1.234,56" → 1234.56.
- amount sign: outgoing/Abbuchung/Lastschrift → NEGATIVE.  Incoming/Eingang/Gutschrift → POSITIVE. The user wants to plot cashflow, so a wrong sign breaks everything.
- bank_name: snap to the canonical form. Allowed examples: {", ".join(BANK_NAMES)}. Anything else → keep verbatim.
- tx_type guidance:
  - SEPA-Überweisung gutschr / Überweisung gutschrift → ueberweisung (positive amount)
  - SEPA-Überweisung belast / Auftrag / SEPA-Lastschrift einmalig outgoing → ueberweisung
  - SEPA-Lastschrift / Lastschrifteinzug → lastschrift
  - Dauerauftrag → dauerauftrag
  - Kartenzahlung / Kreditkartenabrechnung / VISA-Belastung → kartenzahlung
  - Bargeldauszahlung / Geldautomat / GAA → bargeld
  - Lohn/Gehalt/Arbeitgeber → gehalt
  - Kontoführungsgebühr / Entgelte / Gebühr → gebuehr
  - Habenzinsen / Sollzinsen → zinsen
  - everything else → sonstiges
- category guidance:
  - Vermieter / Hausverwaltung / Miete → miete
  - Stadtwerke / Vodafone DSL / Telekom / Vattenfall / EnBW / GEZ → nebenkosten
  - Lidl / Aldi / REWE / Edeka / Penny / Kaufland / Norma / Tegut → lebensmittel
  - Restaurants, Lieferando, Delivery Hero, Wolt, Cafés → essen-ausser-haus
  - DB / VRR / VVS / BVG / Aral / Shell / Total / Esso / car insurance → mobilitaet
  - Allianz / HUK / Generali / DEVK / R+V / Krankenkasse → versicherung
  - Netflix / Spotify / Apple / Amazon Prime / Fitness / Gym / mobile contract → abonnement
  - Apotheke / Arzt / Praxis / Krankenhaus → gesundheit
  - Cinemax / UCI / Eventim / hobbies → freizeit
  - H&M / Zara / Zalando / Bekleidung → bekleidung
  - MediaMarkt / Saturn / Cyberport → elektronik
  - dm / Rossmann / Müller / IKEA → haushalt (drogerie + small homewares)
  - Thalia / Hugendubel / Coursera → bildung
  - "Spende" / "Donation" / charities → spende
  - Lohn / Gehalt / Arbeitgeber → gehalt
  - Rente / Bürgergeld / Kindergeld → rente-zuschuss
  - Steuererstattung / Finanzamt outgoing → steuer
  - "Erstattung von" / refund language → erstattung
  - Zinsen / Dividende → zins-dividende
  - "Übertrag" between own accounts of same holder → uebertrag
  - GAA / Bargeldauszahlung → bargeld
  - bank fees / Kontoführung → gebuehr
  - everything else → sonstiges
- account_iban_token: the IBAN that THIS statement is for. ALWAYS printed near the top of the page next to "IBAN", "Konto-Nr.", "Konto/IBAN", or right after the bank logo and account-type label ("Tagesgeldkonto 1234567, IBAN_001"). Counterparty IBANs appear INSIDE individual transaction rows (next to "BIC / IBAN:") — those go into `counterparty_iban_token`, NEVER into `account_iban_token`. If you can't tell with confidence which IBAN is the user's account, return "" — empty is better than a wrong assignment.
- counterparty_iban_token: the OTHER party's IBAN if the bank prints it on the line, else "".
- period_start / period_end: bracket the actual booking dates that appear in the transaction table. The "Auszug-Nr.", "ausgestellt am", or page-header date is the document creation date — do NOT use that for period_start/end. Example: a Sparkasse "Kontoauszug 1/2025" generated on "1. November 2025" with bookings between 10.10.2025 and 30.10.2025 → period_start=2025-10-10, period_end=2025-10-30.
- Skip pure layout: page numbers, "Bitte beachten Sie", footer disclaimers, BIC-only lines, "Übertrag" line that is just a balance carry-over (already captured in opening/closing).
- If you can't determine a value, return "" or null — never guess.

# Few-shot examples

## Example A — Sparkasse Girokonto

OCR text:
"Sparkasse KölnBonn — Kontoauszug 04/2026
Kontoinhaber: NAME_001
Anschrift: ADDR_001, ADDR_002
IBAN: IBAN_001  BIC: COKSDE33XXX
Auszug Nr. 04/2026  Zeitraum 01.04.2026 – 30.04.2026
Anfangssaldo  +1.234,56 EUR

03.04.2026  SEPA-Lastschrift Stadtwerke München             -89,00
05.04.2026  Kartenzahlung Lidl GmbH Bonn Hauptbahnhof       -45,67
10.04.2026  Lohn/Gehalt Acme GmbH IBAN_002                +3.500,00
15.04.2026  Bargeldauszahlung GAA Sparkasse                 -200,00
20.04.2026  Dauerauftrag Vermieter Müller Miete         -1.150,00

Endsaldo  +3.249,89 EUR"

Output:
{{"bank_name":"Sparkasse","account_iban_token":"IBAN_001","account_holder_token":"NAME_001","period_start":"2026-04-01","period_end":"2026-04-30","statement_no":"04/2026","opening_balance":1234.56,"closing_balance":3249.89,"currency":"EUR","transactions":[{{"booking_date":"2026-04-03","value_date":"","amount":-89.00,"counterparty":"Stadtwerke München","counterparty_iban_token":"","purpose":"SEPA-Lastschrift","tx_type":"lastschrift","category":"nebenkosten"}},{{"booking_date":"2026-04-05","value_date":"","amount":-45.67,"counterparty":"Lidl GmbH","counterparty_iban_token":"","purpose":"Kartenzahlung Bonn Hauptbahnhof","tx_type":"kartenzahlung","category":"lebensmittel"}},{{"booking_date":"2026-04-10","value_date":"","amount":3500.00,"counterparty":"Acme GmbH","counterparty_iban_token":"IBAN_002","purpose":"Lohn/Gehalt","tx_type":"gehalt","category":"gehalt"}},{{"booking_date":"2026-04-15","value_date":"","amount":-200.00,"counterparty":"Sparkasse","counterparty_iban_token":"","purpose":"Bargeldauszahlung GAA","tx_type":"bargeld","category":"bargeld"}},{{"booking_date":"2026-04-20","value_date":"","amount":-1150.00,"counterparty":"Vermieter Müller","counterparty_iban_token":"","purpose":"Dauerauftrag Miete","tx_type":"dauerauftrag","category":"miete"}}]}}

## Example B — DKB Visa Card statement (small)

OCR text:
"DKB AG — Kreditkartenabrechnung
Karteninhaber: NAME_001
Abrechnungszeitraum 01.03.2026 – 31.03.2026
Saldo Vormonat  -120,00
12.03.  Netflix.com Membership                  -12,99
18.03.  SPOTIFY  AB STOCKHOLM                    -9,99
22.03.  Amazon EU SARL Marketplace              -34,50
Neuer Saldo  -177,48"

Output:
{{"bank_name":"DKB","account_iban_token":"","account_holder_token":"NAME_001","period_start":"2026-03-01","period_end":"2026-03-31","statement_no":"","opening_balance":-120.00,"closing_balance":-177.48,"currency":"EUR","transactions":[{{"booking_date":"2026-03-12","value_date":"","amount":-12.99,"counterparty":"Netflix.com","counterparty_iban_token":"","purpose":"Membership","tx_type":"kartenzahlung","category":"abonnement"}},{{"booking_date":"2026-03-18","value_date":"","amount":-9.99,"counterparty":"SPOTIFY AB","counterparty_iban_token":"","purpose":"STOCKHOLM","tx_type":"kartenzahlung","category":"abonnement"}},{{"booking_date":"2026-03-22","value_date":"","amount":-34.50,"counterparty":"Amazon EU SARL","counterparty_iban_token":"","purpose":"Marketplace","tx_type":"kartenzahlung","category":"haushalt"}}]}}

## Example C — PayPal Übersicht

OCR text:
"PayPal-Kontoauszug 02.2026
Inhaber: NAME_001
05.02.2026  An REWE digital GmbH (lebensmittel-online)            -67,89  EUR
10.02.2026  Von Klaus Schmidt (Privat) Rückzahlung                 +50,00  EUR
21.02.2026  An Spotify AB (Premium-Abo)                             -9,99  EUR"

Output:
{{"bank_name":"PayPal","account_iban_token":"","account_holder_token":"NAME_001","period_start":"2026-02-01","period_end":"2026-02-28","statement_no":"","opening_balance":null,"closing_balance":null,"currency":"EUR","transactions":[{{"booking_date":"2026-02-05","value_date":"","amount":-67.89,"counterparty":"REWE digital GmbH","counterparty_iban_token":"","purpose":"lebensmittel-online","tx_type":"kartenzahlung","category":"lebensmittel"}},{{"booking_date":"2026-02-10","value_date":"","amount":50.00,"counterparty":"Klaus Schmidt","counterparty_iban_token":"","purpose":"Privat Rückzahlung","tx_type":"ueberweisung","category":"erstattung"}},{{"booking_date":"2026-02-21","value_date":"","amount":-9.99,"counterparty":"Spotify AB","counterparty_iban_token":"","purpose":"Premium-Abo","tx_type":"kartenzahlung","category":"abonnement"}}]}}

# Reminder

ONE JSON object. No prose. tx_type and category MUST be from the
allowed lists. amount sign matters: incoming positive, outgoing
negative. Keep IBAN_xxx / NAME_xxx / ADDR_xxx tokens verbatim.
"""


_USER_TEMPLATE = (
    "Extract the structured bank statement from this OCR text. "
    "Return the JSON object now.\n\n---\n{text}\n---"
)


# Compact prompt for the per-page extraction path. We send each PDF
# page as its own LLM call, which avoids the output-truncation
# failure mode of single-pass extraction on long Privatgirokontos.
# Header fields are optional per page (only printed on cover/last
# pages); the merger combines them across pages.
_PAGE_SYSTEM_PROMPT = (
    "You extract data from a single OCR'd PAGE of a bank statement.\n\n"
    "Reply with ONE JSON object — no prose, no markdown fences:\n\n"
    "{\n"
    '  "bank_name": "canonical bank name if printed on this page, else \\"\\"",\n'
    '  "account_iban_token": "the user\'s account IBAN_xxx if printed at the top of THIS page, else \\"\\"",\n'
    '  "account_holder_token": "the account holder NAME_xxx if printed, else \\"\\"",\n'
    '  "period_start": "YYYY-MM-DD if a booking-period start is printed, else \\"\\"",\n'
    '  "period_end":   "YYYY-MM-DD if a booking-period end is printed, else \\"\\"",\n'
    '  "statement_no": "Auszug-Nr. if printed, else \\"\\"",\n'
    '  "opening_balance": signed_number_or_null,\n'
    '  "closing_balance": signed_number_or_null,\n'
    '  "currency": "EUR by default",\n'
    '  "transactions": [\n'
    '    {\n'
    '      "booking_date": "YYYY-MM-DD",\n'
    '      "value_date": "YYYY-MM-DD or empty",\n'
    '      "amount": signed_number,\n'
    '      "counterparty": "string",\n'
    '      "counterparty_iban_token": "IBAN_xxx if printed, else empty",\n'
    '      "purpose": "Verwendungszweck / Mandatsreferenz",\n'
    f'      "tx_type": "one of {list(TX_TYPES)}",\n'
    f'      "category": "one of {list(TX_CATEGORIES)}"\n'
    '    }\n'
    '  ]\n'
    "}\n\n"
    "Rules:\n"
    "- Header fields appear only on a few pages (typically the first and last).\n"
    "  When NOT printed on THIS page, return \"\" or null — the merger combines\n"
    "  values across pages.\n"
    "- Read EVERY booking row on the page. Just emit the rows visible on THIS\n"
    "  page — don't re-emit anything from earlier pages.\n"
    "- Numbers: dot is decimal, \"1.234,56\" → 1234.56. Outgoing/Lastschrift/\n"
    "  Belastung → NEGATIVE. Eingang/Gutschrift → POSITIVE.\n"
    "- Skip layout-only lines: \"Übertrag\", \"Saldo neu/alt\", page numbers,\n"
    "  footer disclaimers, BIC-only lines.\n"
    "- Skip the opening / closing balance rows (those carry the running total,\n"
    "  not a booking).\n"
    "- ALWAYS keep IBAN_xxx / NAME_xxx / ADDR_xxx tokens verbatim — do not\n"
    "  invent real-looking values.\n\n"
    "Return ONLY the JSON object."
)

_PAGE_USER_TEMPLATE = (
    "Page {page_no} of {total_pages}. Emit JSON for the bookings on "
    "THIS page only.\n\n---\n{text}\n---"
)


def _parse_response(raw: str) -> dict[str, Any]:
    """Pull the first JSON object out of the model reply (same robust
    decoder as receipts.py — handles markdown fences and stray prose)."""
    raw = (raw or "").strip()
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
    raise ValueError(f"No valid JSON in statement extractor reply: {raw[:200]!r}")


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        # Tolerate German formats: "1.234,56" / "1234,56" / "+3.500,00".
        s = value.strip().replace(" ", "").replace(" ", "")
        s = s.replace(".", "").replace(",", ".") if "," in s and s.count(",") == 1 else s
    else:
        s = value
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _looks_like_lost_bookings(data: dict[str, Any]) -> bool:
    """Heuristic for "the model returned a parseable statement but the
    booking table is suspiciously empty". A real statement with no
    activity has opening_balance == closing_balance (the bank prints
    both numbers identical when nothing happened during the period).
    A non-trivial balance change with zero transactions is the textbook
    signature of an output-truncation or a model that skipped the
    table entirely."""
    if not isinstance(data, dict):
        return False
    txs = data.get("transactions")
    if isinstance(txs, list) and len(txs) > 0:
        return False
    opening = _coerce_float(data.get("opening_balance"))
    closing = _coerce_float(data.get("closing_balance"))
    if opening is None or closing is None:
        # Without both balances we can't tell — accept the empty result
        # rather than retrying every legit-blank statement.
        return False
    return abs(opening - closing) > 0.01


_ISO_RE   = __import__("re").compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DE_RE    = __import__("re").compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
_DE2_RE   = __import__("re").compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{2})$")
_SLASH_RE = __import__("re").compile(r"^(\d{4})/(\d{2})/(\d{2})$")


def _normalise_date(raw: str) -> str:
    """Coerce a booking / value date into ISO YYYY-MM-DD.

    The system prompt asks for ISO, but small local models (Qwen2.5-7B,
    Llama-3.1-8B, …) regularly return the German DD.MM.YYYY shape that
    sat verbatim on the source PDF. SQLite's `strftime` returns NULL on
    those, breaking finance_by_weekday + every monthly aggregate. Catch
    the format here at the single boundary so the rest of the code can
    trust ISO."""
    if not raw:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    s = s.replace(" ", "")[:12]

    def _validate(y: int, mo: int, d: int) -> str:
        # Range checks before formatting — keeps "2024-13-99" out of
        # the table where it would silently break strftime later.
        if not (1900 <= y <= 2100):
            return ""
        if not (1 <= mo <= 12):
            return ""
        if not (1 <= d <= 31):
            return ""
        return f"{y:04d}-{mo:02d}-{d:02d}"

    m = _ISO_RE.match(s)
    if m:
        y, mo, d = m.groups()
        return _validate(int(y), int(mo), int(d))
    m = _SLASH_RE.match(s)
    if m:
        y, mo, d = m.groups()
        return _validate(int(y), int(mo), int(d))
    m = _DE_RE.match(s)
    if m:
        d, mo, y = m.groups()
        return _validate(int(y), int(mo), int(d))
    m = _DE2_RE.match(s)
    if m:
        d, mo, yy = m.groups()
        # Two-digit year: 00..69 → 2000s, 70..99 → 1900s. Same window
        # the classifier uses for document dates.
        y = 2000 + int(yy) if int(yy) < 70 else 1900 + int(yy)
        return _validate(y, int(mo), int(d))
    # Unknown shape — return blank so finance_by_weekday + co. simply
    # skip the row (the v0.25.4 NULL-projection guard keeps /finance
    # from 500'ing). Better than passing garbage downstream.
    return ""


def _normalise_tx(d: dict[str, Any]) -> Transaction | None:
    amount = _coerce_float(d.get("amount"))
    if amount is None:
        return None
    cat = str(d.get("category") or "").strip().lower()
    if cat and cat not in TX_CATEGORIES:
        cat = "sonstiges"
    typ = str(d.get("tx_type") or "").strip().lower()
    if typ and typ not in TX_TYPES:
        typ = "sonstiges"
    return Transaction(
        booking_date=_normalise_date(d.get("booking_date") or ""),
        value_date=_normalise_date(d.get("value_date") or ""),
        amount=amount,
        currency=str(d.get("currency") or "EUR").strip().upper()[:8] or "EUR",
        counterparty=_scrub_residual(str(d.get("counterparty") or "").strip()[:200]),
        counterparty_iban=_scrub_residual(str(d.get("counterparty_iban") or "").strip()[:34]),
        purpose=str(d.get("purpose") or "").strip()[:500],
        tx_type=typ,
        category=cat,
    )


class StatementExtractor:
    """Wraps a Provider to extract a Kontoauszug from OCR text.

    By default the OCR text is pseudonymised before the provider call;
    pass `pseudonymize=False` to skip when running against a local
    provider where data never leaves the box."""

    def __init__(self, provider: Provider, model: str,
                 max_text_chars: int = 32000,
                 holder_names: list[str] | None = None):
        self.provider = provider
        self.model = model
        self.max_text_chars = max_text_chars
        # Pre-seeded household names always get masked, regardless of
        # whether the document text contains a structured detection
        # cue. See FinanceSettings.holder_names for the user-facing
        # rationale.
        self.holder_names = list(holder_names or [])

    def _call_and_parse(self, body: str, *, max_output_tokens: int):
        """One LLM round-trip on the FULL statement (single-pass).
        Used as fallback when the per-page path isn't available."""
        try:
            resp = self.provider.classify(
                system_prompt=SYSTEM_PROMPT,
                user_prompt=_USER_TEMPLATE.format(text=body),
                model=self.model,
                max_output_tokens=max_output_tokens,
                timeout=300.0,
            )
        except ProviderError as exc:
            logger.error("Statement extractor: provider call failed: %s", exc)
            raise
        data = _parse_response(resp.raw_text)
        return resp, data

    def _extract_single_pass(self, body: str):
        """Full-document single-pass extraction. Escalates the output
        budget when the result smells truncated: 16k → 32k → 64k.
        Used when there's no PDF on disk to drive the per-page path."""
        resp, data = self._call_and_parse(body, max_output_tokens=16000)
        if _looks_like_lost_bookings(data):
            logger.warning(
                "Statement extractor: balance mismatch but transactions=[]; "
                "retrying at 32k output budget."
            )
            resp, data = self._call_and_parse(body, max_output_tokens=32000)
        if _looks_like_lost_bookings(data):
            logger.warning(
                "Statement extractor: still empty at 32k; trying 64k."
            )
            resp, data = self._call_and_parse(body, max_output_tokens=64000)
        return resp, data

    def _extract_pages(self, pdf_path: Path, ocr_settings,
                       *, pseudo: Pseudonymizer | None,
                       pseudonymize: bool) -> tuple[dict, Any]:
        """Page-by-page extraction. One LLM call per non-empty page;
        booking lists are concatenated and header fields are merged
        across pages. More reliable on long documents than single-pass
        — no output truncation since each request only emits the
        bookings of one page."""
        from .. import ocr as _ocr_mod

        pages = _ocr_mod.extract_pages(pdf_path, ocr_settings)
        if not pages:
            raise ValueError(f"no pages extracted from {pdf_path}")
        total_pages = len(pages)

        merged: dict[str, Any] = {
            "bank_name": "", "account_iban_token": "",
            "account_holder_token": "", "period_start": "",
            "period_end": "", "statement_no": "",
            "opening_balance": None, "closing_balance": None,
            "currency": "EUR", "transactions": [],
        }
        last_resp = None

        for idx, page_text in enumerate(pages, start=1):
            text = (page_text or "").strip()
            if not text or len(text) < 20:
                continue
            body = text[: self.max_text_chars]
            if pseudonymize and pseudo is not None:
                body = pseudo.pseudonymize(body)
            try:
                resp = self.provider.classify(
                    system_prompt=_PAGE_SYSTEM_PROMPT,
                    user_prompt=_PAGE_USER_TEMPLATE.format(
                        page_no=idx, total_pages=total_pages, text=body,
                    ),
                    model=self.model,
                    # 16k per page is comfortable: a single page caps
                    # out at ~30 bookings, well within budget.
                    max_output_tokens=16000,
                    timeout=300.0,
                )
            except ProviderError as exc:
                logger.warning(
                    "Per-page extract page %d/%d failed: %s",
                    idx, total_pages, exc,
                )
                continue
            last_resp = resp
            try:
                page_data = _parse_response(resp.raw_text)
            except ValueError:
                logger.warning(
                    "Per-page extract: unparseable response on page %d/%d",
                    idx, total_pages,
                )
                continue
            if not isinstance(page_data, dict):
                continue

            # Header merge: first non-empty wins for identity-style fields.
            for key in ("bank_name", "account_iban_token",
                        "account_holder_token", "statement_no"):
                if not merged[key]:
                    v = str(page_data.get(key) or "").strip()
                    if v:
                        merged[key] = v
            cur = str(page_data.get("currency") or "").strip().upper()
            if cur and cur != "EUR" and merged["currency"] == "EUR":
                merged["currency"] = cur

            # Period: earliest start, latest end.
            ps = str(page_data.get("period_start") or "").strip()[:10]
            pe = str(page_data.get("period_end")   or "").strip()[:10]
            if ps and (not merged["period_start"] or ps < merged["period_start"]):
                merged["period_start"] = ps
            if pe and (not merged["period_end"]   or pe > merged["period_end"]):
                merged["period_end"]   = pe

            # Balances: opening from earliest non-null, closing from latest.
            ob = _coerce_float(page_data.get("opening_balance"))
            cb = _coerce_float(page_data.get("closing_balance"))
            if ob is not None and merged["opening_balance"] is None:
                merged["opening_balance"] = ob
            if cb is not None:
                merged["closing_balance"] = cb

            txs = page_data.get("transactions")
            if isinstance(txs, list):
                merged["transactions"].extend(
                    t for t in txs if isinstance(t, dict)
                )

        if last_resp is None:
            raise ValueError("no page produced a parseable response")
        return merged, last_resp

    def extract(self, ocr_text: str, *, pseudonymize: bool = True,
                pdf_path: Path | None = None,
                ocr_settings=None) -> Statement:
        """Extract a Kontoauszug from OCR text.

        Preferred mode is **page-by-page** when `pdf_path` and
        `ocr_settings` are provided: each page becomes a separate
        small LLM call, much more reliable on long documents than
        single-pass extraction (no output truncation).

        Falls back to single-pass with 16k → 32k → 64k output budget
        escalation when the PDF isn't reachable (legacy data with no
        `library_path`)."""
        if not ocr_text:
            raise ValueError("no OCR text provided")

        pseudo: Pseudonymizer | None = None
        if pseudonymize:
            pseudo = Pseudonymizer()
            if self.holder_names:
                pseudo.seed_household_names(self.holder_names)

        warning = ""
        resp = None

        if pdf_path is not None and ocr_settings is not None:
            try:
                data, resp = self._extract_pages(
                    pdf_path, ocr_settings,
                    pseudo=pseudo, pseudonymize=pseudonymize,
                )
            except Exception:
                logger.exception(
                    "Per-page extraction failed for %s — falling back to "
                    "single-pass on stored OCR text.", pdf_path,
                )
                body = ocr_text[: self.max_text_chars]
                if pseudonymize and pseudo is not None:
                    body = pseudo.pseudonymize(body)
                resp, data = self._extract_single_pass(body)
        else:
            body = ocr_text[: self.max_text_chars]
            if pseudonymize and pseudo is not None:
                body = pseudo.pseudonymize(body)
            resp, data = self._extract_single_pass(body)

        if _looks_like_lost_bookings(data):
            warning = (
                "extraction returned 0 transactions but opening/closing "
                "balances differ — likely a model miss. Re-analyse this "
                "statement."
            )
            logger.warning(
                "Statement extractor: balance mismatch but no transactions "
                "for %s — flagging as suspicious.", self.model,
            )
        # Sanity check: if we ended up parsing a single transaction-
        # shaped inner object instead of the outer statement object,
        # the response was truncated. Log loud so the user can tell
        # the difference between "no bookings on this statement" and
        # "the response got cut off and we lost everything".
        if isinstance(data, dict) and "transactions" not in data and "booking_date" in data:
            logger.warning(
                "Statement extractor for %s: response looks truncated "
                "(parsed inner-tx object, no outer 'transactions' field). "
                "Try a model with a larger output budget.",
                self.model,
            )

        # Restore pseudonymised tokens before we copy fields out.
        if pseudo is not None:
            data = pseudo.restore(data)

        # Keep token-named columns optional in the schema; once restored
        # they'll be plain values, but the schema names still carry the
        # "_token" suffix on the way through the LLM. Residual tokens
        # (e.g. the LLM emitted "NAME_001" without us actually masking
        # anything as NAME_001) get blanked rather than stored as-is.
        iban       = _scrub_residual(str(data.get("account_iban_token")    or data.get("account_iban")    or "").strip())
        holder     = _scrub_residual(str(data.get("account_holder_token")  or data.get("account_holder")  or "").strip())
        # NOTE: removed in 0.13.2 — earlier versions fell back to
        # pseudo.ibans[0] when the token field came back empty, but on
        # multi-IBAN statements (counterparty IBAN appears in transaction
        # lines) "first detected" is often a *counterparty* IBAN, not the
        # user's. The fallback then created a bogus account ("Unbekannt
        # …xxxx") that swallowed every statement where the LLM was unsure.
        # Better: leave iban empty so the doc lands without an account
        # association, and surface a "needs review" indicator in /finance.

        bank = str(data.get("bank_name") or "").strip()[:128]

        opening = _coerce_float(data.get("opening_balance"))
        closing = _coerce_float(data.get("closing_balance"))

        txs_raw = data.get("transactions") or []
        txs: list[Transaction] = []
        if isinstance(txs_raw, list):
            for d in txs_raw:
                if not isinstance(d, dict):
                    continue
                # Pre-restore counterparty IBAN token field name.
                if "counterparty_iban" not in d and "counterparty_iban_token" in d:
                    d["counterparty_iban"] = d.pop("counterparty_iban_token")
                tx = _normalise_tx(d)
                if tx is not None:
                    txs.append(tx)

        # Sanity-check the period: if the LLM returns a swapped pair
        # (start > end), flip them. Happens when the model conflates
        # the document-generation date with the booking-period range.
        period_start = str(data.get("period_start") or "").strip()[:10]
        period_end   = str(data.get("period_end")   or "").strip()[:10]
        if period_start and period_end and period_start > period_end:
            period_start, period_end = period_end, period_start

        # Auto-promote transactions where the counterparty matches the
        # account holder name to category=uebertrag. The LLM often gets
        # this wrong on Sparkasse statements where internal transfers
        # appear as "Max Mustermann Erika Mustermann Sonst. Gutschrift"
        # with a generic booking line — easy for it to land on
        # "sonstiges" when it's really an internal move that shouldn't
        # be counted as income / expense in the headline numbers.
        if holder:
            import re as _re
            holder_tokens = {tok.lower() for tok in _re.split(r"[,\s]+", holder) if len(tok) > 2}
            if holder_tokens:
                for tx in txs:
                    cp_tokens = {tok.lower() for tok in _re.split(r"[,\s]+", tx.counterparty or "") if len(tok) > 2}
                    # If the counterparty contains the holder's full name
                    # (possibly alongside a partner's name on a joint
                    # account, e.g. "Max Mustermann Erika Mustermann"
                    # when the holder is "Max Mustermann"), it's an
                    # internal transfer. Direction: holder_tokens MUST
                    # be a subset of counterparty_tokens — getting this
                    # backwards meant the heuristic never fired on
                    # joint-account statements.
                    if cp_tokens and holder_tokens.issubset(cp_tokens):
                        tx.category = "uebertrag"
                        if not tx.tx_type or tx.tx_type == "sonstiges":
                            tx.tx_type = "uebertrag"

        return Statement(
            bank_name=bank,
            iban=iban,
            iban_hash=iban_hash(iban) if iban else "",
            iban_last4=(iban[-4:] if len(iban) >= 4 else ""),
            account_holder=holder[:200],
            period_start=period_start,
            period_end=period_end,
            statement_no=str(data.get("statement_no") or "").strip()[:64],
            opening_balance=opening,
            closing_balance=closing,
            currency=str(data.get("currency") or "EUR").strip().upper()[:8] or "EUR",
            transactions=txs,
            raw_response=resp.raw_text[:64000],
            privacy_mode="pseudonymize" if pseudonymize else "local",
            extraction_warning=warning,
        )


def backfill_statements(settings, db, classifier, *, dry_run: bool = False,
                        local_only: bool = False,
                        inter_request_delay_s: float = 1.5) -> dict:
    """Re-extract statements for every Kontoauszug doc that doesn't have
    one yet. Reads the stored OCR text, no new OCR cost incurred.

    `local_only` skips pseudonymisation (for users on a local provider).
    """
    extractor = StatementExtractor(
        classifier.provider, settings.ai.model,
        max_text_chars=max(settings.ai.max_text_chars, 32000),
        holder_names=settings.finance.holder_names,
    )
    # Also catch legacy bank docs that were filed under category=Bank
    # before v0.13.0 added "Kontoauszug" as a top-level category. We
    # filter on subject keywords ("kontoauszug", "auszug", "girokonto",
    # "tagesgeld", "kreditkarte") rather than subcategory because the
    # classifier has used various subcategories ("Konto", "" …) over
    # versions. After successful extraction the doc gets promoted to
    # category=Kontoauszug so /finance can find it.
    rows = db._conn.execute(
        """SELECT id, category, subcategory, extracted_text, doc_date, subject
           FROM documents
           WHERE deleted_at IS NULL
             AND extracted_text IS NOT NULL AND extracted_text != ''
             AND id NOT IN (SELECT doc_id FROM statements)
             AND (category = 'Kontoauszug'
                  OR (category = 'Bank' AND (
                      subcategory = 'Konto'
                      OR LOWER(COALESCE(subject,'')) LIKE '%kontoauszug%'
                      OR LOWER(COALESCE(subject,'')) LIKE '%girokonto%'
                      OR LOWER(COALESCE(subject,'')) LIKE '%tagesgeld%'
                      OR LOWER(COALESCE(subject,'')) LIKE '%kreditkart%'
                      OR LOWER(COALESCE(subject,'')) LIKE '%paypal%auszug%'
                  )))"""
    ).fetchall()
    import time as _time
    processed: list[int] = []
    failed: list[dict] = []
    empty: list[int] = []
    for idx, r in enumerate(rows):
        doc_id = int(r["id"])
        # Throttle between LLM calls. Anthropic Tier-1 caps at 50k input
        # tokens per minute; with our ~3-6k tokens per statement that's
        # roughly 8-15 statements/minute before we hit a 429. The
        # 1.5s default keeps us comfortably below that.
        if idx > 0 and inter_request_delay_s > 0:
            _time.sleep(inter_request_delay_s)
        try:
            stmt = extractor.extract(r["extracted_text"], pseudonymize=not local_only)
        except Exception as exc:
            failed.append({"doc_id": doc_id, "error": str(exc)})
            logger.warning("Backfill: doc %d failed: %s", doc_id, exc)
            continue
        if not stmt.transactions:
            # Mark as empty so the caller can surface "needs review" docs
            # — but still write the (header-only) statement row so the
            # /finance diagnostics can list it without re-trying every
            # backfill round.
            empty.append(doc_id)
        if dry_run:
            logger.info("Backfill (dry-run): doc %d → bank=%s tx=%d",
                        doc_id, stmt.bank_name, len(stmt.transactions))
            processed.append(doc_id)
            continue
        # Promote the document to category=Kontoauszug if it was filed
        # under the legacy Bank category. Without this, /finance ignores
        # it and the user has to wonder why their data isn't showing up.
        # Skip the promotion if the extractor came back empty — that
        # was likely a non-statement Bank doc (a contract or something)
        # picked up by the subject heuristic.
        if r["category"] == "Bank" and stmt.transactions:
            db._conn.execute(
                "UPDATE documents SET category = 'Kontoauszug', subcategory = '' WHERE id = ?",
                (doc_id,),
            )
        # Compute / lookup the account using iban_hash so multiple
        # statements for the same account auto-merge.
        account_id: int | None = None
        if stmt.iban_hash:
            account_id = db.upsert_account(
                bank_name=stmt.bank_name or "Unbekannt",
                iban=stmt.iban,
                iban_last4=stmt.iban_last4,
                iban_hash=stmt.iban_hash,
                account_holder=stmt.account_holder,
                currency=stmt.currency,
            )
        # Per-tx hash for dedup against overlapping statements.
        from hashlib import sha256
        for tx in stmt.transactions:
            key = (
                stmt.iban_hash + "|" +
                tx.booking_date + "|" +
                f"{tx.amount:.2f}" + "|" +
                tx.purpose
            )
            tx_hash_val = sha256(key.encode("utf-8")).hexdigest()
            tx.tx_hash = tx_hash_val   # type: ignore[attr-defined]
        db.upsert_statement(
            doc_id,
            account_id=account_id,
            period_start=stmt.period_start, period_end=stmt.period_end,
            statement_no=stmt.statement_no,
            opening_balance=stmt.opening_balance,
            closing_balance=stmt.closing_balance,
            currency=stmt.currency,
            file_hash="",
            privacy_mode=stmt.privacy_mode,
            transactions=[
                {**t.as_dict(), "tx_hash": getattr(t, "tx_hash", "")}
                for t in stmt.transactions
            ],
            extra_json=stmt.raw_response,
            extraction_warning=stmt.extraction_warning,
        )
        processed.append(doc_id)
    return {
        "found": len(rows),
        "processed": processed,
        "empty": empty,
        "failed": failed,
        "dry_run": dry_run,
    }
