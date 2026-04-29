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
from typing import Any

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
        booking_date=str(d.get("booking_date") or "").strip()[:10],
        value_date=str(d.get("value_date") or "").strip()[:10],
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
                 max_text_chars: int = 32000):
        self.provider = provider
        self.model = model
        self.max_text_chars = max_text_chars

    def extract(self, ocr_text: str, *, pseudonymize: bool = True) -> Statement:
        if not ocr_text:
            raise ValueError("no OCR text provided")
        body = ocr_text[: self.max_text_chars]

        pseudo: Pseudonymizer | None = None
        if pseudonymize:
            pseudo = Pseudonymizer()
            body = pseudo.pseudonymize(body)

        try:
            resp = self.provider.classify(
                system_prompt=SYSTEM_PROMPT,
                user_prompt=_USER_TEMPLATE.format(text=body),
                model=self.model,
                # Multi-page Privatgirokonto statements can carry many
                # bookings; the JSON response with all of them easily
                # outgrows a small output cap. When the response gets
                # truncated mid-string, the JSON parser can't find a
                # closing brace for the outer object and falls back to
                # whichever inner transaction dict happens to be
                # fully-formed at that point — which leaves bank /
                # period / transactions all empty. Generous ceiling so
                # the model has room to emit every booking on a
                # twelve-page statement.
                max_output_tokens=16000,
                # Generating that much output takes well past the
                # default 60 s. Five minutes covers the worst-case
                # Privatgirokonto with hundreds of bookings on Anthropic
                # Haiku-class models.
                timeout=300.0,
            )
        except ProviderError as exc:
            logger.error("Statement extractor: provider call failed: %s", exc)
            raise

        data = _parse_response(resp.raw_text)
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
        # appear as "Robert Manuwald Steffi Manuwald Sonst. Gutschrift"
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
                    # account, e.g. "Robert Manuwald Steffi Manuwald"
                    # when the holder is "Robert Manuwald"), it's an
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
        )
        processed.append(doc_id)
    return {
        "found": len(rows),
        "processed": processed,
        "empty": empty,
        "failed": failed,
        "dry_run": dry_run,
    }
