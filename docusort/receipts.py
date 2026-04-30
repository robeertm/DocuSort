"""Receipt (Kassenzettel) line-item extractor.

When the main classifier marks a document as `category = "Kassenzettel"`,
this module runs a second LLM pass to pull structured data out of the
OCR text:

  - shop name and type (supermarkt / drogerie / restaurant / tankstelle / …)
  - payment method (bar, girocard, kreditkarte, paypal)
  - total amount + currency
  - per-line items: name, quantity, unit price, line total, item category

The output is stored in the `receipts` and `receipt_items` SQLite tables
(see db.py) and rendered on the document detail page + the analytics
dashboard.

We deliberately keep this a SECOND-PASS extractor (only invoked for receipts)
so the main classifier prompt stays compact and doesn't blow up
prompt-cache effectiveness for non-receipt documents.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from .providers import Provider, ProviderError


logger = logging.getLogger("docusort.receipts")


SHOP_TYPES = (
    "supermarkt", "drogerie", "baumarkt", "restaurant", "cafe",
    "tankstelle", "apotheke", "bekleidung", "elektronik", "buecher",
    "moebel", "versand", "sonstiges",
)

ITEM_CATEGORIES = (
    "lebensmittel", "getraenke", "haushalt", "koerperpflege",
    "elektronik", "bekleidung", "buecher", "essen-trinken-aussehaus",
    "transport", "baumarkt", "tabak", "pfand", "rabatt", "sonstiges",
)

PAYMENT_METHODS = ("bar", "girocard", "kreditkarte", "paypal", "sonstiges")


@dataclass
class ReceiptItem:
    name: str
    quantity: float | None = None
    unit_price: float | None = None
    total_price: float | None = None
    item_category: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "quantity": self.quantity,
            "unit_price": self.unit_price,
            "total_price": self.total_price,
            "item_category": self.item_category,
        }


@dataclass
class Receipt:
    shop_name: str = ""
    shop_type: str = ""
    payment_method: str = ""
    total_amount: float | None = None
    currency: str = "EUR"
    receipt_date: str = ""
    items: list[ReceiptItem] = field(default_factory=list)
    raw_response: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "shop_name": self.shop_name,
            "shop_type": self.shop_type,
            "payment_method": self.payment_method,
            "total_amount": self.total_amount,
            "currency": self.currency,
            "receipt_date": self.receipt_date,
            "items": [i.as_dict() for i in self.items],
        }


SYSTEM_PROMPT = f"""You are a receipt parser for a German personal-finance app. You receive the OCR text of a Kassenzettel (Bon, Quittung, Bewirtungsbeleg) and reply with ONE JSON object describing it.

# Output format (strict)

Reply with ONE JSON object, no prose, no markdown fences, no trailing text.

Schema:
{{
  "shop_name": string,            // shop / restaurant / chain name as printed (e.g. "REWE", "dm", "Aral")
  "shop_type": string,            // EXACTLY one of: {", ".join(SHOP_TYPES)}
  "payment_method": string,       // one of: {", ".join(PAYMENT_METHODS)} or empty if unknown
  "total_amount": number | null,  // GRAND TOTAL including tax, in major units (e.g. 23.87)
  "currency": string,             // 3-letter ISO code; default "EUR"
  "receipt_date": string,         // ISO YYYY-MM-DD; if absent, return ""
  "items": [
    {{
      "name": string,             // human-readable line description ("Bio Vollmilch 1L")
      "quantity": number | null,  // 1 if not printed; for "2 x" lines use 2
      "unit_price": number | null,// per unit before discount (null when only total is printed)
      "total_price": number | null,// signed line total — POSITIVE for purchases, NEGATIVE for discounts/Rabatt/Pfand-Retoure
      "item_category": string     // EXACTLY one of: {", ".join(ITEM_CATEGORIES)}
    }},
    ...
  ]
}}

# Rules

- Numbers use a dot as decimal separator: "1,29" → 1.29.
- Drop pure layout noise: SUMME, ZWISCHENSUMME, MwSt-Tabellen, Kassenstammdaten,
  Adresszeilen, "Vielen Dank", Bon-Nr., Kassierer-Nr.
- KEEP discount lines as separate items with NEGATIVE total_price (e.g.
  "RABATT -1,50" → {{name: "Rabatt", total_price: -1.50}}).
- KEEP Pfand lines (Pfand-Aufschlag positive, Pfand-Rückgabe negative).
- If a line has "2 x 1,29 = 2,58", set quantity=2, unit_price=1.29, total_price=2.58.
- If only one number is printed for a line, set total_price to that number.
- shop_type maps to common chains:
  - REWE, EDEKA, Aldi, Lidl, Kaufland, Penny, Netto, Real, Norma, Tegut → supermarkt
  - dm, Rossmann, Müller (drogerie context), Budni, Douglas → drogerie
  - Obi, Bauhaus, Hornbach, Hagebau, Toom, Globus → baumarkt
  - Aral, Shell, Total, Esso, Star, BP, Jet, OMV → tankstelle
  - Apotheke, Apotheker, "Apo " prefix → apotheke
  - H&M, C&A, Zara, Primark, Tom Tailor → bekleidung
  - MediaMarkt, Saturn, Cyberport, Conrad, Reichelt → elektronik
  - Thalia, Hugendubel, Mayersche → buecher
  - Ikea, Möbel ..., Höffner, XXXLutz → moebel
  - Amazon Lieferschein, Zalando, Otto Versand → versand
  - sit-down places → restaurant; takeaway / coffee → cafe
  - everything else → sonstiges
- item_category guidance:
  - food / fresh produce / pasta / bread / dairy → lebensmittel
  - sodas / juice / beer / wine / spirits → getraenke
  - cleaning supplies / toilet paper / kitchen rolls → haushalt
  - shampoo / lotion / cosmetics / toothpaste → koerperpflege
  - cigarettes → tabak
  - PFAND lines → pfand
  - discount/Rabatt → rabatt
  - fuel → transport
  - in restaurants/cafes, all consumed food/drink → essen-trinken-aussehaus
  - everything else → sonstiges (don't guess if unclear)
- Be defensive about OCR noise: "1.D0" might mean "1.00", "1 .29" → 1.29, "I,29" → 1.29.
- Never invent items not in the text. If the receipt has 5 visible items and the rest is unreadable, return 5 items.

# Few-shot examples

## Example A — REWE supermarket

OCR text:
"REWE Markt GmbH · Königsbrücker Str. 78 · 01099 Dresden
Bon-Nr. 4711 Kasse 02 17:42 12.04.2026
Bio Vollmilch 1L         1,29 A
Vollkornbrot             2,49 A
Tomaten 500g             1,79 A
2 x Joghurt Erdbeer
   à 0,89                1,78 A
PFAND 0,25                0,25 A
RABATT Coupon           -0,50 A
SUMME EUR              23,87
Gegeben girocard       23,87"

Output:
{{"shop_name":"REWE","shop_type":"supermarkt","payment_method":"girocard","total_amount":23.87,"currency":"EUR","receipt_date":"2026-04-12","items":[{{"name":"Bio Vollmilch 1L","quantity":1,"unit_price":1.29,"total_price":1.29,"item_category":"lebensmittel"}},{{"name":"Vollkornbrot","quantity":1,"unit_price":2.49,"total_price":2.49,"item_category":"lebensmittel"}},{{"name":"Tomaten 500g","quantity":1,"unit_price":1.79,"total_price":1.79,"item_category":"lebensmittel"}},{{"name":"Joghurt Erdbeer","quantity":2,"unit_price":0.89,"total_price":1.78,"item_category":"lebensmittel"}},{{"name":"Pfand","quantity":1,"unit_price":0.25,"total_price":0.25,"item_category":"pfand"}},{{"name":"Coupon-Rabatt","quantity":1,"unit_price":-0.50,"total_price":-0.50,"item_category":"rabatt"}}]}}

## Example B — Tankquittung Aral

OCR text:
"Aral Tankstelle · Bautzner Landstr.
Beleg 8821 18.03.2026 09:12
Super E10 41,32 L à 1,789 EUR/L
                       73,93
EC-Karte               73,93"

Output:
{{"shop_name":"Aral","shop_type":"tankstelle","payment_method":"girocard","total_amount":73.93,"currency":"EUR","receipt_date":"2026-03-18","items":[{{"name":"Super E10","quantity":41.32,"unit_price":1.789,"total_price":73.93,"item_category":"transport"}}]}}

## Example C — Restaurantrechnung

OCR text:
"Trattoria Da Vinci · Schloßstr. 12 · 01099 Dresden
22.03.2026 19:42
2 x Spaghetti Carbonara à 14,50    29,00
1 x Salat gemischt                  6,50
2 x Mineralwasser à 3,50            7,00
1 x Tiramisu                        6,00
SUMME                              48,50
Gegeben Kreditkarte                50,00
Trinkgeld                           1,50"

Output:
{{"shop_name":"Trattoria Da Vinci","shop_type":"restaurant","payment_method":"kreditkarte","total_amount":48.50,"currency":"EUR","receipt_date":"2026-03-22","items":[{{"name":"Spaghetti Carbonara","quantity":2,"unit_price":14.50,"total_price":29.00,"item_category":"essen-trinken-aussehaus"}},{{"name":"Salat gemischt","quantity":1,"unit_price":6.50,"total_price":6.50,"item_category":"essen-trinken-aussehaus"}},{{"name":"Mineralwasser","quantity":2,"unit_price":3.50,"total_price":7.00,"item_category":"essen-trinken-aussehaus"}},{{"name":"Tiramisu","quantity":1,"unit_price":6.00,"total_price":6.00,"item_category":"essen-trinken-aussehaus"}}]}}

# Reminder

ONE JSON object. No prose. shop_type and item_category MUST be from the
allowed lists. Numbers as floats, no currency symbols inside the numbers.
"""


_USER_TEMPLATE = (
    "Extract the structured receipt from this OCR text. "
    "Return the JSON object now.\n\n---\n{text}\n---"
)


def _parse_response(raw: str) -> dict[str, Any]:
    """Pull the first JSON object out of the model reply."""
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
    raise ValueError(f"No valid JSON in receipt extractor reply: {raw[:200]!r}")


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise_item(d: dict[str, Any]) -> ReceiptItem | None:
    name = str(d.get("name") or "").strip()
    if not name:
        return None
    cat = str(d.get("item_category") or "").strip().lower()
    if cat and cat not in ITEM_CATEGORIES:
        cat = "sonstiges"
    return ReceiptItem(
        name=name[:128],
        quantity=_coerce_float(d.get("quantity")),
        unit_price=_coerce_float(d.get("unit_price")),
        total_price=_coerce_float(d.get("total_price")),
        item_category=cat,
    )


def backfill_receipts(settings, db, classifier, *, dry_run: bool = False) -> dict:
    """Re-extract receipts for every Kassenzettel doc that doesn't have one
    yet. Useful after upgrading from a version that didn't auto-extract.

    Reads the stored OCR text from `documents.extracted_text`, so no new
    OCR cost is incurred. The LLM call is what's billed.
    """
    extractor = ReceiptExtractor(
        classifier.provider, settings.ai.model,
        max_text_chars=settings.ai.max_text_chars,
        holder_names=settings.finance.holder_names,
        pseudonymize=settings.finance.pseudonymize,
    )
    rows = db._conn.execute(
        """SELECT id, extracted_text, doc_date FROM documents
           WHERE category = 'Kassenzettel' AND deleted_at IS NULL
             AND id NOT IN (SELECT doc_id FROM receipts)
             AND extracted_text IS NOT NULL AND extracted_text != ''"""
    ).fetchall()
    processed: list[int] = []
    failed: list[dict] = []
    for r in rows:
        doc_id = int(r["id"])
        try:
            receipt = extractor.extract(r["extracted_text"])
        except Exception as exc:
            failed.append({"doc_id": doc_id, "error": str(exc)})
            logger.warning("Backfill: doc %d failed: %s", doc_id, exc)
            continue
        if dry_run:
            logger.info("Backfill (dry-run): doc %d -> shop=%s items=%d",
                        doc_id, receipt.shop_name, len(receipt.items))
        else:
            db.upsert_receipt(
                doc_id,
                shop_name=receipt.shop_name, shop_type=receipt.shop_type,
                payment_method=receipt.payment_method,
                total_amount=receipt.total_amount, currency=receipt.currency,
                receipt_date=receipt.receipt_date or (r["doc_date"] or ""),
                items=[i.as_dict() for i in receipt.items],
                extra_json=receipt.raw_response,
            )
        processed.append(doc_id)
    return {
        "found": len(rows),
        "processed": processed,
        "failed": failed,
        "dry_run": dry_run,
    }


_LOCAL_PROVIDERS = ("openai_compat", "bridge")


class ReceiptExtractor:
    """Wraps a Provider to extract structured receipts from OCR text."""

    def __init__(self, provider: Provider, model: str,
                 max_text_chars: int = 12000,
                 holder_names: list[str] | None = None,
                 pseudonymize: bool = True):
        self.provider = provider
        self.model = model
        self.max_text_chars = max_text_chars
        self.holder_names = list(holder_names or [])
        self.pseudonymize = pseudonymize

    def extract(self, ocr_text: str) -> Receipt:
        if not ocr_text:
            raise ValueError("no OCR text provided")
        body = ocr_text[: self.max_text_chars]

        is_local = self.provider.name in _LOCAL_PROVIDERS
        do_pseudo = self.pseudonymize and not is_local
        pseudo = None
        if do_pseudo:
            from .finance.pseudonymizer import pseudonymize_for_cloud
            body, pseudo = pseudonymize_for_cloud(body, self.holder_names)

        try:
            resp = self.provider.classify(
                system_prompt=SYSTEM_PROMPT,
                user_prompt=_USER_TEMPLATE.format(text=body),
                model=self.model,
                max_output_tokens=2000,
            )
        except ProviderError as exc:
            logger.error("Receipt extractor: provider call failed: %s", exc)
            raise

        data = _parse_response(resp.raw_text)
        if pseudo is not None:
            data = pseudo.restore(data)

        shop_type = str(data.get("shop_type") or "").strip().lower()
        if shop_type and shop_type not in SHOP_TYPES:
            shop_type = "sonstiges"

        payment = str(data.get("payment_method") or "").strip().lower()
        if payment and payment not in PAYMENT_METHODS:
            payment = "sonstiges"

        items_raw = data.get("items") or []
        items: list[ReceiptItem] = []
        if isinstance(items_raw, list):
            for d in items_raw:
                if not isinstance(d, dict):
                    continue
                item = _normalise_item(d)
                if item is not None:
                    items.append(item)

        return Receipt(
            shop_name=str(data.get("shop_name") or "").strip()[:128],
            shop_type=shop_type,
            payment_method=payment,
            total_amount=_coerce_float(data.get("total_amount")),
            currency=str(data.get("currency") or "EUR").strip().upper()[:8] or "EUR",
            receipt_date=str(data.get("receipt_date") or "").strip()[:10],
            items=items,
            raw_response=resp.raw_text[:6000],
        )
