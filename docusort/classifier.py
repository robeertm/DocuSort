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
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    model: str = ""

    @property
    def is_confident(self) -> bool:
        return self.confidence >= 0.65


SYSTEM_PROMPT = """You are an expert document classifier for a German personal \
document archive. You receive the extracted text of a scanned document and \
return strict JSON metadata.

Rules:
- Reply with ONE JSON object, no prose, no markdown fences.
- Keys: category, date, sender, subject, confidence, reasoning.
- category MUST be exactly one of the allowed categories the user provides.
- date is the document's own date (Rechnungsdatum, Vertragsdatum, Briefdatum) \
in ISO format YYYY-MM-DD. If you cannot find one, use today's date and lower \
the confidence.
- sender is the organisation or person who issued the document, 1-4 words, \
no legal suffixes (GmbH, AG, e.V. are fine but drop "Firma", "Herr", etc.), \
ASCII-safe where possible.
- subject is a concise description (3-8 words) of what the document is about, \
e.g. "Mobilfunkrechnung Februar", "Jahresmeldung 2025", "Befund Hausarzt".
- confidence is a float 0..1. Use <0.65 when you are unsure about category or \
date; that will route the document to manual review.
- reasoning: one short sentence, why you chose this category.
- Do NOT invent facts. If a field is unknown, leave it empty and lower \
confidence.
"""


def _build_user_message(text: str, categories: list[dict[str, Any]], max_chars: int) -> str:
    allowed = ", ".join(c["name"] for c in categories)
    hints = "\n".join(
        f"- {c['name']}: {c.get('description', '')}" for c in categories
    )
    body = text[:max_chars] if text else "(no text extracted)"
    today = date.today().isoformat()
    return (
        f"Today is {today}.\n\n"
        f"Allowed categories: {allowed}\n\n"
        f"Category hints:\n{hints}\n\n"
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

    def classify(self, text: str) -> Classification:
        user = _build_user_message(text, self.categories, self.settings.max_text_chars)
        logger.debug("Calling Claude model=%s, text_len=%d",
                     self.settings.model, len(text))

        resp = self.client.messages.create(
            model=self.settings.model,
            max_tokens=600,
            system=SYSTEM_PROMPT,
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

        in_tok = int(getattr(resp.usage, "input_tokens", 0) or 0)
        out_tok = int(getattr(resp.usage, "output_tokens", 0) or 0)
        cost = calculate_cost(self.settings.model, in_tok, out_tok)

        return Classification(
            category=category,
            date=str(data.get("date", date.today().isoformat())).strip(),
            sender=str(data.get("sender", "")).strip() or "Unbekannt",
            subject=str(data.get("subject", "")).strip() or "Dokument",
            confidence=float(data.get("confidence", 0.5)),
            reasoning=str(data.get("reasoning", "")).strip(),
            input_tokens=in_tok,
            output_tokens=out_tok,
            cost_usd=cost,
            model=self.settings.model,
        )
