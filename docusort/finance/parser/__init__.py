"""Deterministic regex-based bank-statement parser.

Why: local 7B LLMs hallucinate transactions, miss balance lines, and
produce inconsistent output. For bank statements with a known
table-shaped layout we can do better with explicit per-bank regexes —
no model, no nondeterminism, no hallucinations.

Public entry point: `parse(text) -> ParseResult`. Returns a
`Statement` (compatible with `docusort.finance.extractor.Statement`)
plus a confidence score. Caller decides whether to trust the parser
or fall back to the LLM extractor based on the score.

Layout detection runs first against the page header (bank name +
IBAN format + signature phrases), then the matched layout's parser
walks the OCR text line by line. Each layout is responsible for:

  - Header extraction (period, opening/closing balance, IBAN)
  - Booking row parsing (date, amount, counterparty, purpose)
  - Page-boundary handling (skip headers/footers/page numbers)

Unknown layouts get the `generic` parser as a last resort — it
recognises the most common German booking-row shapes but won't catch
everything. When the generic parser's confidence is below a
threshold, we leave it to the LLM.
"""

from __future__ import annotations

from .base import ParseResult, ParsedStatement, ParsedTransaction
from .layouts import detect_layout, get_layout
from .layouts.generic import GenericLayout


def parse(text: str, *, hint_bank: str | None = None) -> ParseResult:
    """Parse bank-statement OCR text into a structured Statement.

    `hint_bank`: when the caller already has a bank-name guess from
    the classifier (`Sparkasse`, `DKB`, …) we use it to short-circuit
    detection. Without it we sniff the header.

    Always returns a `ParseResult` with a confidence ∈ [0.0, 1.0].
    Callers compare against their own threshold to decide whether to
    trust the result over the LLM extractor.
    """
    if not text or not text.strip():
        return ParseResult(
            statement=ParsedStatement(),
            confidence=0.0,
            layout="empty",
            warnings=["no OCR text provided"],
        )

    layout = None
    if hint_bank:
        layout = get_layout(hint_bank)
    if layout is None:
        layout = detect_layout(text)

    return layout.parse(text)


__all__ = ["parse", "ParseResult", "ParsedStatement", "ParsedTransaction"]
