"""CSV-driven finance module.

This package used to host a complex Kontoauszug-extraction pipeline
(LLM-based statement parsing, deterministic regex parsers, audit
+ rescale + dedup tools, pseudonymisation). All of that was ripped
out in v0.33.0 — bank statements are now a downstream-only artifact:
they get classified as documents like everything else, but their
transaction data comes from CSV exports the user uploads, not from
OCR of the PDFs.

What's left:

- `categories.py` — TX_CATEGORIES + TX_TYPES used by the existing
  /finance dashboards and the transaction-list filters.
- `csv_import.py` — Sparkasse-CSV importer (Giro + Tagesgeld
  exports). Dedup via SHA256(iban|date|amount|purpose|sammlerref).
- `dates.py` — date / IBAN normalisation helpers.
- `reset.py` — drop-everything migration for the one-time v0.33.0
  hard reset and the user-facing "wipe finance data" button.
"""

from .categories import (
    TX_CATEGORIES,
    TX_TYPES,
    BANK_NAMES,
)

__all__ = [
    "TX_CATEGORIES",
    "TX_TYPES",
    "BANK_NAMES",
]
