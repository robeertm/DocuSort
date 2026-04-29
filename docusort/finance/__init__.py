"""Bank statement (Kontoauszug) extraction + pseudonymisation.

When a document is classified as `category = "Kontoauszug"`, this package
runs a second extraction pass — the same pattern as `docusort.receipts`
— but with a twist: before the OCR text is sent to a cloud LLM, IBANs
and other directly-identifying fragments are replaced with stable tokens
(`IBAN_001`, `EMAIL_002`, `ADDR_003`). The LLM sees enough context to
extract counterparty names, purposes, amounts and categories, but never
sees real account numbers belonging to the user.

Tokens are deterministic within a single statement so the LLM can refer
to "the user's account (IBAN_001)" vs "the counterparty's account
(IBAN_002)" reliably. After the response comes back, we walk the JSON
and restore real values from the reverse map.

Users who want zero cloud exposure can flip the
`finance.local_only` setting and the pipeline will hard-route bank
statements to a local provider (Ollama / openai-compat) instead.
"""

from .pseudonymizer import Pseudonymizer, pseudonymize_for_cloud
from .categories import (
    TX_CATEGORIES,
    TX_TYPES,
    BANK_NAMES,
)
from .extractor import (
    StatementExtractor,
    Statement,
    Transaction,
)

__all__ = [
    "Pseudonymizer",
    "pseudonymize_for_cloud",
    "StatementExtractor",
    "Statement",
    "Transaction",
    "TX_CATEGORIES",
    "TX_TYPES",
    "BANK_NAMES",
]
