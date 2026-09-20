"""Cross-provider pricing table.

Each entry is `(input_per_mtok, output_per_mtok)` in USD. Model lookups use
prefix matching so dated suffixes (`claude-haiku-4-5-20251001`) resolve to
the same row as the family (`claude-haiku-4-5`).

Local providers (Ollama, llama.cpp) intentionally have no entries — unknown
model → cost 0, which is correct for self-hosted inference.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("docusort.pricing")


# Anthropic — public list pricing as of 2026.
_ANTHROPIC: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5":  (1.0,  5.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-opus-4-7":  (15.0, 75.0),
    # Older families kept so historical rows still cost-out cleanly.
    "claude-3-5-haiku":  (0.80, 4.0),
    "claude-3-5-sonnet": (3.0, 15.0),
}

# Anthropic-specific cache multipliers (5-minute ephemeral cache).
ANTHROPIC_CACHE_WRITE_MULTIPLIER = 1.25
ANTHROPIC_CACHE_READ_MULTIPLIER  = 0.10

# OpenAI — chat-completion pricing per 1M tokens.
_OPENAI: dict[str, tuple[float, float]] = {
    "gpt-4o-mini":  (0.15, 0.60),
    "gpt-4o":       (2.50, 10.0),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1":      (2.0,  8.0),
    "o4-mini":      (1.10, 4.40),
}

# OpenAI prompt-caching: cached input tokens billed at 50% of normal rate.
OPENAI_CACHE_READ_MULTIPLIER = 0.50

# Google Gemini — pricing for text models.
_GEMINI: dict[str, tuple[float, float]] = {
    "gemini-1.5-flash": (0.075, 0.30),
    "gemini-1.5-pro":   (1.25,  5.0),
    "gemini-2.0-flash": (0.10,  0.40),
    "gemini-2.5-flash": (0.075, 0.30),
    "gemini-2.5-pro":   (1.25,  5.0),
}


_PROVIDER_TABLES: dict[str, dict[str, tuple[float, float]]] = {
    "anthropic": _ANTHROPIC,
    "openai":    _OPENAI,
    "gemini":    _GEMINI,
}


def lookup_pricing(provider: str, model: str) -> tuple[float, float] | None:
    """Return `(input, output)` USD/MTok for the given provider+model, or
    None when the provider is local / unknown."""
    table = _PROVIDER_TABLES.get(provider)
    if not table:
        return None
    for prefix, prices in table.items():
        if model.startswith(prefix):
            return prices
    return None


def calculate_cost(
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    *,
    cache_write: int = 0,
    cache_read: int = 0,
) -> float:
    """USD cost for a single call. Returns 0 for local/unknown models."""
    prices = lookup_pricing(provider, model)
    if prices is None:
        if provider not in ("openai_compat",):
            logger.warning("Unknown model %s for provider %s — cost recorded as 0",
                           model, provider)
        return 0.0
    in_price, out_price = prices
    total = input_tokens * in_price + output_tokens * out_price
    if provider == "anthropic":
        total += cache_write * in_price * ANTHROPIC_CACHE_WRITE_MULTIPLIER
        total += cache_read  * in_price * ANTHROPIC_CACHE_READ_MULTIPLIER
    elif provider == "openai":
        total += cache_read * in_price * OPENAI_CACHE_READ_MULTIPLIER
    return total / 1_000_000


def all_pricing() -> dict[str, dict[str, tuple[float, float]]]:
    """Return the full pricing table — used by the /api/pricing endpoint."""
    return {
        provider: dict(table) for provider, table in _PROVIDER_TABLES.items()
    }
