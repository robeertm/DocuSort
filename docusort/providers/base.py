"""Provider base class + shared response container."""

from __future__ import annotations

from dataclasses import dataclass


class ProviderError(RuntimeError):
    """Raised when a provider call fails (network, auth, malformed response)."""


@dataclass
class ProviderResponse:
    """Normalised result from any provider's classify call."""
    raw_text: str               # the model's JSON reply (verbatim)
    model: str                  # model id actually used
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_tokens: int = 0   # only Anthropic
    cache_read_tokens: int = 0       # only Anthropic
    cost_usd: float = 0.0


class Provider:
    """Abstract provider — concrete subclasses implement `classify`."""

    name: str = "abstract"

    def classify(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        max_output_tokens: int = 600,
    ) -> ProviderResponse:
        raise NotImplementedError
