"""Anthropic Claude provider with prompt caching."""

from __future__ import annotations

from .base import Provider, ProviderError, ProviderResponse
from .pricing import calculate_cost


class AnthropicProvider(Provider):
    name = "anthropic"

    def __init__(self, api_key: str, timeout: int = 60):
        try:
            from anthropic import Anthropic
        except ImportError as exc:
            raise ProviderError("anthropic package not installed") from exc
        self.client = Anthropic(api_key=api_key, timeout=timeout)

    def classify(self, *, system_prompt, user_prompt, model,
                 max_output_tokens: int = 600) -> ProviderResponse:
        try:
            resp = self.client.messages.create(
                model=model,
                max_tokens=max_output_tokens,
                system=[{
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_prompt}],
            )
        except Exception as exc:
            raise ProviderError(f"Anthropic API call failed: {exc}") from exc

        raw = "".join(
            b.text for b in resp.content if getattr(b, "type", "") == "text"
        )
        u = resp.usage
        in_tok    = int(getattr(u, "input_tokens", 0) or 0)
        out_tok   = int(getattr(u, "output_tokens", 0) or 0)
        c_write   = int(getattr(u, "cache_creation_input_tokens", 0) or 0)
        c_read    = int(getattr(u, "cache_read_input_tokens", 0) or 0)
        cost = calculate_cost(
            "anthropic", model, in_tok, out_tok,
            cache_write=c_write, cache_read=c_read,
        )
        return ProviderResponse(
            raw_text=raw, model=model,
            input_tokens=in_tok, output_tokens=out_tok,
            cache_creation_tokens=c_write, cache_read_tokens=c_read,
            cost_usd=cost,
        )
