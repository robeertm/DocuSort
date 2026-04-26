"""OpenAI GPT provider via Chat Completions."""

from __future__ import annotations

from .base import Provider, ProviderError, ProviderResponse
from .pricing import calculate_cost


class OpenAIProvider(Provider):
    name = "openai"

    def __init__(self, api_key: str, timeout: int = 60):
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ProviderError("openai package not installed") from exc
        self.client = OpenAI(api_key=api_key, timeout=timeout)

    def classify(self, *, system_prompt, user_prompt, model,
                 max_output_tokens: int = 600) -> ProviderResponse:
        try:
            resp = self.client.chat.completions.create(
                model=model,
                max_tokens=max_output_tokens,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except Exception as exc:
            raise ProviderError(f"OpenAI API call failed: {exc}") from exc

        raw = resp.choices[0].message.content or ""
        u = resp.usage
        in_tok  = int(getattr(u, "prompt_tokens", 0) or 0)
        out_tok = int(getattr(u, "completion_tokens", 0) or 0)
        # OpenAI prompt-caching is automatic: cached input tokens show up in
        # prompt_tokens_details.cached_tokens (newer SDK) — we capture them
        # as cache_read so the dashboard can credit savings.
        cache_read = 0
        details = getattr(u, "prompt_tokens_details", None)
        if details is not None:
            cache_read = int(getattr(details, "cached_tokens", 0) or 0)
        cost = calculate_cost(
            "openai", model, in_tok, out_tok, cache_read=cache_read,
        )
        return ProviderResponse(
            raw_text=raw, model=model,
            input_tokens=in_tok, output_tokens=out_tok,
            cache_read_tokens=cache_read, cost_usd=cost,
        )
