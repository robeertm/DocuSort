"""Google Gemini provider."""

from __future__ import annotations

from .base import Provider, ProviderError, ProviderResponse
from .pricing import calculate_cost


class GeminiProvider(Provider):
    name = "gemini"

    def __init__(self, api_key: str, timeout: int = 60):
        try:
            from google import genai  # google-genai package
            from google.genai import types  # noqa: F401
        except ImportError as exc:
            raise ProviderError(
                "google-genai package not installed (pip install google-genai)"
            ) from exc
        self._genai = genai
        self.client = genai.Client(api_key=api_key)
        self.timeout = timeout

    def classify(self, *, system_prompt, user_prompt, model,
                 max_output_tokens: int = 600) -> ProviderResponse:
        from google.genai import types
        try:
            resp = self.client.models.generate_content(
                model=model,
                contents=user_prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    max_output_tokens=max_output_tokens,
                    response_mime_type="application/json",
                ),
            )
        except Exception as exc:
            raise ProviderError(f"Gemini API call failed: {exc}") from exc

        raw = (resp.text or "").strip()
        usage = getattr(resp, "usage_metadata", None)
        in_tok  = int(getattr(usage, "prompt_token_count", 0) or 0) if usage else 0
        out_tok = int(getattr(usage, "candidates_token_count", 0) or 0) if usage else 0
        cost = calculate_cost("gemini", model, in_tok, out_tok)
        return ProviderResponse(
            raw_text=raw, model=model,
            input_tokens=in_tok, output_tokens=out_tok, cost_usd=cost,
        )
