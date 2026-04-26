"""OpenAI-Chat-Completions-compatible provider.

Covers Ollama (local), Groq, xAI, Mistral, Together, OpenRouter and any other
service that exposes the OpenAI Chat Completions REST shape. The user picks
this provider and supplies a `base_url`; for Ollama that's typically
`http://localhost:11434/v1`.

For local providers (Ollama) the cost is 0 because there is no per-token
charge — we still record token counts so the UI can display them.
"""

from __future__ import annotations

import json
from typing import Any
from urllib import error, request

from .base import Provider, ProviderError, ProviderResponse
from .pricing import calculate_cost


class OpenAICompatProvider(Provider):
    name = "openai_compat"

    def __init__(self, api_key: str, base_url: str, timeout: int = 60):
        if not base_url:
            raise ProviderError(
                "openai_compat requires base_url (e.g. http://localhost:11434/v1)"
            )
        # Normalise: strip trailing slash, ensure /v1 if user gave the bare host
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key or "ollama"
        self.timeout = timeout

    def classify(self, *, system_prompt, user_prompt, model,
                 max_output_tokens: int = 600) -> ProviderResponse:
        body: dict[str, Any] = {
            "model": model,
            "max_tokens": max_output_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            # Most modern engines honour json_object; Ollama needs format=json
            # in its native API but accepts json_object on /v1 too.
            "response_format": {"type": "json_object"},
        }
        req = request.Request(
            f"{self.base_url}/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:400]
            raise ProviderError(
                f"openai_compat HTTP {exc.code} from {self.base_url}: {detail}"
            ) from exc
        except (error.URLError, TimeoutError) as exc:
            raise ProviderError(
                f"openai_compat could not reach {self.base_url}: {exc}"
            ) from exc

        try:
            choice = payload["choices"][0]
            raw = choice["message"]["content"] or ""
        except (KeyError, IndexError, TypeError) as exc:
            raise ProviderError(
                f"openai_compat returned malformed response: {payload}"
            ) from exc

        usage = payload.get("usage") or {}
        in_tok  = int(usage.get("prompt_tokens", 0) or 0)
        out_tok = int(usage.get("completion_tokens", 0) or 0)
        # Local engines (Ollama, llama.cpp) cost nothing per token; pricing
        # table just returns 0 for unknown models, which is the desired
        # behaviour here.
        cost = calculate_cost("openai_compat", model, in_tok, out_tok)
        return ProviderResponse(
            raw_text=raw, model=model,
            input_tokens=in_tok, output_tokens=out_tok, cost_usd=cost,
        )
