"""Pluggable AI providers for document classification.

Each provider takes the same input — a system prompt, a user prompt, model
name, max output tokens, timeout — and returns a `ProviderResponse` with the
raw JSON text plus token-usage stats so we can price the call.

The `Classifier` in classifier.py picks one of these at construction time
based on `settings.ai.provider` and otherwise stays provider-agnostic.

Supported providers:
  - anthropic:     Claude models with prompt caching (cheapest at scale).
  - openai:        GPT-4o, GPT-4o-mini, etc.
  - gemini:        Google's Gemini Flash / Pro family.
  - openai_compat: any endpoint that speaks the OpenAI Chat Completions API
                   — Ollama (local), Groq, xAI, Mistral, Together, …
  - bridge:        a local-AI bridge — every call is forwarded through a
                   WebSocket reverse tunnel to a Mac (or any other host)
                   running the bridge client. Inference happens on that
                   host (Ollama / MLX) and the answer is streamed back to
                   the server. No data leaves the user's home network.
"""

from __future__ import annotations

from dataclasses import dataclass

from .base import Provider, ProviderError, ProviderResponse


PROVIDERS = ("anthropic", "openai", "gemini", "openai_compat", "bridge")


def build_provider(name: str, *, api_key: str, base_url: str = "",
                   timeout: int = 60) -> Provider:
    """Factory — instantiates the right provider class. Imports are lazy so a
    user who only uses Anthropic doesn't need the openai/google-genai packages
    installed at all."""
    name = (name or "").strip().lower()
    if name == "anthropic":
        from .anthropic_provider import AnthropicProvider
        return AnthropicProvider(api_key=api_key, timeout=timeout)
    if name == "openai":
        from .openai_provider import OpenAIProvider
        return OpenAIProvider(api_key=api_key, timeout=timeout)
    if name == "gemini":
        from .gemini_provider import GeminiProvider
        return GeminiProvider(api_key=api_key, timeout=timeout)
    if name == "openai_compat":
        from .openai_compat import OpenAICompatProvider
        return OpenAICompatProvider(
            api_key=api_key or "ollama", base_url=base_url, timeout=timeout,
        )
    if name == "bridge":
        from .bridge_provider import BridgeProvider
        # The bridge needs a longer default timeout than cloud APIs —
        # local inference on a 7B model can take 30–90 s for a long
        # bank statement, and we don't want a clock check to kill a
        # call that's almost done.
        return BridgeProvider(default_timeout=max(timeout * 3, 180))
    raise ValueError(f"Unknown AI provider: {name!r}. Pick one of {PROVIDERS}")


__all__ = ["Provider", "ProviderError", "ProviderResponse",
           "PROVIDERS", "build_provider"]
