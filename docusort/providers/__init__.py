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
                   Counts as a LOCAL provider (finance.local_only lets it
                   through) and therefore gets the same generous timeout
                   floor as `bridge`: a local model takes minutes, not
                   seconds.
  - bridge:        a local-AI bridge — every call is forwarded through a
                   WebSocket reverse tunnel to a Mac (or any other host)
                   running the bridge client. Inference happens on that
                   host (Ollama / MLX) and the answer is streamed back to
                   the server. No data leaves the user's home network.
"""

from __future__ import annotations

from dataclasses import dataclass

from .base import (
    Provider, ProviderError, ProviderResponse, TransientProviderError,
)


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
        # 🔴 LOKAL IST LANGSAM — UND DAS GALT BISHER NUR FÜR DIE BRÜCKE.
        # Genau darunter steht seit Langem, warum `bridge` mehr Zeit bekommt:
        # „local inference on a 7B model can take 30–90 s for a long bank
        # statement, and we don't want a clock check to kill a call that's almost
        # done." Dieselbe Begründung gilt Wort für Wort für diesen Anbieter —
        # `openai_compat` IST der Weg zu Ollama, und er bekam die Zeitgrenze der
        # Wolke von 60 s. Gemessen auf einer Synology (AMD Ryzen V1500B, vier
        # Kerne, keine Grafikkarte). 🔴 DER BODEN IST 600, NICHT 180 — und
        # das hat der Pruefstand gefunden, nicht ich. Erst stand hier derselbe
        # Boden wie bei der Bruecke. Gemessen:
        #   qwen2.5:3b-instruct, Kontoauszug 1699 Token, rohe Schnittstelle
        #     144 s
        #   qwen2.5:3b-instruct, Stromrechnung, ECHTER Weg des Programms
        #     186 s   ← sechs Sekunden UEBER einem Boden von 180
        #   qwen2.5:7b-instruct, dasselbe Dokument, rohe Schnittstelle
        #     360 s
        # Der Boden der Bruecke war fuer einen Mac gedacht, der 30–90 s
        # braucht. Hier steht ein Rechner ohne Grafikkarte, und der Prompt des
        # echten Weges ist laenger als ein Testprompt: er traegt alle
        # Kategorien, alle Unterkategorien und die Anweisungen mit. 600 deckt
        # die gemessenen 360 s mit Luft und bleibt weit unter den 3,5 Stunden,
        # vor denen der Kommentar der Bruecke eine Zeile weiter unten warnt.
        #
        # 🔑 Wer eine eigene Zeitgrenze einträgt, behält sie: das `max` hebt nur
        # den Boden an. Wer `openai_compat` gegen Groq oder Mistral benutzt
        # (schnell, in der Wolke), merkt davon nichts — eine Grenze, die nie
        # erreicht wird, kostet nichts.
        return OpenAICompatProvider(
            api_key=api_key or "ollama", base_url=base_url,
            timeout=max(timeout * 3, 600),
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
           "TransientProviderError", "PROVIDERS", "build_provider"]
