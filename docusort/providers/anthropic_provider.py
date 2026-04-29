"""Anthropic Claude provider with prompt caching."""

from __future__ import annotations

import logging
import time

from .base import Provider, ProviderError, ProviderResponse
from .pricing import calculate_cost


logger = logging.getLogger("docusort.providers.anthropic")


# Tier-1 input-tokens-per-minute is 50k by default. A burst (e.g. backfill
# of 20 statements at once) can push past that and Anthropic returns 429
# with a `retry-after` header indicating how long to wait. We respect the
# header and retry rather than failing the whole pipeline.
_MAX_RATE_LIMIT_RETRIES = 4
_DEFAULT_RETRY_AFTER_S  = 30.0


class AnthropicProvider(Provider):
    name = "anthropic"

    def __init__(self, api_key: str, timeout: int = 60):
        try:
            from anthropic import Anthropic
        except ImportError as exc:
            raise ProviderError("anthropic package not installed") from exc
        self.client = Anthropic(api_key=api_key, timeout=timeout)

    def classify(self, *, system_prompt, user_prompt, model,
                 max_output_tokens: int = 600,
                 timeout: float | None = None) -> ProviderResponse:
        # Late import so the type can be referenced even when handling errors.
        from anthropic import RateLimitError, APIStatusError

        # Per-request timeout overrides the client default. Long
        # extractions (many transactions, big output budget) push past
        # 60 s easily; the default stays small for cheap classifier
        # calls. Anthropic also recommends streaming for very long
        # responses — we use plain create() because the bumped timeout
        # is enough for current statement sizes.
        if timeout is not None:
            client = self.client.with_options(timeout=timeout)
        else:
            client = self.client

        attempt = 0
        while True:
            try:
                resp = client.messages.create(
                    model=model,
                    max_tokens=max_output_tokens,
                    system=[{
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": user_prompt}],
                )
                break
            except (RateLimitError, APIStatusError) as exc:
                # APIStatusError covers 429 too; prefer the structured
                # status_code over fragile string matching.
                status = getattr(exc, "status_code", None)
                msg = str(exc)
                # Some 400 errors are actually account-level spending caps
                # ("You have reached your specified API usage limits.
                # You will regain access on YYYY-MM-DD"). Retrying won't
                # help — surface the date so the user can plan.
                if "specified API usage limits" in msg or "regain access" in msg:
                    raise ProviderError(
                        "Anthropic spending limit reached. Check your "
                        "API console (Settings → Limits) — the cap "
                        "resets on the date Anthropic gave in the "
                        "error message.  Original error: " + msg
                    ) from exc
                if not isinstance(exc, RateLimitError) and status != 429:
                    raise ProviderError(f"Anthropic API call failed: {exc}") from exc
                if attempt >= _MAX_RATE_LIMIT_RETRIES:
                    raise ProviderError(
                        f"Anthropic rate-limited after {attempt} retries: {exc}"
                    ) from exc
                # Honour the retry-after header when present (seconds or
                # HTTP-date). Anthropic typically returns seconds.
                retry_after = _DEFAULT_RETRY_AFTER_S
                response_headers = getattr(getattr(exc, "response", None), "headers", {}) or {}
                ra_raw = response_headers.get("retry-after")
                if ra_raw:
                    try:
                        retry_after = float(ra_raw)
                    except (TypeError, ValueError):
                        pass
                # Exponential backoff floor so a short server-side hint
                # doesn't put us in a tight retry loop.
                wait_s = max(retry_after, 5.0 * (2 ** attempt))
                attempt += 1
                logger.warning(
                    "Anthropic 429 (attempt %d/%d). Sleeping %.1fs (retry-after=%s).",
                    attempt, _MAX_RATE_LIMIT_RETRIES, wait_s, ra_raw or "—",
                )
                time.sleep(wait_s)
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
