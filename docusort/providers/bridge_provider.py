"""Provider that routes every classify call through the local-AI bridge.

When this provider is selected, no LLM call ever touches a cloud
endpoint from the server itself — the request is forwarded over a
WebSocket to a Mac (or any other host) running the bridge client,
which performs the inference locally (Ollama, MLX, …) and ships the
JSON answer back.

Cost is reported as zero because there is no per-token billing on a
locally-hosted model. Token usage is still surfaced so the dashboards
have something useful to show.
"""

from __future__ import annotations

import logging

from .base import Provider, ProviderError, ProviderResponse


logger = logging.getLogger("docusort.providers.bridge")


class BridgeProvider(Provider):
    name = "bridge"

    def __init__(self, *, default_timeout: float = 180.0) -> None:
        # No client to construct — the provider just looks up the
        # singleton hub at call time. Constructing this provider must
        # not require an actual connection (the hub is empty until the
        # Mac client connects), so we do a lazy lookup in classify().
        self.default_timeout = float(default_timeout)

    def classify(self, *, system_prompt: str, user_prompt: str, model: str,
                 max_output_tokens: int = 600,
                 timeout: float | None = None) -> ProviderResponse:
        from .. import activity
        from ..bridge.server import get_bridge

        bridge = get_bridge()
        if not bridge.is_connected():
            raise ProviderError(
                "Local AI bridge is selected but no client is "
                "connected. Start the bridge on your Mac (see "
                "Settings → Local AI Bridge for the one-line install)."
            )

        # Local inference is slow enough that the cloud-tuned per-call
        # timeouts (300 s in finance/extractor.py) are useless here. A
        # 16k-token bank statement on a busy 7B model can take 10 min;
        # on a 14B/32B it can take 30+. We pick a budget at 8 tok/s
        # (worst-case for a cold model + a busy machine) plus a 120 s
        # startup pad, and floor it at 30 min so even small calls get
        # plenty of breathing room. The caller's timeout is treated
        # as a *minimum* — we never go below what they asked for.
        caller_timeout = float(timeout) if timeout else float(self.default_timeout)
        local_budget   = (max_output_tokens / 8.0) + 120.0
        effective      = max(caller_timeout, local_budget, 1800.0)

        activity.begin_call()
        try:
            data = bridge.call(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=model,
                max_output_tokens=max_output_tokens,
                timeout=effective,
            )
        except TimeoutError as exc:
            raise ProviderError(str(exc)) from exc
        except RuntimeError as exc:
            raise ProviderError(str(exc)) from exc
        except Exception as exc:
            raise ProviderError(f"Bridge call failed: {exc}") from exc
        finally:
            activity.end_call()

        raw = str(data.get("raw_text", "") or "")
        if not raw:
            raise ProviderError("Bridge returned an empty response.")

        return ProviderResponse(
            raw_text=raw,
            model=str(data.get("model", model) or model),
            input_tokens=int(data.get("input_tokens", 0) or 0),
            output_tokens=int(data.get("output_tokens", 0) or 0),
            cost_usd=0.0,  # local inference — no per-token billing
        )
