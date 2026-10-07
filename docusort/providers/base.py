"""Provider base class + shared response container."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class ProviderError(RuntimeError):
    """Raised when a provider call fails (network, auth, malformed response)."""


class TransientProviderError(ProviderError):
    """A provider failure that is expected to resolve on its own — the
    remote is temporarily unavailable rather than the request being bad.

    Examples: the local AI bridge has no client connected (Mac asleep),
    a bridge call timed out, or the client disconnected mid-flight. The
    pipeline treats these specially: instead of consuming the document
    into the review queue (which strands it there until a human retries),
    it leaves the file in the inbox so the next watcher pass reprocesses
    it once the backend is back.
    """


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

    def max_context(self, model: str = "") -> int:
        """How many tokens this model can take in total — 0 when unknown.

        🔴 THE MODEL NAME IS A PARAMETER, not something the provider is
        assumed to remember. It used to be read from whatever the last
        `classify` call had stored — but the caller asks this BEFORE it
        classifies, in order to decide how much text to send. On the first
        document after every restart nothing was stored yet, the answer was
        "unknown", and the limit silently fell back to the old default.

        🔑 Only a provider that can ASK its model answers this. A hosted
        service bills per token, so there the limit that matters is the
        user's wallet, not the architecture; local engines charge nothing
        and the only real ceiling is the model itself. DocuSort uses this
        to send as much of a document as physically fits instead of a fixed
        number that was chosen once and then never matched any model.
        """
        return 0

    def classify(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        max_output_tokens: int = 600,
        timeout: float | None = None,
    ) -> ProviderResponse:
        """Run a single LLM call.

        `timeout` is a per-request timeout in seconds. None falls back
        to whatever the provider was constructed with — useful default
        for the classifier's small responses, but second-pass
        extractors that ask for many transactions in one go need a
        much larger budget than the default 60 s.
        """
        raise NotImplementedError

    def runtime(self) -> dict[str, Any]:
        """Where this provider's model runs, and what it is using there.

        The start page shows load figures, and those are only meaningful
        with a machine attached to them: DocuSort reads /proc and that is
        *its own* host. The model may live somewhere else entirely — on a
        Mac across the bridge, on another box on the LAN, or in a data
        centre nobody here can measure. Only the provider knows which,
        so the answer belongs here and travels with it.

        Keys (all optional, absent means "cannot say"):
          where     'local' | 'lan' | 'bridge' | 'cloud'
          host      the machine's name as far as we can tell
          model     model id actually loaded
          size      bytes the model occupies
          context   usable context window in tokens
          cpu       percent, 0..100, of that machine
          memory    percent, 0..100, of that machine
          reachable bool — False when we asked and got nothing
        """
        return {"where": "unknown", "provider": self.name}
