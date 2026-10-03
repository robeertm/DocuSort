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
        # Normalise: strip the trailing slash, and add `/v1` when the bare host
        # was given.
        #
        # 🔴 DIESE ZEILE FEHLTE, DER KOMMENTAR STAND SCHON DA. Darüber stand
        # „ensure /v1 if user gave the bare host" — getan wurde es nie. Wer
        # `http://localhost:11434` eintrug (genau das, was Ollamas eigene
        # Anleitung zeigt), schickte seine Anfrage an `…:11434/chat/completions`
        # und bekam 404. Die Einstellungsseite rettete das zur Hälfte: der
        # automatische Weg („lokales Modell suchen") hängt `/v1` an, das Feld von
        # Hand nicht. Zwei Wege, einer heil — und die Begründung, warum es nicht
        # passieren kann, stand als Kommentar daneben.
        self.base_url = base_url.rstrip("/")
        # Alles, was mit einem Pfad endet, bleibt unangetastet: `/v1` ist nur die
        # Schreibweise von Ollama und OpenAI. Groq, Mistral und Together haben
        # ihre eigene, und die darf nicht überschrieben werden.
        from urllib.parse import urlsplit
        if not urlsplit(self.base_url).path.strip("/"):
            self.base_url += "/v1"
        self.api_key = api_key or "ollama"
        self.timeout = timeout

    def runtime(self) -> dict[str, Any]:
        """Ask the endpoint what it has loaded.

        Ollama answers /api/ps with the running model, its size and its
        context window. Other OpenAI-compatible servers simply 404 — then
        we still know the HOST, which is the part the page needs most: a
        CPU figure means nothing until you know whose CPU it is.
        """
        from urllib.parse import urlparse
        wurzel = (self.base_url or "").rstrip("/")
        if wurzel.endswith("/v1"):
            wurzel = wurzel[:-3]
        host = urlparse(wurzel).hostname or ""
        lokal = host in ("127.0.0.1", "localhost", "::1", "")
        info: dict[str, Any] = {
            "where": "local" if lokal else "lan",
            "provider": self.name,
            "host": host or "localhost",
        }
        try:
            req = request.Request(wurzel.rstrip("/") + "/api/ps")
            with request.urlopen(req, timeout=3) as r:
                d = json.loads(r.read().decode("utf-8"))
        except Exception:  # noqa: BLE001 — an endpoint that cannot say is not an error
            info["reachable"] = False
            return info
        info["reachable"] = True
        modelle = d.get("models") or []
        if not modelle:
            info["loaded"] = False
            return info
        m = modelle[0]
        det = m.get("details") or {}
        info.update({
            "loaded": True,
            "model": m.get("name") or m.get("model"),
            "size": m.get("size"),
            "context": m.get("context_length"),
            "parameters": det.get("parameter_size"),
            "quantisation": det.get("quantization_level"),
        })
        return info

    def classify(self, *, system_prompt, user_prompt, model,
                 max_output_tokens: int = 600,
                 timeout: float | None = None) -> ProviderResponse:
        # urllib's urlopen takes a per-call timeout — use the override
        # when given (long extractions on a local Ollama can take
        # several minutes).
        request_timeout = timeout if timeout is not None else self.timeout
        # 🔑 0 (oder weniger) HEISST: KEINE ZEITGRENZE — rechnen lassen.
        #    Bei einem lokalen Modell kostet die Zeit nichts ausser Zeit, und
        #    ein Abbruch kurz vor der Antwort wirft die GANZE Rechenzeit weg.
        #    Gemessen auf einer Synology ohne Grafikkarte: ein Dokument lief
        #    ueber zwoelf Minuten, und das war kein Fehler, sondern die
        #    Geschwindigkeit dieser Maschine.
        #
        # 🔴 Das betrifft NUR das Warten auf die Antwort. Ist der Rechner gar
        #    nicht da, scheitert schon der VERBINDUNGSAUFBAU, und der hat
        #    seine eigene, kurze Grenze im Betriebssystem — es haengt also
        #    nicht ewig an einem Rechner, den es nicht gibt.
        if request_timeout is not None and request_timeout <= 0:
            request_timeout = None
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
            with request.urlopen(req, timeout=request_timeout) as resp:
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
