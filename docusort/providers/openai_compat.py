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
        # Einmal gefragt, dann gemerkt: ist das hinten ein Ollama?
        self._ist_ollama: bool | None = None
        self._max_ctx: int | None = None
        # Welches Modell zuletzt gefragt wurde — `max_context()` braucht einen
        # Namen, und `classify` kennt ihn erst beim Aufruf.
        self._modell: str = ""

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
        # 🔑 RECHNET DIE GRAFIKKARTE ODER DER HAUPTPROZESSOR?
        #
        # Das ist die eine Zahl, die den Unterschied zwischen zehn Sekunden
        # und vierzehn Minuten erklaert, und Ollama legt sie offen: `size_vram`
        # ist der Anteil des Modells, der im Grafikspeicher liegt. Auf einem
        # Mac mit Metal ist er gleich `size` — alles auf der GPU. Auf einem
        # Server ohne Grafikkarte ist er 0.
        #
        # 🔴 Von AUSSEN ist das sonst nicht zu erfahren. DocuSort laeuft nicht
        #    auf jener Maschine und soll dort auch nichts installieren muessen
        #    („niemand soll was installieren muessen"). CPU und RAM des fremden
        #    Rechners bleiben deshalb unsichtbar — aber WOMIT dort gerechnet
        #    wird, sagt Ollama selbst, und das ist die nuetzlichere Haelfte.
        groesse = m.get("size") or 0
        vram = m.get("size_vram")
        if vram is not None and groesse:
            anteil = max(0.0, min(1.0, float(vram) / float(groesse)))
            info["vram"] = int(vram)
            info["gpu_anteil"] = round(anteil, 3)
            info["rechenwerk"] = ("gpu" if anteil >= 0.99
                                  else "cpu" if anteil <= 0.01
                                  else "gemischt")
        return info

    def _wurzel(self) -> str:
        """Die Adresse OHNE `/v1` — dort liegt Ollamas eigene Schnittstelle."""
        w = (self.base_url or "").rstrip("/")
        return w[:-3].rstrip("/") if w.endswith("/v1") else w

    def _nebenfrage_timeout(self):
        """Zeitgrenze fuer die kleinen Nebenfragen (`/api/version`, `/api/show`).

        🔴 SIE FOLGT DERSELBEN REGEL WIE DIE EINORDNUNG: `timeout_seconds = 0`
        heisst keine Zeitgrenze. Ich hatte hier zuerst feste 3 und 10 Sekunden
        eingebaut — und damit eine Falle gestellt, die genau den Fehler
        zurueckholt, den dieses Modul behebt: ist der Rechner einen Moment
        beschaeftigt, laeuft die Frage ab, DocuSort haelt ihn fuer „kein
        Ollama" und faellt auf den Weg zurueck, der bei 2048 Token
        abschneidet. Still.

        🔑 Dass es trotzdem nicht ewig haengt, besorgt das Betriebssystem:
        ist der Rechner gar nicht da, scheitert schon der VERBINDUNGSAUFBAU,
        und der hat seine eigene, kurze Grenze.
        """
        return None if (self.timeout or 0) <= 0 else self.timeout

    def ist_ollama(self) -> bool:
        """Antwortet hinten ein Ollama? Gefragt, bis es einmal geklappt hat.

        🔑 Das entscheidet, ob DocuSort sagen DARF, wie gross der Kontext
        sein soll. Ueber die OpenAI-Schnittstelle kann es das nicht: gemessen
        am 07.10.2026 wird `num_ctx` dort in JEDER Schreibweise verworfen —
        als eigenes Feld, in `options`, egal wie. Ollamas eigene
        Schnittstelle nimmt es an.
        """
        if self._ist_ollama:
            return True
        try:
            req = request.Request(self._wurzel() + "/api/version")
            with request.urlopen(req, timeout=self._nebenfrage_timeout()) as r:
                self._ist_ollama = bool(json.loads(r.read().decode()).get("version"))
        except Exception:  # noqa: BLE001
            # 🔴 EIN NEIN WIRD NICHT GEMERKT. Vorher stand hier `= False`, und
            #    damit haette ein einziger Aussetzer die Behebung fuer die
            #    ganze Laufzeit des Prozesses abgeschaltet — jedes weitere
            #    Dokument waere wieder bei 2048 Token abgeschnitten worden,
            #    ohne dass jemals wieder nachgefragt wird. Ein Ja gilt, ein
            #    Nein wird beim naechsten Mal neu geprueft.
            return False
        return bool(self._ist_ollama)

    # 🔴 GEMESSEN, NICHT GESCHAETZT. Ein echter Lauf am 07.10.2026:
    #    32 413 Zeichen Prompt ergaben 10 309 Token, also 3,14 Zeichen je
    #    Token (Deutsch mit Fachbegriffen). Mit 2,5 wird nach oben gerundet —
    #    ein zu grosser Kontext kostet Arbeitsspeicher, ein zu kleiner wirft
    #    die Anweisungen weg, und nur das zweite ist ein Fehler.
    ZEICHEN_JE_TOKEN = 2.5

    @classmethod
    def kontext_fuer(cls, zeichen: int, max_output_tokens: int = 600) -> int:
        """Wie gross muss der Kontext sein, damit ALLES hineinpasst?

        🔴 WARUM DAS UEBERHAUPT GERECHNET WIRD. Ohne Angabe schneidet Ollama
        bei 2048 Token ab — gemessen: von 10 309 Token eines Prompts wurden
        2 050 ausgewertet, 80 % fielen weg. Und zwar STILL: die Antwort kommt,
        sie ist nur schlechter. Am echten Archiv sichtbar als Gefaelle mit der
        Dokumentgroesse — Zuversicht 0,87 unter 2 000 Zeichen gegen 0,67 ueber
        10 000, und 29 Dokumente, auf die das Modell mit einem voellig anderen
        JSON-Schema antwortete, weil die Anweisung dazu abgeschnitten war.

        🔑 Gerechnet wird aus dem, was WIRKLICH geschickt wird, plus Platz
        fuer die Antwort und eine Reserve. Nicht gedeckelt auf einen Wert, der
        „reichen sollte" — der Prompt bestimmt die Groesse, nicht umgekehrt.
        """
        gebraucht = int(zeichen / cls.ZEICHEN_JE_TOKEN) + int(max_output_tokens) + 512
        # Auf das naechste Vielfache von 2048 aufrunden: Ollama rechnet den
        # Zwischenspeicher in Bloecken, und krumme Werte bringen nichts.
        schritt = 2048
        n = ((gebraucht + schritt - 1) // schritt) * schritt
        return max(schritt, n)

    def max_context(self, model: str = "") -> int:
        """Wieviel Token fasst dieses Modell ueberhaupt? 0, wenn unbekannt.

        🔴 DER MODELLNAME KOMMT MIT. Vorher las diese Stelle `self._modell`,
        das erst `classify()` setzt — gefragt wird aber VORHER, naemlich um
        zu entscheiden, wieviel Text ueberhaupt mitgeschickt wird. Beim
        ersten Dokument nach jedem Neustart stand dort nichts, die Antwort
        war „unbekannt", und die Grenze fiel still auf die alte Vorgabe
        zurueck. Gefunden beim Nachmessen an der laufenden Installation:
        „PLATZ jetzt: 12000" statt 66 630.

        🔑 Ollama sagt es selbst: `/api/show` liefert in `model_info` einen
        Schluessel `<architektur>.context_length` — fuer qwen2.5:7b sind das
        32 768. Das steht in den Gewichten des Modells, nicht in einer
        Einstellung, und es ist die einzige Zahl, die wirklich eine Grenze
        ist. Einmal gefragt, dann gemerkt.
        """
        name = model or self._modell
        if name and name != self._modell:
            self._modell, self._max_ctx = name, None
        if self._max_ctx is not None:
            return self._max_ctx
        self._max_ctx = 0
        if self.ist_ollama() and self._modell:
            try:
                req = request.Request(
                    self._wurzel() + "/api/show",
                    data=json.dumps({"model": self._modell}).encode(),
                    headers={"Content-Type": "application/json"}, method="POST")
                with request.urlopen(req, timeout=self._nebenfrage_timeout()) as r:
                    info = (json.loads(r.read().decode()).get("model_info") or {})
                for k, v in info.items():
                    if k.endswith(".context_length") and isinstance(v, int) and v > 0:
                        self._max_ctx = int(v)
                        break
            except Exception:  # noqa: BLE001
                # 🔴 Auch hier nichts Negatives merken: `None` statt `0`, dann
                #    wird beim naechsten Dokument neu gefragt. Sonst haette ein
                #    Aussetzer den Textplatz dauerhaft auf die alte Vorgabe
                #    gesetzt.
                self._max_ctx = None
                return 0
        return self._max_ctx or 0

    def classify(self, *, system_prompt, user_prompt, model,
                 max_output_tokens: int = 600,
                 timeout: float | None = None) -> ProviderResponse:
        # urllib's urlopen takes a per-call timeout — use the override
        # when given (long extractions on a local Ollama can take
        # several minutes).
        request_timeout = timeout if timeout is not None else self.timeout
        if model and model != self._modell:
            self._modell, self._max_ctx = model, None
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
        # 🔑 OLLAMA BEKOMMT SEINEN EIGENEN WEG — und zwar nur deshalb, weil
        #    die OpenAI-Schnittstelle `num_ctx` verwirft und damit bei 2048
        #    Token abschneidet. Alles andere bleibt, wie es war.
        if self.ist_ollama():
            return self._ollama_classify(
                system_prompt=system_prompt, user_prompt=user_prompt, model=model,
                max_output_tokens=max_output_tokens, request_timeout=request_timeout,
            )

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


    def _ollama_classify(self, *, system_prompt: str, user_prompt: str, model: str,
                         max_output_tokens: int, request_timeout) -> ProviderResponse:
        """Ueber Ollamas EIGENE Schnittstelle — die einzige, die `num_ctx` annimmt.

        🔴 WAS HIER BEHOBEN WIRD. Ueber `/v1/chat/completions` wertete Ollama
        von einem 10 309 Token langen Prompt genau 2 050 aus. Nicht wegen
        Zwischenspeicherung — ein frischer, nicht zwischenspeicherbarer Prompt
        wurde zweimal in Folge ebenso abgeschnitten. Die Anweisung, wie die
        Antwort aussehen soll, steht im vorderen Teil des Systemtextes; was
        dahinter liegt, sah das Modell nie. Gemeldet wurde es als Nachlassen
        der Erkennung gegenueber frueher — und genau das war es, messbar.

        🔑 `num_ctx` kommt aus dem, was wirklich geschickt wird. Rechenzeit
        ist bei einem eigenen Rechner kein Argument gegen ein richtiges
        Ergebnis; ein abgeschnittener Prompt ist eines gegen jedes.
        """
        zeichen = len(system_prompt or "") + len(user_prompt or "")
        num_ctx = self.kontext_fuer(zeichen, max_output_tokens)
        # 🔴 NIE MEHR VERLANGEN, ALS DAS MODELL HAT. Ein `num_ctx` ueber der
        #    Architekturgrenze ist keine Grosszuegigkeit — Ollama kappt es
        #    ohnehin, und wer es nicht kappt, belegt Speicher fuer ein Fenster,
        #    das das Modell nicht benutzen kann. Was dann trotzdem nicht
        #    hineinpasst, steht unten im Protokoll.
        grenze = self.max_context(model)
        if grenze > 0:
            num_ctx = min(num_ctx, grenze)
        body = {
            "model": model,
            "stream": False,
            # Ollamas eigenes Wort fuer „antworte in JSON".
            "format": "json",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "options": {"num_ctx": num_ctx, "num_predict": int(max_output_tokens)},
        }
        req = request.Request(
            self._wurzel() + "/api/chat",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=request_timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:400]
            raise ProviderError(
                f"ollama HTTP {exc.code} from {self._wurzel()}: {detail}"
            ) from exc
        except (error.URLError, TimeoutError) as exc:
            raise ProviderError(
                f"ollama could not be reached at {self._wurzel()}: {exc}"
            ) from exc

        raw = ((payload.get("message") or {}).get("content") or "")
        if not raw:
            raise ProviderError(f"ollama returned no content: {str(payload)[:300]}")
        in_tok = int(payload.get("prompt_eval_count") or 0)
        out_tok = int(payload.get("eval_count") or 0)

        # 🔴 STILL SCHLECHTER WERDEN DARF ES NIE WIEDER. Genau das war der
        #    Fehler: die Antwort kam, sie war nur unbrauchbar, und nichts im
        #    Protokoll sagte warum. Reicht der Kontext fuer den Prompt nicht,
        #    steht das jetzt da — mit beiden Zahlen.
        geschaetzt = int(zeichen / self.ZEICHEN_JE_TOKEN)
        if geschaetzt > num_ctx:
            import logging
            logging.getLogger("docusort.providers").warning(
                "Ollama: der Prompt braucht etwa %d Token, der Kontext fasst %d — "
                "%d Token werden abgeschnitten, und das Modell sieht die Anweisung "
                "zur Antwortform womoeglich nicht.",
                geschaetzt, num_ctx, geschaetzt - num_ctx,
            )

        return ProviderResponse(
            raw_text=raw, model=model,
            input_tokens=in_tok, output_tokens=out_tok,
            cost_usd=calculate_cost("openai_compat", model, in_tok, out_tok),
        )
