"""Lightweight internationalisation for DocuSort's web UI.

Strings live in `docusort/locales/<lang>.json` as flat key/value objects.
Templates call `{{ t("some.key") }}`, JS code reads translated strings from
a server-rendered `T` object.

The user's preferred language is chosen like this (first match wins):
1. `lang` cookie set via the language switcher
2. `Accept-Language` header (first supported code)
3. `web.default_language` from config.yaml
4. "de" as the final fallback
"""

from __future__ import annotations

import contextvars
import json
import logging
from pathlib import Path

LOCALES_DIR = Path(__file__).parent / "locales"
SUPPORTED: tuple[str, ...] = ("de", "en", "fr", "es", "it")
LANGUAGE_NAMES: dict[str, str] = {
    "de": "Deutsch",
    "en": "English",
    "fr": "Français",
    "es": "Español",
    "it": "Italiano",
}
FALLBACK = "en"  # use English as the key-naming & fallback language

logger = logging.getLogger("docusort.i18n")

_cache: dict[str, dict[str, str]] = {}


def _load(lang: str) -> dict[str, str]:
    if lang in _cache:
        return _cache[lang]
    path = LOCALES_DIR / f"{lang}.json"
    if not path.exists():
        _cache[lang] = {}
        return _cache[lang]
    try:
        _cache[lang] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Could not load locale %s: %s", lang, exc)
        _cache[lang] = {}
    return _cache[lang]


def translate(key: str, lang: str = FALLBACK, /, **kwargs) -> str:
    """Look up a key in the chosen language, fall back to English, then to
    the key itself. Any kwargs are passed to `.format()` for simple
    placeholder substitution ({name}, {count}, …).
    """
    value = _load(lang).get(key)
    if value is None and lang != FALLBACK:
        value = _load(FALLBACK).get(key)
    if value is None:
        return key
    if kwargs:
        try:
            return value.format(**kwargs)
        except (KeyError, IndexError, ValueError):
            return value
    return value


# ───────────────────────── Die Sprache der laufenden Anfrage ────────────────
#
# 🔴 WARUM DAS HIER STEHT: in `web/app.py` standen 172 `HTTPException`
#    mit einer Meldung, von denen VIER uebersetzt waren — und 76 Stellen in
#    den Vorlagen zeigen `detail` dem Benutzer direkt an. In einer
#    italienischen Installation stand dort also Deutsch oder Englisch.
#
# 🔑 Der Grund dafuer war technisch, nicht nachlaessig: `translate()`
#    braucht die Sprache, und die steckt in der Anfrage. Viele Handler haben
#    kein `request` in der Hand — also haette jede Fehlermeldung eine neue
#    Signatur gebraucht. Mit einer Kontextvariablen, die die Middleware
#    einmal je Anfrage setzt, braucht sie das nicht. `contextvars` ist dabei
#    das Richtige und nicht `threading.local`: die Handler laufen in einer
#    Ereignisschleife, und ein ContextVar gilt je Aufgabe statt je Faden.

_anfrage_sprache: contextvars.ContextVar[str] = contextvars.ContextVar(
    "ds_anfrage_sprache", default=FALLBACK)


def setze_anfrage_sprache(lang: str) -> object:
    """Die Sprache dieser Anfrage festhalten. Gibt das Marke zum Zuruecksetzen."""
    return _anfrage_sprache.set(lang if lang in SUPPORTED else FALLBACK)


def anfrage_sprache() -> str:
    """Die Sprache der laufenden Anfrage — oder die Rueckfallsprache."""
    try:
        return _anfrage_sprache.get()
    except LookupError:          # ausserhalb einer Anfrage, etwa im Prueflauf
        return FALLBACK


def uebersetze_jetzt(schluessel: str, /, **kwargs) -> str:
    """Wie `translate`, aber in der Sprache der laufenden Anfrage.

    🔴 BEIDE PARAMETER SIND NUR POSITIONELL (`/`) — und das ist kein
    Schoenheitswunsch. Ein Platzhalter darf heissen, wie das Feld heisst,
    das er zeigt, und ein Rechenort heisst in diesem Programm `key`. Hiess
    der Parameter hier auch `key`, war `uebersetze_jetzt("err.target_unknown",
    key=key)` ein Namenskonflikt statt einer Meldung:
    *got multiple values for argument 'key'*. Mit `/` landet jedes `key=`
    im Platzhalter-Woerterbuch, wo es hingehoert.
    """
    return translate(schluessel, anfrage_sprache(), **kwargs)


def category_label(name: str, lang: str = FALLBACK) -> str:
    """Localised label for a top-level category. Falls back to the canonical
    German name when the translation is missing — that way custom categories
    added via categories.yaml still render readably."""
    if not name:
        return ""
    return translate(f"cat.{name}", lang) if (
        f"cat.{name}" in _load(lang) or f"cat.{name}" in _load(FALLBACK)
    ) else name


def subcategory_label(parent: str, name: str, lang: str = FALLBACK) -> str:
    """Localised label for a subcategory under a given parent."""
    if not name:
        return ""
    key = f"sub.{parent}.{name}"
    return translate(key, lang) if (
        key in _load(lang) or key in _load(FALLBACK)
    ) else name


def detect_language(
    *,
    cookie: str | None = None,
    accept_language: str | None = None,
    default: str = "de",
) -> str:
    if cookie and cookie in SUPPORTED:
        return cookie
    if accept_language:
        for chunk in accept_language.split(","):
            code = chunk.split(";")[0].strip().split("-")[0].lower()
            if code in SUPPORTED:
                return code
    return default if default in SUPPORTED else FALLBACK


def all_translations_for_js(lang: str) -> dict[str, str]:
    """Merge English with the requested language so JS-side lookups always
    resolve. Keys present in `lang` win over English."""
    merged = dict(_load(FALLBACK))
    merged.update(_load(lang))
    return merged
