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


def load_all() -> None:
    for lang in SUPPORTED:
        _load(lang)


def translate(key: str, lang: str = FALLBACK, **kwargs) -> str:
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
