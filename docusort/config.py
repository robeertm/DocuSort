"""Configuration loader for DocuSort.

Reads YAML config from /app/config/config.yaml (overridable via
DOCUSORT_CONFIG_DIR environment variable) and categories from categories.yaml.

Secrets (AI API tokens) are read from a separate `secrets.yaml` in the same
directory, which is git-ignored. The secrets file is written by the setup
wizard. As a fallback, the historical environment variable
ANTHROPIC_API_KEY is still respected.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


logger = logging.getLogger("docusort.config")
DEFAULT_CONFIG_DIR = Path(os.environ.get("DOCUSORT_CONFIG_DIR", "/app/config"))


@dataclass
class Paths:
    inbox: Path
    library: Path
    review: Path
    processed: Path
    logs: Path
    db: Path


@dataclass
class OCRSettings:
    enabled: bool = True
    languages: str = "deu+eng"
    skip_if_text: bool = True
    deskew: bool = True
    max_parallel: int = 2  # cap concurrent OCR+Claude jobs to avoid OOM


@dataclass
class AISettings:
    """AI provider configuration. The actual API key lives in secrets.yaml,
    not here, and is not loaded into this dataclass — it's fetched lazily
    via `get_api_key(settings)` so the value never gets logged by accident.
    """
    provider: str = "anthropic"  # anthropic | openai | gemini | openai_compat
    model: str = "claude-haiku-4-5-20251001"
    base_url: str = ""           # only used by openai_compat (Ollama, Groq, ...)
    max_text_chars: int = 12000
    min_confidence: float = 0.65
    timeout_seconds: int = 60


# Backwards-compatible alias — older imports of ClaudeSettings still resolve.
ClaudeSettings = AISettings


@dataclass
class WebSettings:
    host: str = "0.0.0.0"
    port: int = 8080
    default_language: str = "de"
    ssl_cert: str = ""   # path to PEM cert (optional)
    ssl_key: str = ""    # path to PEM key  (optional)


@dataclass
class SyncSettings:
    enabled: bool = False
    target_type: str = "local"       # 'local' | 'rclone'
    local_path: str = ""             # only used when target_type == 'local'
    remote: str = ""                 # only used when target_type == 'rclone' — "name:path"
    source: str = "library"          # 'library' (excl. _Trash) | 'library_and_trash'
    extra_flags: list = field(default_factory=list)
    timeout_seconds: int = 1800      # 30 min default


@dataclass
class FinanceSettings:
    """How bank-statement extraction handles privacy."""
    # When True, statements never go to a cloud LLM. Extraction is
    # routed to the user's local provider (openai_compat / Ollama). If
    # no local provider is configured, statement extraction is skipped
    # rather than silently leaking data.
    local_only: bool = False

    # When False (default), pseudonymisation is OFF — only choose this if
    # local_only is True OR the user has explicitly opted in to plain
    # transmission. The web UI defaults to pseudonymisation ON for cloud
    # providers and exposes the toggle in /settings.
    pseudonymize: bool = True

    # Names of household members whose mention should always be masked
    # before the OCR text reaches a cloud LLM, even if no structured
    # detection pattern picks them up. Useful for documents like
    # Darlehensverträge or Karteninhaber-Schreiben where a partner /
    # child is named only in the body and never in a clean address
    # block. Each entry is treated as a literal, case-insensitive
    # whole-word match — so "Mustermann" masks both "Max Mustermann"
    # and "Erika Mustermann" wherever they appear.
    holder_names: list = field(default_factory=list)

    # When True, every newly classified Kontoauszug is paused before the
    # second-pass LLM extraction. The user can review the pseudonymised
    # OCR text on /finance and either approve it (extract) or skip it
    # (no extraction) — useful for spot-checking the masking on
    # sensitive statements before any byte leaves the box. Default OFF
    # so existing pipelines keep working unchanged.
    review_before_send: bool = False


@dataclass
class AppSettings:
    paths: Paths
    categories: list[dict[str, Any]]
    ocr: OCRSettings
    ai: AISettings
    web: WebSettings = field(default_factory=WebSettings)
    sync: SyncSettings = field(default_factory=SyncSettings)
    finance: FinanceSettings = field(default_factory=FinanceSettings)
    keep_original: bool = True
    filename_template: str = "{date}_{category}_{sender}_{subject}"
    max_filename_length: int = 120
    stable_seconds: int = 5  # wait before processing (file still being written)
    dry_run: bool = False
    config_dir: Path = field(default_factory=lambda: DEFAULT_CONFIG_DIR)

    # Backwards-compat: code that historically referenced `settings.claude`
    # still works because `claude` is an alias for the same AI block.
    @property
    def claude(self) -> AISettings:
        return self.ai


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_yaml_required(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(config_dir: Path | None = None) -> AppSettings:
    """Load application configuration from YAML files."""
    config_dir = config_dir or DEFAULT_CONFIG_DIR
    cfg = _load_yaml_required(config_dir / "config.yaml")
    cats = _load_yaml_required(config_dir / "categories.yaml")

    p = cfg.get("paths", {})
    library_path = Path(p.get("library", "/data/library"))
    paths = Paths(
        inbox=Path(p.get("inbox", "/data/inbox")),
        library=library_path,
        review=Path(p.get("review", "/data/library/_Review")),
        processed=Path(p.get("processed", "/data/library/_Processed")),
        logs=Path(p.get("logs", "/app/logs")),
        db=Path(p.get("db", str(library_path / "docusort.db"))),
    )

    ocr_cfg = cfg.get("ocr", {})
    ocr = OCRSettings(
        enabled=ocr_cfg.get("enabled", True),
        languages=ocr_cfg.get("languages", "deu+eng"),
        skip_if_text=ocr_cfg.get("skip_if_text", True),
        deskew=ocr_cfg.get("deskew", True),
        max_parallel=int(ocr_cfg.get("max_parallel", 2)),
    )

    # AI block — accept both the new `ai:` section and the legacy `claude:`
    # section so old config.yaml files keep working unchanged.
    ai_cfg = cfg.get("ai", cfg.get("claude", {}) or {})
    ai = AISettings(
        provider=str(ai_cfg.get("provider", "anthropic")),
        model=str(ai_cfg.get("model", "claude-haiku-4-5-20251001")),
        base_url=str(ai_cfg.get("base_url", "") or ""),
        max_text_chars=int(ai_cfg.get("max_text_chars", 12000)),
        min_confidence=float(ai_cfg.get("min_confidence", 0.65)),
        timeout_seconds=int(ai_cfg.get("timeout_seconds", 60)),
    )

    web_cfg = cfg.get("web", {})
    web = WebSettings(
        host=web_cfg.get("host", "0.0.0.0"),
        port=int(web_cfg.get("port", 8080)),
        default_language=str(web_cfg.get("default_language", "de")),
        ssl_cert=str(web_cfg.get("ssl_cert", "") or ""),
        ssl_key=str(web_cfg.get("ssl_key", "") or ""),
    )

    sync_cfg = cfg.get("sync", {})
    sync = SyncSettings(
        enabled=bool(sync_cfg.get("enabled", False)),
        target_type=str(sync_cfg.get("target_type", "local")),
        local_path=str(sync_cfg.get("local_path", "") or ""),
        remote=str(sync_cfg.get("remote", "")),
        source=str(sync_cfg.get("source", "library")),
        extra_flags=list(sync_cfg.get("extra_flags", []) or []),
        timeout_seconds=int(sync_cfg.get("timeout_seconds", 1800)),
    )

    fin_cfg = cfg.get("finance", {})
    finance = FinanceSettings(
        local_only=bool(fin_cfg.get("local_only", False)),
        pseudonymize=bool(fin_cfg.get("pseudonymize", True)),
        holder_names=[
            str(n).strip() for n in (fin_cfg.get("holder_names") or [])
            if str(n).strip()
        ],
        review_before_send=bool(fin_cfg.get("review_before_send", False)),
    )

    return AppSettings(
        paths=paths,
        categories=cats.get("categories", []),
        ocr=ocr,
        ai=ai,
        web=web,
        sync=sync,
        finance=finance,
        keep_original=cfg.get("keep_original", True),
        filename_template=cfg.get(
            "filename_template", "{date}_{category}_{sender}_{subject}"
        ),
        max_filename_length=cfg.get("max_filename_length", 120),
        stable_seconds=cfg.get("stable_seconds", 5),
        dry_run=cfg.get("dry_run", False),
        config_dir=config_dir,
    )


# ----- Secrets ---------------------------------------------------------------

# Map provider name -> environment variable that legacy installs may have set.
_LEGACY_ENV_KEYS = {
    "anthropic":      "ANTHROPIC_API_KEY",
    "openai":         "OPENAI_API_KEY",
    "gemini":         "GEMINI_API_KEY",
    "openai_compat":  "OPENAI_COMPAT_API_KEY",
}


def secrets_path(config_dir: Path | None = None) -> Path:
    return (config_dir or DEFAULT_CONFIG_DIR) / "secrets.yaml"


def load_secrets(config_dir: Path | None = None) -> dict[str, str]:
    """Read the secrets file. Always returns a dict (possibly empty) and
    silently ignores missing/unreadable files — secrets are optional."""
    path = secrets_path(config_dir)
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            return {}
        # Stringify values defensively so a YAML "bool" or "int" can't crash
        # downstream string handling.
        return {str(k): str(v) for k, v in data.items() if v is not None}
    except Exception as exc:
        logger.warning("Could not read secrets file %s: %s", path, exc)
        return {}


def save_secrets(secrets: dict[str, str], config_dir: Path | None = None) -> Path:
    """Write the secrets file with mode 0600. Existing keys not present in
    `secrets` are overwritten, so callers should pass the full dict."""
    path = secrets_path(config_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {k: v for k, v in secrets.items() if v}
    path.write_text(
        "# DocuSort secrets — written by the setup wizard, do not commit.\n"
        + yaml.safe_dump(payload, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return path


def get_api_key(settings: AppSettings | None = None,
                provider: str | None = None) -> str:
    """Return the API key for the configured (or requested) provider.

    Lookup order:
    1. secrets.yaml (`<provider>_api_key`, e.g. `anthropic_api_key`)
    2. legacy environment variable (e.g. ANTHROPIC_API_KEY)
    3. empty string — caller decides whether that's fatal
    """
    if provider is None:
        if settings is None:
            settings = load_config()
        provider = settings.ai.provider

    secrets = load_secrets(getattr(settings, "config_dir", None) if settings else None)
    key = secrets.get(f"{provider}_api_key", "").strip()
    if key:
        return key

    env_name = _LEGACY_ENV_KEYS.get(provider)
    if env_name:
        return os.environ.get(env_name, "").strip()
    return ""


def is_configured(settings: AppSettings | None = None) -> bool:
    """First-run gate: does the install have enough config to actually
    classify documents? Local providers (Ollama) don't require an API key."""
    if settings is None:
        try:
            settings = load_config()
        except FileNotFoundError:
            return False
    if settings.ai.provider == "openai_compat":
        return bool(settings.ai.base_url)  # local needs a URL, not a key
    if settings.ai.provider == "bridge":
        # The bridge is always "configured" from the server's
        # perspective: a token is auto-minted on first read. Whether a
        # client is actually connected is a runtime question handled
        # by the BridgeProvider itself.
        return True
    return bool(get_api_key(settings))
