"""Configuration loader for DocuSort.

Reads YAML config from /app/config/config.yaml (overridable via DOCUSORT_CONFIG
environment variable) and categories from categories.yaml. Secrets come from
environment variables – never from YAML.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field  # noqa: F401
from pathlib import Path
from typing import Any

import yaml


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


@dataclass
class ClaudeSettings:
    model: str = "claude-haiku-4-5-20251001"
    max_text_chars: int = 12000
    min_confidence: float = 0.65
    timeout_seconds: int = 60


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
    remote: str = ""                 # e.g. "icloud:DocuSort"
    source: str = "library"          # 'library' (excl. _Trash) | 'library_and_trash'
    extra_flags: list = field(default_factory=list)
    timeout_seconds: int = 1800      # 30 min default


@dataclass
class AppSettings:
    paths: Paths
    categories: list[dict[str, Any]]
    ocr: OCRSettings
    claude: ClaudeSettings
    web: WebSettings = field(default_factory=WebSettings)
    sync: SyncSettings = field(default_factory=SyncSettings)
    keep_original: bool = True
    filename_template: str = "{date}_{category}_{sender}_{subject}"
    max_filename_length: int = 120
    stable_seconds: int = 5  # wait before processing (file still being written)
    dry_run: bool = False


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(config_dir: Path | None = None) -> AppSettings:
    """Load application configuration from YAML files."""
    config_dir = config_dir or DEFAULT_CONFIG_DIR
    cfg = _load_yaml(config_dir / "config.yaml")
    cats = _load_yaml(config_dir / "categories.yaml")

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
    )

    cl_cfg = cfg.get("claude", {})
    claude = ClaudeSettings(
        model=cl_cfg.get("model", "claude-haiku-4-5-20251001"),
        max_text_chars=cl_cfg.get("max_text_chars", 12000),
        min_confidence=cl_cfg.get("min_confidence", 0.65),
        timeout_seconds=cl_cfg.get("timeout_seconds", 60),
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
        remote=str(sync_cfg.get("remote", "")),
        source=str(sync_cfg.get("source", "library")),
        extra_flags=list(sync_cfg.get("extra_flags", []) or []),
        timeout_seconds=int(sync_cfg.get("timeout_seconds", 1800)),
    )

    return AppSettings(
        paths=paths,
        categories=cats.get("categories", []),
        ocr=ocr,
        claude=claude,
        web=web,
        sync=sync,
        keep_original=cfg.get("keep_original", True),
        filename_template=cfg.get(
            "filename_template", "{date}_{category}_{sender}_{subject}"
        ),
        max_filename_length=cfg.get("max_filename_length", 120),
        stable_seconds=cfg.get("stable_seconds", 5),
        dry_run=cfg.get("dry_run", False),
    )


def get_api_key() -> str:
    """Return the Anthropic API key from environment. Raises if missing."""
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY environment variable is not set. "
            "Configure it in your docker-compose.yml or .env file."
        )
    return key
