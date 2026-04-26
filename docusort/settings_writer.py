"""Write user-editable settings back to config.yaml.

The setup wizard and /settings page POST a small JSON blob; this module
merges it into the existing config.yaml on disk and re-writes the file.
We use ruamel-style careful merging (preserve unknown keys) but rely on
PyYAML for portability — the trade-off is that comments in config.yaml
are lost on first save. That's acceptable since config is regenerated
through the UI and documented elsewhere.
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

import yaml

from .config import (
    AppSettings, DEFAULT_CONFIG_DIR, load_secrets, save_secrets, secrets_path,
)


logger = logging.getLogger("docusort.settings_writer")


def _config_path(config_dir: Path | None = None) -> Path:
    return (config_dir or DEFAULT_CONFIG_DIR) / "config.yaml"


def _read_raw(config_dir: Path | None = None) -> dict[str, Any]:
    path = _config_path(config_dir)
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _write_raw(data: dict[str, Any], config_dir: Path | None = None) -> Path:
    path = _config_path(config_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(data, default_flow_style=False, allow_unicode=True,
                       sort_keys=False),
        encoding="utf-8",
    )
    return path


def update_ai(
    *,
    provider: str,
    model: str,
    base_url: str = "",
    api_key: str | None = None,
    config_dir: Path | None = None,
) -> Path:
    """Persist the AI provider choice + model + base_url to config.yaml.
    The api_key (if given and non-empty) is stored separately in secrets.yaml.
    """
    cfg = _read_raw(config_dir)
    ai = cfg.get("ai") or {}
    ai["provider"]  = provider.strip()
    ai["model"]     = model.strip()
    ai["base_url"]  = base_url.strip() if provider == "openai_compat" else ""
    cfg["ai"] = ai
    # Drop the legacy "claude:" block so the next load can't pick a stale value.
    cfg.pop("claude", None)
    out = _write_raw(cfg, config_dir)

    if api_key is not None and api_key.strip():
        secrets = load_secrets(config_dir)
        secrets[f"{provider}_api_key"] = api_key.strip()
        save_secrets(secrets, config_dir)
    return out


def update_paths(
    *,
    inbox: str = "",
    library: str = "",
    config_dir: Path | None = None,
) -> Path:
    cfg = _read_raw(config_dir)
    paths = cfg.get("paths") or {}
    if inbox:
        paths["inbox"] = inbox
    if library:
        paths["library"] = library
        # Re-derive the convention-based subpaths so we don't end up pointing
        # at a stale directory under the previous library root.
        paths["review"]    = str(Path(library) / "_Review")
        paths["processed"] = str(Path(library) / "_Processed")
        paths["db"]        = str(Path(library) / "docusort.db")
    cfg["paths"] = paths
    return _write_raw(cfg, config_dir)


def update_web(
    *,
    default_language: str | None = None,
    config_dir: Path | None = None,
) -> Path:
    cfg = _read_raw(config_dir)
    web = cfg.get("web") or {}
    if default_language:
        web["default_language"] = default_language
    cfg["web"] = web
    return _write_raw(cfg, config_dir)


def update_sync(
    *,
    enabled: bool,
    target_type: str = "local",
    local_path: str = "",
    remote: str = "",
    source: str = "library",
    config_dir: Path | None = None,
) -> Path:
    cfg = _read_raw(config_dir)
    sync = cfg.get("sync") or {}
    sync["enabled"]     = bool(enabled)
    sync["target_type"] = target_type
    sync["local_path"]  = local_path.strip()
    sync["remote"]      = remote.strip()
    sync["source"]      = source
    cfg["sync"] = sync
    return _write_raw(cfg, config_dir)


def remove_secret(provider: str, config_dir: Path | None = None) -> None:
    secrets = load_secrets(config_dir)
    secrets.pop(f"{provider}_api_key", None)
    save_secrets(secrets, config_dir)
