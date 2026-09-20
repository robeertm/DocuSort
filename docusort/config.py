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
    # Hard wall-clock limit for a single OCR run (ocrmypdf / Tesseract).
    # A hung Tesseract process used to be able to freeze a pipeline slot
    # indefinitely; with only max_parallel slots, two stuck jobs stall
    # everything behind them. On timeout we fall back to the un-OCR'd
    # text (usually empty → routed to review) instead of blocking.
    timeout_seconds: int = 300


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
    # Output-token cap for a single classification. The classifier only
    # ever emits a tiny JSON object, so a few hundred tokens is plenty.
    # This matters for the local AI bridge: its per-call timeout budget
    # scales with max_output_tokens (~8 tok/s), so the old 100k value
    # produced a ~3.5 h timeout — one hung Mac could wedge a pipeline
    # slot for hours. Capping it keeps the worst-case wait sane.
    classify_max_tokens: int = 1500


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

    # ---- Salary-period cashflow tracker (v0.37) -------------------
    # The user's "month" runs from the salary credit (≈ 23rd/24th) to the
    # day before the next one, not Jan..Dec. These settings tune how the
    # /finance tracker slices the transaction stream into those periods
    # and how the budget alarm is computed.

    # Substring matched (case-insensitive) against counterparty + purpose
    # to pin the exact salary booking, e.g. "ACME GMBH". Empty →
    # fall back to whatever the CSV importer tagged as `gehalt`.
    salary_match: str = ""

    # Day-of-month the salary period starts on when no salary booking is
    # detected for a month (fallback + gap-filler). 23 matches the owner's
    # statements; clamped to a valid day per month.
    period_anchor_day: int = 23

    # Fixed monthly spending budget for the budget alarm. When > 0, the
    # "noch verfügbar" figure counts down from this budget; when 0, it
    # counts down from the income actually received in the period (what's
    # left of this month's paycheck).
    monthly_budget: float = 0.0


@dataclass
class NotificationSettings:
    """Where + when to send out-of-band notifications.

    Channel credentials (Telegram bot token, SMTP password) live in
    secrets.yaml so they don't end up in the regular config file. The
    rest is here.
    """
    enabled: bool = False

    # Per-event toggles — let the user turn down the noise without
    # disabling the whole subsystem.
    event_doc_review: bool = True   # low-confidence / review queue
    event_doc_failed: bool = True   # classifier raised
    event_doc_filed:  bool = False  # off by default — too chatty
    event_bulk_done:  bool = True   # background jobs (analyze-all, …)
    event_sync_failed: bool = True  # backup failed or is stale
    event_deadline:   bool = True   # a document deadline is coming up

    # Telegram channel
    telegram_enabled:  bool = False
    telegram_chat_id:  str  = ""    # numeric id from @BotFather

    # Email channel
    email_enabled:  bool = False
    smtp_host:      str  = ""
    smtp_port:      int  = 587
    smtp_user:      str  = ""
    smtp_from:      str  = ""
    smtp_to:        str  = ""       # comma-separated recipients
    smtp_starttls:  bool = True


@dataclass
class AppSettings:
    paths: Paths
    categories: list[dict[str, Any]]
    ocr: OCRSettings
    ai: AISettings
    web: WebSettings = field(default_factory=WebSettings)
    sync: SyncSettings = field(default_factory=SyncSettings)
    finance: FinanceSettings = field(default_factory=FinanceSettings)
    notifications: NotificationSettings = field(default_factory=NotificationSettings)
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


def _clamp_anchor_day(v: Any) -> int:
    """Salary-period anchor day, coerced into a sane 1..31 range."""
    try:
        d = int(v)
    except (TypeError, ValueError):
        return 23
    return max(1, min(31, d))


def _safe_float(v: Any) -> float:
    """Parse a possibly-German-shaped number ('1.234,56') into a
    non-negative float; 0.0 on garbage."""
    if v is None:
        return 0.0
    try:
        s = str(v).strip().replace(" ", "")
        if "," in s and s.count(",") == 1:
            s = s.replace(".", "").replace(",", ".")
        return max(0.0, round(float(s), 2))
    except (TypeError, ValueError):
        return 0.0


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
        timeout_seconds=int(ocr_cfg.get("timeout_seconds", 300)),
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
        classify_max_tokens=int(ai_cfg.get("classify_max_tokens", 1500)),
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
        salary_match=str(fin_cfg.get("salary_match", "") or "").strip(),
        period_anchor_day=_clamp_anchor_day(fin_cfg.get("period_anchor_day", 23)),
        monthly_budget=_safe_float(fin_cfg.get("monthly_budget", 0.0)),
    )

    n_cfg = cfg.get("notifications", {}) or {}
    notifications = NotificationSettings(
        enabled=bool(n_cfg.get("enabled", False)),
        event_doc_review=bool(n_cfg.get("event_doc_review", True)),
        event_doc_failed=bool(n_cfg.get("event_doc_failed", True)),
        event_doc_filed=bool(n_cfg.get("event_doc_filed", False)),
        event_bulk_done=bool(n_cfg.get("event_bulk_done", True)),
        event_sync_failed=bool(n_cfg.get("event_sync_failed", True)),
        event_deadline=bool(n_cfg.get("event_deadline", True)),
        telegram_enabled=bool(n_cfg.get("telegram_enabled", False)),
        telegram_chat_id=str(n_cfg.get("telegram_chat_id", "") or ""),
        email_enabled=bool(n_cfg.get("email_enabled", False)),
        smtp_host=str(n_cfg.get("smtp_host", "") or ""),
        smtp_port=int(n_cfg.get("smtp_port", 587) or 587),
        smtp_user=str(n_cfg.get("smtp_user", "") or ""),
        smtp_from=str(n_cfg.get("smtp_from", "") or ""),
        smtp_to=str(n_cfg.get("smtp_to", "") or ""),
        smtp_starttls=bool(n_cfg.get("smtp_starttls", True)),
    )

    return AppSettings(
        paths=paths,
        categories=cats.get("categories", []),
        ocr=ocr,
        ai=ai,
        web=web,
        sync=sync,
        finance=finance,
        notifications=notifications,
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
