"""Notification channels for DocuSort.

Two transports out of the box: Telegram (bot API) and email (SMTP). Both
are stdlib-only so they work on a fresh install without extra wheels.

Events the rest of the code can fire:

  - ``doc_review``  — a document landed in the review queue (low
                       confidence or extraction couldn't run).
  - ``doc_failed``  — classification raised an exception.
  - ``doc_filed``   — a document was filed successfully (off by default;
                       opt-in because it's noisy).
  - ``bulk_done``   — a background job (analyze-all, retry-review,
                       approve-all-pending) finished.
  - ``test``        — synthetic event the Settings → Test button fires;
                       always delivered regardless of the per-event
                       enable toggles.

The dispatcher fires each channel in a daemon thread so the calling
thread (the watcher, a worker) never blocks on a slow SMTP server or a
flaky Telegram round-trip. Failures are logged and dropped — a
notification path can never break the document pipeline.
"""

from __future__ import annotations

import json
import logging
import smtplib
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import Optional


logger = logging.getLogger("docusort.notifier")


@dataclass
class NotificationEvent:
    """One thing the user might want to know about."""
    kind: str
    title: str
    body: str = ""
    doc_id: Optional[int] = None
    url: Optional[str] = None    # clickable link (server origin + /document/<id>)


# ----------------------------------------------------------------- channels
class Notifier:
    """Abstract base. Concrete subclasses implement send()."""
    name: str = "abstract"

    def send(self, event: NotificationEvent) -> None:
        raise NotImplementedError


class TelegramNotifier(Notifier):
    """Posts to api.telegram.org via the Bot API. The user creates a
    bot via @BotFather, sends a message to it once to obtain a chat_id
    (https://api.telegram.org/bot<TOKEN>/getUpdates), and pastes both
    into Settings."""
    name = "telegram"

    def __init__(self, bot_token: str, chat_id: str) -> None:
        if not bot_token:
            raise ValueError("telegram: bot_token required")
        if not chat_id:
            raise ValueError("telegram: chat_id required")
        self.bot_token = bot_token
        self.chat_id   = str(chat_id)

    def send(self, event: NotificationEvent) -> None:
        # Markdown attempt: title bold, body/link escaped so a stray
        # ``_`` in a document name can't trip Telegram's parser.
        md_text = f"*{_md_escape(event.title)}*"
        if event.body:
            md_text += f"\n\n{_md_escape(event.body)}"
        if event.url:
            md_text += f"\n\n[Open]({event.url})"
        try:
            self._post(md_text, parse_mode="Markdown")
            return
        except _TelegramParseError:
            # A malformed-entity error is never worth losing the whole
            # notification over — resend the same content as plain text.
            logger.info("Telegram: Markdown rejected, retrying as plain text")
        plain = event.title
        if event.body:
            plain += f"\n\n{event.body}"
        if event.url:
            plain += f"\n\n{event.url}"
        self._post(plain, parse_mode=None)

    def _post(self, text: str, parse_mode: Optional[str]) -> None:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text":    text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                if r.status >= 300:
                    raise RuntimeError(f"Telegram HTTP {r.status}")
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", "replace")[:400]
            except Exception:
                pass
            low = detail.lower()
            if parse_mode and "can't parse entities" in low:
                raise _TelegramParseError(detail) from exc
            raise RuntimeError(_telegram_hint(exc.code, detail)) from exc
        except urllib.error.URLError as exc:
            # No HTTP status — DNS failure, no route, TLS problem, timeout.
            raise RuntimeError(
                f"Telegram unreachable: {getattr(exc, 'reason', exc)} "
                "(no internet on the server, or api.telegram.org blocked?)"
            ) from exc


class _TelegramParseError(RuntimeError):
    """Internal: Telegram rejected our Markdown — retry as plain text."""


def _telegram_hint(code: int, detail: str) -> str:
    """Turn a raw Telegram Bot-API error into something Robert can act on."""
    low = (detail or "").lower()
    if code == 401 or "unauthorized" in low:
        return ("Telegram 401 Unauthorized — the bot token is wrong or was "
                "revoked. Copy it again from @BotFather.")
    if "chat not found" in low:
        return ("Telegram: chat not found — the Chat-ID is wrong, or you "
                "haven't messaged the bot yet. Open the bot in Telegram, "
                "send it any message, then re-check the Chat-ID via "
                "api.telegram.org/bot<TOKEN>/getUpdates.")
    if "bot was blocked" in low:
        return "Telegram: you blocked the bot — unblock it and try again."
    if "bots can't send messages to bots" in low:
        return "Telegram: the Chat-ID points at another bot, not your account."
    return f"Telegram HTTP {code}: {detail}"


def _md_escape(s: str) -> str:
    r"""Telegram Markdown V1 escape — only the special chars `_ * [ \``."""
    return (s.replace("\\", "\\\\")
             .replace("_", "\\_")
             .replace("*", "\\*")
             .replace("[", "\\[")
             .replace("`", "\\`"))


class EmailNotifier(Notifier):
    """SMTP (with optional STARTTLS). Plain auth via username/password."""
    name = "email"

    def __init__(self, *, smtp_host: str, smtp_port: int,
                 smtp_user: str, smtp_password: str,
                 from_addr: str, to_addrs: list[str],
                 use_starttls: bool = True) -> None:
        if not smtp_host:
            raise ValueError("email: smtp_host required")
        if not from_addr:
            raise ValueError("email: from address required")
        if not to_addrs:
            raise ValueError("email: at least one recipient required")
        self.smtp_host     = smtp_host
        self.smtp_port     = int(smtp_port)
        self.smtp_user     = smtp_user
        self.smtp_password = smtp_password
        self.from_addr     = from_addr
        self.to_addrs      = [t.strip() for t in to_addrs if t.strip()]
        self.use_starttls  = use_starttls

    def send(self, event: NotificationEvent) -> None:
        msg = EmailMessage()
        msg["From"]    = self.from_addr
        msg["To"]      = ", ".join(self.to_addrs)
        msg["Subject"] = f"[DocuSort] {event.title}"
        body = event.body or event.title
        if event.url:
            body = f"{body}\n\nOpen: {event.url}"
        msg.set_content(body)

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=15) as s:
            s.ehlo()
            if self.use_starttls:
                s.starttls()
                s.ehlo()
            if self.smtp_user:
                s.login(self.smtp_user, self.smtp_password)
            s.send_message(msg)


# ----------------------------------------------------------------- dispatch
@dataclass
class _DispatcherState:
    channels: list[Notifier] = field(default_factory=list)
    events_enabled: dict[str, bool] = field(default_factory=dict)


class CompositeDispatcher:
    """Holds the active channel list + per-event toggles. Sends are
    fire-and-forget (one daemon thread per channel) so a slow SMTP
    can never stall the watcher loop."""

    def __init__(self, channels: list[Notifier],
                 events_enabled: dict[str, bool]) -> None:
        self._state = _DispatcherState(
            channels=list(channels),
            events_enabled=dict(events_enabled or {}),
        )

    def channels_summary(self) -> list[str]:
        return [ch.name for ch in self._state.channels]

    def is_enabled_for(self, kind: str) -> bool:
        return bool(self._state.events_enabled.get(kind, False))

    def fire(self, event: NotificationEvent) -> None:
        if not self._state.channels:
            return
        # `test` always goes through — it's the explicit "send me one"
        # button in the UI, gated only by channel availability.
        if event.kind != "test" and not self.is_enabled_for(event.kind):
            return
        for ch in list(self._state.channels):
            t = threading.Thread(
                target=self._send_safe, args=(ch, event),
                daemon=True, name=f"notify-{ch.name}",
            )
            t.start()

    def send_now(self, event: NotificationEvent) -> list[dict]:
        """Send synchronously to every channel and report the outcome.

        Unlike :meth:`fire` this blocks and returns a per-channel result
        list so the caller (the Settings → Test button) can tell the user
        *why* a message didn't arrive instead of a hollow "sent". Each
        entry is ``{"channel": str, "ok": bool, "error": str|None}``."""
        results: list[dict] = []
        for ch in list(self._state.channels):
            try:
                ch.send(event)
                results.append({"channel": ch.name, "ok": True, "error": None})
                logger.info("Sent %s via %s: %s",
                            event.kind, ch.name, event.title)
            except Exception as exc:
                results.append({"channel": ch.name, "ok": False,
                                "error": str(exc)})
                logger.warning("Notification via %s failed: %s", ch.name, exc)
        return results

    @staticmethod
    def _send_safe(channel: Notifier, event: NotificationEvent) -> None:
        try:
            channel.send(event)
            logger.info("Sent %s via %s: %s",
                        event.kind, channel.name, event.title)
        except Exception as exc:
            logger.warning("Notification via %s failed: %s", channel.name, exc)


# ------------------------------------------------------------- module singleton
_dispatcher: Optional[CompositeDispatcher] = None
_dispatcher_lock = threading.Lock()


def configure(settings) -> CompositeDispatcher:
    """Build (or rebuild) the dispatcher from the live AppSettings.
    Reads SMTP password and Telegram token from secrets.yaml."""
    global _dispatcher
    from .config import load_secrets

    n = getattr(settings, "notifications", None)
    channels: list[Notifier] = []
    events: dict[str, bool] = {}

    if n is None or not getattr(n, "enabled", False):
        with _dispatcher_lock:
            _dispatcher = CompositeDispatcher([], {})
        return _dispatcher

    secrets = load_secrets(getattr(settings, "config_dir", None))

    if getattr(n, "telegram_enabled", False):
        token = (secrets.get("telegram_bot_token") or "").strip()
        chat  = (n.telegram_chat_id or "").strip()
        if token and chat:
            try:
                channels.append(TelegramNotifier(token, chat))
            except Exception as exc:
                logger.warning("Telegram channel skipped: %s", exc)
        else:
            logger.info("Telegram enabled but missing token/chat — skipped.")

    if getattr(n, "email_enabled", False):
        password = (secrets.get("smtp_password") or "").strip()
        try:
            channels.append(EmailNotifier(
                smtp_host=n.smtp_host, smtp_port=n.smtp_port,
                smtp_user=n.smtp_user, smtp_password=password,
                from_addr=n.smtp_from,
                to_addrs=[t.strip() for t in (n.smtp_to or "").split(",") if t.strip()],
                use_starttls=n.smtp_starttls,
            ))
        except Exception as exc:
            logger.warning("Email channel skipped: %s", exc)

    events = {
        "doc_review":  bool(getattr(n, "event_doc_review",  True)),
        "doc_failed":  bool(getattr(n, "event_doc_failed",  True)),
        "doc_filed":   bool(getattr(n, "event_doc_filed",   False)),
        "bulk_done":   bool(getattr(n, "event_bulk_done",   True)),
        "sync_failed": bool(getattr(n, "event_sync_failed", True)),
        "deadline":    bool(getattr(n, "event_deadline",    True)),
    }

    with _dispatcher_lock:
        _dispatcher = CompositeDispatcher(channels, events)
    logger.info(
        "Notifications: %d channel(s) active (%s), events=%s",
        len(channels), ", ".join(ch.name for ch in channels) or "none", events,
    )
    return _dispatcher


def get_dispatcher() -> CompositeDispatcher:
    with _dispatcher_lock:
        if _dispatcher is None:
            return CompositeDispatcher([], {})
        return _dispatcher


def fire(event: NotificationEvent) -> None:
    """Public API: dispatch an event. Safe to call before configure()."""
    get_dispatcher().fire(event)
