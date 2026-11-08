"""Tests fuer den Webhook-Kanal — gegen einen lokalen HTTP-Server, nie gegen
das Netz. Geprueft wird, was beim Empfaenger WIRKLICH ankommt (Rumpf, Kopf),
dass jeder Fehlschlag als Exception hochkommt und dass das Token nirgends im
Log oder in einer Fehlermeldung steht."""
from __future__ import annotations

import json
import logging
import shutil
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from docusort import __version__, notifier
from docusort.notifier import NotificationEvent, WebhookNotifier

REPO_CONFIG = Path(__file__).resolve().parent.parent / "config"
TOKEN = "wh-test-token-0123456789abcdef"   # Wegwerfwert nur fuer diesen Test


class _Empfaenger:
    """Ein kleiner Webhook-Empfaenger mit einstellbarer Antwort."""

    def __init__(self, status: int = 204, delay: float = 0.0) -> None:
        self.status = status
        self.delay = delay
        self.requests: list[dict] = []
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                laenge = int(self.headers.get("Content-Length") or 0)
                roh = self.rfile.read(laenge)
                outer.requests.append({
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": roh,
                })
                if outer.delay:
                    time.sleep(outer.delay)
                self.send_response(outer.status)
                # Ein boeser Empfaenger spiegelt den Kopf zurueck — das darf
                # nie in einer Fehlermeldung landen.
                antwort = json.dumps({"echo": self.headers.get("Authorization")}).encode()
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(antwort)))
                self.end_headers()
                try:
                    self.wfile.write(antwort)
                except OSError:
                    pass

            def log_message(self, *a):  # still
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def url(self, pfad: str = "/hook?key=geheim-in-der-query") -> str:
        return f"http://127.0.0.1:{self.port}{pfad}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture()
def empfaenger():
    e = _Empfaenger()
    yield e
    e.close()


def _event(**kw) -> NotificationEvent:
    base = dict(kind="deadline", title="Frist", body="Text", doc_id=7,
                url="https://docs.example/document/7",
                data={"count": 1, "items": [{"doc_id": 7}]})
    base.update(kw)
    return NotificationEvent(**base)


# ------------------------------------------------------------------ Vertrag
def test_body_and_headers_with_token(empfaenger):
    WebhookNotifier(empfaenger.url(), TOKEN).send(_event())
    assert len(empfaenger.requests) == 1
    req = empfaenger.requests[0]
    h = req["headers"]
    assert h["content-type"] == "application/json"
    assert h["user-agent"] == f"DocuSort/{__version__}"
    assert h["authorization"] == f"Bearer {TOKEN}"
    body = json.loads(req["body"].decode("utf-8"))
    assert set(body) == {"source", "version", "event", "title", "body",
                         "doc_id", "url", "data", "sent_at"}
    assert body["source"] == "docusort"
    assert body["version"] == __version__
    assert body["event"] == "deadline"
    assert body["title"] == "Frist" and body["body"] == "Text"
    assert body["doc_id"] == 7
    assert body["url"] == "https://docs.example/document/7"
    assert body["data"] == {"count": 1, "items": [{"doc_id": 7}]}
    # ISO-8601 MIT Zeitzone
    stamp = datetime.fromisoformat(body["sent_at"])
    assert stamp.tzinfo is not None


def test_no_token_no_authorization_header(empfaenger):
    WebhookNotifier(empfaenger.url(), "").send(
        NotificationEvent(kind="test", title="t"))
    req = empfaenger.requests[0]
    assert "authorization" not in req["headers"]
    body = json.loads(req["body"])
    assert body["doc_id"] is None and body["url"] is None
    assert body["data"] == {} and body["body"] == ""


@pytest.mark.parametrize("status", [200, 201, 202, 204])
def test_2xx_is_success(status):
    e = _Empfaenger(status=status)
    try:
        WebhookNotifier(e.url(), TOKEN).send(_event())
    finally:
        e.close()


@pytest.mark.parametrize("status", [400, 401, 404, 500, 503])
def test_4xx_5xx_raise_without_secrets(status):
    e = _Empfaenger(status=status)
    try:
        with pytest.raises(RuntimeError) as ei:
            WebhookNotifier(e.url(), TOKEN).send(_event())
    finally:
        e.close()
    text = str(ei.value)
    assert str(status) in text
    assert "127.0.0.1" in text
    assert TOKEN not in text
    assert "geheim-in-der-query" not in text and "/hook" not in text


def test_timeout_raises(monkeypatch):
    e = _Empfaenger(delay=2.0)
    monkeypatch.setattr(WebhookNotifier, "TIMEOUT_S", 0.5)
    try:
        with pytest.raises(RuntimeError) as ei:
            WebhookNotifier(e.url(), TOKEN).send(_event())
    finally:
        e.close()
    assert TOKEN not in str(ei.value)


def test_unreachable_raises():
    # Port 9 auf localhost: niemand hoert zu → Verbindung abgelehnt.
    with pytest.raises(RuntimeError) as ei:
        WebhookNotifier("http://127.0.0.1:9/x?key=geheim", TOKEN).send(_event())
    assert "geheim" not in str(ei.value) and TOKEN not in str(ei.value)


def test_default_timeout_is_10s():
    assert WebhookNotifier.TIMEOUT_S == 10


@pytest.mark.parametrize("url", ["", "ftp://host/x", "file:///etc/passwd",
                                 "javascript:alert(1)", "host/only", "http://"])
def test_bad_urls_rejected(url):
    assert not notifier.webhook_url_ok(url)
    with pytest.raises(ValueError):
        WebhookNotifier(url, TOKEN)


def test_good_urls_accepted():
    assert notifier.webhook_url_ok("http://192.168.0.10:8080/hook")
    assert notifier.webhook_url_ok("https://example.com/a?b=c")


def test_token_never_logged(empfaenger, caplog):
    """send_now loggt Erfolg und Fehlschlag — beides ohne Token und ohne
    Pfad/Query der Adresse."""
    caplog.set_level(logging.DEBUG)
    disp = notifier.CompositeDispatcher(
        [WebhookNotifier(empfaenger.url(), TOKEN)], {})
    ok = disp.send_now(_event(kind="test"))
    assert ok[0]["ok"] is True
    empfaenger.status = 500
    bad = disp.send_now(_event(kind="test"))
    assert bad[0]["ok"] is False and bad[0]["channel"] == "webhook"
    log = caplog.text + json.dumps(ok) + json.dumps(bad)
    assert TOKEN not in log
    assert "geheim-in-der-query" not in log


# --------------------------------------------------------- Einstellungen
@pytest.fixture()
def app_env(tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from docusort.config import load_config
    from docusort.db import Database
    from docusort.web.app import create_app

    cfg_dir = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, cfg_dir)
    settings = load_config(cfg_dir)
    for name in ("inbox", "library", "review", "processed", "logs"):
        d = tmp_path / name
        d.mkdir(exist_ok=True)
        setattr(settings.paths, name, d)
    settings.paths.db = tmp_path / "library" / "docusort.db"
    settings.ai.provider = "openai_compat"
    settings.ai.base_url = "http://127.0.0.1:9"
    db = Database(settings.paths.db)
    client = TestClient(create_app(settings, db), follow_redirects=False)
    yield client, settings, cfg_dir
    notifier.configure(type("S", (), {"notifications": None})())


def test_settings_roundtrip_and_masking(app_env, empfaenger):
    client, settings, cfg_dir = app_env
    from docusort import settings_writer
    from docusort.config import load_config, load_secrets

    # Speichern ueber den Schreiber (derselbe Weg wie die API)
    settings_writer.update_notifications(
        enabled=True, webhook_enabled=True, webhook_url=empfaenger.url(),
        webhook_token=TOKEN, config_dir=cfg_dir)
    raw = (cfg_dir / "config.yaml").read_text(encoding="utf-8")
    assert TOKEN not in raw, "Token darf nie in config.yaml stehen"
    assert load_secrets(cfg_dir).get("webhook_token") == TOKEN

    neu = load_config(cfg_dir)
    assert neu.notifications.webhook_enabled is True
    assert neu.notifications.webhook_url == empfaenger.url()

    # Leeres/fehlendes Token ueberschreibt ein gesetztes NICHT
    settings_writer.update_notifications(webhook_token="", config_dir=cfg_dir)
    settings_writer.update_notifications(webhook_token=None, config_dir=cfg_dir)
    assert load_secrets(cfg_dir).get("webhook_token") == TOKEN

    # configure(): Kanal aktiv, test geht immer durch
    disp = notifier.configure(neu)
    assert "webhook" in disp.channels_summary()
    res = disp.send_now(NotificationEvent(kind="test", title="t"))
    assert res == [{"channel": "webhook", "ok": True, "error": None}]
    assert empfaenger.requests[-1]["headers"]["authorization"] == f"Bearer {TOKEN}"


def test_configure_respects_switches(tmp_path, empfaenger):
    from docusort.config import NotificationSettings

    class S:
        config_dir = tmp_path
        notifications = NotificationSettings(
            enabled=True, webhook_enabled=True, webhook_url="")
    assert "webhook" not in notifier.configure(S()).channels_summary()

    S.notifications = NotificationSettings(
        enabled=True, webhook_enabled=False, webhook_url=empfaenger.url())
    assert "webhook" not in notifier.configure(S()).channels_summary()

    S.notifications = NotificationSettings(
        enabled=True, webhook_enabled=True, webhook_url=empfaenger.url(),
        event_deadline=False)
    disp = notifier.configure(S())
    assert disp.channels_summary() == ["webhook"]
    assert disp.is_enabled_for("deadline") is False
    assert disp.is_enabled_for("ai_down") is True
    notifier.configure(type("X", (), {"notifications": None})())


def test_api_get_masks_and_post_validates(app_env, empfaenger):
    client, settings, cfg_dir = app_env
    from docusort.config import load_secrets

    # Die Einstellungs-API steht hinter der Anmeldung; der Test spricht die
    # Handler darum direkt ueber die App-Routen an.
    app = client.app
    routes = {(r.path, tuple(sorted(getattr(r, "methods", []) or []))): r
              for r in app.routes if hasattr(r, "endpoint")}
    get = routes[("/api/settings/notifications", ("GET",))].endpoint
    post = routes[("/api/settings/notifications", ("POST",))].endpoint

    with pytest.raises(Exception) as ei:
        post({"webhook_url": "ftp://example.com/x"})
    assert getattr(ei.value, "status_code", None) == 400

    out = post({"enabled": True, "webhook_enabled": True,
                "webhook_url": "  " + empfaenger.url() + "  ",
                "webhook_token": TOKEN})
    assert "webhook" in out["channels_active"]
    assert load_secrets(cfg_dir).get("webhook_token") == TOKEN

    g = get()
    assert g["webhook_enabled"] is True
    assert g["webhook_url"] == empfaenger.url()
    assert g["webhook_token_set"] is True
    assert TOKEN not in json.dumps(g), "GET darf das Token nie zeigen"

    # Speichern ohne Token behaelt das gespeicherte
    post({"webhook_token": ""})
    post({"webhook_token": None, "webhook_enabled": True})
    assert load_secrets(cfg_dir).get("webhook_token") == TOKEN


# ------------------------------------------------- Einmalpasswort-Rueckfall
@pytest.mark.parametrize("status,weg", [(204, "kanal"), (500, "datei")])
def test_one_time_password_falls_back_to_file(tmp_path, caplog, status, weg):
    """Scheitert der Webhook, landet das Einmalpasswort in der Datei — und
    weder das Passwort noch das Token stehen je im Log."""
    from docusort import auth, notzugang
    from docusort.db import Database

    caplog.set_level(logging.DEBUG)
    db = Database(tmp_path / "docusort.db")
    db.user_create("owner", auth.hash_password("Altes-Passwort-123"), "admin")
    e = _Empfaenger(status=status)
    try:
        notifier._dispatcher = notifier.CompositeDispatcher(
            [WebhookNotifier(e.url(), TOKEN)], {})
        erg = notzugang.einmalpasswort_anfordern(db, tmp_path)
    finally:
        notifier._dispatcher = None
        e.close()
    assert erg["ok"] is True and erg["weg"] == weg
    body = json.loads(e.requests[0]["body"])
    assert body["event"] == "password_reset"
    code = body["body"].split("Passwort:")[1].split()[0]
    assert code and code not in caplog.text and TOKEN not in caplog.text
    if weg == "datei":
        assert (tmp_path / erg["dateiname"]).exists()
        assert TOKEN not in json.dumps(erg)
