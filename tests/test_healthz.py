"""Tests for the unauthenticated /healthz probe and the container probe."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from docusort import auth, healthcheck  # noqa: E402
from docusort.config import load_config  # noqa: E402
from docusort.db import Database  # noqa: E402

REPO_CONFIG = Path(__file__).resolve().parent.parent / "config"


@pytest.fixture()
def env(tmp_path):
    cfg_dir = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, cfg_dir)
    settings = load_config(cfg_dir)
    for name in ("inbox", "library", "review", "processed", "logs"):
        d = tmp_path / name
        d.mkdir(exist_ok=True)
        setattr(settings.paths, name, d)
    settings.paths.db = tmp_path / "library" / "docusort.db"
    settings.paths.db.parent.mkdir(exist_ok=True)
    settings.ai.provider = "openai_compat"
    settings.ai.base_url = "http://127.0.0.1:9"
    db = Database(settings.paths.db)
    from docusort.web.app import create_app
    return TestClient(create_app(settings, db), follow_redirects=False), db


def test_healthz_open_and_minimal(env):
    client, _ = env
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
    assert r.headers["cache-control"] == "no-store"


def test_healthz_503_when_database_broken(env, monkeypatch):
    client, db = env
    monkeypatch.setattr(db, "ping", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    r = client.get("/healthz")
    assert r.status_code == 503
    assert r.json() == {"status": "error"}


def test_healthz_is_one_exact_public_path():
    assert auth.is_public_path("/healthz")
    for p in ("/healthz/", "/healthz/x", "/healthzz", "/health", "/api/health"):
        assert not auth.is_public_path(p), p


def test_nothing_else_became_public(env):
    client, _ = env
    for p in ("/", "/api/health", "/api/status/1", "/healthz/extra", "/documents"):
        r = client.get(p)
        # Not served without a session (503 = no administrator set up yet).
        assert r.status_code in (303, 401, 404, 405, 503), (p, r.status_code)


def test_probe_follows_server_answer(monkeypatch):
    class R:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(healthcheck.urllib.request, "urlopen", lambda *a, **k: R())
    assert healthcheck.probe(9876)
    def boom(*a, **k): raise OSError("refused")
    monkeypatch.setattr(healthcheck.urllib.request, "urlopen", boom)
    assert not healthcheck.probe(9876)
