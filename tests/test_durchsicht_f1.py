"""Fixes from the walkthrough: empty search hint, missing-file hint,
polling requests that end with the page."""
from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from test_content_api import env  # noqa: E402,F401 — fixture reused


def _login(env):
    from docusort import auth
    env["db"].user_create("t", auth.hash_password("Test-Passwort-123!"), "admin")
    r = env["client"].post("/login", data={"username": "t", "password": "Test-Passwort-123!"},
                           follow_redirects=False)
    assert r.status_code in (200, 302, 303), r.status_code


def test_empty_search_has_status_line_and_no_upload_button(env):
    _login(env)
    r = env["client"].get("/library?q=zzzkeinTreffer")
    assert r.status_code == 200
    assert "role=\"status\" data-leer-hinweis" in r.text
    assert "btn-primary mt-4" not in r.text
    assert "role=\"status\" data-leer-hinweis" in r.text


def test_library_with_documents_has_no_empty_hint(env):
    _login(env)
    t = env["client"].get("/library").text
    assert "role=\"status\" data-leer-hinweis" not in t
    # Jinja reads "{#" inside the inline CSS as a comment and swallows the page.
    assert 'id="grid"' in t and "#lib-wrap:has([data-leer-hinweis])" in t


def test_missing_file_shows_hint(env):
    _login(env)
    r = env["client"].get("/document/%d" % env["ids"]["pdf"])
    assert "data-datei-fehlt" not in r.text
    env["pdf"].unlink()
    r = env["client"].get("/document/%d" % env["ids"]["pdf"])
    assert "data-datei-fehlt" in r.text
    assert "<iframe" not in r.text.split("data-datei-fehlt")[1].split("</div>")[0]


def test_polling_uses_abort_signal(env):
    _login(env)
    assert "dsAbortSignal" in env["client"].get("/library").text
