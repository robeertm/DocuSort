"""Tests for the read-only API: the listing and the "open one document" paths.

Run with `python3 -m pytest tests/`. The preview tests need `pdftoppm`
(poppler-utils) and are skipped where it is missing.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from docusort import auth, preview, readonly_api, readonly_token  # noqa: E402
from docusort.config import load_config  # noqa: E402
from docusort.db import Database, DocumentRecord  # noqa: E402

REPO_CONFIG = Path(__file__).resolve().parent.parent / "config"
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
# Every name that would point at the file on disk. None may appear in an answer.
WITHHELD = ("library_path", "processed_path", "extracted_text", "cost_usd",
            "input_tokens", "output_tokens", "original_name", "content_hash")

needs_poppler = pytest.mark.skipif(not preview.available(),
                                   reason="pdftoppm (poppler-utils) not installed")


def _pdf(path: Path, pages: int) -> Path:
    """A tiny valid PDF with `pages` blank pages."""
    from pypdf import PdfWriter
    w = PdfWriter()
    for _ in range(pages):
        w.add_blank_page(width=200, height=280)
    with path.open("wb") as f:
        w.write(f)
    return path


def _record(library_path: Path | str, **over) -> DocumentRecord:
    felder = dict(
        filename="2026-10-01_Rechnung_Stadtwerke.pdf",
        original_name="scan_0001.pdf", category="Rechnungen",
        doc_date="2026-10-01", sender="Stadtwerke", subject="Abschlag Oktober",
        confidence=0.9, reasoning="Monthly instalment.",
        library_path=str(library_path), processed_path="/data/processed/scan_0001.pdf",
        file_size=1234, page_count=None, ocr_used=False, model="test",
        input_tokens=10, output_tokens=5, cost_usd=0.01, status="filed",
        extracted_text="SECRET FULL TEXT",
    )
    felder.update(over)
    return DocumentRecord(**felder)


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """An app on a fresh library, with the read-only token switched on."""
    cfg_dir = tmp_path / "config"
    shutil.copytree(REPO_CONFIG, cfg_dir)
    lib = tmp_path / "library"
    lib.mkdir()
    settings = load_config(cfg_dir)
    for name in ("inbox", "library", "review", "processed", "logs"):
        d = tmp_path / name
        d.mkdir(exist_ok=True)
        setattr(settings.paths, name, d)
    settings.paths.db = lib / "docusort.db"
    # Past the first-run gate without any API key: a local provider only
    # needs an address (nothing is ever sent to it here).
    settings.ai.provider = "openai_compat"
    settings.ai.base_url = "http://127.0.0.1:9"
    # The test client's sender is not an IP address; the network check is
    # tested on its own below.
    monkeypatch.setenv(readonly_api.ALLOW_ENV, "any")

    db = Database(settings.paths.db)
    pdf = _pdf(lib / "letter.pdf", pages=3)
    txt = lib / "note.txt"
    txt.write_text("plain text", encoding="utf-8")
    ids = {
        "pdf": db.insert_document(_record(pdf)),
        "txt": db.insert_document(_record(txt, filename="note.txt", subject="Note")),
        "dup": db.insert_document(_record(pdf, status="duplicate")),
    }

    from docusort.web.app import create_app
    app = create_app(settings, db)
    token = readonly_token.new_token(cfg_dir)
    client = TestClient(app)
    return {"client": client, "token": token, "ids": ids, "db": db,
            "settings": settings, "cfg": cfg_dir, "pdf": pdf}


def _hdr(token: str) -> dict[str, str]:
    return {"Authorization": "Bearer " + token}


def _no_leak(body: str, env) -> None:
    for name in WITHHELD:
        assert name not in body, name
    assert str(env["pdf"]) not in body
    assert str(env["pdf"].parent) not in body
    assert "SECRET FULL TEXT" not in body
    assert "/data/processed" not in body


# ── allowlist ───────────────────────────────────────────────────────────────

def test_allowlist_holds_exact_paths_only():
    for p in ("/api/readonly/documents", "/api/readonly/document",
              "/api/readonly/preview"):
        assert p in auth.PUBLIC_PATHS
        assert auth.is_public_path(p)
    assert "/api/readonly/" not in auth.PUBLIC_PATHS
    for p in ("/api/readonly/", "/api/readonly/other", "/api/readonly/documents/x",
              "/api/readonly/preview/x", "/api/readonly/document2"):
        assert not auth.is_public_path(p), p


# ── the gate is the same on every path ──────────────────────────────────────

PATHS = ("/api/readonly/documents", "/api/readonly/document?id=1",
         "/api/readonly/preview?id=1&seite=1")


@pytest.mark.parametrize("path", PATHS)
def test_token_required(env, path):
    c = env["client"]
    r = c.get(path)
    assert r.status_code == 401
    assert r.json() == {"ok": False, "error": "unauthorized"}
    assert r.headers["www-authenticate"] == "Bearer"
    r = c.get(path, headers=_hdr("wrong"))
    assert r.status_code == 401


@pytest.mark.parametrize("path", PATHS)
def test_off_without_token_file(env, path):
    readonly_token.disable(env["cfg"])
    r = env["client"].get(path, headers=_hdr(env["token"]))
    assert r.status_code == 404
    assert r.json() == {"ok": False, "error": "not found"}


@pytest.mark.parametrize("path", PATHS)
def test_network_check(env, path, monkeypatch):
    monkeypatch.setenv(readonly_api.ALLOW_ENV, "10.0.0.0/8")
    r = env["client"].get(path, headers=_hdr(env["token"]))
    assert r.status_code == 403


@pytest.mark.parametrize("path", PATHS)
def test_brake_shared(env, path):
    c = env["client"]
    for _ in range(10):
        assert c.get("/api/readonly/documents").status_code == 401
    r = c.get(path, headers=_hdr(env["token"]))
    assert r.status_code == 429
    assert int(r.headers["retry-after"]) >= 1


# ── /api/readonly/document ──────────────────────────────────────────────────

@needs_poppler
def test_document_metadata(env):
    c, i = env["client"], env["ids"]
    r = c.get(f"/api/readonly/document?id={i['pdf']}", headers=_hdr(env["token"]))
    assert r.status_code == 200
    assert r.headers["cache-control"] == "no-store"
    data = r.json()
    assert data["ok"] is True
    doc = data["document"]
    listed = c.get("/api/readonly/documents", headers=_hdr(env["token"])).json()
    same = next(d for d in listed["documents"] if d["id"] == i["pdf"])
    # Exactly the listing's fields, plus the two preview fields.
    assert set(doc) == set(same) | {"seiten", "vorschau_verfuegbar"}
    assert {k: doc[k] for k in same} == same
    assert doc["seiten"] == 3
    assert doc["vorschau_verfuegbar"] is True
    _no_leak(r.text, env)


def test_document_without_preview(env):
    r = env["client"].get(f"/api/readonly/document?id={env['ids']['txt']}",
                          headers=_hdr(env["token"]))
    assert r.status_code == 200
    doc = r.json()["document"]
    assert doc["seiten"] == 0
    assert doc["vorschau_verfuegbar"] is False
    _no_leak(r.text, env)


def test_document_preview_fields_when_pdftoppm_missing(env, monkeypatch):
    monkeypatch.setattr(preview, "available", lambda: False)
    r = env["client"].get(f"/api/readonly/document?id={env['ids']['pdf']}",
                          headers=_hdr(env["token"]))
    doc = r.json()["document"]
    assert doc["seiten"] == 0 and doc["vorschau_verfuegbar"] is False


@pytest.mark.parametrize("key", ["unknown", "dup"])
def test_document_unknown_or_hidden(env, key):
    doc_id = 99999 if key == "unknown" else env["ids"][key]
    r = env["client"].get(f"/api/readonly/document?id={doc_id}",
                          headers=_hdr(env["token"]))
    assert r.status_code == 404
    assert r.json() == {"ok": False, "error": "not found"}


@pytest.mark.parametrize("raw", ["", "abc", "0", "-3", "1.5"])
def test_document_bad_id(env, raw):
    r = env["client"].get(f"/api/readonly/document?id={raw}",
                          headers=_hdr(env["token"]))
    assert r.status_code == 400
    assert r.json()["ok"] is False


def test_document_missing_id(env):
    r = env["client"].get("/api/readonly/document", headers=_hdr(env["token"]))
    assert r.status_code == 400


# ── /api/readonly/preview ───────────────────────────────────────────────────

@needs_poppler
@pytest.mark.parametrize("seite", ["", "1", "2", "3", "99", "-5"])
def test_preview_png(env, seite):
    q = f"id={env['ids']['pdf']}" + (f"&seite={seite}" if seite else "")
    r = env["client"].get(f"/api/readonly/preview?{q}", headers=_hdr(env["token"]))
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"
    assert r.content.startswith(PNG_MAGIC)
    assert b"%PDF" not in r.content
    assert "content-disposition" not in r.headers
    for v in r.headers.values():
        assert str(env["pdf"].parent) not in v


@needs_poppler
def test_preview_page_is_capped(env):
    hdr = _hdr(env["token"])
    base = f"/api/readonly/preview?id={env['ids']['pdf']}"
    assert env["client"].get(base + "&seite=99", headers=hdr).content == \
        env["client"].get(base + "&seite=3", headers=hdr).content


def test_preview_not_a_pdf(env):
    r = env["client"].get(f"/api/readonly/preview?id={env['ids']['txt']}",
                          headers=_hdr(env["token"]))
    assert r.status_code == 404
    assert r.headers["content-type"].startswith("application/json")
    assert r.json() == {"ok": False, "error": "preview not available"}
    _no_leak(r.text, env)


def test_preview_pdftoppm_missing(env, monkeypatch):
    monkeypatch.setattr(preview, "available", lambda: False)
    # No cached image from an earlier test may slip through either.
    shutil.rmtree(env["settings"].paths.db.parent / preview.CACHE_DIRNAME,
                  ignore_errors=True)
    r = env["client"].get(f"/api/readonly/preview?id={env['ids']['pdf']}",
                          headers=_hdr(env["token"]))
    assert r.status_code == 404
    assert r.json() == {"ok": False, "error": "preview not available"}
    _no_leak(r.text, env)


def test_preview_file_gone(env):
    env["pdf"].unlink()
    r = env["client"].get(f"/api/readonly/preview?id={env['ids']['pdf']}",
                          headers=_hdr(env["token"]))
    assert r.status_code == 404
    _no_leak(r.text, env)


@pytest.mark.parametrize("key", ["unknown", "dup"])
def test_preview_unknown_or_hidden(env, key):
    doc_id = 99999 if key == "unknown" else env["ids"][key]
    r = env["client"].get(f"/api/readonly/preview?id={doc_id}",
                          headers=_hdr(env["token"]))
    assert r.status_code == 404
    assert r.json() == {"ok": False, "error": "not found"}


@pytest.mark.parametrize("q", ["id=x", "id=1&seite=x", ""])
def test_preview_bad_params(env, q):
    r = env["client"].get(f"/api/readonly/preview?{q}", headers=_hdr(env["token"]))
    assert r.status_code == 400


# ── the listing stays as it was ─────────────────────────────────────────────

def test_listing_unchanged(env):
    r = env["client"].get("/api/readonly/documents", headers=_hdr(env["token"]))
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    ids = {d["id"] for d in data["documents"]}
    assert ids == {env["ids"]["pdf"], env["ids"]["txt"]}
    for d in data["documents"]:
        assert "seiten" not in d and "vorschau_verfuegbar" not in d
    _no_leak(r.text, env)
