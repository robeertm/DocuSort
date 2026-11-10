"""Tests for the page-text store (1.8.0): the content door reads page texts
from the database, a low-priority backfill fills them, a changed file
invalidates them.

Run with `python3 -m pytest tests/`. Everything runs on a throw-away library.
"""
from __future__ import annotations

import os

import pytest

pytest.importorskip("fastapi")

from docusort import page_texts, readonly_api  # noqa: E402
from test_content_api import _hdr, env, text_pdf  # noqa: E402,F401 — fixture reused


def _bt(env, **kw):
    kw.setdefault("isolated", False)
    kw.setdefault("pause_min", 0)
    kw.setdefault("pause_factor", 0)
    return page_texts.Backfill(env["db"], start_delay=0, round_s=0.01, **kw)


def _no_reading(monkeypatch):
    """From here on every attempt to open a PDF fails loudly."""
    def boom(*a, **k):
        raise AssertionError("PDF was read although its pages are stored")
    monkeypatch.setattr(page_texts, "extract", boom)
    monkeypatch.setattr(page_texts, "extract_isolated", boom)
    import pypdf
    monkeypatch.setattr(pypdf, "PdfReader", boom)


def _rows(env, doc_id):
    with env["db"]._lock:
        return env["db"]._conn.execute(
            "SELECT COUNT(*) FROM document_pages WHERE doc_id = ?", (doc_id,)).fetchone()[0]


def test_backfill_stores_pages_and_door_reads_only_the_db(env, monkeypatch):
    pdf_id = env["ids"]["pdf"]
    offen = page_texts.pending(env["db"])
    assert [d for d, _, _ in offen] == [pdf_id]          # duplicate and non-PDF are skipped
    assert _bt(env).run_round() == 1
    assert page_texts.pending(env["db"]) == []
    assert _rows(env, pdf_id) == 3
    assert page_texts.counts(env["db"]) == {"stored": 1, "failed": 0}

    _no_reading(monkeypatch)                            # 🔑 Gegenprobe: nichts darf mehr das PDF oeffnen
    d = env["client"].get("/api/content/text?id=%d" % pdf_id, headers=_hdr(env["token"])).json()
    assert d["source"] == "pdf" and d["pages_total"] == 3
    assert "87,50 EUR" in d["pages"][1]["text"]
    assert "3704" not in d["pages"][2]["text"]           # masked at the door as before
    s = env["client"].get("/api/content/search?q=Abschlag%20November", headers=_hdr(env["token"])).json()
    assert s["hits"][0]["page"] == 2


def test_first_door_access_stores_for_next_time(env, monkeypatch):
    pdf_id = env["ids"]["pdf"]
    assert _rows(env, pdf_id) == 0
    r = env["client"].get("/api/content/text?id=%d" % pdf_id, headers=_hdr(env["token"]))
    assert r.status_code == 200 and _rows(env, pdf_id) == 3
    _no_reading(monkeypatch)
    d = env["client"].get("/api/content/text?id=%d&seite=2" % pdf_id, headers=_hdr(env["token"])).json()
    assert d["pages"][0]["seite"] == 2 and "Abschlag" in d["pages"][0]["text"]


def test_changed_file_invalidates_the_stored_pages(env):
    pdf_id = env["ids"]["pdf"]
    _bt(env).run_round()
    pdf = env["pdf"]
    alt = pdf.stat().st_mtime_ns
    text_pdf(pdf, ["Neue Fassung Seite eins", "Neue Fassung Seite zwei"])
    os.utime(pdf, ns=(alt + 5_000_000_000, alt + 5_000_000_000))
    assert [d for d, _, _ in page_texts.pending(env["db"])] == [pdf_id]
    d = env["client"].get("/api/content/text?id=%d" % pdf_id, headers=_hdr(env["token"])).json()
    assert d["pages_total"] == 2 and "Neue Fassung" in d["pages"][0]["text"]
    assert _rows(env, pdf_id) == 2
    assert page_texts.pending(env["db"]) == []


def test_moving_a_file_keeps_the_stored_pages(env, monkeypatch):
    pdf_id = env["ids"]["pdf"]
    _bt(env).run_round()
    neu = env["pdf"].with_name("umbenannt.pdf")
    os.rename(env["pdf"], neu)                          # rename keeps size and mtime
    env["db"].update_paths(pdf_id, str(neu))
    assert page_texts.pending(env["db"]) == []
    _no_reading(monkeypatch)
    d = env["client"].get("/api/content/text?id=%d" % pdf_id, headers=_hdr(env["token"])).json()
    assert d["source"] == "pdf" and d["pages_total"] == 3


def test_unreadable_file_is_remembered_and_door_falls_back(env):
    pdf_id = env["ids"]["pdf"]
    env["pdf"].write_bytes(b"%PDF-1.4 kaputt")
    assert _bt(env).run_round() == 1
    assert page_texts.counts(env["db"]) == {"stored": 0, "failed": 1}
    assert page_texts.pending(env["db"]) == []           # not retried until the file changes
    d = env["client"].get("/api/content/text?id=%d" % pdf_id, headers=_hdr(env["token"])).json()
    assert d["source"] == "stored" and d["pages"][0]["seite"] is None


def test_isolated_extraction_in_own_process(env):
    total, seiten = page_texts.extract_isolated(env["pdf"])
    assert total == 3 and "Abschlag" in seiten[1]
    env["pdf"].write_bytes(b"kein pdf")
    with pytest.raises(Exception):
        page_texts.extract_isolated(env["pdf"])


def test_backfill_with_isolated_process(env):
    assert _bt(env, isolated=True).run_round() == 1
    assert _rows(env, env["ids"]["pdf"]) == 3


def test_backfill_waits_while_door_is_off_or_switched_off(env, monkeypatch):
    assert _bt(env, enabled=lambda: False).run_round() == 0
    monkeypatch.setenv(page_texts.ENV_SWITCH, "0")
    assert _bt(env).run_round() == 0
    assert page_texts.start_backfill(env["db"], env["cfg"]) is None
    assert page_texts.pending(env["db"]) != []


def test_backfill_pauses_after_each_document(env, monkeypatch):
    gewartet = []
    bt = _bt(env, pause_min=0.25)
    monkeypatch.setattr(bt._stop, "wait", lambda s: gewartet.append(s))
    bt.run_round()
    assert gewartet and gewartet[0] >= 0.25


def test_purge_removes_stored_pages(env):
    pdf_id = env["ids"]["pdf"]
    _bt(env).run_round()
    env["db"].purge(pdf_id)
    assert _rows(env, pdf_id) == 0
    with env["db"]._lock:
        assert env["db"]._conn.execute(
            "SELECT COUNT(*) FROM document_pages_src WHERE doc_id = ?", (pdf_id,)).fetchone()[0] == 0


def test_known_page_count_spares_opening_the_pdf(env, monkeypatch):
    from docusort import preview
    monkeypatch.setattr(preview, "available", lambda: True)
    import pypdf

    def boom(*a, **k):
        raise AssertionError("opened")
    monkeypatch.setattr(pypdf, "PdfReader", boom)
    assert readonly_api.preview_pages(env["pdf"], 3) == 3


def test_background_thread_wakes_on_nudge(env, monkeypatch):
    bt = page_texts.Backfill(env["db"], start_delay=0, round_s=60, isolated=False,
                             pause_min=0, pause_factor=0)
    monkeypatch.setattr(page_texts, "_laufend", bt)
    bt.start()
    try:
        import time
        frist = time.monotonic() + 10
        while bt.done < 1 and time.monotonic() < frist:
            time.sleep(0.05)
        assert bt.done == 1
        text_pdf(env["pdf"], ["Nach dem Einsortieren"])
        page_texts.nudge()                              # wakes long before the 60-s round
        frist = time.monotonic() + 10
        while bt.done < 2 and time.monotonic() < frist:
            time.sleep(0.05)
        assert bt.done == 2
    finally:
        bt.stop()
