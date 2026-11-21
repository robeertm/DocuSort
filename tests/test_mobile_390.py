"""Rueckfall-Test Handy-Breite 390 px: kein seitliches Scrollen, Eingaben >= 16 px,
kleine Knoepfe/Links mit Daumenfeld (.tap), lange Titel brechen um statt abzuschneiden."""
from __future__ import annotations

import shutil
import socket
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
sync_api = pytest.importorskip("playwright.sync_api")
import uvicorn  # noqa: E402

from docusort import auth  # noqa: E402
from docusort.config import load_config  # noqa: E402
from docusort.db import Database, DocumentRecord  # noqa: E402

REPO_CONFIG = Path(__file__).resolve().parent.parent / "config"
PAGES = ["/", "/library", "/upload", "/rechnungen", "/finance", "/budget",
         "/fixkosten", "/transactions", "/analytics", "/duplicates",
         "/settings", "/users", "/konto", "/document/1"]
LONG = "Kuendigungsbestaetigung Internetanschluss mit einem sehr langen Betreff der umbrechen muss"

CHECK = """() => {
  const vw = innerWidth, r = {sw: document.documentElement.scrollWidth, vw, small: [], inp: [], clip: []};
  const vis = e => { const b = e.getBoundingClientRect(), s = getComputedStyle(e);
    return b.width > 0 && b.height > 0 && s.visibility !== 'hidden' && s.display !== 'none'; };
  document.querySelectorAll('input:not([type=hidden]):not([type=checkbox]):not([type=radio]),select,textarea')
    .forEach(e => { if (vis(e) && parseFloat(getComputedStyle(e).fontSize) < 16) r.inp.push(e.name || e.type); });
  document.querySelectorAll('a[href^="/document/"]').forEach(e => {
    if (vis(e) && e.textContent.trim().length > 3 && !e.querySelector('[class*=line-clamp]') && e.scrollWidth > e.clientWidth + 1) r.clip.push(e.textContent.trim().slice(0, 30)); });
  document.querySelectorAll('button,a.rounded-lg,summary').forEach(e => {
    if (!vis(e)) return; const b = e.getBoundingClientRect();
    const hit = e.classList.contains('tap') ? 44 : b.height;   // .tap legt ein 44x44-Feld darueber
    if (hit < 44 && b.width < 44 && !e.closest('label')) r.small.push((e.textContent || '').trim().slice(0, 20)); });
  return r; }"""


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("m390")
    cfg_dir = tmp / "config"
    shutil.copytree(REPO_CONFIG, cfg_dir)
    s = load_config(cfg_dir)
    for n in ("inbox", "library", "review", "processed", "logs"):
        (tmp / n).mkdir()
        setattr(s.paths, n, tmp / n)
    s.paths.db = tmp / "library" / "docusort.db"
    s.ai.provider = "bridge"
    db = Database(s.paths.db)
    db.user_create("demo", auth.hash_password("Demo-Passwort-123!"), "admin")
    for i in range(3):
        db.insert_document(DocumentRecord(
            filename=f"d{i}.pdf", original_name=f"d{i}.pdf", category="Rechnungen",
            doc_date="2026-01-15", sender="Beispiel GmbH", subject=LONG, confidence=0.5,
            reasoning="Demo", library_path="", processed_path="", file_size=1, page_count=1,
            ocr_used=False, model="t", input_tokens=0, output_tokens=0, cost_usd=0.0,
            status="review" if i else "filed", content_hash=f"h{i}"))
    from docusort.web.app import create_app
    app = create_app(s, db)
    with socket.socket() as k:
        k.bind(("127.0.0.1", 0)); port = k.getsockname()[1]
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    t = threading.Thread(target=srv.run, daemon=True); t.start()
    for _ in range(100):
        if srv.started:
            break
        time.sleep(0.1)
    yield f"http://127.0.0.1:{port}"
    srv.should_exit = True; t.join(5)


def test_handy_390_kein_seitliches_scrollen_und_ziele(server):
    with sync_api.sync_playwright() as p:
        br = p.chromium.launch()
        ctx = br.new_context(viewport={"width": 390, "height": 844}, has_touch=True, is_mobile=True, locale="de-DE")
        pg = ctx.new_page()
        pg.goto(server + "/login")
        pg.fill("input[name=username]", "demo"); pg.fill("input[name=password]", "Demo-Passwort-123!")
        pg.click("button[type=submit]"); pg.wait_for_load_state("networkidle")
        probleme = []
        for u in PAGES:
            pg.goto(server + u); pg.wait_for_load_state("networkidle"); pg.wait_for_timeout(300)
            r = pg.evaluate(CHECK)
            if r["sw"] > r["vw"]: probleme.append(f"{u}: scrollWidth {r['sw']} > {r['vw']}")
            if r["inp"]: probleme.append(f"{u}: Eingaben < 16 px {r['inp']}")
            if r["clip"]: probleme.append(f"{u}: abgeschnittene Titel {r['clip']}")
            if r["small"]: probleme.append(f"{u}: Ziele ohne Daumenfeld {r['small']}")
        br.close()
    assert not probleme, "\n".join(probleme)
