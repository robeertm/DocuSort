"""Tests for the read-only CONTENT door (1.7.0): text by page and search with snippets.

Run with `python3 -m pytest tests/`. Everything runs on a throw-away library.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from docusort import auth, content_api, content_token, readonly_api, readonly_token  # noqa: E402
from docusort.config import load_config  # noqa: E402
from docusort.db import Database, DocumentRecord  # noqa: E402

REPO_CONFIG = Path(__file__).resolve().parent.parent / "config"
WITHHELD = ("library_path", "processed_path", "cost_usd", "input_tokens",
            "output_tokens", "original_name", "content_hash")
IBAN = "DE89 3704 0044 0532 0130 00"
CARD = "4111 1111 1111 1111"


def text_pdf(path: Path, pages: list[str]) -> Path:
    """A minimal valid PDF with one line of real text per page (Helvetica)."""
    objs: list[bytes] = []
    kids = []
    n_pages = len(pages)
    # 1 catalog, 2 pages, 3 font, then (page, content) pairs
    for i, line in enumerate(pages):
        kids.append(4 + 2 * i)
    objs.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objs.append(("<< /Type /Pages /Kids [%s] /Count %d >>" % (
        " ".join("%d 0 R" % k for k in kids), n_pages)).encode())
    objs.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    for i, line in enumerate(pages):
        esc = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        stream = ("BT /F1 10 Tf 20 200 Td (%s) Tj ET" % esc).encode("latin-1")
        objs.append(("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 400 280] "
                     "/Resources << /Font << /F1 3 0 R >> >> /Contents %d 0 R >>" % (5 + 2 * i)).encode())
        objs.append(b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream")
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for nr, body in enumerate(objs, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % nr + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1)
    for o in offsets:
        out += b"%010d 00000 n \n" % o
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objs) + 1, xref)
    path.write_bytes(bytes(out))
    return path


def _record(library_path, **over) -> DocumentRecord:
    felder = dict(
        filename="2026-10-01_Rechnung_Stadtwerke.pdf", original_name="scan_0001.pdf",
        category="Rechnungen", doc_date="2026-10-01", sender="Stadtwerke",
        subject="Abschlag Oktober", confidence=0.9, reasoning="Monthly instalment.",
        library_path=str(library_path), processed_path="/data/processed/scan_0001.pdf",
        file_size=1234, page_count=None, ocr_used=False, model="test",
        input_tokens=10, output_tokens=5, cost_usd=0.01, status="filed",
        extracted_text="",
    )
    felder.update(over)
    return DocumentRecord(**felder)


@pytest.fixture()
def env(tmp_path, monkeypatch):
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
    settings.ai.provider = "openai_compat"
    settings.ai.base_url = "http://127.0.0.1:9"
    monkeypatch.setenv(readonly_api.ALLOW_ENV, "any")

    db = Database(settings.paths.db)
    seiten = ["Stadtwerke Rechnung Oktober Kundennummer 4711",
              "Ihr monatlicher Abschlag betraegt 87,50 EUR ab November",
              "Bankverbindung IBAN %s Karte %s" % (IBAN, CARD)]
    pdf = text_pdf(lib / "rechnung.pdf", seiten)
    scan = lib / "scan.txt"
    scan.write_text("x", encoding="utf-8")
    ids = {
        "pdf": db.insert_document(_record(pdf, extracted_text="\n".join(seiten))),
        "stored": db.insert_document(_record(scan, filename="Versicherung.pdf",
                                             subject="Kfz-Versicherung Police",
                                             sender="Versicherung Beispiel",
                                             extracted_text="Die Police laeuft bis 31.12.2027 "
                                                            "Vertragsnummer KV-998877")),
        "dup": db.insert_document(_record(pdf, status="duplicate",
                                          extracted_text="Abschlag doppelt")),
    }
    from docusort.web.app import create_app
    app = create_app(settings, db)
    token = content_token.new_token(cfg_dir)
    return {"client": TestClient(app), "token": token, "ids": ids, "db": db,
            "cfg": cfg_dir, "pdf": pdf}


def _hdr(token: str) -> dict[str, str]:
    return {"Authorization": "Bearer " + token}


def _no_leak(body: str, env) -> None:
    for name in WITHHELD:
        assert name not in body, name
    assert str(env["pdf"].parent) not in body
    assert "/data/processed" not in body


PATHS = ("/api/content/text?id=1", "/api/content/search?q=Abschlag")


# ── allowlist and gate ──────────────────────────────────────────────────────

def test_allowlist_holds_exact_paths_only():
    for p in ("/api/content/text", "/api/content/search"):
        assert p in auth.PUBLIC_PATHS and auth.is_public_path(p)
    for p in ("/api/content/", "/api/content/other", "/api/content/text/x", "/api/content"):
        assert not auth.is_public_path(p), p


@pytest.mark.parametrize("path", PATHS)
def test_token_required(env, path):
    c = env["client"]
    assert c.get(path).status_code == 401
    assert c.get(path, headers=_hdr("wrong")).status_code == 401
    assert c.get(path, headers=_hdr(env["token"])).status_code == 200


@pytest.mark.parametrize("path", PATHS)
def test_readonly_token_does_not_open_content_door(env, path):
    other = readonly_token.new_token(env["cfg"])
    assert env["client"].get(path, headers=_hdr(other)).status_code == 401
    # …and the content token does not open the listing door
    r = env["client"].get("/api/readonly/documents", headers=_hdr(env["token"]))
    assert r.status_code == 401


@pytest.mark.parametrize("path", PATHS)
def test_off_without_token_file(env, path):
    content_token.disable(env["cfg"])
    assert env["client"].get(path, headers=_hdr(env["token"])).status_code == 404


def test_network_check(env, monkeypatch):
    monkeypatch.setenv(readonly_api.ALLOW_ENV, "10.99.0.0/16")
    r = env["client"].get(PATHS[0], headers=_hdr(env["token"]))
    assert r.status_code == 403


def test_failure_brake(env):
    c = env["client"]
    for _ in range(10):
        assert c.get(PATHS[0], headers=_hdr("x")).status_code == 401
    assert c.get(PATHS[0], headers=_hdr(env["token"])).status_code == 429


def test_volume_brake(env, monkeypatch):
    lim = content_api.RateLimit(max_requests=3, window=60)
    assert [lim.take() for _ in range(3)] == [0, 0, 0]
    assert lim.take() > 0


# ── text ────────────────────────────────────────────────────────────────────

def test_text_by_page_from_text_layer(env):
    r = env["client"].get("/api/content/text?id=%d" % env["ids"]["pdf"], headers=_hdr(env["token"]))
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] and d["source"] == "pdf" and d["pages_total"] == 3
    assert [p["seite"] for p in d["pages"]] == [1, 2, 3]
    assert "87,50 EUR" in d["pages"][1]["text"]
    assert d["document"]["title"] == "Abschlag Oktober"
    _no_leak(r.text, env)


def test_text_masks_iban_and_card(env):
    r = env["client"].get("/api/content/text?id=%d&seite=3" % env["ids"]["pdf"], headers=_hdr(env["token"]))
    d = r.json()
    assert [p["seite"] for p in d["pages"]] == [3]
    t = d["pages"][0]["text"]
    assert IBAN not in t and IBAN.replace(" ", "") not in t and "3704" not in t
    assert "3000" in t or "30 00" in t or "•" in t
    assert CARD not in t and "1111 1111 1111 1111" not in t
    assert d["masked"]["iban"] == 1 and d["masked"]["card"] == 1


def test_page_is_capped(env):
    r = env["client"].get("/api/content/text?id=%d&seite=99" % env["ids"]["pdf"], headers=_hdr(env["token"]))
    assert [p["seite"] for p in r.json()["pages"]] == [3]


def test_text_without_text_layer_falls_back_to_stored_text(env):
    r = env["client"].get("/api/content/text?id=%d" % env["ids"]["stored"], headers=_hdr(env["token"]))
    d = r.json()
    assert d["source"] == "stored"
    assert d["pages"] == [{"seite": None, "text": "Die Police laeuft bis 31.12.2027 Vertragsnummer •••877"}]


@pytest.mark.parametrize("key", ["dup"])
def test_hidden_documents(env, key):
    r = env["client"].get("/api/content/text?id=%d" % env["ids"][key], headers=_hdr(env["token"]))
    assert r.status_code == 404


def test_text_unknown_and_bad_id(env):
    c = env["client"]
    assert c.get("/api/content/text?id=999", headers=_hdr(env["token"])).status_code == 404
    for raw in ("x", "0", "-1", ""):
        assert c.get("/api/content/text?id=" + raw, headers=_hdr(env["token"])).status_code == 400


def test_text_is_limited(env, monkeypatch):
    monkeypatch.setattr(content_api, "TEXT_MAX_CHARS", 60)
    d = env["client"].get("/api/content/text?id=%d" % env["ids"]["pdf"], headers=_hdr(env["token"])).json()
    assert sum(len(p["text"]) for p in d["pages"]) <= 60
    assert d["truncated"] is True


# ── search ──────────────────────────────────────────────────────────────────

def test_search_finds_content_with_snippet_and_page(env):
    r = env["client"].get("/api/content/search?q=Abschlag%20November", headers=_hdr(env["token"]))
    d = r.json()
    assert d["ok"] and d["count"] == 1
    hit = d["hits"][0]
    assert hit["id"] == env["ids"]["pdf"]
    assert "[" in hit["snippet"] and "]" in hit["snippet"]
    assert hit["page"] == 2
    _no_leak(r.text, env)


def test_search_snippet_is_masked(env):
    d = env["client"].get("/api/content/search?q=Bankverbindung", headers=_hdr(env["token"])).json()
    assert d["count"] == 1
    assert "3704" not in d["hits"][0]["snippet"]
    assert d["hits"][0]["page"] == 3


def test_search_stored_text_and_hidden(env):
    d = env["client"].get("/api/content/search?q=Police%20laeuft", headers=_hdr(env["token"])).json()
    assert [h["id"] for h in d["hits"]] == [env["ids"]["stored"]]
    d = env["client"].get("/api/content/search?q=doppelt", headers=_hdr(env["token"])).json()
    assert d["count"] == 0


def test_search_params(env):
    c = env["client"]
    assert c.get("/api/content/search", headers=_hdr(env["token"])).status_code == 400
    assert c.get("/api/content/search?q=" + "a" * 121, headers=_hdr(env["token"])).status_code == 400
    assert c.get("/api/content/search?q=x&limit=zz", headers=_hdr(env["token"])).status_code == 400
    assert c.get("/api/content/search?q=Stadtwerke&limit=500", headers=_hdr(env["token"])).status_code == 200


def test_mask_text_leaves_ordinary_numbers():
    t, z = content_api.mask_text("Bestellung 1234567890123 Rechnung 2026-10-01 Betrag 87,50")
    assert t == "Bestellung 1234567890123 Rechnung 2026-10-01 Betrag 87,50" and z["iban"] == z["card"] == 0


@pytest.mark.parametrize("roh,rein,raus", [
    ("Vertragsnummer: AB-123-XYZ", "•••XYZ", "AB-123"),
    ("Ihr Vertrag 4711-0815-22", "•••522", "4711"),
    ("Vertrags-Nr. VK 2024 7781 33", "•••133", "7781"),
    ("Kfz-Kennzeichen: BZ-AB 123", "•••", "BZ-AB"),
    ("Kennzeichen: BZ AB 1234", "•••", "AB 1234"),
    ("amtl. Kennzeichen BZ-RM 42", "•••", "BZ-RM"),
    ("Fahrzeug BZ-RM 4242", "•••", "4242"),
    ("iban: de89 3704 0044 0532 0130 00", "DE••", "3704"),
    ("IBAN: DE89  3704  0044  0532  0130  00", "DE••", "0044"),
    ("Zählernummer 1ESY1160123456", "•••456", "1160123"),
    ("Versicherungsschein Vertragsnummer KV-998877-12 Kunde 4711", "•••712", "998877"),
    ("Rechnungs-Nr. R45671407 25,09 EUR", "•••407 25,09 EUR", "4567"),
    ("Deine Vertragsnummer: AB-123-XYZ, bitte", "•••XYZ, bitte", "AB-123"),
    ("Zahlungsweise jährlichBZ-XD 212 29.900 km", "jährlich••• 29.900 km", "BZ-XD"),
])
def test_mask_text_1_7_1(roh, rein, raus):
    t, _ = content_api.mask_text(roh)
    assert rein in t and raus not in t, t


@pytest.mark.parametrize("roh", ["Beitrag 412,30 EUR ab 01.01.2026", "Ihr Vertrag vom 3.11.2026",
                                 "Kontoauszug 10/2026", "Abschlag 87,50 EUR monatlich", "Vertrag mit der GmbH",
                                 "Mo 12 Uhr bis Fr 18 Uhr geöffnet", "in KW 45 und KW 46 wird abgelesen",
                                 "Zählerstand 12345 kWh", "Zählerstand neu 46 912,4 kWh", "Rechnungsbetrag 99,99 €",
                                 "Gesamtbetrag 12.345,67 €", "Am 12 Mai kommt der Ableser vorbei"])
def test_mask_text_laesst_betraege_und_daten(roh):
    assert content_api.mask_text(roh)[0] == roh


def test_iban_braucht_gueltige_pruefsumme():
    assert content_api.iban_gueltig("DE89370400440532013000")
    assert not content_api.iban_gueltig("DE89370400440532013001")
    assert not content_api.iban_gueltig("MO12UHRBISFR18")
    t, z = content_api.mask_text("iban de89 3704 0044\n0532 0130 00 bitte")
    assert "3704" not in t and t.endswith(" bitte") and z["iban"] == 1
