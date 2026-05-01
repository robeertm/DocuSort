"""FastAPI application for DocuSort's web UI.

The watcher runs in a separate thread (started by main.py). This module
exposes a thin, synchronous HTTP layer over the shared SQLite database
and the inbox / library folders.
"""

from __future__ import annotations

import logging
import shutil
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..classifier import Classifier
from ..config import AppSettings, get_api_key, is_configured, load_secrets
from ..db import Database, MODEL_PRICING
from ..i18n import (
    LANGUAGE_NAMES, SUPPORTED, all_translations_for_js, category_label,
    detect_language, subcategory_label, translate,
)
from ..providers import PROVIDERS
from ..providers.pricing import all_pricing
from .. import __version__


logger = logging.getLogger("docusort.web")
ALLOWED_SUFFIXES = {".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}


def _fs_shortcuts(settings) -> list[dict]:
    """Quick-jump targets shown above the directory list. Each shortcut is
    only included when it actually exists and is readable — irrelevant entries
    just hide themselves rather than rendering as broken jumps. The library's
    parent directory is included so the user can drop a backup folder right
    next to where the library already lives, without navigating through the
    whole filesystem."""
    home = Path.home()
    candidates: list[tuple[str, str]] = [
        ("Home", str(home)),
    ]

    # Add the library's parent — useful default location for a sibling
    # backup folder. We label it with the parent's basename so the user
    # immediately recognises it as "next to my library".
    try:
        lib_parent = settings.paths.library.parent
        if lib_parent.is_dir() and lib_parent != home:
            candidates.append((lib_parent.name or str(lib_parent), str(lib_parent)))
    except (AttributeError, OSError):
        pass

    candidates += [
        ("/mnt",   "/mnt"),
        ("/media", "/media"),
        ("/tmp",   "/tmp"),
        ("/data",  "/data"),
    ]

    out = []
    seen: set[str] = set()
    for label, path in candidates:
        try:
            p = Path(path)
            if not p.is_dir():
                continue
            if str(p) in seen:
                continue
            seen.add(str(p))
            out.append({"label": label, "path": str(p)})
        except (OSError, PermissionError):
            pass
    return out


def _human_size(n: int | None) -> str:
    if not n:
        return "–"
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _eur(usd: float) -> str:
    return f"{usd * 0.93:.2f} €"


def _usd(usd: float) -> str:
    if usd < 0.01:
        return f"${usd:.4f}"
    return f"${usd:.2f}"


def _coerce_int(v: str | int | None) -> int | None:
    """Treat empty form values as missing. FastAPI's `int | None` rejects
    "" with a 422, but the filter forms emit "" for "all"-style choices —
    coerce here instead of raising."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _coerce_float(v: str | float | None) -> float | None:
    """Same idea as _coerce_int, used for amount-min / amount-max filters."""
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None


# Per-document long-running jobs (statement / receipt extraction). Process-
# local, in-memory: a request that hits api_extract_statement registers
# itself here so concurrent doc-page reloads can see "Auswertung läuft"
# instead of falling back to the stale "klick Auswerten" prompt. The
# registry is wiped on restart, which is fine — the worst case is a stuck
# entry whose request thread already died, and the cleanup_after grace
# below times those out automatically.
_doc_jobs: dict[int, dict] = {}
_doc_jobs_lock = threading.Lock()
_DOC_JOB_MAX_AGE_S = 30 * 60  # entries older than this are considered orphaned


def _doc_job_start(doc_id: int, kind: str) -> bool:
    """Register a running job for `doc_id`. Returns False if a job is
    already registered for this document — used to short-circuit concurrent
    clicks from refresh-happy users without piling extractions onto the
    same expensive PDF."""
    now = time.time()
    with _doc_jobs_lock:
        existing = _doc_jobs.get(doc_id)
        if existing and (now - existing["started_at"]) < _DOC_JOB_MAX_AGE_S:
            return False
        _doc_jobs[doc_id] = {"kind": kind, "started_at": now}
        return True


def _doc_job_end(doc_id: int) -> None:
    with _doc_jobs_lock:
        _doc_jobs.pop(doc_id, None)


def _doc_job_status(doc_id: int) -> dict | None:
    now = time.time()
    with _doc_jobs_lock:
        j = _doc_jobs.get(doc_id)
        if not j:
            return None
        elapsed = now - j["started_at"]
        if elapsed > _DOC_JOB_MAX_AGE_S:
            # Probably orphaned by a process restart or hard crash. Drop
            # it so the UI doesn't lie about an extraction that no longer
            # has a thread behind it.
            _doc_jobs.pop(doc_id, None)
            return None
        return {"kind": j["kind"], "started_at": j["started_at"], "elapsed_s": elapsed}


def create_app(
    settings: AppSettings,
    db: Database,
    classifier: Classifier | None = None,
) -> FastAPI:
    app = FastAPI(title="DocuSort", version=__version__)
    templates_dir = Path(__file__).parent / "templates"
    static_dir = Path(__file__).parent / "static"
    static_dir.mkdir(exist_ok=True)

    templates = Jinja2Templates(directory=str(templates_dir))
    templates.env.filters["human_size"] = _human_size
    templates.env.filters["eur"] = _eur
    templates.env.filters["usd"] = _usd

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # ---------- First-run gate ----------
    # Paths that always work even when the install is unconfigured (the
    # wizard, language switcher, static files, and a few read-only API
    # endpoints the wizard itself calls).
    _setup_open = (
        "/setup", "/static/", "/upload-sw.js",
        "/api/setup/", "/api/settings/", "/api/sync/", "/api/language/",
        "/api/version", "/api/pricing",
    )

    @app.middleware("http")
    async def first_run_gate(request: Request, call_next):
        path = request.url.path
        if not is_configured(settings) and not any(
            path == p or path.startswith(p) for p in _setup_open
        ):
            # Browsers get a real redirect, JSON callers get 503 so they
            # can show a clean error instead of an HTML page.
            if request.headers.get("accept", "").startswith("application/json"):
                return JSONResponse(
                    {"detail": "DocuSort is not yet configured — open /setup"},
                    status_code=503,
                )
            return RedirectResponse("/setup", status_code=303)
        return await call_next(request)

    # Serve the upload service worker at root so its default scope is "/".
    # A SW at /static/upload-sw.js would only control /static/*.
    @app.get("/upload-sw.js", include_in_schema=False)
    def upload_sw():
        return FileResponse(
            str(static_dir / "upload-sw.js"),
            media_type="application/javascript",
            headers={
                "Cache-Control": "no-cache, must-revalidate",
                "Service-Worker-Allowed": "/",
            },
        )

    category_names = [c["name"] for c in settings.categories]
    subcategory_map: dict[str, list[str]] = {
        c["name"]: list(c.get("subcategories") or []) for c in settings.categories
    }

    def _lang(request: Request) -> str:
        return detect_language(
            cookie=request.cookies.get("lang"),
            accept_language=request.headers.get("accept-language"),
            default=settings.web.default_language,
        )

    def base_ctx(request: Request) -> dict:
        lang = _lang(request)
        # Pre-compute the localised label map for the JS-driven subcategory
        # dropdown — Alpine looks up sub_labels[category][canonical] = label.
        sub_labels = {
            cat: {sub: subcategory_label(cat, sub, lang) for sub in subs}
            for cat, subs in subcategory_map.items()
        }
        return {
            "request": request,
            "version": __version__,
            "categories": category_names,
            "subcategory_map": subcategory_map,
            "subcategory_labels": sub_labels,
            "lang": lang,
            "supported_langs": [(code, LANGUAGE_NAMES[code]) for code in SUPPORTED],
            "t": lambda key, **kw: translate(key, lang, **kw),
            "cat": lambda name: category_label(name, lang),
            "sub": lambda parent, name: subcategory_label(parent, name, lang),
            "js_translations": all_translations_for_js(lang),
        }

    # ---------- Dashboard ----------
    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request):
        stats = db.stats()
        recent = db.list_documents(limit=8, order_by="created_at")
        review_count = db.count_documents(status="review")
        # Cheap library-wide duplicate count (groups, not docs) for the
        # dashboard hint. Same query the /duplicates page uses, just
        # the COUNT.
        with db._lock:
            dup_row = db._conn.execute(
                """SELECT COUNT(*) AS n FROM (
                     SELECT 1 FROM documents
                     WHERE deleted_at IS NULL
                       AND content_hash IS NOT NULL AND content_hash != ''
                     GROUP BY content_hash HAVING COUNT(*) > 1
                   )"""
            ).fetchone()
            duplicate_groups = int(dup_row["n"]) if dup_row else 0
        return templates.TemplateResponse(
            request, "dashboard.html",
            {**base_ctx(request), "stats": stats, "recent": recent,
             "review_count": review_count,
             "duplicate_groups": duplicate_groups},
        )

    # ---------- Library ----------
    @app.get("/library", response_class=HTMLResponse)
    def library(
        request: Request,
        category: str | None = Query(None),
        subcategory: str | None = Query(None),
        tag: str | None = Query(None),
        status: str | None = Query(None),
        year: str | None = Query(None),
        q: str | None = Query(None),
        trash: bool = Query(False),
        partial: bool = Query(False),
    ):
        docs = db.list_documents(
            category=category or None, subcategory=subcategory or None,
            tag=tag or None, status=status or None,
            year=year or None, query=q or None, trash=trash, limit=500,
        )
        for d in docs:
            _decode_tags(d)
        years = db.distinct_years()
        tree = db.tree()
        tags = db.all_tags(trash=trash)
        tpl = "_card_grid.html" if partial else "library.html"
        return templates.TemplateResponse(
            request, tpl,
            {**base_ctx(request), "docs": docs, "years": years, "tree": tree,
             "tags": tags, "trash": trash,
             "filter": {"category": category, "subcategory": subcategory,
                        "tag": tag, "status": status, "year": year, "q": q}},
        )

    def _decode_tags(doc: dict) -> dict:
        """Add a `tags_list` Python list alongside the raw JSON `tags` string."""
        import json as _json
        try:
            doc["tags_list"] = _json.loads(doc.get("tags") or "[]") or []
        except Exception:
            doc["tags_list"] = []
        return doc

    # ---------- Document detail ----------
    @app.get("/document/{doc_id}", response_class=HTMLResponse)
    def document_detail(request: Request, doc_id: int):
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "Document not found")
        _decode_tags(doc)
        receipt = db.get_receipt(doc_id) if doc.get("category") == "Kassenzettel" else None
        # Statement card surfaces for Kontoauszug AND any legacy Bank
        # document (the classifier picked Bank for actual statements
        # before v0.13.0, sometimes with subcategory=Konto, sometimes
        # without — depending on which categories.yaml was active at
        # the time). Either way the user gets the manual "extract"
        # button and the privacy preview. Non-statement Bank docs (a
        # contract, a Wertpapier-Abrechnung, …) just return zero
        # transactions on extract — wasted LLM call but not harmful.
        is_statement_candidate = doc.get("category") in ("Kontoauszug", "Bank")
        statement = db.get_statement(doc_id) if is_statement_candidate else None
        from ..receipts import SHOP_TYPES, ITEM_CATEGORIES, PAYMENT_METHODS
        return templates.TemplateResponse(
            request, "document.html",
            {**base_ctx(request), "doc": doc, "receipt": receipt,
             "statement": statement,
             "is_statement_candidate": is_statement_candidate,
             "ai_provider": settings.ai.provider,
             "shop_types": list(SHOP_TYPES),
             "item_categories": list(ITEM_CATEGORIES),
             "payment_methods": list(PAYMENT_METHODS)},
        )

    @app.get("/document/{doc_id}/file")
    def document_file(doc_id: int, download: bool = False):
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "Document not found")
        path = Path(doc["library_path"])
        if not path.exists():
            raise HTTPException(404, "File missing on disk")
        headers = {}
        if download:
            headers["Content-Disposition"] = f'attachment; filename="{path.name}"'
        media = "application/pdf" if path.suffix.lower() == ".pdf" else "application/octet-stream"
        return FileResponse(path, media_type=media, headers=headers)

    @app.post("/document/{doc_id}/edit")
    def edit_document(
        doc_id: int,
        category: str = Form(...),
        subcategory: str = Form(""),
        tags: str = Form(""),
        doc_date: str = Form(""),
        sender: str = Form(""),
        subject: str = Form(""),
    ):
        from ..organizer import target_path

        if category not in category_names:
            raise HTTPException(400, f"Unknown category: {category}")
        allowed_subs = subcategory_map.get(category, [])
        sub = subcategory.strip()
        if sub and sub not in allowed_subs:
            raise HTTPException(400, f"Unknown subcategory {sub!r} under {category}")

        # tags: comma-separated text → cleaned list of <=3 lowercase short labels
        tag_list: list[str] = []
        seen: set[str] = set()
        for raw in tags.split(","):
            t = raw.strip().lower()
            if t and t not in seen and len(t) <= 32:
                tag_list.append(t)
                seen.add(t)
            if len(tag_list) >= 8:
                break

        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "Document not found")
        if doc.get("deleted_at"):
            raise HTTPException(400, "Document is in trash — restore first")

        old_path = Path(doc["library_path"])
        if not old_path.exists():
            raise HTTPException(404, "File missing on disk")

        new_path = target_path(
            settings.paths.library,
            doc_date or doc["created_at"][:10],
            category, sender.strip(), subject.strip(),
            settings.filename_template,
            settings.max_filename_length,
            old_path.suffix,
            subcategory=sub,
            current_path=old_path,
        )
        if new_path != old_path:
            shutil.move(str(old_path), str(new_path))

        db.update_metadata(
            doc_id,
            category=category,
            subcategory=sub,
            tags=tag_list,
            doc_date=doc_date,
            sender=sender.strip(),
            subject=subject.strip(),
            filename=new_path.name,
            library_path=str(new_path),
        )
        logger.info("Edited doc %d -> %s (sub=%s tags=%s)",
                    doc_id, new_path.name, sub, tag_list)
        return RedirectResponse(f"/document/{doc_id}", status_code=303)

    # ---------- Upload ----------
    @app.get("/upload", response_class=HTMLResponse)
    def upload_page(request: Request):
        return templates.TemplateResponse(
            request, "upload.html", {**base_ctx(request)},
        )

    @app.post("/upload")
    async def upload_file(files: list[UploadFile] = File(...)):
        saved = []
        rejected = []
        for up in files:
            suffix = Path(up.filename or "").suffix.lower()
            if suffix not in ALLOWED_SUFFIXES:
                rejected.append(up.filename)
                continue

            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            unique = uuid.uuid4().hex[:6]
            safe_name = f"{stamp}-{unique}{suffix}"
            target = settings.paths.inbox / safe_name
            with target.open("wb") as f:
                shutil.copyfileobj(up.file, f)
            saved.append({"inbox_name": safe_name, "original_name": up.filename})
            logger.info("Uploaded %s -> %s", up.filename, safe_name)

        return {"saved": saved, "rejected": rejected}

    @app.get("/api/status/{inbox_name}")
    def upload_status(inbox_name: str):
        """Tell the upload UI whether the pipeline is done with a given file.

        States:
          queued     — file still sits in inbox/, waiting for stable size
          processing — file still in inbox/ and has been there >5s (OCR running)
          done       — classified and filed, doc_id + category returned
          review     — classified with low confidence, doc_id returned
          failed     — OCR or classification failed, doc_id returned
          duplicate  — SHA256 matched an existing document
          unknown    — neither in inbox nor in DB (cleaned up without record)
        """
        inbox_file = settings.paths.inbox / inbox_name
        if inbox_file.exists():
            age = datetime.now().timestamp() - inbox_file.stat().st_mtime
            return {"status": "processing" if age > settings.stable_seconds else "queued"}

        # Not in inbox → search DB by original_name (matches the safe_name we wrote)
        d = db.find_by_original_name(inbox_name)
        if d:
            return {
                "status": d["status"],  # filed | review | failed | duplicate
                "doc_id": d["id"],
                "category": d["category"],
                "confidence": d["confidence"],
                "cost_usd": d["cost_usd"],
            }
        return {"status": "unknown"}

    # ---------- JSON stats (for the cost chart) ----------
    @app.get("/api/stats")
    def api_stats():
        return db.stats()

    @app.get("/api/pricing")
    def api_pricing():
        return {
            "models": {
                prefix: {"input_per_mtok": inp, "output_per_mtok": out}
                for prefix, (inp, out) in MODEL_PRICING.items()
            }
        }

    # ---------- Language ----------
    @app.post("/api/language/{lang}")
    def set_language(lang: str):
        if lang not in SUPPORTED:
            raise HTTPException(400, f"Unsupported language: {lang}")
        resp = JSONResponse({"lang": lang})
        # one-year cookie; SameSite=Lax plays nice with the PR-style
        # navigation the UI does after switching language.
        resp.set_cookie(
            "lang", lang, max_age=365 * 24 * 3600,
            samesite="lax", httponly=False, path="/",
        )
        return resp

    # ---------- Receipts (Kassenzettel) ----------
    @app.get("/analytics", response_class=HTMLResponse)
    def analytics_page(
        request: Request,
        shop_type: str | None = Query(None),
        start: str | None = Query(None),
        end: str | None = Query(None),
        q: str | None = Query(None),
    ):
        from ..receipts import SHOP_TYPES, ITEM_CATEGORIES
        summary  = db.receipt_summary()
        monthly  = db.receipt_monthly(months=12)
        receipts = db.receipts_list(shop_type=shop_type, start=start, end=end, limit=50)
        items    = db.receipt_items_search(query=q, shop_type=shop_type,
                                            start=start, end=end, limit=200)
        top      = db.top_items(limit=15)
        return templates.TemplateResponse(
            request, "analytics.html",
            {**base_ctx(request),
             "summary": summary, "monthly": monthly,
             "receipts": receipts, "items": items, "top_items": top,
             "shop_types": list(SHOP_TYPES),
             "item_categories": list(ITEM_CATEGORIES),
             "filter": {"shop_type": shop_type, "start": start, "end": end, "q": q}},
        )

    @app.get("/api/receipts/stats")
    def api_receipts_stats():
        return {
            "summary": db.receipt_summary(),
            "monthly": db.receipt_monthly(months=12),
            "top_items": db.top_items(limit=15),
        }

    @app.get("/api/receipts/items")
    def api_receipts_items(
        q: str | None = Query(None),
        item_category: str | None = Query(None),
        shop_type: str | None = Query(None),
        start: str | None = Query(None),
        end: str | None = Query(None),
        limit: int = Query(200),
    ):
        return {"items": db.receipt_items_search(
            query=q, item_category=item_category, shop_type=shop_type,
            start=start, end=end, limit=limit,
        )}

    @app.post("/api/document/{doc_id}/receipt/extract")
    def api_extract_receipt(doc_id: int):
        if classifier is None:
            raise HTTPException(503, "classifier not available — finish /setup first")
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "document not found")
        text = doc.get("extracted_text") or ""
        if not text:
            raise HTTPException(400, "no OCR text stored — re-classify the document first")
        if not _doc_job_start(doc_id, "receipt"):
            raise HTTPException(
                409,
                "extraction already running for this document — wait for the "
                "current run to finish before retrying",
            )
        try:
            from ..receipts import ReceiptExtractor
            extractor = ReceiptExtractor(
                classifier.provider, settings.ai.model,
                max_text_chars=settings.ai.max_text_chars,
                holder_names=settings.finance.holder_names,
                pseudonymize=settings.finance.pseudonymize,
            )
            try:
                r = extractor.extract(text)
            except Exception as exc:
                logger.exception("Receipt extract failed for %d", doc_id)
                raise HTTPException(500, f"extract failed: {exc}")
            db.upsert_receipt(
                doc_id,
                shop_name=r.shop_name, shop_type=r.shop_type,
                payment_method=r.payment_method, total_amount=r.total_amount,
                currency=r.currency,
                receipt_date=r.receipt_date or doc.get("doc_date") or "",
                items=[i.as_dict() for i in r.items],
                extra_json=r.raw_response,
            )
            return {"ok": True, "items": len(r.items),
                    "total": r.total_amount, "shop": r.shop_name}
        finally:
            _doc_job_end(doc_id)

    @app.patch("/api/document/{doc_id}/receipt")
    def api_patch_receipt(doc_id: int, payload: dict):
        """Manual edit: header fields + full items list, atomic replace.

        The frontend always submits the whole list (no per-row PATCH) so we
        keep line ordering deterministic and avoid drift between client and
        server. OCR errors on totals or item names get fixed here without
        a re-extract round trip."""
        from ..receipts import SHOP_TYPES, ITEM_CATEGORIES, PAYMENT_METHODS
        if not db.get(doc_id):
            raise HTTPException(404, "document not found")

        def _opt_float(value, name):
            if value in (None, ""):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                raise HTTPException(400, f"{name} must be a number")

        shop_name = (payload.get("shop_name") or "").strip()[:200]
        shop_type = (payload.get("shop_type") or "").strip().lower()
        if shop_type and shop_type not in SHOP_TYPES:
            raise HTTPException(400, f"shop_type must be one of {list(SHOP_TYPES)}")
        payment_method = (payload.get("payment_method") or "").strip().lower()
        if payment_method and payment_method not in PAYMENT_METHODS:
            raise HTTPException(400, f"payment_method must be one of {list(PAYMENT_METHODS)}")
        total_amount = _opt_float(payload.get("total_amount"), "total_amount")
        currency = (payload.get("currency") or "EUR").strip()[:8] or "EUR"
        receipt_date = (payload.get("receipt_date") or "").strip()[:10]

        raw_items = payload.get("items") or []
        if not isinstance(raw_items, list):
            raise HTTPException(400, "items must be a list")
        items = []
        for idx, it in enumerate(raw_items):
            if not isinstance(it, dict):
                raise HTTPException(400, f"item {idx} must be an object")
            name = (it.get("name") or "").strip()[:200]
            if not name:
                continue   # skip blank rows silently
            cat = (it.get("item_category") or "").strip().lower()
            if cat and cat not in ITEM_CATEGORIES:
                raise HTTPException(400, f"item_category must be one of {list(ITEM_CATEGORIES)}")
            items.append({
                "name": name,
                "quantity":     _opt_float(it.get("quantity"),    f"items[{idx}].quantity"),
                "unit_price":   _opt_float(it.get("unit_price"),  f"items[{idx}].unit_price"),
                "total_price":  _opt_float(it.get("total_price"), f"items[{idx}].total_price"),
                "item_category": cat,
            })

        db.upsert_receipt(
            doc_id,
            shop_name=shop_name, shop_type=shop_type,
            payment_method=payment_method, total_amount=total_amount,
            currency=currency, receipt_date=receipt_date,
            items=items, extra_json="manual_edit",
        )
        return {"ok": True, "items": len(items), "total": total_amount}

    # ---------- Finance (Kontoauszüge) ----------
    @app.get("/finance", response_class=HTMLResponse)
    def finance_page(
        request: Request,
        # account_id is declared as a string and parsed manually because the
        # filter form sends an empty value when "All accounts" is selected,
        # and FastAPI's strict int coercion 422s on "" instead of treating
        # it as None.
        account_id: str | None = Query(None),
        category: str | None = Query(None),
        direction: str | None = Query(None),
        start: str | None = Query(None),
        end: str | None = Query(None),
        q: str | None = Query(None),
        heatmap_year:  str | None = Query(None),
        heatmap_month: str | None = Query(None),
        cat_start:     str | None = Query(None),
        cat_end:       str | None = Query(None),
    ):
        account_id = _coerce_int(account_id)
        from ..finance.categories import TX_CATEGORIES, TX_TYPES
        summary       = db.finance_summary()
        monthly       = db.finance_monthly(months=12)
        accounts      = db.list_accounts()
        top_outgoing  = db.finance_top_counterparties(direction="expense", limit=10)
        top_incoming  = db.finance_top_counterparties(direction="income",  limit=10)
        recurring     = db.finance_recurring(min_months=3, limit=20)
        transactions  = db.transactions_list(
            account_id=account_id, category=category, direction=direction,
            start=start, end=end, query=q, limit=200,
        )
        periods = db.finance_available_periods()
        # If the user hasn't explicitly picked a range for the category-
        # trend chart, default to the most recent 12 months that actually
        # contain bookings — full-history bars get too narrow to read on
        # accounts with several years of data, but the user can still
        # open the range to "all" via the dropdown.
        if not cat_start and not cat_end and periods.get("months"):
            visible_months = periods["months"][:12]
            if visible_months:
                cat_start = visible_months[-1]
                cat_end   = visible_months[0]
        # Charts data — only computed when there's actually something
        # to plot, otherwise the empty-state card on /finance covers it.
        heatmap_grid: dict[str, Any] = {
            "mode": "year", "year": "", "month": "",
            "weeks": [], "max_spend": 0.0,
        }
        if summary.get("tx_count", 0) > 0 or summary.get("transfer_count", 0) > 0:
            mode = "month" if heatmap_month else "year"
            heatmap_data    = db.finance_heatmap(year=heatmap_year, month=heatmap_month)
            cat_monthly     = db.finance_category_monthly(start=cat_start, end=cat_end)
            cat_pie_spend   = db.finance_category_totals(start=cat_start, end=cat_end, direction="spend")
            cat_pie_income  = db.finance_category_totals(start=cat_start, end=cat_end, direction="income")
            by_weekday       = db.finance_by_weekday()
            by_day_of_month  = db.finance_by_day_of_month()
            by_tx_type       = db.finance_by_tx_type()
            largest_tx       = db.finance_largest_tx(limit=15)
            balance_history  = db.finance_balance_history(account_id=account_id)
            cp_treemap       = db.finance_counterparty_treemap(limit=24)
            kpis             = db.finance_kpis()

            # Reshape the daily heatmap rows into a week × weekday
            # grid the template can render with a plain double loop.
            # Year mode walks Jan 1 → Dec 31 of the chosen year;
            # month mode walks the whole month with leading blanks
            # so the first day lines up with its weekday column.
            from datetime import date as _date, timedelta as _td
            spend_by_date = {r["date"]: float(r["spend"]) for r in heatmap_data["days"]}
            heatmap_grid = {
                "mode":  heatmap_data["mode"],
                "year":  heatmap_data["year"],
                "month": heatmap_data["month"],
                "weeks": [], "max_spend": 0.0,
            }
            if heatmap_data["days"] or heatmap_data["year"]:
                if heatmap_data["mode"] == "year" and heatmap_data["year"]:
                    yr = int(heatmap_data["year"])
                    start_dt = _date(yr, 1, 1)
                    end_dt   = _date(yr, 12, 31)
                elif heatmap_data["mode"] == "month" and heatmap_data["month"]:
                    ym = heatmap_data["month"]
                    yr_ = int(ym[:4]); mo_ = int(ym[5:7])
                    start_dt = _date(yr_, mo_, 1)
                    if mo_ == 12:
                        end_dt = _date(yr_, 12, 31)
                    else:
                        end_dt = _date(yr_, mo_ + 1, 1) - _td(days=1)
                else:
                    start_dt = end_dt = None

                if start_dt and end_dt:
                    first_monday = start_dt - _td(days=start_dt.weekday())
                    last_sunday  = end_dt + _td(days=6 - end_dt.weekday())
                    # Skip the absolute max — a single end-of-quarter
                    # transfer or account closure can be 100x normal
                    # daily spend, which then squashes every other day
                    # into the lowest colour bin. Use the 90th
                    # percentile so 90 % of days spread across all five
                    # bins; days above p90 get clamped to the brightest
                    # colour.
                    vals = sorted(v for v in spend_by_date.values() if v > 0)
                    if vals:
                        p90_idx = max(0, int(len(vals) * 0.9) - 1)
                        max_spend = max(vals[p90_idx], 1.0)
                    else:
                        max_spend = 0.0
                    weeks = []
                    cur = first_monday
                    while cur <= last_sunday:
                        cells = []
                        for d in range(7):
                            cell_date = cur + _td(days=d)
                            iso = cell_date.isoformat()
                            if cell_date < start_dt or cell_date > end_dt:
                                cells.append({"date": iso, "spend": None, "out": True})
                            else:
                                cells.append({
                                    "date": iso,
                                    "spend": spend_by_date.get(iso, 0.0),
                                    "out": False,
                                })
                        weeks.append({
                            "label": cur.strftime("%d.%m"),
                            "month_label": cur.strftime("%b"),
                            "cells": cells,
                        })
                        cur = cur + _td(weeks=1)
                    heatmap_grid["weeks"]     = weeks
                    heatmap_grid["max_spend"] = max_spend

            # Pre-compute the max-month total for the stacked category
            # chart so the template doesn't need a Jinja namespace just
            # to track a max across an outer loop.
            if cat_monthly["matrix"]:
                cat_monthly["max_total"] = max(
                    (sum(row["values"]) for row in cat_monthly["matrix"]),
                    default=0.0,
                )
            else:
                cat_monthly["max_total"] = 0.0
        else:
            cat_monthly = {"months": [], "categories": [], "matrix": [], "max_total": 0.0}
            cat_pie_spend = []; cat_pie_income = []
            by_weekday = []; by_day_of_month = []; by_tx_type = []
            largest_tx = []; balance_history = []; cp_treemap = []
            kpis = {}
        # Inspect the privacy mode of the most recent statement so the
        # header badge tells the user how their last upload was handled.
        with db._lock:
            last_mode_row = db._conn.execute(
                "SELECT privacy_mode FROM statements ORDER BY id DESC LIMIT 1"
            ).fetchone()
            # Surface "needs review" docs prominently — the user expects
            # every uploaded Kontoauszug to be evaluated, so empties get
            # called out instead of silently inflating the statement
            # count.
            empty_stmts = db._conn.execute(
                """SELECT s.doc_id, COALESCE(d.subject, d.filename) AS subject,
                          d.doc_date, a.bank_name
                   FROM statements s
                   JOIN documents d ON d.id = s.doc_id
                   LEFT JOIN accounts a ON a.id = s.account_id
                   WHERE d.deleted_at IS NULL
                     AND d.category = 'Kontoauszug'
                     AND COALESCE(s.acknowledged_empty, 0) = 0
                     AND s.id NOT IN (SELECT statement_id FROM transactions)
                   ORDER BY d.doc_date DESC, d.id DESC
                   LIMIT 20"""
            ).fetchall()
            kontoauszug_total = db._conn.execute(
                """SELECT COUNT(*) FROM documents
                   WHERE category = 'Kontoauszug' AND deleted_at IS NULL"""
            ).fetchone()[0]
            pending_review = db._conn.execute(
                """SELECT id AS doc_id, COALESCE(subject, filename) AS subject,
                          doc_date, sender
                   FROM documents
                   WHERE status = 'pending_review'
                     AND category = 'Kontoauszug'
                     AND deleted_at IS NULL
                   ORDER BY doc_date DESC, id DESC"""
            ).fetchall()
        privacy_mode = (last_mode_row["privacy_mode"] if last_mode_row else "") or ""
        return templates.TemplateResponse(
            request, "finance.html",
            {**base_ctx(request),
             "summary": summary, "monthly": monthly, "accounts": accounts,
             "top_outgoing": top_outgoing, "top_incoming": top_incoming,
             "recurring": recurring, "transactions": transactions,
             "tx_categories": list(TX_CATEGORIES),
             "tx_types": list(TX_TYPES),
             "privacy_mode": privacy_mode,
             "empty_statements":   [dict(r) for r in empty_stmts],
             "kontoauszug_total":  int(kontoauszug_total),
             "pending_review":     [dict(r) for r in pending_review],
             "heatmap_grid":    heatmap_grid,
             "heatmap_periods": periods,
             "cat_monthly":     cat_monthly,
             "cat_pie_spend":   cat_pie_spend,
             "cat_pie_income":  cat_pie_income,
             "cat_range":       {"start": cat_start, "end": cat_end},
             "by_weekday":      by_weekday,
             "by_day_of_month": by_day_of_month,
             "by_tx_type":      by_tx_type,
             "largest_tx":      largest_tx,
             "balance_history": balance_history,
             "cp_treemap":      cp_treemap,
             "kpis":            kpis,
             "filter": {"account_id": account_id, "category": category,
                        "direction": direction, "start": start, "end": end, "q": q}},
        )

    @app.get("/api/finance/stats")
    def api_finance_stats():
        return {
            "summary": db.finance_summary(),
            "monthly": db.finance_monthly(months=12),
            "accounts": db.list_accounts(),
            "recurring": db.finance_recurring(),
        }

    @app.get("/api/finance/diagnostics")
    def api_finance_diagnostics():
        """Surface statements that came back without any transactions —
        the user wants every uploaded Kontoauszug in the evaluation, so
        we list the gaps explicitly rather than burying them in the
        aggregate counts."""
        with db._lock:
            empty = db._conn.execute(
                """SELECT s.id AS stmt_id, s.doc_id, a.bank_name,
                          s.period_start, s.period_end, s.account_id,
                          d.subject, d.doc_date, d.created_at
                   FROM statements s
                   JOIN documents d ON d.id = s.doc_id
                   LEFT JOIN accounts a ON a.id = s.account_id
                   WHERE d.deleted_at IS NULL
                     AND d.category = 'Kontoauszug'
                     AND s.id NOT IN (SELECT statement_id FROM transactions)
                   ORDER BY d.doc_date DESC, d.id DESC"""
            ).fetchall()
            unattached = db._conn.execute(
                """SELECT COUNT(*) FROM statements s
                   JOIN documents d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL AND s.account_id IS NULL"""
            ).fetchone()[0]
            kontoauszug_total = db._conn.execute(
                """SELECT COUNT(*) FROM documents
                   WHERE category = 'Kontoauszug' AND deleted_at IS NULL"""
            ).fetchone()[0]
            statements_total = db._conn.execute(
                """SELECT COUNT(*) FROM statements s
                   JOIN documents d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND d.category = 'Kontoauszug'"""
            ).fetchone()[0]
        return {
            "kontoauszug_docs":       int(kontoauszug_total),
            "statements_extracted":   int(statements_total),
            "without_transactions":   [dict(r) for r in empty],
            "without_account":        int(unattached),
        }

    @app.post("/api/finance/reocr-all")
    def api_reocr_all():
        """Re-read every Kontoauszug / Bank PDF and refresh the stored
        OCR text. Pure local work — no LLM call, no cost. Fixes
        truncated text from earlier installs that capped at the
        classifier's input limit, leaving the booking table in
        multi-page statements unreadable to the second-pass extractor."""
        from ..ocr import extract_text as _extract_text
        with db._lock:
            rows = db._conn.execute(
                """SELECT id, library_path, length(extracted_text) AS old_len
                   FROM documents
                   WHERE deleted_at IS NULL
                     AND category IN ('Kontoauszug', 'Bank')
                     AND library_path IS NOT NULL"""
            ).fetchall()
        refreshed: list[dict] = []
        failed: list[dict] = []
        for r in rows:
            doc_id = int(r["id"])
            path = Path(r["library_path"])
            if not path.exists():
                failed.append({"doc_id": doc_id, "error": "file missing"})
                continue
            try:
                ocr_res = _extract_text(path, settings.ocr)
            except Exception as exc:
                failed.append({"doc_id": doc_id, "error": str(exc)})
                continue
            new_text = ocr_res.text[:200_000]
            with db._lock:
                db._conn.execute(
                    "UPDATE documents SET extracted_text = ?, ocr_used = ? WHERE id = ?",
                    (new_text, 1 if ocr_res.ocr_used else 0, doc_id),
                )
            refreshed.append({
                "doc_id": doc_id,
                "old_len": int(r["old_len"] or 0),
                "new_len": len(new_text),
            })
        return {"found": len(rows), "refreshed": refreshed, "failed": failed}

    @app.post("/api/finance/analyze-all")
    def api_analyze_all_statements():
        """One-click bulk analysis for every Kontoauszug that does not
        yet have a populated statement row. Covers two distinct cases
        the user can hit:

        - Document classified as Kontoauszug but **no statement row**
          (extraction was skipped at upload time, e.g. the bridge
          was offline, or local_only blocked a cloud call).
        - Statement row exists but has **zero transactions** (extraction
          ran but the model returned an empty/garbled JSON).

        Runs in a background thread so the HTTP request returns
        instantly. Local 7B inference can spend 30–90 s per statement,
        and a backlog of dozens would otherwise run past every
        reasonable HTTP timeout. Poll /api/finance/analyze-progress."""
        if classifier is None:
            raise HTTPException(503, "classifier not available — finish /setup first")
        is_local = settings.ai.provider in ("openai_compat", "bridge")
        if settings.finance.local_only and not is_local:
            raise HTTPException(
                403,
                "finance.local_only is enabled but the active provider is not local.",
            )
        from .. import activity
        existing = activity.get_job("analyze-statements")
        if existing.running:
            return {"started": False, "reason": "already running",
                    **existing.as_dict()}

        # Two SELECTs unioned manually so we can keep the queries
        # readable. Both return doc rows with stored OCR text — we
        # never re-OCR, since /api/finance/reocr-all is the entry
        # point for that.
        #
        # The "missing" leg also catches legacy Bank-tagged docs that
        # are clearly statements (subject mentions Kontoauszug /
        # Girokonto / Tagesgeld / Kreditkarte / paypal-auszug, or the
        # subcategory says Konto). After successful extraction the
        # worker promotes them to category=Kontoauszug so /finance and
        # all category filters pick them up — same logic backfill
        # already uses.
        with db._lock:
            missing = db._conn.execute(
                """SELECT d.id AS doc_id, d.category AS category,
                          COALESCE(d.subject, d.filename) AS subject,
                          d.extracted_text AS text
                   FROM documents d
                   LEFT JOIN statements s ON s.doc_id = d.id
                   WHERE d.deleted_at IS NULL
                     AND s.id IS NULL
                     AND d.extracted_text IS NOT NULL AND d.extracted_text != ''
                     AND (
                       d.category = 'Kontoauszug'
                       OR (d.category = 'Bank' AND (
                         d.subcategory = 'Konto'
                         OR d.subcategory = 'Karte'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%kontoauszug%'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%girokonto%'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%tagesgeld%'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%kreditkart%'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%paypal%auszug%'
                       ))
                     )"""
            ).fetchall()
            empty = db._conn.execute(
                """SELECT d.id AS doc_id, d.category AS category,
                          COALESCE(d.subject, d.filename) AS subject,
                          d.extracted_text AS text
                   FROM documents d
                   JOIN statements s ON s.doc_id = d.id
                   WHERE d.deleted_at IS NULL
                     AND COALESCE(s.acknowledged_empty, 0) = 0
                     AND s.id NOT IN (SELECT statement_id FROM transactions)
                     AND d.extracted_text IS NOT NULL AND d.extracted_text != ''"""
            ).fetchall()

        # Dedup on doc_id (a doc could in theory match both queries,
        # though our schema keeps them disjoint). Order: missing first,
        # then empty — feels more natural progress-wise.
        seen: set[int] = set()
        targets: list[tuple[int, str, str, str]] = []  # (doc_id, subject, text, category)
        for r in list(missing) + list(empty):
            doc_id = int(r["doc_id"])
            if doc_id in seen:
                continue
            seen.add(doc_id)
            cat = (r["category"] if "category" in r.keys() else None) or ""
            targets.append((doc_id, r["subject"] or f"doc {doc_id}", r["text"], cat))

        if not targets:
            return {"started": False, "reason": "nothing to analyse",
                    "total": 0, "approved": [], "failed": []}

        do_pseudo = settings.finance.pseudonymize and not is_local
        activity.start_job("analyze-statements", total=len(targets))

        def worker():
            from ..finance import StatementExtractor
            from hashlib import sha256
            import time as _time
            extractor = StatementExtractor(
                classifier.provider, settings.ai.model,
                max_text_chars=max(settings.ai.max_text_chars, 32000),
                holder_names=settings.finance.holder_names,
            )
            for idx, (doc_id, subject, text, category) in enumerate(targets):
                if idx > 0:
                    _time.sleep(0.6)
                activity.update_job(
                    "analyze-statements",
                    current=str(subject)[:120], current_doc_id=doc_id,
                )
                try:
                    stmt = extractor.extract(text, pseudonymize=do_pseudo)
                    if not stmt.transactions:
                        # Persist the empty result with whatever
                        # metadata we got — diag banner will still
                        # call it out and the user can retry per-doc.
                        db.upsert_statement(
                            doc_id, account_id=None,
                            period_start=stmt.period_start,
                            period_end=stmt.period_end,
                            statement_no=stmt.statement_no,
                            opening_balance=stmt.opening_balance,
                            closing_balance=stmt.closing_balance,
                            currency=stmt.currency, file_hash="",
                            privacy_mode=stmt.privacy_mode,
                            transactions=[],
                            extra_json=stmt.raw_response,
                        )
                        job = activity.get_job("analyze-statements")
                        job.failed.append({"doc_id": doc_id,
                                           "error": "no transactions extracted"})
                        activity.update_job(
                            "analyze-statements", done=idx + 1,
                            last_error="empty result",
                        )
                        continue
                    account_id = None
                    if stmt.iban_hash:
                        account_id = db.upsert_account(
                            bank_name=stmt.bank_name or "Unbekannt",
                            iban=stmt.iban, iban_last4=stmt.iban_last4,
                            iban_hash=stmt.iban_hash,
                            account_holder=stmt.account_holder,
                            currency=stmt.currency,
                        )
                    tx_payload = []
                    for tx in stmt.transactions:
                        key = (
                            (stmt.iban_hash or "no-iban") + "|" +
                            tx.booking_date + "|" + f"{tx.amount:.2f}" +
                            "|" + tx.purpose
                        )
                        d = tx.as_dict()
                        d["tx_hash"] = sha256(key.encode("utf-8")).hexdigest()
                        tx_payload.append(d)
                    db.upsert_statement(
                        doc_id, account_id=account_id,
                        period_start=stmt.period_start,
                        period_end=stmt.period_end,
                        statement_no=stmt.statement_no,
                        opening_balance=stmt.opening_balance,
                        closing_balance=stmt.closing_balance,
                        currency=stmt.currency, file_hash="",
                        privacy_mode=stmt.privacy_mode,
                        transactions=tx_payload,
                        extra_json=stmt.raw_response,
                    )
                    # Promote a legacy Bank-tagged doc to category=
                    # Kontoauszug now that we've confirmed it's a real
                    # statement (transactions came back). Same logic
                    # as backfill_statements.
                    if category == "Bank":
                        with db._lock:
                            db._conn.execute(
                                "UPDATE documents SET category = 'Kontoauszug', "
                                "subcategory = '' WHERE id = ?",
                                (doc_id,),
                            )
                            db._conn.commit()
                    job = activity.get_job("analyze-statements")
                    job.approved.append(doc_id)
                    activity.update_job("analyze-statements", done=idx + 1)
                except Exception as exc:
                    job = activity.get_job("analyze-statements")
                    job.failed.append({"doc_id": doc_id, "error": str(exc)})
                    activity.update_job(
                        "analyze-statements", done=idx + 1,
                        last_error=str(exc),
                    )
            activity.finish_job("analyze-statements", current="")
            try:
                from .. import notifier as _n
                job = activity.get_job("analyze-statements")
                ok, fail_n = len(job.approved), len(job.failed)
                _n.fire(_n.NotificationEvent(
                    kind="bulk_done",
                    title=f"Statement analysis done — {ok} ok, {fail_n} failed",
                    body=f"Processed {ok + fail_n} of {len(targets)} unanalysed Kontoauszüge.",
                ))
            except Exception:
                pass

        threading.Thread(target=worker, name="analyze-statements",
                         daemon=True).start()
        return {"started": True,
                **activity.get_job("analyze-statements").as_dict()}

    @app.get("/api/finance/analyze-progress")
    def api_analyze_progress():
        from .. import activity
        return activity.get_job("analyze-statements").as_dict()

    @app.get("/api/finance/unanalyzed-count")
    def api_unanalyzed_count():
        """Cheap count for the UI banner — how many statement-shaped
        docs still need a populated statement row? Includes legacy
        Bank-tagged docs that look like statements (subject mentions
        Kontoauszug / Girokonto / Tagesgeld / Kreditkarte / paypal-auszug)
        — those get promoted to category=Kontoauszug after extraction."""
        with db._lock:
            missing = db._conn.execute(
                """SELECT COUNT(*) AS n FROM documents d
                   LEFT JOIN statements s ON s.doc_id = d.id
                   WHERE d.deleted_at IS NULL
                     AND s.id IS NULL
                     AND d.extracted_text IS NOT NULL AND d.extracted_text != ''
                     AND (
                       d.category = 'Kontoauszug'
                       OR (d.category = 'Bank' AND (
                         d.subcategory = 'Konto'
                         OR d.subcategory = 'Karte'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%kontoauszug%'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%girokonto%'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%tagesgeld%'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%kreditkart%'
                         OR LOWER(COALESCE(d.subject,'')) LIKE '%paypal%auszug%'
                       ))
                     )"""
            ).fetchone()
            empty = db._conn.execute(
                """SELECT COUNT(*) AS n FROM documents d
                   JOIN statements s ON s.doc_id = d.id
                   WHERE d.deleted_at IS NULL
                     AND d.category = 'Kontoauszug'
                     AND COALESCE(s.acknowledged_empty, 0) = 0
                     AND s.id NOT IN (SELECT statement_id FROM transactions)
                     AND d.extracted_text IS NOT NULL AND d.extracted_text != ''"""
            ).fetchone()
        return {"missing": int(missing["n"] if missing else 0),
                "empty":   int(empty["n"]   if empty   else 0)}

    @app.get("/api/document/{doc_id}/status")
    def api_doc_status(doc_id: int):
        """Snapshot of everything the document page needs to render the
        right state without having to refresh by hand. Returned on first
        load and polled while a job is running so the user can see live
        progress instead of a stale "klick Auswerten" prompt after
        navigating away mid-extraction."""
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "document not found")
        running = _doc_job_status(doc_id)
        category = doc.get("category") or ""
        statement = None
        receipt = None
        if category in ("Kontoauszug", "Bank"):
            stmt = db.get_statement(doc_id)
            if stmt:
                statement = {
                    "transactions":   len(stmt.get("transactions") or []),
                    "period_start":   stmt.get("period_start"),
                    "period_end":     stmt.get("period_end"),
                    "iban_last4":     stmt.get("iban_last4"),
                    "bank_name":      stmt.get("bank_name"),
                    "acknowledged_empty": bool(stmt.get("acknowledged_empty") or 0),
                }
        if category in ("Rechnung", "Quittung", "Kassenzettel"):
            r = db.get_receipt(doc_id)
            if r:
                receipt = {
                    "items": len(r.get("items") or []),
                    "total": r.get("total_amount"),
                    "shop":  r.get("shop_name"),
                }
        return {
            "doc_id":   doc_id,
            "status":   doc.get("status"),
            "category": category,
            "running":  running,
            "statement": statement,
            "receipt":   receipt,
        }

    @app.post("/api/document/{doc_id}/statement/dismiss-empty")
    def api_dismiss_empty_statement(doc_id: int):
        """Mark this statement as 'genuinely empty, stop nagging about
        it'. The diag banner and the bulk-analyse counter will skip
        it from now on. Reversible by clicking 'Erneut auswerten' on
        the document page — that flips the flag back to 0 and runs
        extraction afresh."""
        with db._lock:
            row = db._conn.execute(
                "SELECT id FROM statements WHERE doc_id = ?", (doc_id,),
            ).fetchone()
            if not row:
                raise HTTPException(404, "no statement row for this doc")
            db._conn.execute(
                "UPDATE statements SET acknowledged_empty = 1 WHERE doc_id = ?",
                (doc_id,),
            )
            db._conn.commit()
        return {"ok": True, "doc_id": doc_id}

    @app.post("/api/finance/reextract-empty")
    def api_reextract_empty():
        """One-click bulk re-extraction for every statement that came
        back without any transactions. Cheaper than --backfill-statements
        because it doesn't touch already-extracted statements; uses the
        stored OCR text so no re-OCR cost either."""
        if classifier is None:
            raise HTTPException(503, "classifier not available — finish /setup first")
        is_local = settings.ai.provider in ("openai_compat", "bridge")
        if settings.finance.local_only and not is_local:
            raise HTTPException(
                403,
                "finance.local_only is enabled but the active provider is not local.",
            )
        do_pseudo = settings.finance.pseudonymize and not is_local

        from ..finance import StatementExtractor
        from hashlib import sha256
        import time as _time

        extractor = StatementExtractor(
            classifier.provider, settings.ai.model,
            max_text_chars=max(settings.ai.max_text_chars, 32000),
            holder_names=settings.finance.holder_names,
        )

        with db._lock:
            empty_rows = db._conn.execute(
                """SELECT s.doc_id, d.extracted_text
                   FROM statements s
                   JOIN documents d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND COALESCE(s.acknowledged_empty, 0) = 0
                     AND s.id NOT IN (SELECT statement_id FROM transactions)
                     AND d.extracted_text IS NOT NULL AND d.extracted_text != ''"""
            ).fetchall()

        recovered: list[int] = []
        still_empty: list[int] = []
        failed: list[dict] = []
        for idx, row in enumerate(empty_rows):
            if idx > 0:
                _time.sleep(1.5)   # gentle pacing for rate limits
            doc_id = int(row["doc_id"])
            try:
                stmt = extractor.extract(row["extracted_text"], pseudonymize=do_pseudo)
            except Exception as exc:
                failed.append({"doc_id": doc_id, "error": str(exc)})
                continue
            if not stmt.transactions:
                still_empty.append(doc_id)
                continue
            account_id = None
            if stmt.iban_hash:
                account_id = db.upsert_account(
                    bank_name=stmt.bank_name or "Unbekannt",
                    iban=stmt.iban, iban_last4=stmt.iban_last4,
                    iban_hash=stmt.iban_hash, account_holder=stmt.account_holder,
                    currency=stmt.currency,
                )
            tx_payload = []
            for tx in stmt.transactions:
                key = (
                    (stmt.iban_hash or "no-iban") + "|" + tx.booking_date + "|"
                    + f"{tx.amount:.2f}" + "|" + tx.purpose
                )
                d = tx.as_dict()
                d["tx_hash"] = sha256(key.encode("utf-8")).hexdigest()
                tx_payload.append(d)
            db.upsert_statement(
                doc_id, account_id=account_id,
                period_start=stmt.period_start, period_end=stmt.period_end,
                statement_no=stmt.statement_no,
                opening_balance=stmt.opening_balance,
                closing_balance=stmt.closing_balance,
                currency=stmt.currency, file_hash="",
                privacy_mode=stmt.privacy_mode,
                transactions=tx_payload, extra_json=stmt.raw_response,
            )
            recovered.append(doc_id)
        return {
            "found": len(empty_rows),
            "recovered": recovered,
            "still_empty": still_empty,
            "failed": failed,
        }

    @app.post("/api/finance/backfill-statements")
    def api_backfill_statements():
        """Run the full statement backfill — picks up every Kontoauszug
        / legacy Bank doc that has stored OCR text but NO statement row
        yet, and runs the extractor on it. Recovery path for the data
        loss caused by 0.15.1's startup cleanup. Idempotent thanks to
        the tx_hash unique constraint, so it's safe to call repeatedly.
        Same per-call rate-limit pacing as the CLI flag."""
        if classifier is None:
            raise HTTPException(503, "classifier not available — finish /setup first")
        is_local = settings.ai.provider in ("openai_compat", "bridge")
        if settings.finance.local_only and not is_local:
            raise HTTPException(
                403,
                "finance.local_only is enabled but the active provider is not local.",
            )
        from ..finance.extractor import backfill_statements
        local_only = settings.finance.local_only and is_local
        try:
            result = backfill_statements(
                settings, db, classifier,
                dry_run=False, local_only=local_only,
            )
        except Exception as exc:
            logger.exception("Statement backfill via API failed")
            raise HTTPException(500, f"backfill failed: {exc}")
        return result

    @app.delete("/api/finance/account/{account_id}")
    def api_delete_account(account_id: int):
        """Delete a bogus account (typically the "Unbekannt" entry that
        came from a counterparty IBAN being mistakenly used as the user's
        account in older versions). Statements currently pointing at this
        account get their `account_id` set to NULL via the FK ON DELETE
        SET NULL — they stay in the DB so re-extraction can re-attach
        them properly."""
        with db._lock:
            row = db._conn.execute(
                "SELECT id FROM accounts WHERE id = ?", (account_id,)
            ).fetchone()
            if not row:
                raise HTTPException(404, "account not found")
            tx_count = db._conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE account_id = ?", (account_id,)
            ).fetchone()[0]
            db._conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        return {"ok": True, "deleted_account_id": account_id, "freed_transactions": int(tx_count)}

    @app.get("/api/finance/transactions")
    def api_finance_transactions(
        account_id: str | None = Query(None),
        category: str | None = Query(None),
        direction: str | None = Query(None),
        start: str | None = Query(None),
        end: str | None = Query(None),
        q: str | None = Query(None),
        limit: int = Query(200),
    ):
        return {"transactions": db.transactions_list(
            account_id=_coerce_int(account_id), category=category,
            direction=direction, start=start, end=end, query=q, limit=limit,
        )}

    # ---------- Transactions explorer ----------
    # The /transactions page renders a static shell; everything dynamic
    # (filter, list, aggregates) is fetched live from the endpoints
    # below so the user can iterate on the filter without a full page
    # round-trip.

    def _tx_explorer_filters(
        account_id: str | None, category: str | None, direction: str | None,
        start: str | None, end: str | None, q: str | None,
        amount_min: str | None, amount_max: str | None,
    ) -> dict:
        return {
            "account_id": _coerce_int(account_id),
            "category":   (category or "") or None,
            "direction":  (direction or "") or None,
            "start":      start or None,
            "end":        end or None,
            "query":      q or None,
            "amount_min": _coerce_float(amount_min),
            "amount_max": _coerce_float(amount_max),
        }

    @app.get("/api/transactions/search")
    def api_transactions_search(
        account_id: str | None = Query(None),
        category: str | None = Query(None),
        direction: str | None = Query(None),
        start: str | None = Query(None),
        end: str | None = Query(None),
        q: str | None = Query(None),
        amount_min: str | None = Query(None),
        amount_max: str | None = Query(None),
        limit: int = Query(500),
        offset: int = Query(0),
        with_aggregates: int = Query(1),
    ):
        f = _tx_explorer_filters(account_id, category, direction, start, end,
                                 q, amount_min, amount_max)
        rows = db.transactions_list(limit=limit, offset=offset, **f)
        result: dict = {
            "transactions": rows,
            "filter": {
                **{k: v for k, v in f.items()},
                "limit": limit, "offset": offset,
            },
        }
        if with_aggregates:
            result["aggregate"] = db.transactions_aggregate(**f)
        return result

    @app.get("/api/transactions/payee-suggest")
    def api_transactions_payee_suggest(
        q: str = Query(""), limit: int = Query(12),
    ):
        return {"suggestions": db.transactions_payee_suggest(q, limit=limit)}

    @app.post("/api/transactions/categorize")
    def api_transactions_categorize(payload: dict):
        """Bulk-recategorise. Body: {"ids": [int, ...], "category": str}.
        The category must be one of the canonical TX_CATEGORIES so the
        existing per-category dashboards keep working."""
        from ..finance.categories import TX_CATEGORIES
        ids = payload.get("ids") or []
        cat = (payload.get("category") or "").strip()
        if not isinstance(ids, list) or not ids:
            raise HTTPException(400, "ids must be a non-empty list")
        if cat not in TX_CATEGORIES:
            raise HTTPException(
                400, f"unknown category {cat!r} — must be one of {list(TX_CATEGORIES)}",
            )
        try:
            int_ids = [int(i) for i in ids]
        except (TypeError, ValueError):
            raise HTTPException(400, "ids must be integers")
        n = db.transactions_set_category(int_ids, cat)
        return {"ok": True, "updated": n, "category": cat}

    @app.get("/transactions", response_class=HTMLResponse)
    def transactions_page(request: Request):
        from ..finance.categories import TX_CATEGORIES
        accounts = db.list_accounts()
        return templates.TemplateResponse(
            request, "transactions.html",
            {**base_ctx(request),
             "accounts": accounts,
             "tx_categories": list(TX_CATEGORIES)},
        )

    @app.post("/api/settings/finance")
    def api_save_finance(payload: dict):
        """Persist the privacy toggles. Local-only forces statement
        extraction to skip when no local provider is configured."""
        from ..settings_writer import update_finance
        local_only   = bool(payload.get("local_only", False))
        pseudonymize = bool(payload.get("pseudonymize", True))
        review_before_send = bool(payload.get("review_before_send", False))
        holder_names_raw = payload.get("holder_names")
        # Accept the field as either a list (UI) or a comma/newline
        # separated string (legacy clients) so the settings page can
        # use whichever shape is more convenient.
        holder_names: list[str] | None
        if holder_names_raw is None:
            holder_names = None
        elif isinstance(holder_names_raw, list):
            holder_names = [str(n) for n in holder_names_raw]
        else:
            import re as _re
            holder_names = [s for s in _re.split(r"[\n,;]+", str(holder_names_raw))]
        update_finance(
            local_only=local_only,
            pseudonymize=pseudonymize,
            holder_names=holder_names,
            review_before_send=review_before_send,
            config_dir=settings.config_dir,
        )
        # Reflect the change in-process so the next pipeline run picks
        # it up without a service restart.
        settings.finance.local_only   = local_only
        settings.finance.pseudonymize = pseudonymize
        settings.finance.review_before_send = review_before_send
        if holder_names is not None:
            settings.finance.holder_names = [n.strip() for n in holder_names if (n or "").strip()]
        return {
            "ok": True,
            "local_only": local_only,
            "pseudonymize": pseudonymize,
            "holder_names": settings.finance.holder_names,
            "review_before_send": review_before_send,
        }

    @app.get("/api/document/{doc_id}/statement/preview")
    def api_statement_preview(doc_id: int):
        """Show the user EXACTLY what would leave the box for this
        document if they triggered statement extraction right now.

        Returns the pseudonymised OCR text (the bytes that hit the LLM),
        plus the reverse map and counts of each token kind. Cheap: no
        LLM call, just runs the local pseudonymiser on stored OCR text.

        The reverse map values are the user's REAL data, so this
        endpoint stays local-only by design — the values are never
        included in any API call to a third party."""
        from ..finance import Pseudonymizer
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "document not found")
        text = doc.get("extracted_text") or ""
        if not text:
            raise HTTPException(400, "no OCR text stored — re-classify first")

        is_local = settings.ai.provider in ("openai_compat", "bridge")
        will_pseudonymize = settings.finance.pseudonymize and not is_local
        will_skip = settings.finance.local_only and not is_local

        if will_pseudonymize:
            pseudo = Pseudonymizer()
            if settings.finance.holder_names:
                pseudo.seed_household_names(settings.finance.holder_names)
            pseudo_text = pseudo.pseudonymize(text)
            counts: dict[str, int] = {}
            for tok in pseudo.reverse_map:
                kind = tok.split("_")[0]
                counts[kind] = counts.get(kind, 0) + 1
            return {
                "privacy_mode": "pseudonymize",
                "provider": settings.ai.provider,
                "model": settings.ai.model,
                "will_skip": will_skip,
                "char_count_original":      len(text),
                "char_count_to_be_sent":    len(pseudo_text),
                "text_to_be_sent":          pseudo_text,
                "reverse_map":              pseudo.reverse_map,
                "token_counts":             counts,
                "ibans_detected":           pseudo.ibans,
            }
        # Either local-only or pseudonymisation disabled — be honest
        # about what travels.
        return {
            "privacy_mode": "local" if is_local else "plain",
            "provider": settings.ai.provider,
            "model": settings.ai.model,
            "will_skip": will_skip,
            "char_count_original":   len(text),
            "char_count_to_be_sent": len(text),
            "text_to_be_sent":       text,
            "reverse_map":           {},
            "token_counts":          {},
            "ibans_detected":        [],
        }

    @app.post("/api/document/{doc_id}/statement/extract")
    def api_extract_statement(doc_id: int):
        """Manually re-trigger statement extraction for a Kontoauszug
        document — useful after a config change or when the first run
        failed."""
        if classifier is None:
            raise HTTPException(503, "classifier not available — finish /setup first")
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "document not found")
        # Block duplicate clicks: if an extraction is already running for
        # this doc, surface a 409 instead of letting two LLM calls race.
        if not _doc_job_start(doc_id, "statement"):
            raise HTTPException(
                409,
                "extraction already running for this document — wait for the "
                "current run to finish before retrying",
            )
        try:
            return _run_statement_extract(doc_id, doc)
        finally:
            _doc_job_end(doc_id)

    def _run_statement_extract(doc_id: int, doc: dict) -> dict:
        text = doc.get("extracted_text") or ""
        if not text:
            # Old imports never persisted the OCR text. Recover it from
            # the on-disk file so the user doesn't have to first run a
            # full re-classification just to populate the column.
            src = Path(doc.get("library_path") or doc.get("processed_path") or "")
            if not src.exists():
                raise HTTPException(
                    400,
                    "no OCR text stored and the source file is missing — "
                    "cannot recover automatically",
                )
            from ..ocr import extract_text as _ocr
            try:
                ocr_res = _ocr(src, settings.ocr)
            except Exception as exc:
                logger.exception("Fallback OCR failed for %d", doc_id)
                raise HTTPException(500, f"OCR failed: {exc}")
            text = (ocr_res.text or "").strip()
            if not text:
                raise HTTPException(
                    400,
                    "OCR returned no text — the file may be empty or unreadable",
                )
            with db._lock:
                db._conn.execute(
                    "UPDATE documents SET extracted_text = ? WHERE id = ?",
                    (text[: max(settings.ai.max_text_chars, 200_000)], doc_id),
                )
                db._conn.commit()
            logger.info("doc %d: filled missing extracted_text via fallback OCR (%d chars)",
                        doc_id, len(text))
        from ..finance import StatementExtractor
        from hashlib import sha256
        is_local = settings.ai.provider in ("openai_compat", "bridge")
        if settings.finance.local_only and not is_local:
            raise HTTPException(
                403,
                "finance.local_only is enabled but the active provider is not local. "
                "Switch to a local provider in /setup or turn off local-only.",
            )
        do_pseudo = settings.finance.pseudonymize and not is_local
        extractor = StatementExtractor(
            classifier.provider, settings.ai.model,
            max_text_chars=max(settings.ai.max_text_chars, 32000),
            holder_names=settings.finance.holder_names,
        )
        try:
            stmt = extractor.extract(text, pseudonymize=do_pseudo)
        except Exception as exc:
            logger.exception("Statement extract failed for %d", doc_id)
            raise HTTPException(500, f"extract failed: {exc}")
        account_id = None
        if stmt.iban_hash:
            account_id = db.upsert_account(
                bank_name=stmt.bank_name or "Unbekannt",
                iban=stmt.iban, iban_last4=stmt.iban_last4,
                iban_hash=stmt.iban_hash, account_holder=stmt.account_holder,
                currency=stmt.currency,
            )
        tx_payload = []
        for tx in stmt.transactions:
            key = (
                (stmt.iban_hash or "no-iban") + "|" + tx.booking_date + "|"
                + f"{tx.amount:.2f}" + "|" + tx.purpose
            )
            d = tx.as_dict()
            d["tx_hash"] = sha256(key.encode("utf-8")).hexdigest()
            tx_payload.append(d)
        db.upsert_statement(
            doc_id, account_id=account_id,
            period_start=stmt.period_start, period_end=stmt.period_end,
            statement_no=stmt.statement_no,
            opening_balance=stmt.opening_balance,
            closing_balance=stmt.closing_balance,
            currency=stmt.currency, file_hash="",
            privacy_mode=stmt.privacy_mode,
            transactions=tx_payload, extra_json=stmt.raw_response,
        )
        # If this doc was waiting in the review queue, promote it to
        # 'filed' now that the user has approved + extraction succeeded.
        if (doc.get("status") or "") == "pending_review":
            with db._lock:
                db._conn.execute(
                    "UPDATE documents SET status = 'filed' WHERE id = ?",
                    (doc_id,),
                )
        return {
            "ok": True, "transactions": len(stmt.transactions),
            "bank": stmt.bank_name,
            "period": [stmt.period_start, stmt.period_end],
            "privacy_mode": stmt.privacy_mode,
        }

    @app.post("/api/document/{doc_id}/statement/skip")
    def api_skip_statement(doc_id: int):
        """User opened the review preview, decided NOT to send this
        statement to the AI, and wants it out of the queue. The doc
        keeps its category but transitions to 'filed' so the pending
        banner stops flagging it. No LLM call is made."""
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "document not found")
        with db._lock:
            db._conn.execute(
                "UPDATE documents SET status = 'filed' WHERE id = ?",
                (doc_id,),
            )
        return {"ok": True, "doc_id": doc_id, "status": "filed"}

    @app.get("/api/finance/pending-review")
    def api_pending_review():
        """Lightweight polling endpoint: list of Kontoauszug docs that
        are paused waiting for the user's approval. Powers the
        self-refreshing banner on /finance."""
        with db._lock:
            rows = db._conn.execute(
                """SELECT id AS doc_id, COALESCE(subject, filename) AS subject,
                          doc_date, sender
                   FROM documents
                   WHERE status = 'pending_review'
                     AND category = 'Kontoauszug'
                     AND deleted_at IS NULL
                   ORDER BY doc_date DESC, id DESC"""
            ).fetchall()
        return {"items": [dict(r) for r in rows]}

    @app.post("/api/finance/approve-all-pending")
    def api_approve_all_pending():
        """Bulk-release every Kontoauszug currently in the review queue.
        Returns immediately with a job descriptor; the actual extraction
        runs in a background thread. Poll /api/finance/approve-progress
        to follow along."""
        if classifier is None:
            raise HTTPException(503, "classifier not available — finish /setup first")
        from .. import activity
        existing = activity.get_job("approve-pending")
        if existing.running:
            return {
                "started": False, "reason": "already running",
                **existing.as_dict(),
            }
        with db._lock:
            rows = db._conn.execute(
                """SELECT id, COALESCE(subject, filename) AS subject FROM documents
                   WHERE status = 'pending_review' AND deleted_at IS NULL
                     AND category = 'Kontoauszug'
                   ORDER BY created_at ASC"""
            ).fetchall()
        if not rows:
            return {"started": False, "reason": "queue empty",
                    "total": 0, "approved": [], "failed": []}

        doc_ids = [(int(r["id"]), r["subject"] or f"doc {r['id']}") for r in rows]
        activity.start_job("approve-pending", total=len(doc_ids))

        def worker():
            import time as _time
            for idx, (doc_id, subject) in enumerate(doc_ids):
                # Re-read the latest state in case the user navigated
                # away and triggered a stop, or the doc got deleted.
                if idx > 0:
                    _time.sleep(1.5)   # rate-limit pacing
                activity.update_job(
                    "approve-pending",
                    current=str(subject)[:120], current_doc_id=doc_id,
                )
                try:
                    api_extract_statement(doc_id)
                    job = activity.get_job("approve-pending")
                    job.approved.append(doc_id)
                    activity.update_job("approve-pending", done=idx + 1)
                except HTTPException as exc:
                    job = activity.get_job("approve-pending")
                    job.failed.append({"doc_id": doc_id, "error": str(exc.detail)})
                    activity.update_job(
                        "approve-pending", done=idx + 1,
                        last_error=str(exc.detail),
                    )
                    # Stop on hard errors (rate limit, spending cap) so
                    # we don't burn through more docs against the same wall.
                    if exc.status_code in (429, 503, 403):
                        break
                except Exception as exc:
                    job = activity.get_job("approve-pending")
                    job.failed.append({"doc_id": doc_id, "error": str(exc)})
                    activity.update_job(
                        "approve-pending", done=idx + 1, last_error=str(exc),
                    )
            activity.finish_job("approve-pending", current="")

        threading.Thread(target=worker, name="approve-pending",
                         daemon=True).start()
        snapshot = activity.get_job("approve-pending").as_dict()
        return {"started": True, **snapshot}

    @app.get("/api/finance/approve-progress")
    def api_approve_progress():
        """Live progress of the currently-running (or last completed)
        approve-pending job — drives the live progress bar in the UI."""
        from .. import activity
        return activity.get_job("approve-pending").as_dict()

    # ----------------------------------------------------------- Duplicates
    @app.get("/duplicates", response_class=HTMLResponse)
    def duplicates_page(request: Request):
        groups = _duplicate_groups()
        return templates.TemplateResponse(
            request, "duplicates.html",
            {**base_ctx(request),
             "groups":     groups,
             "group_count": len(groups),
             "doc_count":   sum(len(g["docs"]) for g in groups),
             "extra_count": sum(len(g["docs"]) - 1 for g in groups),
             "wasted_bytes": sum(g["docs"][0]["file_size"] * (len(g["docs"]) - 1)
                                 for g in groups if g["docs"][0].get("file_size")),
            },
        )

    def _duplicate_groups() -> list[dict]:
        """Return all hash collisions across non-deleted documents.
        Each group is sorted oldest → newest so the UI can default to
        'keep first, trash rest' without further work."""
        with db._lock:
            hash_rows = db._conn.execute(
                """SELECT content_hash, COUNT(*) AS n FROM documents
                   WHERE deleted_at IS NULL
                     AND content_hash IS NOT NULL AND content_hash != ''
                   GROUP BY content_hash HAVING n > 1
                   ORDER BY n DESC, content_hash"""
            ).fetchall()
            groups: list[dict] = []
            for hr in hash_rows:
                rows = db._conn.execute(
                    """SELECT id, filename, doc_date, created_at, category,
                              subcategory, sender, subject, file_size,
                              status, library_path
                       FROM documents
                       WHERE content_hash = ? AND deleted_at IS NULL
                       ORDER BY datetime(created_at) ASC""",
                    (hr["content_hash"],),
                ).fetchall()
                groups.append({
                    "content_hash": hr["content_hash"],
                    "n":            int(hr["n"]),
                    "docs":         [dict(r) for r in rows],
                })
        return groups

    @app.get("/api/library/duplicates")
    def api_library_duplicates():
        groups = _duplicate_groups()
        return {
            "groups":      groups,
            "group_count": len(groups),
            "doc_count":   sum(len(g["docs"]) for g in groups),
            "extra_count": sum(len(g["docs"]) - 1 for g in groups),
        }

    @app.post("/api/library/duplicates/clean")
    def api_library_duplicates_clean(payload: dict):
        """Trash every duplicate except one keeper per group. The
        keeper id can be picked client-side; if none is given we keep
        the oldest (first inserted) doc.
        Body shape: ``{"keepers": {"<hash>": <doc_id>, ...}}`` or
        ``{}`` for the keep-oldest default applied to every group.
        """
        from ..trash import delete_document
        keepers: dict[str, int] = {
            str(k): int(v) for k, v in (payload.get("keepers") or {}).items()
        }
        groups = _duplicate_groups()
        trashed: list[int] = []
        failed: list[dict]  = []
        for g in groups:
            doc_ids = [d["id"] for d in g["docs"]]
            keeper  = keepers.get(g["content_hash"], doc_ids[0])
            if keeper not in doc_ids:
                # Caller picked an id that isn't part of this group —
                # safer to skip the whole group than to wipe a row by
                # accident.
                failed.append({"hash": g["content_hash"],
                               "error": f"keeper {keeper} not in group"})
                continue
            for doc_id in doc_ids:
                if doc_id == keeper:
                    continue
                try:
                    delete_document(doc_id, settings, db)
                    trashed.append(doc_id)
                except Exception as exc:
                    failed.append({"doc_id": doc_id, "error": str(exc)})
        return {"trashed":  trashed,
                "trashed_n": len(trashed),
                "failed":    failed,
                "remaining_groups": len(_duplicate_groups())}

    @app.post("/api/library/retry-all-review")
    def api_retry_all_review():
        """Re-classify every document currently in `status='review'` —
        useful after the AI provider was switched (e.g. cloud → local
        bridge) so the existing review pile gets a fresh look without
        the user having to click each one. Returns immediately; work
        runs in a background thread. Poll /api/library/retry-progress."""
        if classifier is None:
            raise HTTPException(503, "classifier not available — finish /setup first")
        from .. import activity
        existing = activity.get_job("retry-review")
        if existing.running:
            return {"started": False, "reason": "already running",
                    **existing.as_dict()}
        with db._lock:
            rows = db._conn.execute(
                """SELECT id, COALESCE(subject, filename) AS subject FROM documents
                   WHERE status = 'review' AND deleted_at IS NULL
                   ORDER BY created_at ASC"""
            ).fetchall()
        if not rows:
            return {"started": False, "reason": "queue empty",
                    "total": 0, "approved": [], "failed": []}

        doc_ids = [(int(r["id"]), r["subject"] or f"doc {r['id']}") for r in rows]
        activity.start_job("retry-review", total=len(doc_ids))

        from ..retry import retry_document

        def worker():
            import time as _time
            for idx, (doc_id, subject) in enumerate(doc_ids):
                if idx > 0:
                    _time.sleep(0.8)  # gentle pacing — local is slower per call
                activity.update_job(
                    "retry-review",
                    current=str(subject)[:120], current_doc_id=doc_id,
                )
                try:
                    retry_document(doc_id, settings, classifier, db)
                    job = activity.get_job("retry-review")
                    job.approved.append(doc_id)
                    activity.update_job("retry-review", done=idx + 1)
                except ValueError as exc:
                    # Bad input (no text, file missing) — skip, keep going.
                    job = activity.get_job("retry-review")
                    job.failed.append({"doc_id": doc_id, "error": str(exc)})
                    activity.update_job(
                        "retry-review", done=idx + 1, last_error=str(exc),
                    )
                except Exception as exc:
                    job = activity.get_job("retry-review")
                    job.failed.append({"doc_id": doc_id, "error": str(exc)})
                    activity.update_job(
                        "retry-review", done=idx + 1, last_error=str(exc),
                    )
            activity.finish_job("retry-review", current="")
            try:
                from .. import notifier as _n
                job = activity.get_job("retry-review")
                ok, fail_n = len(job.approved), len(job.failed)
                _n.fire(_n.NotificationEvent(
                    kind="bulk_done",
                    title=f"Review re-queue done — {ok} ok, {fail_n} failed",
                    body=f"Re-classified {ok + fail_n} review documents.",
                ))
            except Exception:
                pass

        threading.Thread(target=worker, name="retry-review",
                         daemon=True).start()
        return {"started": True, **activity.get_job("retry-review").as_dict()}

    @app.get("/api/library/retry-progress")
    def api_retry_progress():
        from .. import activity
        return activity.get_job("retry-review").as_dict()

    @app.get("/api/activity")
    def api_activity():
        """Global AI-activity snapshot used by the header indicator.
        Returns the in-flight provider-call counter, the timestamp of
        the most-recent provider call, and the current state of every
        named background job."""
        from .. import activity
        snap = activity.snapshot()
        with db._lock:
            queue = db._conn.execute(
                """SELECT COUNT(*) FROM documents
                   WHERE deleted_at IS NULL
                     AND status IN ('pending_review','processing')"""
            ).fetchone()[0]
        snap["pending_or_processing"] = int(queue or 0)
        return snap

    # ---------- Bulk operations ----------
    @app.post("/api/bulk/delete")
    def bulk_delete(payload: dict):
        from ..trash import delete_document as _delete
        ids = payload.get("ids") or []
        ok, errors = [], []
        for doc_id in ids:
            try:
                _delete(int(doc_id), settings, db)
                ok.append(int(doc_id))
            except Exception as exc:
                errors.append({"id": int(doc_id), "error": str(exc)})
        return {"ok": ok, "errors": errors}

    @app.post("/api/bulk/restore")
    def bulk_restore(payload: dict):
        from ..trash import restore_document as _restore
        ids = payload.get("ids") or []
        ok, errors = [], []
        for doc_id in ids:
            try:
                _restore(int(doc_id), settings, db)
                ok.append(int(doc_id))
            except Exception as exc:
                errors.append({"id": int(doc_id), "error": str(exc)})
        return {"ok": ok, "errors": errors}

    @app.post("/api/bulk/purge")
    def bulk_purge(payload: dict):
        from ..trash import purge_document as _purge
        ids = payload.get("ids") or []
        ok, errors = [], []
        for doc_id in ids:
            try:
                _purge(int(doc_id), settings, db)
                ok.append(int(doc_id))
            except Exception as exc:
                errors.append({"id": int(doc_id), "error": str(exc)})
        return {"ok": ok, "errors": errors}

    @app.post("/api/bulk/recategorize")
    def bulk_recategorize(payload: dict):
        ids = payload.get("ids") or []
        category = payload.get("category", "")
        if category not in category_names:
            raise HTTPException(400, f"Unknown category: {category}")
        ok, errors = [], []
        for doc_id in ids:
            try:
                doc = db.get(int(doc_id))
                if not doc:
                    raise ValueError("not found")
                old_path = Path(doc["library_path"])
                if not old_path.exists():
                    raise ValueError("library file missing")
                year = (doc["doc_date"] or doc["created_at"])[:4]
                new_dir = settings.paths.library / year / category
                new_dir.mkdir(parents=True, exist_ok=True)
                new_path = new_dir / old_path.name
                if new_path.exists() and new_path != old_path:
                    # uniquify
                    i = 2
                    while True:
                        cand = new_dir / f"{old_path.stem}-{i}{old_path.suffix}"
                        if not cand.exists():
                            new_path = cand
                            break
                        i += 1
                shutil.move(str(old_path), str(new_path))
                db.update_category(int(doc_id), category)
                db.update_paths(int(doc_id), str(new_path))
                ok.append(int(doc_id))
            except Exception as exc:
                errors.append({"id": int(doc_id), "error": str(exc)})
        return {"ok": ok, "errors": errors}

    # ---------- Trash: delete / restore / purge ----------
    @app.post("/api/document/{doc_id}/delete")
    def delete_document(doc_id: int):
        from ..trash import delete_document as _delete
        try:
            return _delete(doc_id, settings, db)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @app.post("/api/document/{doc_id}/restore")
    def restore_document(doc_id: int):
        from ..trash import restore_document as _restore
        try:
            return _restore(doc_id, settings, db)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @app.post("/api/document/{doc_id}/purge")
    def purge_document(doc_id: int):
        from ..trash import purge_document as _purge
        try:
            return _purge(doc_id, settings, db)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @app.post("/api/trash/empty")
    def empty_trash_route():
        from ..trash import empty_trash
        return empty_trash(settings, db)

    # ---------- Export ----------
    @app.get("/api/export.zip")
    def export_zip(
        category: str | None = Query(None),
        year: str | None = Query(None),
        ids: str | None = Query(None, description="comma-separated doc IDs"),
        include_trash: bool = Query(False),
    ):
        from ..export import stream_zip, suggested_filename
        id_list: list[int] | None = None
        if ids:
            try:
                id_list = [int(x) for x in ids.split(",") if x.strip()]
            except ValueError:
                raise HTTPException(400, "Invalid ids parameter")
        name = suggested_filename(category=category, year=year, trash=include_trash)
        return StreamingResponse(
            stream_zip(
                settings, db, category=category, year=year,
                include_trash=include_trash, ids=id_list,
            ),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )

    # ---------- Sync ----------
    @app.get("/api/sync/status")
    def sync_status():
        from .. import sync as sync_mod
        from .. import rclone_setup
        base = sync_mod.status(settings)
        base["rclone_version"] = rclone_setup.rclone_version()
        return base

    @app.post("/api/sync/run")
    def sync_run():
        from .. import sync as sync_mod
        if not settings.sync.enabled:
            raise HTTPException(400, "sync disabled — set sync.enabled=true in config.yaml")
        if not sync_mod.rclone_available():
            raise HTTPException(
                503,
                "rclone is not installed. On Debian: sudo apt install rclone, "
                "then run `rclone config` once to set up your remote.",
            )
        return sync_mod.run_sync_async(settings)

    # ---------- Sync: headless rclone remote setup ----------
    @app.get("/api/sync/remotes")
    def sync_list_remotes():
        from .. import rclone_setup
        return {
            "rclone_installed": rclone_setup.rclone_available(),
            "rclone_version": rclone_setup.rclone_version(),
            "conf_path": str(rclone_setup.conf_path()),
            "remotes": rclone_setup.list_remotes(),
            "supported_backends": list(rclone_setup.SUPPORTED_BACKENDS),
            "oauth_backends": sorted(rclone_setup.OAUTH_BACKENDS),
        }

    @app.get("/api/sync/authorize-command/{backend}")
    def sync_authorize_command(backend: str):
        from .. import rclone_setup
        try:
            return {"command": rclone_setup.authorize_command(backend),
                    "backend": backend}
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @app.post("/api/sync/remote/oauth")
    def sync_add_oauth_remote(payload: dict):
        from .. import rclone_setup
        try:
            path = rclone_setup.add_oauth_remote(
                name=payload.get("name", ""),
                backend=payload.get("backend", ""),
                token_json=payload.get("token", ""),
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return {"ok": True, "conf_path": str(path)}

    @app.post("/api/sync/remote/s3")
    def sync_add_s3_remote(payload: dict):
        from .. import rclone_setup
        try:
            path = rclone_setup.add_s3_remote(
                name=payload.get("name", ""),
                access_key_id=payload.get("access_key_id", ""),
                secret_access_key=payload.get("secret_access_key", ""),
                region=payload.get("region", ""),
                endpoint=payload.get("endpoint", ""),
                provider=payload.get("provider", "Other"),
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return {"ok": True, "conf_path": str(path)}

    @app.post("/api/sync/remote/webdav")
    def sync_add_webdav_remote(payload: dict):
        from .. import rclone_setup
        try:
            path = rclone_setup.add_webdav_remote(
                name=payload.get("name", ""),
                url=payload.get("url", ""),
                user=payload.get("user", ""),
                password=payload.get("password", ""),
                vendor=payload.get("vendor", "other"),
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return {"ok": True, "conf_path": str(path)}

    @app.post("/api/sync/remote/sftp")
    def sync_add_sftp_remote(payload: dict):
        from .. import rclone_setup
        try:
            path = rclone_setup.add_sftp_remote(
                name=payload.get("name", ""),
                host=payload.get("host", ""),
                user=payload.get("user", ""),
                port=int(payload.get("port", 22) or 22),
                password=payload.get("password", ""),
                key_file=payload.get("key_file", ""),
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return {"ok": True, "conf_path": str(path)}

    @app.delete("/api/sync/remote/{name}")
    def sync_delete_remote(name: str):
        from .. import rclone_setup
        ok = rclone_setup.remove_remote(name)
        if not ok:
            raise HTTPException(404, f"remote {name!r} not found in rclone.conf")
        return {"ok": True}

    @app.post("/api/sync/test/{name}")
    def sync_test_remote(name: str):
        from .. import rclone_setup
        return rclone_setup.test_remote(name)

    # ---------- Setup wizard + Settings page ----------
    @app.get("/setup", response_class=HTMLResponse)
    def setup_page(request: Request):
        return templates.TemplateResponse(
            request, "setup.html",
            {**base_ctx(request),
             "providers": list(PROVIDERS),
             "configured": is_configured(settings),
             "current_provider": settings.ai.provider,
             "current_model": settings.ai.model,
             "current_base_url": settings.ai.base_url,
             "library_path": str(settings.paths.library),
             "inbox_path": str(settings.paths.inbox),
             "web_host": settings.web.host,
             "web_port": settings.web.port},
        )

    @app.get("/settings", response_class=HTMLResponse)
    def settings_page(request: Request):
        from .. import rclone_setup
        # Build a per-provider "do we have a key?" map covering BOTH the
        # secrets.yaml store and legacy env vars (ANTHROPIC_API_KEY etc.) so
        # an instance configured the old way doesn't show "no key stored".
        masked: dict[str, str] = {}
        for prov in PROVIDERS:
            key = get_api_key(settings, prov)
            if key:
                tail = key[-4:] if len(key) > 4 else ""
                masked[f"{prov}_api_key"] = ("•" * 8 + tail) if tail else ("•" * len(key))
        return templates.TemplateResponse(
            request, "settings.html",
            {**base_ctx(request),
             "providers": list(PROVIDERS),
             "current_provider": settings.ai.provider,
             "current_model": settings.ai.model,
             "current_base_url": settings.ai.base_url,
             "library_path": str(settings.paths.library),
             "inbox_path": str(settings.paths.inbox),
             "stored_secrets": masked,
             "web_host":         settings.web.host,
             "web_port":         settings.web.port,
             "web_ssl_enabled":  bool(settings.web.ssl_cert and settings.web.ssl_key),
             "sync_enabled":     settings.sync.enabled,
             "sync_target_type": settings.sync.target_type,
             "sync_local_path":  settings.sync.local_path,
             "sync_remote":      settings.sync.remote,
             "sync_source":      settings.sync.source,
             "rclone_installed": rclone_setup.rclone_available(),
             "rclone_version":   rclone_setup.rclone_version(),
             "rclone_remotes":   rclone_setup.list_remotes(),
             "supported_backends": list(rclone_setup.SUPPORTED_BACKENDS),
             "oauth_backends":   sorted(rclone_setup.OAUTH_BACKENDS),
             "finance_local_only":   settings.finance.local_only,
             "finance_pseudonymize": settings.finance.pseudonymize,
             "finance_holder_names": list(settings.finance.holder_names),
             "finance_review_before_send": settings.finance.review_before_send,
             "has_local_provider":   settings.ai.provider == "openai_compat"},
        )

    # ----------------------------------------------------------- Local Ollama
    # Same-host shortcut: if DocuSort is running directly on a Mac /
    # Linux box / Windows machine that ALSO has Ollama installed,
    # there's no point in routing through the bridge — the simplest
    # path is the openai_compat provider pointing at localhost:11434.
    # These two endpoints (probe + apply) make that a one-click setup.
    @app.get("/api/local-ai/probe")
    def api_local_ai_probe(url: str = "http://127.0.0.1:11434"):
        """Check whether an Ollama (or other openai_compat) server is
        reachable on the same machine and report its model list. The
        UI uses this to decide whether the "Use local Ollama on this
        machine" button should be enabled or grayed out."""
        import urllib.request as _ur
        import urllib.error  as _ue
        base = url.rstrip("/")
        try:
            with _ur.urlopen(base + "/api/tags", timeout=2) as r:
                data = json.loads(r.read().decode("utf-8"))
            models = [m.get("name", "") for m in (data.get("models") or [])]
            return {"reachable": True, "url": base,
                    "models": [m for m in models if m]}
        except _ue.URLError as exc:
            return {"reachable": False, "url": base, "error": str(exc.reason)}
        except Exception as exc:
            return {"reachable": False, "url": base, "error": str(exc)}

    @app.post("/api/local-ai/apply")
    def api_local_ai_apply(payload: dict):
        """Switch the AI provider to openai_compat targeting a same-host
        Ollama. Saves config + flips settings.ai in place so the next
        request sees the change. The classifier still references the
        old provider until the user restarts the service — same as
        the regular /api/settings/ai handler."""
        from .. import settings_writer
        url   = (payload.get("url") or "http://127.0.0.1:11434").rstrip("/")
        model = (payload.get("model") or "").strip()
        if not model:
            raise HTTPException(400, "model required")
        # The openai_compat provider expects an OpenAI-style /v1 base
        # URL; Ollama exposes that at /v1.
        base_url = url + "/v1" if not url.endswith("/v1") else url
        settings_writer.update_ai(
            provider="openai_compat", model=model, base_url=base_url,
            api_key=None, config_dir=settings.config_dir,
        )
        settings.ai.provider = "openai_compat"
        settings.ai.model    = model
        settings.ai.base_url = base_url
        return {"ok": True, "restart_required": True,
                "provider": "openai_compat", "base_url": base_url, "model": model}

    @app.post("/api/settings/ai")
    def api_settings_ai(payload: dict):
        from .. import settings_writer
        provider = (payload.get("provider") or "").strip()
        if provider not in PROVIDERS:
            raise HTTPException(400, f"unknown provider: {provider}")
        model = (payload.get("model") or "").strip()
        if not model:
            raise HTTPException(400, "model required")
        base_url = (payload.get("base_url") or "").strip()
        if provider == "openai_compat" and not base_url:
            raise HTTPException(400, "openai_compat requires base_url")
        api_key = payload.get("api_key")  # may be empty if user doesn't change it
        if provider not in ("openai_compat", "bridge") and api_key is not None and not api_key.strip():
            # Allow blank when there's already a key — either in secrets.yaml
            # or in the legacy environment variable (ANTHROPIC_API_KEY etc.).
            if not get_api_key(settings, provider):
                raise HTTPException(400, "api_key required for this provider")
            api_key = None  # don't overwrite
        if provider == "bridge":
            api_key = None  # bridge does not use an API key at all

        settings_writer.update_ai(
            provider=provider, model=model, base_url=base_url,
            api_key=api_key, config_dir=settings.config_dir,
        )
        # Mirror into the live AppSettings so the next request sees the new
        # values without a restart for read-only purposes (the running
        # classifier still references the old provider — we expose a
        # "restart required" flag so the UI can prompt).
        settings.ai.provider = provider
        settings.ai.model    = model
        settings.ai.base_url = base_url
        return {"ok": True, "restart_required": True}

    # ------------------------------------------------------------- Local AI Bridge
    # The Mac client opens an outbound WebSocket to /api/llm-bridge/ws
    # carrying a shared-secret token. Once attached, every classify
    # call routed through the BridgeProvider goes through this socket
    # and the answer comes back the same way. See docusort/bridge/.
    @app.get("/api/bridge/status")
    def api_bridge_status():
        from ..bridge.server import get_bridge, get_or_create_token
        bridge = get_bridge()
        info   = bridge.info()
        info["token"]      = get_or_create_token(settings.config_dir)
        info["provider_active"] = settings.ai.provider == "bridge"
        return info

    @app.get("/api/bridge/installer")
    def api_bridge_installer(request: Request, os: str = "mac"):
        """Return a ready-to-run launcher with the server URL and token
        already baked in — saves the user from copy-pasting a long
        command into a terminal. Just download and double-click.

        - os=mac    → `.command` (macOS Terminal opens on double-click)
        - os=linux  → `.sh`      (chmod +x and run)
        - os=windows→ `.bat`     (double-click to launch)

        The token is sensitive (it grants the bridge access to drive
        AI calls on the server), so the file is generated on demand
        and the response is marked no-store. Filename includes the
        host so multiple servers can co-exist in the Downloads folder.
        """
        from ..bridge.server import get_or_create_token
        token = get_or_create_token(settings.config_dir)
        # Reconstruct the public origin from the incoming request so
        # the launcher will work on whichever interface the user is
        # currently using (LAN ip vs Tailscale vs reverse proxy).
        scheme = request.url.scheme
        host   = request.headers.get("host") or request.url.netloc
        origin = f"{scheme}://{host}".rstrip("/")
        script_url = f"{origin}/static/scripts/docusort_mac_bridge.py"

        # `.command` files trip macOS Gatekeeper on first run; the
        # script tells the user to right-click → Open if that happens.
        # The token MUST be single-quoted in the bash launcher and
        # we escape any single quote inside it the standard way.
        sh_token  = token.replace("'", "'\\''")
        # In the .bat we use double-quote for the token; the token is
        # url-safe base64 so it never contains a quote, but be defensive.
        bat_token = token.replace('"', '""')

        os_norm = (os or "").lower()
        host_short = host.split(":")[0]

        # Helper: pack a shell script into a zip with the executable
        # bit set on the inner file. Browsers strip the +x bit when
        # they save a downloaded file directly, so a bare .command
        # would fail with "you don't have permission to execute" on
        # double-click. Inside a zip the Unix mode survives, and
        # macOS's Archive Utility / Linux's unzip both restore it.
        def _zipped(inner_filename: str, body_text: str) -> bytes:
            import io as _io, zipfile as _zip
            buf = _io.BytesIO()
            with _zip.ZipFile(buf, "w", _zip.ZIP_DEFLATED) as zf:
                zi = _zip.ZipInfo(inner_filename)
                zi.create_system = 3                # Unix
                zi.external_attr = (0o755 << 16)    # rwxr-xr-x
                zf.writestr(zi, body_text)
            return buf.getvalue()

        if os_norm in ("mac", "darwin", "macos"):
            body_text = (
                "#!/bin/bash\n"
                "# DocuSort Local AI Bridge — generated launcher.\n"
                "# Double-click this file to start the bridge in Terminal.\n"
                "# If macOS blocks it the first time:  right-click → Open.\n"
                "set -e\n"
                'echo "DocuSort Local AI Bridge"\n'
                f'SERVER="{origin}"\n'
                f'TOKEN=\'{sh_token}\'\n'
                'TMP="$(mktemp -t docusort_bridge.XXXXXX).py"\n'
                f'curl -fsSL "{script_url}" -o "$TMP"\n'
                'exec /usr/bin/env python3 "$TMP" --server "$SERVER" --token "$TOKEN"\n'
            )
            inner = f"docusort-bridge-{host_short}.command"
            content  = _zipped(inner, body_text)
            filename = f"docusort-bridge-{host_short}-mac.zip"
            media    = "application/zip"
        elif os_norm == "linux":
            body_text = (
                "#!/bin/bash\n"
                "# DocuSort Local AI Bridge — generated launcher.\n"
                "set -e\n"
                f'SERVER="{origin}"\n'
                f'TOKEN=\'{sh_token}\'\n'
                'TMP="$(mktemp -t docusort_bridge.XXXXXX.py)"\n'
                f'curl -fsSL "{script_url}" -o "$TMP"\n'
                'exec python3 "$TMP" --server "$SERVER" --token "$TOKEN"\n'
            )
            inner = f"docusort-bridge-{host_short}.sh"
            content  = _zipped(inner, body_text)
            filename = f"docusort-bridge-{host_short}-linux.zip"
            media    = "application/zip"
        elif os_norm in ("win", "windows"):
            # Windows .bat doesn't need an executable bit — drop the
            # zip wrapping for the cleanest one-click UX.
            content = (
                "@echo off\r\n"
                "REM DocuSort Local AI Bridge -- generated launcher.\r\n"
                "REM Double-click to launch.\r\n"
                f'set SERVER={origin}\r\n'
                f'set TOKEN={bat_token}\r\n'
                f'set SCRIPT_URL={script_url}\r\n'
                'set TMP_PY=%TEMP%\\docusort_bridge.py\r\n'
                'powershell -NoProfile -Command "Invoke-WebRequest \'%SCRIPT_URL%\' -OutFile \'%TMP_PY%\'"\r\n'
                'if errorlevel 1 (echo Download failed.& pause & exit /b 1)\r\n'
                'python "%TMP_PY%" --server "%SERVER%" --token "%TOKEN%"\r\n'
                'pause\r\n'
            ).encode("utf-8")
            filename = f"docusort-bridge-{host_short}.bat"
            media    = "application/x-bat"
        else:
            raise HTTPException(400, f"unknown os: {os!r}")

        from fastapi.responses import Response
        return Response(
            content=content, media_type=media,
            headers={
                "Content-Disposition":
                    f'attachment; filename="{filename}"',
                # The token is in the body — make absolutely sure
                # nothing along the way caches this response.
                "Cache-Control": "no-store, max-age=0",
            },
        )

    @app.post("/api/bridge/test")
    def api_bridge_test():
        """Round-trip a trivial prompt through whatever client is currently
        connected. Lets the user verify the connection from the UI without
        running a real document through the pipeline."""
        from ..bridge.server import get_bridge
        bridge = get_bridge()
        if not bridge.is_connected():
            raise HTTPException(409, "no bridge client is connected")
        try:
            data = bridge.call(
                system_prompt="You are a JSON echo. Reply with exactly: "
                              "{\"ok\": true, \"echo\": \"<msg>\"}",
                user_prompt="msg=docusort-bridge-test",
                model=settings.ai.model or "qwen2.5:7b-instruct",
                max_output_tokens=80,
                timeout=60.0,
            )
        except TimeoutError as exc:
            raise HTTPException(504, str(exc))
        except RuntimeError as exc:
            raise HTTPException(502, str(exc))
        return {
            "ok": True,
            "raw_text": data.get("raw_text", ""),
            "model": data.get("model", ""),
            "input_tokens":  data.get("input_tokens", 0),
            "output_tokens": data.get("output_tokens", 0),
        }

    @app.post("/api/bridge/regenerate-token")
    def api_bridge_regenerate_token():
        from ..bridge.server import get_bridge, regenerate_token
        # Force-disconnect the active client too — its old token is no
        # longer accepted, but it would happily keep its current socket
        # open until the next reconnect attempt without this nudge.
        bridge = get_bridge()
        token  = regenerate_token(settings.config_dir)
        try:
            client = bridge._client  # noqa: SLF001  — singleton, we own it
            loop   = bridge._loop    # noqa: SLF001
        except Exception:
            client = loop = None
        if client is not None and loop is not None:
            import asyncio
            async def _kick():
                try:
                    await client.close(code=1000, reason="token regenerated")
                except Exception:
                    pass
            try:
                asyncio.run_coroutine_threadsafe(_kick(), loop)
            except Exception:
                pass
        return {"ok": True, "token": token}

    @app.websocket("/api/llm-bridge/ws")
    async def llm_bridge_ws(ws: WebSocket):
        from ..bridge.server import get_bridge, get_or_create_token
        import secrets as _secrets
        token_q  = ws.query_params.get("token", "") or ""
        expected = get_or_create_token(settings.config_dir)
        # Constant-time compare so a wrong token can't be brute-forced
        # by timing the close.
        if not _secrets.compare_digest(token_q, expected):
            client_host = ws.client.host if ws.client else "?"
            tok_preview = (token_q[:8] + "…") if token_q else "(empty)"
            logger.warning(
                "Bridge: rejected connect from %s with token=%s (does not match server token)",
                client_host, tok_preview,
            )
            # Record the rejection in the bridge state too so the UI can
            # surface it instead of just showing "offline".
            try:
                bridge_obj = get_bridge()
                bridge_obj._last_reject = {  # noqa: SLF001
                    "host":  client_host,
                    "token_preview": tok_preview,
                    "at":    __import__("time").time(),
                }
            except Exception:
                pass
            await ws.close(code=4401, reason="invalid token")
            return
        await ws.accept()
        bridge = get_bridge()
        try:
            # First message must be the hello envelope. Optional
            # `queued_responses` is a list of response messages the
            # client computed before its previous WS dropped — we
            # ingest those before redelivering pending requests so
            # the client never re-runs work it already completed.
            hello_raw = await ws.receive_json()
            if hello_raw.get("type") != "hello":
                await ws.close(code=4400, reason="expected hello")
                return
            await bridge.attach_client(ws, hello_raw.get("client", {}) or {})
            queued = hello_raw.get("queued_responses") or []
            if queued:
                await bridge.ingest_queued_responses(queued)
            # Re-send any requests that are still pending after the
            # ingest step — these are the ones the client never got
            # to (or got, processed, but failed to send the response).
            await bridge.redeliver_pending()
            await ws.send_json({
                "type":    "welcome",
                "server":  "docusort",
                "version": __version__,
            })
            while True:
                msg = await ws.receive_json()
                await bridge.handle_message(msg)
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            logger.warning("Bridge WS error: %s", exc)
        finally:
            await bridge.detach_client(ws)

    # ---------------------------------------------------------- Notifications
    @app.get("/api/settings/notifications")
    def api_settings_notifications_get():
        n = settings.notifications
        secrets = load_secrets(settings.config_dir)
        return {
            "enabled":           n.enabled,
            "event_doc_review":  n.event_doc_review,
            "event_doc_failed":  n.event_doc_failed,
            "event_doc_filed":   n.event_doc_filed,
            "event_bulk_done":   n.event_bulk_done,
            "telegram_enabled":  n.telegram_enabled,
            "telegram_chat_id":  n.telegram_chat_id,
            # Tell the UI whether a token is already on disk without
            # leaking it. The `••••` marker keeps the field dimmed
            # without offering anything to copy.
            "telegram_bot_token_set": bool((secrets.get("telegram_bot_token") or "").strip()),
            "email_enabled":     n.email_enabled,
            "smtp_host":         n.smtp_host,
            "smtp_port":         n.smtp_port,
            "smtp_user":         n.smtp_user,
            "smtp_from":         n.smtp_from,
            "smtp_to":           n.smtp_to,
            "smtp_starttls":     n.smtp_starttls,
            "smtp_password_set": bool((secrets.get("smtp_password") or "").strip()),
            "channels_active":   list(__import__("docusort.notifier", fromlist=["get_dispatcher"]).get_dispatcher().channels_summary()),
        }

    @app.post("/api/settings/notifications")
    def api_settings_notifications(payload: dict):
        from .. import settings_writer, notifier as _notifier
        # Empty-string token/password mean "leave the existing value
        # alone". The UI sends None when the field hasn't been touched.
        settings_writer.update_notifications(
            enabled=payload.get("enabled"),
            event_doc_review=payload.get("event_doc_review"),
            event_doc_failed=payload.get("event_doc_failed"),
            event_doc_filed=payload.get("event_doc_filed"),
            event_bulk_done=payload.get("event_bulk_done"),
            telegram_enabled=payload.get("telegram_enabled"),
            telegram_chat_id=payload.get("telegram_chat_id"),
            telegram_bot_token=(payload.get("telegram_bot_token") or None),
            email_enabled=payload.get("email_enabled"),
            smtp_host=payload.get("smtp_host"),
            smtp_port=payload.get("smtp_port"),
            smtp_user=payload.get("smtp_user"),
            smtp_from=payload.get("smtp_from"),
            smtp_to=payload.get("smtp_to"),
            smtp_starttls=payload.get("smtp_starttls"),
            smtp_password=(payload.get("smtp_password") or None),
            config_dir=settings.config_dir,
        )
        # Mirror into the live AppSettings so the in-memory dispatcher
        # picks up the change immediately, and rebuild the dispatcher.
        n = settings.notifications
        for key in ("enabled","event_doc_review","event_doc_failed",
                    "event_doc_filed","event_bulk_done",
                    "telegram_enabled","telegram_chat_id",
                    "email_enabled","smtp_host","smtp_port","smtp_user",
                    "smtp_from","smtp_to","smtp_starttls"):
            if payload.get(key) is not None:
                setattr(n, key, payload[key])
        _notifier.configure(settings)
        return {"ok": True,
                "channels_active": _notifier.get_dispatcher().channels_summary()}

    @app.post("/api/notifications/test")
    def api_notifications_test():
        from .. import notifier as _notifier
        disp = _notifier.get_dispatcher()
        if not disp.channels_summary():
            raise HTTPException(409, "no channel configured / enabled")
        _notifier.fire(_notifier.NotificationEvent(
            kind="test",
            title="DocuSort test notification",
            body=("If you are reading this, the channel is wired up "
                  "correctly. You can safely ignore this message."),
        ))
        return {"ok": True, "channels": disp.channels_summary()}

    @app.post("/api/settings/sync")
    def api_settings_sync(payload: dict):
        from .. import settings_writer
        enabled     = bool(payload.get("enabled", False))
        target_type = (payload.get("target_type") or "local").strip()
        local_path  = (payload.get("local_path") or "").strip()
        remote      = (payload.get("remote") or "").strip()
        source      = (payload.get("source") or "library").strip()

        if target_type not in ("local", "rclone"):
            raise HTTPException(400, "target_type must be 'local' or 'rclone'")
        if source not in ("library", "library_and_trash"):
            raise HTTPException(400, "source must be 'library' or 'library_and_trash'")
        if enabled and target_type == "local" and not local_path:
            raise HTTPException(400, "local_path required when sync is enabled")
        if enabled and target_type == "rclone" and not remote:
            raise HTTPException(400, "remote required when sync is enabled")

        # Validate the local path: must be writable and must NOT be the
        # library itself (we'd be syncing onto our own source).
        if target_type == "local" and local_path:
            p = Path(local_path).expanduser()
            try:
                p.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise HTTPException(400, f"cannot create {p}: {exc}")
            if p == settings.paths.library or settings.paths.library in p.parents:
                raise HTTPException(400, f"target {p} overlaps the library — pick a different folder")

        settings_writer.update_sync(
            enabled=enabled, target_type=target_type, local_path=local_path,
            remote=remote, source=source, config_dir=settings.config_dir,
        )
        settings.sync.enabled     = enabled
        settings.sync.target_type = target_type
        settings.sync.local_path  = local_path
        settings.sync.remote      = remote
        settings.sync.source      = source
        return {"ok": True}

    @app.get("/api/fs/list")
    def api_fs_list(path: str = Query("")):
        """Browse server-side directories so the UI can offer a real folder
        picker. Returns the absolute path, parent, and the immediate
        subdirectories. Files are deliberately excluded — we're picking a
        backup target, not an existing file.

        Default path is the user's home directory — `/` is rarely a useful
        starting point for picking a backup target (it's full of system
        directories the user doesn't care about).
        """
        try:
            start = (path or str(Path.home())).strip()
            target = Path(start).expanduser().resolve()
        except Exception as exc:
            raise HTTPException(400, f"invalid path: {exc}")
        if not target.exists():
            # Fall back to the closest existing ancestor — useful when the
            # user pastes a path that doesn't exist yet.
            candidate = target
            while candidate != candidate.parent and not candidate.exists():
                candidate = candidate.parent
            target = candidate
        if not target.is_dir():
            target = target.parent

        entries: list[dict] = []
        try:
            for child in sorted(target.iterdir(), key=lambda p: p.name.lower()):
                if child.name.startswith("."):
                    continue
                try:
                    if child.is_dir():
                        entries.append({"name": child.name, "path": str(child)})
                except OSError:
                    pass  # broken symlink / permission issue — skip silently
        except PermissionError:
            return {
                "path": str(target),
                "parent": str(target.parent) if target != target.parent else "",
                "entries": [],
                "error": "permission denied",
                "shortcuts": _fs_shortcuts(settings),
            }
        return {
            "path": str(target),
            "parent": str(target.parent) if target != target.parent else "",
            "entries": entries,
            "shortcuts": _fs_shortcuts(settings),
        }

    @app.post("/api/sync/check-path")
    def api_sync_check_path(payload: dict):
        """Quickly probe whether a path is writable, so the UI can give live
        feedback as the user types or picks a folder."""
        path = (payload.get("path") or "").strip()
        if not path:
            return {"ok": False, "error": "no path"}
        p = Path(path).expanduser()
        if p == settings.paths.library or settings.paths.library in p.parents:
            return {"ok": False, "error": "overlaps library"}
        try:
            p.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return {"ok": False, "error": str(exc)}
        # Try a tiny touch-and-delete to confirm writability.
        probe = p / ".docusort-probe"
        try:
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
        except OSError as exc:
            return {"ok": False, "error": f"not writable: {exc}"}
        try:
            usage = shutil.disk_usage(p)
            return {"ok": True, "free_bytes": usage.free, "total_bytes": usage.total}
        except OSError:
            return {"ok": True}

    @app.post("/api/settings/language")
    def api_settings_language(payload: dict):
        from .. import settings_writer
        lang = (payload.get("default_language") or "").strip()
        if lang not in SUPPORTED:
            raise HTTPException(400, f"unsupported language: {lang}")
        settings_writer.update_web(
            default_language=lang, config_dir=settings.config_dir,
        )
        settings.web.default_language = lang
        return {"ok": True}

    @app.post("/api/settings/web")
    def api_settings_web(payload: dict):
        from .. import settings_writer
        host = (payload.get("host") or "").strip()
        try:
            port = int(payload.get("port") or 0)
        except (TypeError, ValueError):
            raise HTTPException(400, "port must be an integer")
        if port < 1 or port > 65535:
            raise HTTPException(400, "port must be between 1 and 65535")
        if host and host not in ("0.0.0.0", "127.0.0.1", "::", "::1") \
                and not host.replace(".", "").replace(":", "").replace("-", "").isalnum():
            # Light sanity check — accept hostnames + dotted IPs, refuse weird input.
            raise HTTPException(400, f"unusual host value: {host!r}")
        settings_writer.update_web(
            host=host or None, port=port, config_dir=settings.config_dir,
        )
        if host:
            settings.web.host = host
        settings.web.port = port
        return {"ok": True, "restart_required": True}

    @app.get("/api/setup/state")
    def api_setup_state():
        return {
            "configured": is_configured(settings),
            "provider":   settings.ai.provider,
            "model":      settings.ai.model,
            "has_api_key": bool(get_api_key(settings)),
        }

    @app.post("/api/setup/restart")
    def api_setup_restart():
        """Trigger the same systemd restart path the auto-updater uses, so
        the wizard can hand over to a fully reloaded process running with
        the new provider config."""
        from .. import updater
        return updater.restart_service()

    # ---------- Retry failed / review docs ----------
    @app.post("/api/document/{doc_id}/retry")
    def retry_doc(doc_id: int):
        if classifier is None:
            raise HTTPException(503, "classifier not available in this process")
        from ..retry import retry_document
        try:
            return retry_document(doc_id, settings, classifier, db)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        except Exception as exc:
            logger.exception("retry failed for %d", doc_id)
            raise HTTPException(500, f"retry failed: {exc}")

    # ---------- Updater ----------
    @app.get("/api/version")
    def api_version():
        from .. import updater
        return updater.version_info()

    @app.post("/api/update")
    def api_update(tag: str | None = Query(None,
                   description="Force-install this exact tag (e.g. 'v0.17.2'). "
                               "Skips the GitHub release-info lookup, useful when "
                               "the unauthenticated GitHub API is rate-limited.")):
        from .. import updater
        try:
            result = updater.install_latest(tag=tag)
        except Exception as exc:
            logger.exception("Update failed")
            raise HTTPException(500, f"Update failed: {exc}")
        if result.get("updated"):
            restart = updater.restart_service()
            result["restart"] = restart
        return result

    return app
