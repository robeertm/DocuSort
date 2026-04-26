"""FastAPI application for DocuSort's web UI.

The watcher runs in a separate thread (started by main.py). This module
exposes a thin, synchronous HTTP layer over the shared SQLite database
and the inbox / library folders.
"""

from __future__ import annotations

import logging
import shutil
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
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
        return templates.TemplateResponse(
            request, "dashboard.html",
            {**base_ctx(request), "stats": stats, "recent": recent,
             "review_count": review_count},
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
        return templates.TemplateResponse(
            request, "document.html",
            {**base_ctx(request), "doc": doc, "receipt": receipt},
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
        from ..receipts import ReceiptExtractor
        extractor = ReceiptExtractor(
            classifier.provider, settings.ai.model,
            max_text_chars=settings.ai.max_text_chars,
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
             "oauth_backends":   sorted(rclone_setup.OAUTH_BACKENDS)},
        )

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
        if provider != "openai_compat" and api_key is not None and not api_key.strip():
            # Allow blank when there's already a key — either in secrets.yaml
            # or in the legacy environment variable (ANTHROPIC_API_KEY etc.).
            if not get_api_key(settings, provider):
                raise HTTPException(400, "api_key required for this provider")
            api_key = None  # don't overwrite

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
    def api_update():
        from .. import updater
        try:
            result = updater.install_latest()
        except Exception as exc:
            logger.exception("Update failed")
            raise HTTPException(500, f"Update failed: {exc}")
        if result.get("updated"):
            restart = updater.restart_service()
            result["restart"] = restart
        return result

    return app
