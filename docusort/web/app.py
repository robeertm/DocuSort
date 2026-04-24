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
from ..config import AppSettings
from ..db import Database, MODEL_PRICING
from ..i18n import (
    LANGUAGE_NAMES, SUPPORTED, all_translations_for_js, detect_language, translate,
)
from .. import __version__


logger = logging.getLogger("docusort.web")
ALLOWED_SUFFIXES = {".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}


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

    def _lang(request: Request) -> str:
        return detect_language(
            cookie=request.cookies.get("lang"),
            accept_language=request.headers.get("accept-language"),
            default=settings.web.default_language,
        )

    def base_ctx(request: Request) -> dict:
        lang = _lang(request)
        return {
            "request": request,
            "version": __version__,
            "categories": category_names,
            "lang": lang,
            "supported_langs": [(code, LANGUAGE_NAMES[code]) for code in SUPPORTED],
            "t": lambda key, **kw: translate(key, lang, **kw),
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
        status: str | None = Query(None),
        year: str | None = Query(None),
        q: str | None = Query(None),
        trash: bool = Query(False),
        partial: bool = Query(False),
    ):
        docs = db.list_documents(
            category=category or None, status=status or None,
            year=year or None, query=q or None, trash=trash, limit=500,
        )
        years = db.distinct_years()
        tree = db.tree()
        tpl = "_card_grid.html" if partial else "library.html"
        return templates.TemplateResponse(
            request, tpl,
            {**base_ctx(request), "docs": docs, "years": years, "tree": tree, "trash": trash,
             "filter": {"category": category, "status": status, "year": year, "q": q}},
        )

    # ---------- Document detail ----------
    @app.get("/document/{doc_id}", response_class=HTMLResponse)
    def document_detail(request: Request, doc_id: int):
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "Document not found")
        return templates.TemplateResponse(
            request, "document.html", {**base_ctx(request), "doc": doc},
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

    @app.post("/document/{doc_id}/recategorize")
    def recategorize(
        doc_id: int,
        category: str = Form(...),
    ):
        if category not in category_names:
            raise HTTPException(400, f"Unknown category: {category}")
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "Document not found")

        old_path = Path(doc["library_path"])
        if not old_path.exists():
            raise HTTPException(404, "Original file missing on disk")

        year = (doc["doc_date"] or doc["created_at"])[:4]
        new_dir = settings.paths.library / year / category
        new_dir.mkdir(parents=True, exist_ok=True)
        new_path = new_dir / old_path.name
        shutil.move(str(old_path), str(new_path))

        db.update_category(doc_id, category)
        db.update_paths(doc_id, str(new_path))
        logger.info("Recategorised doc %d: %s -> %s", doc_id, doc["category"], category)
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
        include_trash: bool = Query(False),
    ):
        from ..export import stream_zip, suggested_filename
        name = suggested_filename(category=category, year=year, trash=include_trash)
        return StreamingResponse(
            stream_zip(settings, db, category=category, year=year, include_trash=include_trash),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )

    # ---------- Sync ----------
    @app.get("/api/sync/status")
    def sync_status():
        from .. import sync as sync_mod
        return sync_mod.status(settings)

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
