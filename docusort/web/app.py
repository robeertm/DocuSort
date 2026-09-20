"""FastAPI application for DocuSort's web UI.

The watcher runs in a separate thread (started by main.py). This module
exposes a thin, synchronous HTTP layer over the shared SQLite database
and the inbox / library folders.
"""

from __future__ import annotations

import json
import logging
import shutil
import threading
import time
import uuid
from urllib.parse import quote
from typing import Any
from datetime import datetime
from pathlib import Path

from fastapi import Body, FastAPI, File, Form, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .. import auth as _auth
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
    """Convert a USD AI cost into euros. 🔴 NOT a euro formatter — passing
    an amount that is already in euros through this silently shows 93 % of
    it. Use the `money` filter for real euro amounts."""
    return f"{usd * 0.93:.2f} €"


def _money(value: float | None) -> str:
    """Format an amount that is ALREADY in euros, German style."""
    if value is None:
        return "—"
    text = f"{value:,.2f}"
    return text.replace(",", "\x00").replace(".", ",").replace("\x00", ".") + " €"


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


def _report_to_dict(report) -> dict:
    """Convert an `ImportReport` dataclass into a JSON-friendly dict
    for the upload endpoint. Kept here (rather than as a method)
    because the dataclass lives in finance/csv_import.py and we want
    the API shape decoupled from the importer."""
    return {
        "file_label":       report.file_label,
        "rows_seen":        report.rows_seen,
        "rows_inserted":    report.rows_inserted,
        "rows_duplicate":   report.rows_duplicate,
        "rows_skipped":     report.rows_skipped,
        "rows_invalid":     report.rows_invalid,
        "accounts_touched": report.accounts_touched,
        "period_start":     report.period_start,
        "period_end":       report.period_end,
        "transfers_tagged": report.transfers_tagged,
        "bank":             report.bank,
        "rows_repaired":    getattr(report, "rows_repaired", 0),
        "balance_note":     getattr(report, "balance_note", ""),
        "gaps_filled":      getattr(report, "gaps_filled", 0),
        "synthetic_replaced": getattr(report, "synthetic_replaced", 0),
        "rows_pending":     getattr(report, "rows_pending", 0),
        "pending_moved":    getattr(report, "pending_moved", 0),
        "rows_overlap":     getattr(report, "rows_overlap", 0),
        "statements":       getattr(report, "statements", 0),
        "statements_skipped": getattr(report, "statements_skipped", 0),
        "queued":           False,
        "errors":           report.errors,
    }


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
        return {
            "kind": j["kind"],
            "started_at": j["started_at"],
            "elapsed_s": elapsed,
            "current_page": j.get("current_page", 0),
            "total_pages":  j.get("total_pages", 0),
        }


# Global single-instance receipt re-extraction job. Started from the
# /analytics page or via the API; the watcher thread iterates all
# Kassenzettel docs and updates the shared status dict so the UI can
# poll progress.
_receipt_reextract_lock = threading.Lock()
_receipt_reextract: dict | None = None


def _doc_job_progress(doc_id: int, *, current_page: int, total_pages: int) -> None:
    """Update the per-doc job entry with per-page progress. Called by
    the extractor's `on_page_progress` callback so the doc-detail page
    can show "Seite X/Y" while a long PDF is grinding through."""
    with _doc_jobs_lock:
        j = _doc_jobs.get(doc_id)
        if j is None:
            return
        j["current_page"] = int(current_page)
        j["total_pages"] = int(total_pages)


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
    templates.env.filters["money"] = _money

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


    # ---------- Authentication & permissions (0.48.0) ----------
    # the owner's rule: he is admin and may do everything; users may add
    # documents but never delete, and never reach the settings.
    # `auth.USER_ALLOW` is an allowlist, so a route added next week is
    # admin-only until someone opens it on purpose.

    _login_fails: dict[str, list[float]] = {}
    _LOGIN_MAX_FAILS = 10
    _LOGIN_WINDOW = 900.0  # seconds

    def _login_blocked(key: str) -> bool:
        now = time.time()
        hits = [t for t in _login_fails.get(key, []) if now - t < _LOGIN_WINDOW]
        _login_fails[key] = hits
        return len(hits) >= _LOGIN_MAX_FAILS

    def _login_failed(key: str) -> None:
        _login_fails.setdefault(key, []).append(time.time())

    def _wants_json(request: Request) -> bool:
        return (request.url.path.startswith("/api/")
                or request.headers.get("accept", "").startswith("application/json"))

    def _deny_html(title: str, body: str, status: int) -> HTMLResponse:
        """Self-contained refusal page.

        Middleware runs before routing, so rendering base.html here would
        mean rebuilding its whole context by hand. A standalone page has
        no such dependency and cannot break when base.html changes.
        """
        return HTMLResponse(
            "<!doctype html><meta charset=utf-8>"
            "<meta name=viewport content='width=device-width,initial-scale=1'>"
            f"<title>{title}</title>"
            "<style>body{font:16px/1.6 system-ui,sans-serif;margin:0;min-height:100vh;"
            "display:grid;place-items:center;background:#0f172a;color:#e2e8f0}"
            ".c{max-width:30rem;padding:2rem;text-align:center}"
            "h1{font-size:1.25rem;margin:0 0 .5rem}p{margin:0 0 1.5rem;color:#94a3b8}"
            "a{color:#38bdf8}</style>"
            f"<div class=c><h1>{title}</h1><p>{body}</p>"
            "<a href='/'>&larr; zurück</a></div>",
            status_code=status,
        )

    def _set_session_cookie(resp, token: str, request: Request) -> None:
        resp.set_cookie(
            _auth.SESSION_COOKIE, token,
            max_age=_auth.SESSION_DAYS * 86400,
            httponly=True,
            samesite="lax",
            # Only mark Secure when the request actually arrived over
            # HTTPS — otherwise a plain-HTTP install could never log in,
            # because the browser would refuse to send the cookie back.
            secure=request.url.scheme == "https",
            path="/",
        )

    @app.middleware("http")
    async def auth_gate(request: Request, call_next):
        path = request.url.path
        request.state.user = None

        if _auth.is_public_path(path):
            return await call_next(request)

        # No administrator yet — an install upgrading from <= 0.47.x lands
        # here. Everything funnels into the claim page so the owner takes
        # ownership before anyone else can.
        if db.admin_count() == 0:
            if _wants_json(request):
                return JSONResponse(
                    {"detail": "DocuSort has no administrator yet — open /setup/admin"},
                    status_code=503,
                )
            return RedirectResponse("/setup/admin", status_code=303)

        token = request.cookies.get(_auth.SESSION_COOKIE, "")
        row = db.session_user(_auth.session_token_hash(token)) if token else None
        if row is None:
            if _wants_json(request):
                return JSONResponse({"detail": "not authenticated"}, status_code=401)
            nxt = request.url.path
            if request.url.query:
                nxt += "?" + request.url.query
            return RedirectResponse(f"/login?next={quote(nxt)}", status_code=303)

        user = _auth.User(
            id=int(row["id"]), username=row["username"],
            display_name=row["display_name"] or "", role=row["role"],
            must_change_password=bool(row["must_change_password"]),
        )
        request.state.user = user

        # A user invited with a one-time password gets nowhere until it
        # is replaced.
        if user.must_change_password and not (
            path == "/konto" or path.startswith("/api/me") or path == "/logout"
        ):
            if _wants_json(request):
                return JSONResponse({"detail": "password change required"}, status_code=403)
            return RedirectResponse("/konto", status_code=303)

        if not _auth.may_access(user, request.method, path):
            logger.info("auth: %s (%s) denied %s %s",
                        user.username, user.role, request.method, path)
            if _wants_json(request):
                return JSONResponse(
                    {"detail": "forbidden — this action is reserved for the administrator"},
                    status_code=403,
                )
            return _deny_html(
                "Kein Zugriff",
                "Dieser Bereich ist dem Administrator vorbehalten.",
                403,
            )
        return await call_next(request)

    def _require_admin(request: Request) -> _auth.User:
        """Guard for handlers. The middleware already refused non-admins
        on admin routes; this is the second lock, so a route that is ever
        mis-listed in the allowlist still cannot be abused."""
        me = getattr(request.state, "user", None)
        if me is None or not me.is_admin:
            raise HTTPException(status_code=403, detail="admin only")
        return me

    # ----- first-run claim -----
    @app.get("/setup/admin", response_class=HTMLResponse)
    def setup_admin_page(request: Request):
        if db.admin_count() > 0:
            return RedirectResponse("/login", status_code=303)
        return templates.TemplateResponse(
            request, "setup_admin.html", {**base_ctx(request)},
        )

    @app.post("/setup/admin")
    def setup_admin_submit(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
        password2: str = Form(""),
        display_name: str = Form(""),
    ):
        # Refuse to create a second "first" admin — without this check the
        # claim page would stay open forever.
        if db.admin_count() > 0:
            return RedirectResponse("/login", status_code=303)
        lang = _lang(request)
        err = _auth.username_problem(username) or _auth.password_problem(password)
        if not err and password != password2:
            err = "auth.err.password_mismatch"
        if not err and db.user_by_name(username):
            err = "auth.err.username_taken"
        if err:
            return templates.TemplateResponse(
                request, "setup_admin.html",
                {**base_ctx(request), "error": translate(err, lang),
                 "username": username, "display_name": display_name},
                status_code=400,
            )
        uid = db.user_create(username, _auth.hash_password(password),
                             _auth.ROLE_ADMIN, display_name)
        token = _auth.new_session_token()
        db.session_create(_auth.session_token_hash(token), uid, _auth.SESSION_DAYS,
                          request.headers.get("user-agent", ""))
        db.user_touch_login(uid)
        logger.info("auth: administrator '%s' claimed this install", username)
        resp = RedirectResponse("/", status_code=303)
        _set_session_cookie(resp, token, request)
        return resp

    # ----- login / logout -----
    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request, next: str = "/"):
        if db.admin_count() == 0:
            return RedirectResponse("/setup/admin", status_code=303)
        token = request.cookies.get(_auth.SESSION_COOKIE, "")
        if token and db.session_user(_auth.session_token_hash(token)):
            return RedirectResponse("/", status_code=303)
        return templates.TemplateResponse(
            request, "login.html", {**base_ctx(request), "next": next},
        )

    @app.post("/login")
    def login_submit(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
        next: str = Form("/"),
    ):
        lang = _lang(request)
        key = (username or "").strip().lower()
        if _login_blocked(key):
            return templates.TemplateResponse(
                request, "login.html",
                {**base_ctx(request), "next": next,
                 "error": translate("auth.err.too_many", lang), "username": username},
                status_code=429,
            )
        row = db.user_by_name(username or "")
        ok = bool(row) and row["is_active"] and _auth.verify_password(
            password, row["password_hash"] or "")
        if not ok:
            _login_failed(key)
            logger.warning("auth: failed login for '%s' from %s",
                           username, request.client.host if request.client else "?")
            return templates.TemplateResponse(
                request, "login.html",
                {**base_ctx(request), "next": next,
                 "error": translate("auth.err.bad_credentials", lang),
                 "username": username},
                status_code=401,
            )
        _login_fails.pop(key, None)
        token = _auth.new_session_token()
        db.session_create(_auth.session_token_hash(token), int(row["id"]),
                          _auth.SESSION_DAYS, request.headers.get("user-agent", ""))
        db.user_touch_login(int(row["id"]))
        # Only ever redirect to a path on this site — an open redirect
        # would turn the login into a phishing hop.
        target = next if next.startswith("/") and not next.startswith("//") else "/"
        resp = RedirectResponse(target, status_code=303)
        _set_session_cookie(resp, token, request)
        return resp

    @app.get("/logout")
    @app.post("/logout")
    def logout(request: Request):
        token = request.cookies.get(_auth.SESSION_COOKIE, "")
        if token:
            db.session_delete(_auth.session_token_hash(token))
        resp = RedirectResponse("/login", status_code=303)
        resp.delete_cookie(_auth.SESSION_COOKIE, path="/")
        return resp

    # ----- own account -----
    @app.get("/konto", response_class=HTMLResponse)
    def account_page(request: Request):
        return templates.TemplateResponse(
            request, "konto.html", {**base_ctx(request)},
        )

    @app.get("/api/me")
    def api_me(request: Request):
        me = getattr(request.state, "user", None)
        if me is None:
            raise HTTPException(status_code=401, detail="not authenticated")
        return {"id": me.id, "username": me.username, "display_name": me.display_name,
                "role": me.role, "is_admin": me.is_admin,
                "must_change_password": me.must_change_password}

    @app.post("/api/me/password")
    def api_me_password(request: Request, payload: dict = Body(...)):
        me = getattr(request.state, "user", None)
        if me is None:
            raise HTTPException(status_code=401, detail="not authenticated")
        current = str(payload.get("current") or "")
        new = str(payload.get("new") or "")
        row = db.user_get(me.id)
        if not row or not _auth.verify_password(current, row["password_hash"] or ""):
            raise HTTPException(status_code=403, detail="current password is wrong")
        problem = _auth.password_problem(new)
        if problem:
            raise HTTPException(status_code=400,
                                detail=translate(problem, _lang(request)))
        db.user_update(me.id, password_hash=_auth.hash_password(new),
                       must_change_password=0)
        # Every other device is logged out, then this one gets a fresh
        # session — a password change has to invalidate what it replaces.
        db.session_delete_for_user(me.id)
        token = _auth.new_session_token()
        db.session_create(_auth.session_token_hash(token), me.id, _auth.SESSION_DAYS,
                          request.headers.get("user-agent", ""))
        resp = JSONResponse({"ok": True})
        _set_session_cookie(resp, token, request)
        return resp

    # ----- user administration -----
    @app.get("/users", response_class=HTMLResponse)
    def users_page(request: Request):
        _require_admin(request)
        return templates.TemplateResponse(
            request, "users.html",
            {**base_ctx(request), "users": db.user_list()},
        )

    @app.get("/api/users")
    def api_users(request: Request):
        _require_admin(request)
        return {"users": db.user_list(), "admins": db.admin_count()}

    @app.post("/api/users")
    def api_user_create(request: Request, payload: dict = Body(...)):
        _require_admin(request)
        lang = _lang(request)
        username = str(payload.get("username") or "").strip()
        password = str(payload.get("password") or "")
        role = str(payload.get("role") or _auth.ROLE_USER)
        display_name = str(payload.get("display_name") or "").strip()
        if role not in _auth.ROLES:
            raise HTTPException(status_code=400, detail="unknown role")
        problem = _auth.username_problem(username) or _auth.password_problem(password)
        if problem:
            raise HTTPException(status_code=400, detail=translate(problem, lang))
        if db.user_by_name(username):
            raise HTTPException(status_code=409,
                                detail=translate("auth.err.username_taken", lang))
        uid = db.user_create(username, _auth.hash_password(password), role,
                             display_name, must_change=bool(payload.get("must_change", True)))
        logger.info("auth: user '%s' created with role %s", username, role)
        return {"ok": True, "id": uid}

    @app.patch("/api/users/{user_id}")
    def api_user_update(request: Request, user_id: int, payload: dict = Body(...)):
        me = _require_admin(request)
        lang = _lang(request)
        row = db.user_get(user_id)
        if not row:
            raise HTTPException(status_code=404, detail="no such user")
        fields: dict = {}
        if "display_name" in payload:
            fields["display_name"] = str(payload["display_name"] or "").strip()
        if "role" in payload:
            role = str(payload["role"])
            if role not in _auth.ROLES:
                raise HTTPException(status_code=400, detail="unknown role")
            fields["role"] = role
        if "is_active" in payload:
            fields["is_active"] = 1 if payload["is_active"] else 0
        if payload.get("password"):
            problem = _auth.password_problem(str(payload["password"]))
            if problem:
                raise HTTPException(status_code=400, detail=translate(problem, lang))
            fields["password_hash"] = _auth.hash_password(str(payload["password"]))
            fields["must_change_password"] = 1 if payload.get("must_change", True) else 0

        # The last active administrator may not demote, disable or lock
        # himself out. Without this the archive becomes unreachable and
        # only a hand-edit of the database gets it back.
        loses_admin = (fields.get("role") == _auth.ROLE_USER
                       or fields.get("is_active") == 0)
        if loses_admin and row["role"] == _auth.ROLE_ADMIN and db.admin_count() <= 1:
            raise HTTPException(status_code=409,
                                detail=translate("auth.err.last_admin", lang))
        db.user_update(user_id, **fields)
        # A changed password, role or activation must end the sessions
        # that were issued under the old state.
        if {"password_hash", "role", "is_active"} & set(fields):
            db.session_delete_for_user(user_id)
            if user_id == me.id and "password_hash" in fields:
                token = _auth.new_session_token()
                db.session_create(_auth.session_token_hash(token), me.id,
                                  _auth.SESSION_DAYS, request.headers.get("user-agent", ""))
                resp = JSONResponse({"ok": True})
                _set_session_cookie(resp, token, request)
                return resp
        return {"ok": True}

    @app.delete("/api/users/{user_id}")
    def api_user_delete(request: Request, user_id: int):
        me = _require_admin(request)
        lang = _lang(request)
        row = db.user_get(user_id)
        if not row:
            raise HTTPException(status_code=404, detail="no such user")
        if row["role"] == _auth.ROLE_ADMIN and db.admin_count() <= 1:
            raise HTTPException(status_code=409,
                                detail=translate("auth.err.last_admin", lang))
        if user_id == me.id:
            raise HTTPException(status_code=409,
                                detail=translate("auth.err.self_delete", lang))
        db.session_delete_for_user(user_id)
        db.user_delete(user_id)
        logger.info("auth: user '%s' deleted", row["username"])
        return {"ok": True}

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

    def _cat_labels(lang: str) -> dict[str, str]:
        from ..finance.categories import TX_CATEGORIES
        out = {c: translate(f"finance.cat.{c}", lang) for c in TX_CATEGORIES}
        for c in db.finance_custom_categories():
            out[c["key"]] = c["label"]
        return out

    def _cat_keys(request: Request) -> list[str]:
        """Category keys sorted by their label in the user's language —
        the selects read like an index, `sonstiges` stays last."""
        labels = _cat_labels(_lang(request))
        keys = db.finance_category_keys()
        body = sorted((k for k in keys if k != "sonstiges"), key=lambda k: labels.get(k, k).casefold())
        return body + (["sonstiges"] if "sonstiges" in keys else [])

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
            # The signed-in user (None while unauthenticated). Templates
            # hide admin-only controls on this; the server still enforces
            # every rule independently in `auth_gate`.
            "me": getattr(request.state, "user", None),
            "categories": category_names,
            "subcategory_map": subcategory_map,
            "subcategory_labels": sub_labels,
            "lang": lang,
            "supported_langs": [(code, LANGUAGE_NAMES[code]) for code in SUPPORTED],
            "t": lambda key, **kw: translate(key, lang, **kw),
            "cat": lambda name: category_label(name, lang),
            "sub": lambda parent, name: subcategory_label(parent, name, lang),
            "js_translations": all_translations_for_js(lang),
            # Category labels: built-in via i18n, user-defined from the DB.
            "cat_labels": _cat_labels(lang),
            "custom_categories": db.finance_custom_categories(),
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
    # Public sort keys the /library UI may request. Anything else falls
    # back to doc_date in db.list_documents (whitelist enforced there).
    _LIBRARY_SORTS = {
        "doc_date", "created_at", "sender", "subject",
        "category", "file_size", "confidence", "page_count", "relevance",
    }

    @app.get("/library", response_class=HTMLResponse)
    def library(
        request: Request,
        category: str | None = Query(None),
        subcategory: str | None = Query(None),
        tag: str | None = Query(None),
        status: str | None = Query(None),
        year: str | None = Query(None),
        q: str | None = Query(None),
        sort: str | None = Query(None),
        dir: str | None = Query(None),
        doc_from: str | None = Query(None),
        doc_to: str | None = Query(None),
        scan_from: str | None = Query(None),
        scan_to: str | None = Query(None),
        trash: bool = Query(False),
        partial: bool = Query(False),
    ):
        # The trash lives at /library?trash=1 — a query parameter, which
        # the path allowlist in auth.py cannot express. Deleted documents
        # are the admin's business, so refuse that view here.
        _me_lib = getattr(request.state, "user", None)
        if trash and _me_lib is not None and not _me_lib.is_admin:
            raise HTTPException(status_code=403, detail="admin only")

        # Sanitise sort + direction here so the template can echo the
        # exact effective values back into its controls.
        sort_key = sort if sort in _LIBRARY_SORTS else "doc_date"
        # Relevance only makes sense with an active query.
        if sort_key == "relevance" and not (q or "").strip():
            sort_key = "doc_date"
        sort_dir = "asc" if (dir or "").lower() == "asc" else "desc"

        docs = db.list_documents(
            category=category or None, subcategory=subcategory or None,
            tag=tag or None, status=status or None,
            year=year or None, query=q or None, trash=trash,
            order_by=sort_key, sort_dir=sort_dir,
            doc_from=doc_from or None, doc_to=doc_to or None,
            scan_from=scan_from or None, scan_to=scan_to or None,
            limit=500,
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
                        "tag": tag, "status": status, "year": year, "q": q,
                        "sort": sort_key, "dir": sort_dir,
                        "doc_from": doc_from, "doc_to": doc_to,
                        "scan_from": scan_from, "scan_to": scan_to}},
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
    def document_detail(
        request: Request,
        doc_id: int,
        # Sibling-nav filters: when the user clicked through from
        # /library?category=Kontoauszug&year=2026 the link forwards
        # those query params here, so ←/→ keys can step through the
        # same filtered slice instead of jumping randomly across the
        # whole archive.
        category: str | None = Query(None),
        subcategory: str | None = Query(None),
        tag: str | None = Query(None),
        status: str | None = Query(None),
        year: str | None = Query(None),
        q: str | None = Query(None),
        trash: bool = Query(False),
    ):
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "Document not found")
        if doc.get("category") == "_csv_container":
            # The per-account CSV-import container is not a document: send
            # the visitor to that account's bookings instead of a page that
            # tries to load a PDF that never existed.
            acc = db._conn.execute(
                "SELECT account_id FROM statements WHERE doc_id = ? LIMIT 1", (doc_id,)
            ).fetchone()
            target = f"/transactions?account_id={acc['account_id']}" if acc and acc["account_id"] else "/transactions"
            return RedirectResponse(target, status_code=302)
        _decode_tags(doc)
        nav_filters = {
            "category": category or None,
            "subcategory": subcategory or None,
            "tag": tag or None,
            "status": status or None,
            "year": year or None,
            "q": q or None,
            "trash": "1" if trash else None,
        }
        # Default the filter to the doc's own category + year when
        # no explicit filter came in via the URL — direct visits
        # (notification, bookmarked URL) still get sibling nav inside
        # something sensible.
        if not any(v for v in nav_filters.values()):
            nav_filters["category"] = doc.get("category") or None
            d_date = doc.get("doc_date") or ""
            if len(d_date) >= 4 and d_date[:4].isdigit():
                nav_filters["year"] = d_date[:4]
        siblings = db.siblings_of(
            doc_id,
            category=nav_filters["category"],
            subcategory=nav_filters["subcategory"],
            tag=nav_filters["tag"],
            status=nav_filters["status"],
            year=nav_filters["year"],
            query=nav_filters["q"],
            trash=trash,
        )
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
        from ..finance.categories import TX_CATEGORIES, TX_TYPES
        return templates.TemplateResponse(
            request, "document.html",
            {**base_ctx(request), "doc": doc, "receipt": receipt,
             "statement": statement,
             "payment": db.deadline_payment(doc_id),
             "is_statement_candidate": is_statement_candidate,
             "ai_provider": settings.ai.provider,
             "shop_types": list(SHOP_TYPES),
             "item_categories": list(ITEM_CATEGORIES),
             "payment_methods": list(PAYMENT_METHODS),
             "tx_categories": _cat_keys(request),
             "tx_types":      list(TX_TYPES),
             "siblings":      siblings,
             "nav_filters":   nav_filters},
        )

    def path_is_file(p: str) -> bool:
        try:
            return Path(p).is_file()
        except OSError:
            return False

    @app.get("/document/{doc_id}/file")
    def document_file(doc_id: int, download: bool = False):
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "Document not found")
        # CSV-import container stubs have no file (library_path ''), and
        # Path('') is '.', which exists → FileResponse on a directory → 500.
        if not doc.get("library_path") or not path_is_file(doc["library_path"]):
            raise HTTPException(404, "File missing on disk")
        path = Path(doc["library_path"])
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
        due_date: str = Form(""),
        due_kind: str = Form(""),
    ):
        from ..organizer import target_path
        from ..finance.dates import normalise_date

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

        # Deadline: normalise the entered date; an empty field clears it.
        clean_due = normalise_date(due_date.strip()) if due_date.strip() else ""
        clean_due_kind = due_kind.strip().lower()
        if clean_due_kind not in ("zahlung", "kuendigung"):
            clean_due_kind = ""
        if not clean_due:
            clean_due_kind = ""

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
            due_date=clean_due,
            due_kind=clean_due_kind,
        )
        logger.info("Edited doc %d -> %s (sub=%s tags=%s)",
                    doc_id, new_path.name, sub, tag_list)
        return RedirectResponse(f"/document/{doc_id}", status_code=303)

    # ---------- Upload ----------
    @app.get("/upload", response_class=HTMLResponse)
    def upload_page(request: Request):
        # Upload is the ONE place: documents, receipts, Kontoauszug-PDFs and
        # bank CSVs all go in here. The only thing the page cannot decide by
        # itself is which account a CSV without its own IBAN belongs to
        # (N26, comdirect) — so it gets the account list and asks inline.
        me = getattr(request.state, "user", None)
        accounts: list[dict] = []
        if me is not None and me.is_admin:
            for a in db.list_accounts():
                iban = str(a.get("iban") or "")
                if not iban or iban.startswith("CARD-"):
                    continue
                accounts.append({
                    "iban": iban,
                    "label": f"{a.get('bank_name') or '?'} ···{iban[-4:]}",
                })
        return templates.TemplateResponse(
            request, "upload.html",
            {**base_ctx(request), "upload_accounts": accounts},
        )

    @app.post("/upload")
    async def upload_file(files: list[UploadFile] = File(...),
                          account_iban: str = Form("")):
        saved = []
        rejected = []
        imported = []
        for up in files:
            suffix = Path(up.filename or "").suffix.lower()

            # A bank CSV dropped here goes straight into the finances
            # instead of being refused. the owner asked for "egal wo" — the
            # upload page and the finance page should both work, so the
            # file type decides the route, not the page.
            if suffix == ".csv":
                from ..finance.csv_import import import_csv
                data = up.file.read()
                try:
                    rep = import_csv(db, data, file_label=up.filename or "",
                                     account_iban_hint=account_iban.strip())
                    # 🔴 import_csv reports a missing own IBAN through
                    # `report.errors`, NOT by raising — a file from N26 or
                    # comdirect would otherwise look like a clean import
                    # that silently stored nothing.
                    if rep.errors:
                        imported.append({
                            "file_label": up.filename or "",
                            "error": "; ".join(rep.errors),
                            "hint_iban": True,
                        })
                        logger.warning("CSV-Upload %s ohne eigene IBAN: %s",
                                       up.filename, rep.errors)
                    else:
                        imported.append(_report_to_dict(rep))
                        logger.info("Uploaded CSV %s -> %d Buchungen",
                                    up.filename, rep.rows_inserted)
                except Exception as exc:  # noqa: BLE001
                    # The usual cause is a file without its own IBAN (N26,
                    # comdirect). Say so and point at the one place that can
                    # ask for it, instead of a bare failure.
                    imported.append({
                        "file_label": up.filename or "",
                        "error": f"{exc}",
                        "hint_iban": True,
                    })
                    logger.warning("CSV-Upload %s fehlgeschlagen: %s", up.filename, exc)
                continue

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

        return {"saved": saved, "rejected": rejected, "imported": imported}

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
        # v0.55: Wurde der Kassenzettel auf dem Konto wiedergefunden? Wenn
        # nicht — und der Zeitraum ist eingelesen — war es Bargeld, und
        # Bargeld lässt sich hier einer Kategorie zuordnen.
        pay = db.receipt_payment_overview(start=start or "", end=end or "")
        return templates.TemplateResponse(
            request, "analytics.html",
            {**base_ctx(request),
             "summary": summary, "monthly": monthly, "pay": pay,
             "tx_categories": _cat_keys(request),
             "cat_labels": _cat_labels(_lang(request)),
             "receipts": receipts, "items": items, "top_items": top,
             "shop_types": list(SHOP_TYPES),
             "item_categories": list(ITEM_CATEGORIES),
             "filter": {"shop_type": shop_type, "start": start, "end": end, "q": q}},
        )

    @app.post("/api/receipts/{receipt_id}/cash-category")
    def api_receipt_cash_category(receipt_id: int, payload: dict = Body(default={})):
        """Kategorie für einen bar bezahlten Kassenzettel setzen (leer = weg)."""
        cat = str((payload or {}).get("category") or "").strip()
        known = set(db.finance_category_keys())
        if cat and cat not in known:
            raise HTTPException(400, f"unknown category {cat!r}")
        if not db.receipt_set_cash_category(receipt_id, cat):
            raise HTTPException(404, "receipt not found")
        return {"ok": True, "category": cat}

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

    @app.post("/api/receipts/reextract")
    def api_reextract_receipts(force: bool = Query(True)):
        """Start a background job that re-runs the receipt extractor on
        every Kassenzettel doc (force=True) or only on those without a
        receipt row yet (force=False). Single-instance: returns 409 if a
        run is already in flight. Poll /api/receipts/reextract/status."""
        if classifier is None:
            raise HTTPException(503, "classifier not available — finish /setup first")
        global _receipt_reextract
        with _receipt_reextract_lock:
            if _receipt_reextract is not None and _receipt_reextract.get("running"):
                raise HTTPException(409, "a receipt re-extract is already running")
            _receipt_reextract = {
                "running": True,
                "force": bool(force),
                "started_at": time.time(),
                "current": 0,
                "total": 0,
                "ok": 0,
                "failed": 0,
                "last_doc_id": None,
                "last_shop": None,
                "last_error": None,
                "finished_at": None,
            }

        def _progress(idx: int, total: int, doc_id: int, receipt, error):
            with _receipt_reextract_lock:
                if _receipt_reextract is None:
                    return
                _receipt_reextract["current"] = idx
                _receipt_reextract["total"] = total
                _receipt_reextract["last_doc_id"] = doc_id
                if error is not None:
                    _receipt_reextract["failed"] += 1
                    _receipt_reextract["last_error"] = error[:200]
                else:
                    _receipt_reextract["ok"] += 1
                    if receipt is not None:
                        _receipt_reextract["last_shop"] = receipt.shop_name

        def _run():
            global _receipt_reextract
            try:
                from ..receipts import backfill_receipts
                backfill_receipts(
                    settings, db, classifier,
                    dry_run=False, force=force, progress_cb=_progress,
                )
            except Exception as exc:
                logger.exception("Receipt re-extract job crashed")
                with _receipt_reextract_lock:
                    if _receipt_reextract is not None:
                        _receipt_reextract["last_error"] = str(exc)[:200]
            finally:
                with _receipt_reextract_lock:
                    if _receipt_reextract is not None:
                        _receipt_reextract["running"] = False
                        _receipt_reextract["finished_at"] = time.time()

        threading.Thread(target=_run, name="receipt-reextract", daemon=True).start()
        return {"started": True, "force": bool(force)}

    @app.get("/api/receipts/reextract/status")
    def api_reextract_receipts_status():
        with _receipt_reextract_lock:
            if _receipt_reextract is None:
                return {"running": False, "started": False}
            return dict(_receipt_reextract)

    @app.get("/api/receipts/salvage/scan")
    def api_receipts_salvage_scan(limit: int = Query(200)):
        """Scan misclassified docs (Rechnungen, Sonstiges, …) for ones
        that look like Kassenzettel based on OCR signals — Bon-Nr.,
        Terminal-ID, ZU ZAHLEN, etc. No LLM call. The frontend shows
        the list and asks the user to confirm a bulk promotion."""
        from ..receipts_salvage import scan_misclassified
        candidates = scan_misclassified(db, limit=limit)
        return {"candidates": candidates, "count": len(candidates)}

    @app.get("/api/document/{doc_id}/diagnostics")
    def api_document_diagnostics(doc_id: int):
        """Diagnostic dump for one doc — used to figure out why a bon
        wasn't picked up by the salvage banner or why receipt extraction
        produced 0 items. No mutations, no LLM calls."""
        from ..receipts_salvage import (
            text_looks_like_kassenzettel,
            _RECEIPT_SIGNALS,
            _INVOICE_BLOCKERS,
            _PROMOTABLE_CATEGORIES,
        )
        doc = db.get(doc_id)
        if not doc:
            raise HTTPException(404, "document not found")
        text = doc.get("extracted_text") or ""
        is_receipt, signals = text_looks_like_kassenzettel(text)
        blockers_hit = [
            i for i, rx in enumerate(_INVOICE_BLOCKERS) if rx.search(text or "")
        ]
        receipt = db.get_receipt(doc_id)
        items = receipt.get("items", []) if receipt else []
        return {
            "doc_id":            doc_id,
            "category":          doc.get("category"),
            "subcategory":       doc.get("subcategory") or "",
            "status":            doc.get("status"),
            "filename":          doc.get("filename"),
            "library_path":      doc.get("library_path"),
            "deleted_at":        doc.get("deleted_at"),
            "ocr_chars":         len(text),
            "ocr_preview":       text[:1500] if text else "",
            "matched_signals":   signals,
            "signal_count":      len(signals),
            "invoice_blockers_hit": len(blockers_hit),
            "salvage_eligible":  is_receipt and doc.get("category") in _PROMOTABLE_CATEGORIES,
            "salvage_would_promote": is_receipt,
            "salvage_promotable_categories": list(_PROMOTABLE_CATEGORIES),
            "receipt_exists":    receipt is not None,
            "receipt_items":     len(items),
            "receipt_total":     receipt.get("total_amount") if receipt else None,
            "receipt_shop":      receipt.get("shop_name") if receipt else None,
        }

    @app.post("/api/receipts/salvage/promote")
    def api_receipts_salvage_promote(payload: dict = Body(...)):
        """Accept a list of doc IDs the user reviewed and confirmed,
        bulk-update their category to Kassenzettel with status='review'
        so the user double-checks each one. Receipt extraction is NOT
        kicked off automatically — that's a separate user action so the
        token cost is explicit."""
        from ..receipts_salvage import promote_to_kassenzettel
        raw_ids = payload.get("doc_ids") or []
        doc_ids: list[int] = []
        for v in raw_ids:
            try:
                doc_ids.append(int(v))
            except (TypeError, ValueError):
                continue
        if not doc_ids:
            raise HTTPException(400, "doc_ids must be a non-empty list of integers")
        return promote_to_kassenzettel(db, doc_ids)

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

    # ---------- Monats-Ausgaben dashboard ----------
    # Localised month names, indexed 1..12. Kept here rather than relying on
    # the platform locale (the container ships without German locales), so
    # "2026-08" renders as "August 2026" regardless of host settings.
    _MONTH_NAMES = {
        "de": ["", "Januar", "Februar", "März", "April", "Mai", "Juni", "Juli",
               "August", "September", "Oktober", "November", "Dezember"],
        "en": ["", "January", "February", "March", "April", "May", "June", "July",
               "August", "September", "October", "November", "December"],
    }

    def _month_label(ym: str, lang: str) -> str:
        """'2026-08' → 'August 2026' (localised, EN fallback)."""
        try:
            y, m = ym.split("-")
            names = _MONTH_NAMES.get(lang, _MONTH_NAMES["en"])
            return f"{names[int(m)]} {y}"
        except (ValueError, IndexError, KeyError):
            return ym

    def _dmy(iso: str) -> str:
        return f"{iso[8:10]}.{iso[5:7]}.{iso[0:4]}" if iso and len(iso) >= 10 else iso

    def _spending_data(request: Request, month: str | None, mode: str | None,
                       account_ids: list[int] | None = None) -> dict:
        """Everything /ausgaben shows, as JSON — the page renders it with
        Alpine and refetches after a re-categorisation."""
        from datetime import date as _dt
        lang = _lang(request)
        mode = "calendar" if (mode or "").lower() == "calendar" else "salary"
        periods = None
        if mode == "salary":
            fin = settings.finance
            periods = db.finance_salary_periods(
                salary_match=fin.salary_match, anchor_day=fin.period_anchor_day,
                monthly_budget=fin.monthly_budget, today=_dt.today().isoformat(),
            )
            if not periods:
                mode = "calendar"
        data = db.finance_spend_by_category(month=month, periods=periods,
                                            account_ids=account_ids)

        def _label(key: str, a: str, b: str) -> str:
            if not key:
                return ""
            if data["mode"] == "salary":
                return f"{_dmy(a)} – {_dmy(b)}"
            return _month_label(key, lang)

        month_options = []
        if data["mode"] == "salary":
            for p in reversed(periods or []):
                month_options.append({"key": p["start"], "label": f"{_dmy(p['start'])} – {_dmy(p['end'])}"
                                      + (" · " + translate("finance.periods.now", lang) if p.get("is_current") else "")})
        else:
            month_options = [{"key": m, "label": _month_label(m, lang)} for m in data["months"]]
        data["month_label"] = _label(data["month"], data["range_start"], data["range_end"])
        data["prev_month_label"] = _label(data["prev_month"], data["prev_range_start"], data["prev_range_end"])
        data["range_label"] = f"{_dmy(data['range_start'])} – {_dmy(data['range_end'])}" if data.get("range_start") else ""
        data["month_options"] = month_options
        data["labels"] = _cat_labels(lang)
        data["cats"] = _cat_keys(request)
        # Spar-Spiel: die Wertung des gezeigten Zeitraums UND die
        # Bestenliste über alle — beide aus derselben Funktion, damit
        # Karte und Liste nicht auseinanderlaufen können.
        from ..finance.game import score_days
        data["game"] = score_days(data.get("days") or [],
                                  float(data.get("income_total") or 0.0))
        try:
            ranking = db.finance_game_ranking(periods=periods, account_ids=account_ids)
        except Exception as exc:  # noqa: BLE001
            logger.warning("game ranking failed: %s", exc)
            ranking = []
        for r in ranking:
            r["label"] = _label(r["key"], r["start"], r["end"])
        data["ranking"] = ranking
        # Welche Konten gibt es, und welche sind gerade gewählt? Die Seite
        # zeigt beides, damit eine gefilterte Zahl nie wie eine Gesamtzahl
        # aussieht.
        data["accounts"] = db.account_picks(account_ids)
        data["accounts_filtered"] = bool(account_ids) and any(
            not a["selected"] for a in data["accounts"])
        return data

    @app.get("/api/ausgaben/data")
    def api_spending_data(request: Request, month: str | None = Query(None),
                          mode: str | None = Query(None),
                          accounts: list[int] = Query(default=[])):
        return _spending_data(request, month, mode, accounts or None)

    @app.get("/ausgaben", response_class=HTMLResponse)
    def spending_page(request: Request, month: str | None = Query(None),
                      mode: str | None = Query(None),
                      accounts: list[int] = Query(default=[])):
        """Spending of one month by transaction category — the same
        categories as the explorer, so every re-categorisation shows up
        here at once. `mode` = 'salary' (default, Gehalt bis Gehalt) or
        'calendar'. See db.finance_spend_by_category."""
        data = _spending_data(request, month, mode, accounts or None)
        summary = db.finance_summary()
        has_data = bool(db.list_accounts()) and summary.get("tx_count", 0) > 0
        # „wieviel darf ich noch ausgeben" — dieselben Zahlen wie der
        # Budget-Wecker auf /finance, NICHT eine zweite Rechnung daneben.
        # Der Wecker kennt nur den laufenden Gehaltsmonat; deckt sich der
        # gewählte Zeitraum damit, wird er hier gezeigt, sonst nichts.
        budget_period = None
        try:
            fin = settings.finance
            periods = db.finance_salary_periods(
                salary_match=fin.salary_match,
                anchor_day=fin.period_anchor_day,
                monthly_budget=fin.monthly_budget,
                today=datetime.now().date().isoformat(),
            )
            cur = periods[-1] if periods else None
            if (cur and cur.get("is_current") and cur.get("start") == data.get("range_start")
                    and not data.get("accounts_filtered")):
                budget_period = {
                    "remaining":  cur.get("remaining"),
                    "budget":     cur.get("budget"),
                    "expense":    cur.get("expense"),
                    "days_left":  cur.get("days_left"),
                    "due_date":   cur.get("due_date") or "",
                    "fixed":      bool(fin.monthly_budget and fin.monthly_budget > 0),
                }
        except Exception as exc:  # noqa: BLE001
            logger.warning("budget period for /ausgaben failed: %s", exc)
        return templates.TemplateResponse(
            request, "spending.html",
            {**base_ctx(request), "data": data, "has_data": has_data, "mode": data["mode"],
             "budget_period": budget_period},
        )

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
        from datetime import date as _today_date
        today_iso = _today_date.today().isoformat()
        # Net worth ("Gesamtvermögen") + salary-period cashflow — the new
        # primary view. Uses the accounts' is_savings flag + start_balance
        # and the salary-detection settings.
        fin = settings.finance
        net_worth = db.finance_net_worth()
        try:
            saved_external = db.finance_saved_external()
        except Exception as exc:  # noqa: BLE001
            logger.warning("saved_external failed: %s", exc)
            saved_external = {"total": 0.0, "by_category": [], "first": "", "last": ""}
        salary_periods = db.finance_salary_periods(
            salary_match=fin.salary_match,
            anchor_day=fin.period_anchor_day,
            monthly_budget=fin.monthly_budget,
            today=today_iso,
        )
        current_period = salary_periods[-1] if salary_periods else None
        # Reverse so the period cashflow list reads newest-first in the UI.
        periods_recent = list(reversed(salary_periods))[:18]
        finance_cfg = {
            "salary_match": fin.salary_match,
            "period_anchor_day": fin.period_anchor_day,
            "monthly_budget": fin.monthly_budget,
        }
        summary       = db.finance_summary()
        # v0.43: unpaired transfer legs = gaps in the exports; open „sonstiges"
        try:
            transfer_check = db.finance_transfer_check()
            transfer_check["synthetic"] = db.finance_synthetic_legs()
        except Exception as exc:  # noqa: BLE001
            logger.warning("transfer check failed: %s", exc)
            transfer_check = {"pairs": 0, "unpaired": [], "gaps": [], "gap_sum_by_account": {}, "synthetic": []}
        try:
            _open = db.finance_unresolved_merchants(limit=2000)
            open_categories = {
                "count": sum(m["count"] for m in _open),
                "merchants": len(_open),
                "amount": sum(m["total"] for m in _open),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("open categories failed: %s", exc)
            open_categories = {"count": 0, "merchants": 0, "amount": 0.0}
        # months=None → full timeline; the chart renderer scales itself.
        monthly       = db.finance_monthly()
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
        try:
            statements_overview = [a for a in db.finance_statement_overview() if a["statements"] or a["gaps"]]
        except Exception as exc:  # noqa: BLE001
            logger.warning("statement overview failed: %s", exc)
            statements_overview = []
        return templates.TemplateResponse(
            request, "finance.html",
            {**base_ctx(request),
             "transfer_check": transfer_check,
             "statements_overview": statements_overview,
             "open_categories": open_categories,
             "saved_external": saved_external,
             "net_worth": net_worth,
             "salary_periods": periods_recent,
             "current_period": current_period,
             "finance_cfg": finance_cfg,
             "today_iso": today_iso,
             "summary": summary, "monthly": monthly, "accounts": accounts,
             "top_outgoing": top_outgoing, "top_incoming": top_incoming,
             "recurring": recurring, "transactions": transactions,
             "tx_categories": _cat_keys(request),
             "tx_types": list(TX_TYPES),
             "has_data": bool(accounts) and summary.get("tx_count", 0) > 0,
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
            "monthly": db.finance_monthly(),
            "accounts": db.list_accounts(),
            "recurring": db.finance_recurring(),
        }

    @app.get("/api/dashboard")
    def api_dashboard():
        """Live state aggregator for the activity hub on /. Pulls
        every signal the dashboard needs in one round-trip so the
        page can poll cheaply (every 2-3s) without firing six
        independent calls. All counts honor `deleted_at IS NULL`."""
        from .. import activity as _activity
        bridge_status = {"connected": False, "info": None}
        try:
            from ..bridge.server import get_bridge
            bridge = get_bridge()
            bridge_status["connected"] = bridge.is_connected()
            client_info = getattr(bridge, "last_client_info", None)
            if client_info:
                bridge_status["info"] = {
                    "host":  client_info.get("host"),
                    "model": client_info.get("model"),
                }
        except Exception:
            pass

        with db._lock:
            counts = {
                "total":     int(db._conn.execute(
                    "SELECT COUNT(*) FROM documents WHERE deleted_at IS NULL AND category != '_csv_container'"
                ).fetchone()[0]),
                "review":    int(db._conn.execute(
                    "SELECT COUNT(*) FROM documents "
                    "WHERE deleted_at IS NULL AND category != '_csv_container' AND status = 'review'"
                ).fetchone()[0]),
                "pending_review": int(db._conn.execute(
                    "SELECT COUNT(*) FROM documents "
                    "WHERE deleted_at IS NULL AND category != '_csv_container' AND status = 'pending_review'"
                ).fetchone()[0]),
                "failed":    int(db._conn.execute(
                    "SELECT COUNT(*) FROM documents "
                    "WHERE deleted_at IS NULL AND category != '_csv_container' AND status = 'failed'"
                ).fetchone()[0]),
                "duplicate": int(db._conn.execute(
                    "SELECT COUNT(*) FROM documents "
                    "WHERE deleted_at IS NULL AND category != '_csv_container' AND status = 'duplicate'"
                ).fetchone()[0]),
                "kontoauszug": int(db._conn.execute(
                    "SELECT COUNT(*) FROM documents "
                    "WHERE deleted_at IS NULL AND category = 'Kontoauszug'"
                ).fetchone()[0]),
                "empty_statements": int(db._conn.execute(
                    "SELECT COUNT(*) FROM statements s "
                    "JOIN documents d ON d.id = s.doc_id "
                    "WHERE d.deleted_at IS NULL "
                    "  AND COALESCE(s.acknowledged_empty,0) = 0 "
                    "  AND s.id NOT IN (SELECT statement_id FROM transactions)"
                ).fetchone()[0]),
            }
            # 12 most-recent docs (regardless of status) — feed.
            recent = [dict(r) for r in db._conn.execute(
                "SELECT id, filename, original_name, category, subcategory, "
                "       sender, subject, doc_date, status, confidence, "
                "       created_at, content_hash "
                "FROM documents WHERE deleted_at IS NULL AND category != '_csv_container' "
                "ORDER BY datetime(created_at) DESC LIMIT 12"
            ).fetchall()]
            # Failed docs (always relevant, small list)
            failed = [dict(r) for r in db._conn.execute(
                "SELECT id, filename, original_name, sender, subject, "
                "       doc_date, status, reasoning, created_at "
                "FROM documents WHERE deleted_at IS NULL AND category != '_csv_container' "
                "  AND status IN ('failed','review','pending_review') "
                "ORDER BY datetime(created_at) DESC LIMIT 50"
            ).fetchall()]

        bulk_job = _activity.get_job("analyze-statements").as_dict()
        snap = _activity.snapshot()
        in_flight = int(snap.get("in_flight") or 0)

        try:
            deadlines = db.upcoming_deadlines(within_days=30, overdue_grace_days=21)
        except Exception:
            deadlines = []

        return {
            "now":           datetime.now().isoformat(timespec="seconds"),
            "counts":        counts,
            "bridge":        bridge_status,
            "in_flight":     in_flight,
            "bulk_job":      bulk_job,
            "recent":        recent,
            "failed":        failed,
            "deadlines":     deadlines,
            "version":       __version__,
        }

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
        # When the bulk worker is currently processing THIS doc, the
        # per-doc registry is empty (the bulk path uses the activity
        # tracker instead). Synthesise an equivalent running-snapshot
        # so the doc-detail page shows "wird im Bulk-Run analysiert ·
        # Seite 3/9" without the user having to know which entry
        # point started the work.
        from .. import activity as _activity
        bulk = _activity.get_job("analyze-statements")
        bulk_status: dict | None = None
        if bulk.running or bulk.paused:
            # Snapshot the bulk job so every doc-detail page shows
            # whether a bulk run is in flight, even if THIS doc isn't
            # the current one. Approved + failed lists tell the user
            # whether their doc is already done.
            bulk_status = {
                "running":        bool(bulk.running),
                "paused":         bool(bulk.paused),
                "done":           int(bulk.done or 0),
                "total":          int(bulk.total or 0),
                "current_doc_id": int(bulk.current_doc_id or 0),
                "current_page":   int(bulk.current_page or 0),
                "total_pages":    int(bulk.total_pages or 0),
                # Has THIS specific doc already been processed in
                # this bulk run? approved + failed are doc_ids.
                "this_doc_done":   int(doc_id) in (bulk.approved or []),
                "this_doc_failed": any(
                    isinstance(f, dict) and f.get("doc_id") == int(doc_id)
                    for f in (bulk.failed or [])
                ),
                "this_doc_current": int(bulk.current_doc_id or 0) == int(doc_id),
            }
        if running is None and bulk.running and int(bulk.current_doc_id or 0) == int(doc_id):
            started_at = float(bulk.started_at or 0.0)
            running = {
                "kind":        "statement",
                "started_at":  started_at,
                "elapsed_s":   max(0.0, time.time() - started_at) if started_at else 0.0,
                "current_page": int(bulk.current_page or 0),
                "total_pages":  int(bulk.total_pages or 0),
                "via_bulk":    True,
                "bulk_done":   int(bulk.done or 0),
                "bulk_total":  int(bulk.total or 0),
            }
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
            "bulk":      bulk_status,
        }

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

    @app.patch("/api/transaction/{tx_id}")
    def api_transaction_update(request: Request, tx_id: int, payload: dict = Body(...)):
        """Edit a single transaction. Body fields are all optional —
        only the keys present in `payload` get updated. Allowed:
        booking_date, value_date, amount, counterparty,
        counterparty_iban, purpose, tx_type, category.

        Recomputes tx_hash whenever amount / booking_date / purpose
        change so future re-extracts dedup against the corrected
        row. Used by the per-statement editor on /document/{id}."""
        from ..finance.categories import TX_CATEGORIES, TX_TYPES
        from hashlib import sha256
        with db._lock:
            row = db._conn.execute(
                "SELECT t.*, a.iban_hash AS account_iban_hash "
                "FROM transactions t "
                "LEFT JOIN accounts a ON a.id = t.account_id "
                "WHERE t.id = ?",
                (tx_id,),
            ).fetchone()
        if row is None:
            raise HTTPException(404, f"transaction {tx_id} not found")
        if not isinstance(payload, dict):
            raise HTTPException(400, "payload must be an object")

        allowed = {"booking_date", "value_date", "amount",
                   "counterparty", "counterparty_iban", "purpose",
                   "tx_type", "category"}
        # A non-admin may categorise a booking but not rewrite the
        # bookkeeping itself — amount, date and counterparty stay put.
        _me_tx = getattr(request.state, "user", None)
        if _me_tx is not None and not _me_tx.is_admin:
            allowed = allowed & _auth.USER_TX_FIELDS
        updates: dict = {}
        for k, v in payload.items():
            if k not in allowed:
                continue
            if k == "amount":
                try:
                    updates[k] = round(float(v), 2)
                except (TypeError, ValueError):
                    raise HTTPException(400, f"amount must be a number, got {v!r}") from None
                if abs(updates[k]) > 10_000_000:
                    raise HTTPException(400, "amount out of safe range")
            elif k == "category":
                cat = str(v).strip().lower()
                if cat and cat not in db.finance_category_keys():
                    raise HTTPException(400, f"category must be one of {db.finance_category_keys()}")
                updates[k] = cat
            elif k == "tx_type":
                t = str(v).strip().lower()
                if t and t not in TX_TYPES:
                    raise HTTPException(400, f"tx_type must be one of {list(TX_TYPES)}")
                updates[k] = t
            elif k in ("booking_date", "value_date"):
                from ..finance.dates import normalise_date as _normalise_date
                updates[k] = _normalise_date(str(v or "").strip())
            else:
                updates[k] = str(v or "").strip()[:500]
        if not updates:
            return {"ok": True, "updated": 0, "tx_id": tx_id}

        # Recompute tx_hash when amount / booking_date / purpose
        # change. Keep account_iban_hash from the existing row.
        new_amount = updates.get("amount", row["amount"])
        new_bd = updates.get("booking_date", row["booking_date"])
        new_pp = updates.get("purpose", row["purpose"])
        h_iban = row["account_iban_hash"] or "no-iban"
        key = f"{h_iban}|{new_bd or ''}|{float(new_amount):.2f}|{new_pp or ''}"
        updates["tx_hash"] = sha256(key.encode("utf-8")).hexdigest()

        cols = ", ".join(f"{k} = ?" for k in updates)
        params = list(updates.values()) + [tx_id]
        with db._lock:
            try:
                db._conn.execute(
                    f"UPDATE transactions SET {cols} WHERE id = ?",
                    params,
                )
                db._conn.commit()
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(500, f"update failed: {exc}") from exc
        return {"ok": True, "updated": 1, "tx_id": tx_id, "fields": list(updates)}

    @app.delete("/api/transaction/{tx_id}")
    def api_transaction_delete(tx_id: int):
        """Hard-delete a transaction. Used by the manual editor when
        a row is wrong / a duplicate that the dedup tool didn't
        catch / a balance row that snuck in."""
        with db._lock:
            cur = db._conn.execute(
                "DELETE FROM transactions WHERE id = ?", (tx_id,),
            )
            db._conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(404, f"transaction {tx_id} not found")
        return {"ok": True, "deleted": 1, "tx_id": tx_id}

    @app.post("/api/account/{account_id}/transaction")
    def api_transaction_create(account_id: int, payload: dict = Body(...)):
        """Add a manual transaction to an account. Required body
        fields: booking_date, amount. Optional: value_date,
        counterparty, counterparty_iban, purpose, tx_type, category.

        Used when a CSV import missed a booking and the user wants
        to enter it by hand."""
        from ..finance.categories import TX_CATEGORIES, TX_TYPES
        from ..finance.dates import normalise_date as _normalise_date
        from hashlib import sha256
        if not isinstance(payload, dict):
            raise HTTPException(400, "payload must be an object")
        with db._lock:
            account = db._conn.execute(
                "SELECT * FROM accounts WHERE id = ?", (account_id,),
            ).fetchone()
        if account is None:
            raise HTTPException(404, f"account {account_id} not found")

        try:
            amount = round(float(payload.get("amount")), 2)
        except (TypeError, ValueError):
            raise HTTPException(400, "amount is required and must be a number") from None
        if abs(amount) > 10_000_000:
            raise HTTPException(400, "amount out of safe range")
        booking_date = _normalise_date(str(payload.get("booking_date") or "").strip())
        if not booking_date:
            raise HTTPException(400, "booking_date is required (ISO YYYY-MM-DD)")
        value_date = _normalise_date(str(payload.get("value_date") or "").strip())
        counterparty = str(payload.get("counterparty") or "").strip()[:200]
        counterparty_iban = str(payload.get("counterparty_iban") or "").strip()[:34]
        purpose = str(payload.get("purpose") or "").strip()[:500]
        tx_type = str(payload.get("tx_type") or "").strip().lower()
        if tx_type and tx_type not in TX_TYPES:
            raise HTTPException(400, f"tx_type must be one of {list(TX_TYPES)}")
        category = str(payload.get("category") or "").strip().lower()
        if category and category not in db.finance_category_keys():
            raise HTTPException(400, f"category must be one of {db.finance_category_keys()}")

        h_iban = account["iban_hash"] or "no-iban"
        key = f"{h_iban}|{booking_date}|{amount:.2f}|{purpose}"
        tx_hash = sha256(key.encode("utf-8")).hexdigest()

        with db._lock:
            try:
                cur = db._conn.execute(
                    "INSERT INTO transactions (statement_id, account_id, "
                    " booking_date, value_date, amount, currency, "
                    " counterparty, counterparty_iban, purpose, tx_type, "
                    " category, tx_hash) "
                    "VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?)",
                    (account_id, booking_date, value_date,
                     amount, account["currency"] or "EUR", counterparty,
                     counterparty_iban, purpose, tx_type or "sonstiges",
                     category or "sonstiges", tx_hash),
                )
                db._conn.commit()
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(500, f"insert failed: {exc}") from exc
        return {"ok": True, "tx_id": cur.lastrowid, "tx_hash": tx_hash}

    @app.post("/api/finance/import-csv")
    async def api_finance_import_csv(request: Request):
        """Import a bank CSV export (Sparkasse, DKB, ING, Volksbank, comdirect, …).

        Optional form field / query param `account_iban`: the account's own
        IBAN for files that carry none (comdirect, N26).

        Accepts either:
        - `multipart/form-data` with one or more files in the `files`
          field — typical from a `<input type=file multiple>` upload
        - raw CSV body (`text/csv` or `text/plain`) — the simplest
          curl-friendly form

        Each file produces an `ImportReport` with `rows_inserted`,
        `rows_duplicate`, `rows_invalid`, `accounts_touched`. Dedup
        is automatic (UNIQUE constraint on `transactions.tx_hash`)
        so re-importing an overlapping CSV is a safe no-op.
        """
        from ..finance.csv_import import import_csv, ImportReport
        ct = (request.headers.get("content-type") or "").lower()
        reports: list[dict] = []
        if ct.startswith("multipart/"):
            form = await request.form()
            files = form.getlist("files")
            if not files:
                # Some browsers post a single "file" field name.
                files = form.getlist("file")
            if not files:
                raise HTTPException(400, "no files in upload")
            iban_hint = str(form.get("account_iban") or "").strip()
            for f in files:
                if not hasattr(f, "filename"):
                    continue
                data = await f.read()
                fname = f.filename or ""
                if fname.lower().endswith(".pdf") or data[:5] == b"%PDF-":
                    # A Kontoauszug-PDF takes the normal document route:
                    # inbox → OCR → library, and the pipeline reads its
                    # bookings into the finances (v0.47.0). Duplicates are
                    # caught by the document hash, then by the statement's
                    # file hash and period.
                    try:
                        inbox = settings.paths.inbox
                        inbox.mkdir(parents=True, exist_ok=True)
                        safe = Path(fname).name or "kontoauszug.pdf"
                        target = inbox / safe
                        n = 1
                        while target.exists():
                            n += 1
                            target = inbox / f"{Path(safe).stem}-{n}{Path(safe).suffix}"
                        target.write_bytes(data)
                    except Exception as exc:  # noqa: BLE001
                        raise HTTPException(500, f"ablegen {fname!r}: {exc}") from exc
                    reports.append({**_report_to_dict(ImportReport(file_label=fname)), "queued": True, "bank": "Kontoauszug"})
                    continue
                try:
                    rep = import_csv(db, data, file_label=fname, account_iban_hint=iban_hint)
                except Exception as exc:  # noqa: BLE001
                    raise HTTPException(500, f"import {fname!r}: {exc}") from exc
                reports.append(_report_to_dict(rep))
        else:
            body = await request.body()
            if not body:
                raise HTTPException(400, "request body is empty")
            try:
                rep = import_csv(db, body, file_label="(uploaded)",
                                 account_iban_hint=str(request.query_params.get("account_iban") or ""))
            except Exception as exc:  # noqa: BLE001
                raise HTTPException(500, f"import failed: {exc}") from exc
            reports.append(_report_to_dict(rep))

        total_inserted = sum(r["rows_inserted"] for r in reports)
        total_dup = sum(r["rows_duplicate"] for r in reports)
        return {
            "files":           reports,
            "total_inserted":  total_inserted,
            "total_duplicate": total_dup,
        }

    @app.post("/api/finance/reset")
    def api_finance_reset(payload: dict = Body(default={})):
        """Wipe transactions / statements / accounts so the user can
        re-import their CSV exports from a clean slate. Requires
        explicit confirmation: pass `{"confirm": true}`. Pass
        `{"dry_run": true}` for a preview."""
        from ..finance.reset import reset_finance_data
        dry = bool(payload.get("dry_run") or False) if isinstance(payload, dict) else False
        confirmed = bool(payload.get("confirm") or False) if isinstance(payload, dict) else False
        if not dry and not confirmed:
            raise HTTPException(
                400,
                "Pass {\"confirm\": true} to actually wipe, or "
                "{\"dry_run\": true} to preview.",
            )
        try:
            return reset_finance_data(db, dry_run=dry)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(500, f"reset failed: {type(exc).__name__}: {exc}") from exc

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
        if cat not in db.finance_category_keys():
            raise HTTPException(
                400, f"unknown category {cat!r} — must be one of {db.finance_category_keys()}",
            )
        try:
            int_ids = [int(i) for i in ids]
        except (TypeError, ValueError):
            raise HTTPException(400, "ids must be integers")
        from ..finance.categories import INCOME_CATEGORIES
        if cat in INCOME_CATEGORIES:
            marks = ",".join("?" * len(int_ids))
            with db._lock:
                debits = db._conn.execute(
                    f"SELECT COUNT(*) FROM transactions WHERE id IN ({marks}) AND amount < 0", int_ids
                ).fetchone()[0]
            if debits:
                raise HTTPException(400, "Ausgaben sind nie Erstattungen — eine Abbuchung kann keine Einnahme-Kategorie bekommen.")
        # v0.43: a manual assignment teaches a rule for the counterparty
        # (IBAN + name) and applies it to that counterparty's other bookings
        # — unless the caller says learn=false („nur diese Buchung").
        learn = payload.get("learn", True)
        learn = bool(learn) if not isinstance(learn, str) else learn.lower() not in ("0", "false", "no")
        scope = (payload.get("scope") or "auto")
        if scope not in ("auto", "merchant", "amount"):
            scope = "auto"
        res = db.transactions_set_category_learn(int_ids, cat, learn=learn, scope=scope)
        return {"ok": True, "category": cat, **res}

    # ---------- v0.43: explainable categories, learned rules, gaps, fixed costs ----------

    @app.get("/api/finance/categories")
    def api_finance_categories(request: Request):
        lang = _lang(request)
        return {"keys": _cat_keys(request), "labels": _cat_labels(lang),
                "custom": db.finance_custom_categories()}

    @app.post("/api/finance/categories")
    def api_finance_category_add(payload: dict, request: Request):
        try:
            c = db.finance_category_add(str(payload.get("label") or ""), is_fixed=bool(payload.get("is_fixed")),
                                        is_saving=bool(payload.get("is_saving")))
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return {"ok": True, "category": c, "keys": _cat_keys(request), "labels": _cat_labels(_lang(request))}

    @app.patch("/api/finance/categories/{key}")
    def api_finance_category_update(key: str, payload: dict, request: Request):
        ok = db.finance_category_update(
            key, label=payload.get("label"),
            is_fixed=(bool(payload["is_fixed"]) if "is_fixed" in payload else None),
            is_saving=(bool(payload["is_saving"]) if "is_saving" in payload else None),
        )
        if not ok:
            raise HTTPException(404, "category not found")
        return {"ok": True, "labels": _cat_labels(_lang(request))}

    @app.delete("/api/finance/categories/{key}")
    def api_finance_category_delete(key: str, request: Request):
        res = db.finance_category_delete(key)
        if not res["deleted"]:
            raise HTTPException(404, "category not found")
        return {"ok": True, **res, "keys": _cat_keys(request), "labels": _cat_labels(_lang(request))}

    @app.get("/api/finance/unresolved")
    def api_finance_unresolved(category: str = Query("sonstiges"), start: str | None = Query(None),
                               end: str | None = Query(None), limit: int = Query(300)):
        """Counterparties whose bookings sit in `category` — the work list
        for „Sonstiges aufdröseln"."""
        return {"merchants": db.finance_unresolved_merchants(
            limit=max(1, min(2000, limit)), category=category or "sonstiges",
            start=start or None, end=end or None,
        )}

    @app.get("/api/finance/rules")
    def api_finance_rules():
        return {"rules": db.finance_rules_list()}

    @app.post("/api/finance/rules")
    def api_finance_rule_set(payload: dict):
        """Teach one rule for a counterparty and apply it to every unpinned
        booking of that counterparty. Body: {kind: 'iban'|'merchant',
        value, category, name?}. Bookings the user pinned by hand stay."""
        from ..finance.categories import TX_CATEGORIES
        kind = (payload.get("kind") or "").strip()
        value = (payload.get("value") or "").strip()
        cat = (payload.get("category") or "").strip()
        name = (payload.get("name") or "").strip()
        if kind not in ("iban", "merchant", "merchant_amount") or not value:
            raise HTTPException(400, "kind must be 'iban', 'merchant' or 'merchant_amount' and value non-empty")
        if cat not in db.finance_category_keys():
            raise HTTPException(400, f"unknown category {cat!r}")
        direction = (payload.get("direction") or "").strip()
        rid = db.finance_rule_upsert(kind, value, cat, source="user", sample_name=name, direction=direction)
        # A merchant with an IBAN gets both keys so name variants are covered.
        targets = [(kind, value)]
        extra_iban = (payload.get("iban") or "").replace(" ", "").upper()
        if kind == "merchant" and extra_iban and len(extra_iban) >= 8:
            db.finance_rule_upsert("iban", extra_iban, cat, source="user", sample_name=name, direction=direction)
            targets.append(("iban", extra_iban))
        applied = db._apply_rules_to_merchants(targets)
        return {"ok": True, "rule_id": rid, "applied": applied, "category": cat}

    @app.delete("/api/finance/rules/{rule_id}")
    def api_finance_rule_delete(rule_id: int):
        rules = {r["id"]: r for r in db.finance_rules_list()}
        r = rules.get(int(rule_id))
        if not r:
            raise HTTPException(404, "rule not found")
        db.finance_rule_delete(int(rule_id))
        # Bookings of that counterparty fall back to the built-in patterns.
        reverted = db._apply_rules_to_merchants([(r["match_kind"], r["match_value"])])
        return {"ok": True, "reverted": reverted}

    @app.post("/api/finance/reclassify")
    def api_finance_reclassify(payload: dict | None = None):
        """Re-run the explainable classifier over all bookings (manual pins,
        transfers and learned rules keep their say)."""
        force = bool((payload or {}).get("force"))
        return {"ok": True, **db.finance_reclassify(force=force)}

    @app.post("/api/finance/ai-categorize")
    def api_finance_ai_categorize(payload: dict | None = None):
        """Let the local AI propose a category for every counterparty still
        in „sonstiges". Proposals become rules with source='ai' (a manual
        rule always wins) and are applied immediately. Background job,
        progress via /api/finance/ai-progress."""
        if classifier is None:
            raise HTTPException(503, "KI nicht verfügbar — bitte zuerst /setup abschließen")
        from .. import activity
        existing = activity.get_job("kategorien-ai")
        if existing.running:
            return {"started": False, "reason": "already running", **existing.as_dict()}
        cat = (payload or {}).get("category") or "sonstiges"
        merchants = db.finance_unresolved_merchants(limit=400, category=cat)
        merchants = [m for m in merchants if not m["key"].startswith("purpose:")]
        if not merchants:
            return {"started": False, "reason": "nothing to classify", "total": 0}
        activity.start_job("kategorien-ai", total=len(merchants))
        from ..finance import merchant_ai

        def worker():
            done_count = 0
            applied = 0
            try:
                def _progress(done, total):
                    activity.update_job("kategorien-ai", done=done)
                mapping = merchant_ai.classify_categories(
                    classifier.provider, classifier.settings.model, merchants, progress=_progress,
                )
                by_key = {m["key"]: m for m in merchants}
                targets = []
                for key, category in mapping.items():
                    m = by_key.get(key, {})
                    kind, value = ("iban", key[5:]) if key.startswith("iban:") else ("merchant", key)
                    if db.finance_rule_upsert(kind, value, category, source="ai",
                                              sample_name=(m.get("name") or "")[:200]):
                        targets.append((kind, value))
                        done_count += 1
                if targets:
                    applied = db._apply_rules_to_merchants(targets)
            except Exception as exc:  # noqa: BLE001
                activity.update_job("kategorien-ai", last_error=str(exc))
            finally:
                activity.finish_job("kategorien-ai", current="")
                try:
                    from .. import notifier as _n
                    _n.fire(_n.NotificationEvent(
                        kind="bulk_done",
                        title=f"Kategorien-KI fertig — {done_count} Empfänger vorgeschlagen",
                        body=f"{applied} Buchungen neu zugeordnet. Vorschläge sind als „KI“ markiert und lassen sich überstimmen.",
                    ))
                except Exception:
                    pass

        threading.Thread(target=worker, name="kategorien-ai", daemon=True).start()
        return {"started": True, "total": len(merchants), **activity.get_job("kategorien-ai").as_dict()}

    @app.get("/api/finance/ai-progress")
    def api_finance_ai_progress():
        from .. import activity
        job = activity.get_job("kategorien-ai").as_dict()
        job["available"] = classifier is not None
        return job

    @app.get("/api/finance/statements")
    def api_finance_statements():
        """Imported Kontoauszüge per account + the holes between them."""
        return {"accounts": db.finance_statement_overview()}

    @app.post("/api/finance/import-statements")
    def api_finance_import_statements():
        """Read every Kontoauszug-PDF in the library into the finances that
        is not in yet (v0.47.0). Safe to repeat."""
        from ..finance.pdf_statement import backfill_library_statements
        try:
            out = backfill_library_statements(db, settings=settings)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(500, f"Kontoauszüge einlesen: {exc}") from exc
        db.meta_set("finance.statement_backfill", "2")
        return out

    @app.post("/api/finance/gap-ack")
    def api_finance_gap_ack(payload: dict = Body(...)):
        """Tick off a hole in the statement chain that cannot be filled.

        The bridging booking stays, so balances keep adding up — this only
        stops the chain from asking for a statement that does not exist.
        """
        try:
            account_id = int(payload.get("account_id"))
        except (TypeError, ValueError):
            raise HTTPException(400, "account_id must be a number") from None
        after = str(payload.get("after") or "").strip()
        before = str(payload.get("before") or "").strip()
        if not after or not before:
            raise HTTPException(400, "after and before are required")
        ack = bool(payload.get("acknowledged", True))
        db.finance_gap_acknowledge(account_id, after, before, ack)
        return {"ok": True, "acknowledged": ack}

    @app.get("/api/finance/transfer-check")
    def api_finance_transfer_check():
        out = db.finance_transfer_check()
        out["synthetic"] = db.finance_synthetic_legs()
        return out

    @app.post("/api/finance/fill-gaps")
    def api_finance_fill_gaps():
        return {"ok": True, **db.finance_fill_transfer_gaps()}

    @app.get("/api/finance/periods")
    def api_finance_periods():
        """Salary periods (Gehaltsmonate) as pickable ranges, newest first."""
        from datetime import date as _dt
        fin = settings.finance
        periods = db.finance_salary_periods(
            salary_match=fin.salary_match, anchor_day=fin.period_anchor_day,
            monthly_budget=fin.monthly_budget, today=_dt.today().isoformat(),
        )
        out = []
        for p in reversed(periods):
            out.append({"start": p["start"], "end": p["end"], "is_current": p["is_current"],
                        "income": p["income"], "expense": p["expense"], "salary": p["salary"],
                        "label": f"{p['start'][8:10]}.{p['start'][5:7]}.{p['start'][0:4]} – "
                                 f"{p['end'][8:10]}.{p['end'][5:7]}.{p['end'][0:4]}"})
        return {"periods": out, "anchor_day": fin.period_anchor_day, "salary_match": fin.salary_match}

    @app.get("/api/finance/fixed-costs")
    def api_finance_fixed_costs(months: int = Query(24),
                                accounts: list[int] = Query(default=[])):
        return db.finance_fixed_costs(months_back=max(1, min(60, months)),
                                      account_ids=accounts or None)

    @app.get("/api/finance/fixed-costs/categories")
    def api_finance_fixed_categories(request: Request):
        return {"categories": db.finance_fixed_categories(), "all": _cat_keys(request),
                "default": list(db.DEFAULT_FIXED_CATEGORIES)}

    @app.post("/api/finance/fixed-costs/categories")
    def api_finance_fixed_categories_set(payload: dict):
        cats = payload.get("categories")
        if not isinstance(cats, list):
            raise HTTPException(400, "categories must be a list")
        return {"ok": True, "categories": db.finance_set_fixed_categories([str(c) for c in cats])}

    @app.get("/api/finance/pending")
    def api_finance_pending(account_id: int | None = None):
        """Bookings the bank still lists as „vorgemerkt" — shown on
        /transactions, counted nowhere, replaced by every newer export."""
        rows = db.pending_list(account_id)
        return {"pending": rows, "count": len(rows),
                "sum_out": round(sum(r["amount"] for r in rows if r["amount"] < 0), 2)}

    @app.post("/api/finance/fixed-costs/override")
    def api_finance_fixed_costs_override(payload: dict):
        key = (payload.get("key") or "").strip()
        mode = (payload.get("mode") or "").strip()
        if not key or mode not in ("in", "out", ""):
            raise HTTPException(400, "key required, mode in|out|''")
        return {"ok": True, "overrides": db.finance_fixed_cost_override(key, mode)}

    @app.get("/fixkosten", response_class=HTMLResponse)
    def fixkosten_page(request: Request):
        from ..finance.categories import TX_CATEGORIES
        return templates.TemplateResponse(
            request, "fixkosten.html",
            {**base_ctx(request), "tx_categories": _cat_keys(request),
             "accounts": db.account_picks(None)},
        )

    @app.get("/transactions", response_class=HTMLResponse)
    def transactions_page(request: Request):
        from ..finance.categories import TX_CATEGORIES
        accounts = db.list_accounts()
        from ..finance.classify import SOURCE_LABELS
        return templates.TemplateResponse(
            request, "transactions.html",
            {**base_ctx(request),
             "accounts": accounts,
             "tx_categories": _cat_keys(request),
             "source_labels": SOURCE_LABELS},
        )

    @app.post("/api/settings/finance")
    def api_save_finance(payload: dict):
        """Persist the finance settings. Handles two callers: the privacy
        card in /settings and the salary-period tracker card in /finance.
        Every field is optional and only written when present, so the
        tracker card can save its three fields without clobbering the
        privacy toggles (and vice versa)."""
        from ..settings_writer import update_finance
        if not isinstance(payload, dict):
            raise HTTPException(400, "payload must be an object")

        kwargs: dict = {"config_dir": settings.config_dir}

        if "local_only" in payload:
            kwargs["local_only"] = bool(payload.get("local_only"))
        if "pseudonymize" in payload:
            kwargs["pseudonymize"] = bool(payload.get("pseudonymize"))
        if "review_before_send" in payload:
            kwargs["review_before_send"] = bool(payload.get("review_before_send"))
        if "salary_match" in payload:
            kwargs["salary_match"] = str(payload.get("salary_match") or "").strip()[:120]
        if "period_anchor_day" in payload:
            kwargs["period_anchor_day"] = payload.get("period_anchor_day")
        if "monthly_budget" in payload:
            from ..config import _safe_float
            kwargs["monthly_budget"] = _safe_float(payload.get("monthly_budget"))

        holder_names: list[str] | None = None
        if "holder_names" in payload:
            holder_names_raw = payload.get("holder_names")
            # Accept the field as either a list (UI) or a comma/newline
            # separated string (legacy clients).
            if holder_names_raw is None:
                holder_names = None
            elif isinstance(holder_names_raw, list):
                holder_names = [str(n) for n in holder_names_raw]
            else:
                import re as _re
                holder_names = [s for s in _re.split(r"[\n,;]+", str(holder_names_raw))]
            kwargs["holder_names"] = holder_names

        update_finance(**kwargs)

        # Reflect the change in-process so the next request / pipeline run
        # picks it up without a service restart.
        if "local_only" in kwargs:
            settings.finance.local_only = kwargs["local_only"]
        if "pseudonymize" in kwargs:
            settings.finance.pseudonymize = kwargs["pseudonymize"]
        if "review_before_send" in kwargs:
            settings.finance.review_before_send = kwargs["review_before_send"]
        if "salary_match" in kwargs:
            settings.finance.salary_match = kwargs["salary_match"]
        if "period_anchor_day" in kwargs:
            try:
                settings.finance.period_anchor_day = max(1, min(31, int(kwargs["period_anchor_day"])))
            except (TypeError, ValueError):
                settings.finance.period_anchor_day = 23
        if "monthly_budget" in kwargs:
            settings.finance.monthly_budget = float(kwargs["monthly_budget"] or 0.0)
        if holder_names is not None:
            settings.finance.holder_names = [n.strip() for n in holder_names if (n or "").strip()]
        return {
            "ok": True,
            "local_only": settings.finance.local_only,
            "pseudonymize": settings.finance.pseudonymize,
            "holder_names": settings.finance.holder_names,
            "review_before_send": settings.finance.review_before_send,
            "salary_match": settings.finance.salary_match,
            "period_anchor_day": settings.finance.period_anchor_day,
            "monthly_budget": settings.finance.monthly_budget,
        }

    @app.post("/api/finance/account/{account_id}/meta")
    def api_account_meta(account_id: int, payload: dict = Body(...)):
        """Set the savings flag / opening balance / display name of an
        account. Body: any of {is_savings: bool, start_balance: number,
        bank_name: str}. Drives the net-worth split and lets the user
        mark which accounts are Sparkonten."""
        if not isinstance(payload, dict):
            raise HTTPException(400, "payload must be an object")
        is_savings = payload.get("is_savings")
        start_balance = payload.get("start_balance")
        bank_name = payload.get("bank_name")
        sb: float | None = None
        if start_balance is not None and start_balance != "":
            sb = _coerce_float(start_balance)
            if sb is None:
                raise HTTPException(400, "start_balance must be a number")
        row = db.set_account_meta(
            account_id,
            is_savings=None if is_savings is None else bool(is_savings),
            start_balance=sb,
            bank_name=None if bank_name is None else str(bank_name),
        )
        if row is None:
            raise HTTPException(404, f"account {account_id} not found")
        return {"ok": True, "account": {
            "id": row["id"],
            "bank_name": row["bank_name"],
            "is_savings": int(row["is_savings"] or 0),
            "start_balance": float(row["start_balance"] or 0.0),
        }}

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
        # rclone is only needed for the cloud backend. A local-folder mirror
        # uses rsync (with a pure-Python fallback), so don't gate it on rclone.
        if settings.sync.target_type == "rclone" and not sync_mod.rclone_available():
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
            "event_sync_failed": n.event_sync_failed,
            "event_deadline":    n.event_deadline,
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
            event_sync_failed=payload.get("event_sync_failed"),
            event_deadline=payload.get("event_deadline"),
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
                    "event_sync_failed","event_deadline",
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
        # Send synchronously so we can report the *real* outcome — a
        # fire-and-forget test that always says "sent" is exactly why a
        # broken Telegram setup looks like it works.
        results = disp.send_now(_notifier.NotificationEvent(
            kind="test",
            title="DocuSort test notification",
            body=("If you are reading this, the channel is wired up "
                  "correctly. You can safely ignore this message."),
        ))
        ok_channels   = [r["channel"] for r in results if r["ok"]]
        failed        = [r for r in results if not r["ok"]]
        if not ok_channels:
            # Every channel failed — hand the first concrete reason back to
            # the UI so the owner sees what to fix.
            detail = "; ".join(f"{r['channel']}: {r['error']}" for r in failed)
            raise HTTPException(502, detail or "test delivery failed")
        return {"ok": True, "channels": ok_channels, "results": results}

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

    # ---------- Deadlines: tick off as erledigt ----------
    @app.post("/api/document/{doc_id}/paid")
    def api_document_paid(doc_id: int, payload: dict = Body(default={})):
        """Link this deadline to a booking, or clear the link.

        The automatic matcher can only be as right as the data; this is the
        undo. Clearing a link also lets the matcher try again on the next
        pass, so a wrong guess is not permanent.
        """
        raw = (payload or {}).get("tx_id", None)
        tx_id = None if raw in (None, "", 0) else int(raw)
        if not db.set_deadline_paid(doc_id, tx_id):
            raise HTTPException(404, "document or booking not found")
        return {"ok": True, "payment": db.deadline_payment(doc_id)}

    @app.post("/api/document/{doc_id}/deadline-done")
    def api_deadline_done(doc_id: int, done: bool = Query(True)):
        """Mark a document's deadline as done (default) — it disappears from
        the dashboard 'Fällig demnächst' card — or restore it with ?done=false.
        The due_date is preserved on the document itself."""
        if not db.set_deadline_done(doc_id, done):
            raise HTTPException(404, f"document {doc_id} not found")
        return {"ok": True, "id": doc_id, "done": done}

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
