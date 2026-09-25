"""Users, passwords and sessions for DocuSort's web UI.

DocuSort ran without any authentication until 0.48.0. Adding it to a
live install has exactly one hard requirement: the owner must never be
locked out of his own archive. Everything here follows from that.

* Passwords are hashed with `hashlib.scrypt` from the standard library.
  No new dependency reaches the VM, so a deploy stays a `git pull`.
* Sessions live in SQLite, not in a signed cookie. A signed cookie
  cannot be revoked; a row can. Logging a user out has to actually log
  them out.
* Permissions are an allowlist (`USER_ALLOW`), not a blocklist. The app
  has 111 routes and grows most weeks — with a blocklist every new
  route would be public until someone remembered it. Here every new
  route is admin-only until it is deliberately opened.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import re
import secrets
from dataclasses import dataclass

logger = logging.getLogger("docusort.auth")

ROLE_ADMIN = "admin"
ROLE_USER = "user"
ROLES = (ROLE_ADMIN, ROLE_USER)

SESSION_COOKIE = "ds_session"
SESSION_DAYS = 30

# scrypt cost. 2**14 * 8 * 128 B ≈ 16 MB of memory per hash, which is
# under OpenSSL's 32 MB default cap and still costs ~100 ms here —
# slow enough to make offline guessing expensive, fast enough that a
# login does not feel laggy on the VM.
_SCRYPT_N = 2 ** 14
_SCRYPT_R = 8
_SCRYPT_P = 1
_DKLEN = 32

MIN_PASSWORD_LEN = 8
USERNAME_RE = re.compile(r"^[a-zA-Z0-9._-]{2,32}$")


# ---------------------------------------------------------------- passwords

def hash_password(password: str) -> str:
    """Return a self-describing scrypt hash: `scrypt$n$r$p$salt$hash`.

    The parameters travel with the hash so raising the cost later does
    not invalidate existing passwords.
    """
    if not isinstance(password, str) or not password:
        raise ValueError("empty password")
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(
        password.encode("utf-8"), salt=salt,
        n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, dklen=_DKLEN,
    )
    b64 = lambda raw: base64.b64encode(raw).decode("ascii")  # noqa: E731
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${b64(salt)}${b64(dk)}"


def verify_password(password: str, stored: str) -> bool:
    """Constant-time check of `password` against a stored hash.

    Returns False for malformed or empty hashes rather than raising —
    an account whose hash is unset (a freshly invited user) simply
    cannot log in until a password is set.
    """
    if not password or not stored:
        return False
    try:
        scheme, n_s, r_s, p_s, salt_b64, hash_b64 = stored.split("$")
        if scheme != "scrypt":
            return False
        dk = hashlib.scrypt(
            password.encode("utf-8"),
            salt=base64.b64decode(salt_b64),
            n=int(n_s), r=int(r_s), p=int(p_s),
            dklen=len(base64.b64decode(hash_b64)),
        )
    except Exception:  # noqa: BLE001 — a broken hash is a failed login, not a crash
        logger.warning("auth: malformed password hash, refusing login")
        return False
    return hmac.compare_digest(dk, base64.b64decode(hash_b64))


def password_problem(password: str) -> str | None:
    """Return a translation key for why `password` is unacceptable."""
    if not password or len(password) < MIN_PASSWORD_LEN:
        return "auth.err.password_short"
    return None


def username_problem(username: str) -> str | None:
    if not username or not USERNAME_RE.match(username):
        return "auth.err.username_invalid"
    return None


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def session_token_hash(token: str) -> str:
    """Sessions are stored hashed, so a stolen DB is not a set of keys."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# -------------------------------------------------------------- permissions

@dataclass(frozen=True)
class User:
    id: int
    username: str
    display_name: str
    role: str
    must_change_password: bool = False

    @property
    def is_admin(self) -> bool:
        return self.role == ROLE_ADMIN

    @property
    def label(self) -> str:
        return self.display_name or self.username


# Paths reachable with no session at all. Deliberately tiny.
# `/api/version` stays open because the deploy script curls it to prove
# a release landed — putting it behind the login would break the
# deploy's own success check.
PUBLIC_PATHS = (
    "/login", "/logout", "/setup/admin",
    "/static/", "/upload-sw.js", "/favicon.ico",
    "/api/version",
)

# What a non-admin may reach. Anything not listed is admin-only.
# the owner's rule, 2026-09-20: add documents yes, delete no, settings no;
# finance readable and categorisable; all documents visible; editing a
# document's metadata is part of filing it.
USER_ALLOW: tuple[tuple[str, str], ...] = (
    # --- pages -----------------------------------------------------
    ("GET", r"^/$"),
    ("GET", r"^/library$"),
    ("GET", r"^/analytics$"),
    ("GET", r"^/upload$"),
    ("GET", r"^/finance$"),
    ("GET", r"^/ausgaben$"),
    ("GET", r"^/transactions$"),
    ("GET", r"^/fixkosten$"),
    ("GET", r"^/document/[^/]+$"),
    ("GET", r"^/document/[^/]+/file$"),
    ("GET", r"^/konto$"),
    # --- adding documents ------------------------------------------
    ("POST", r"^/upload$"),
    ("GET", r"^/api/status/[^/]+$"),
    # --- filing / correcting what was added -------------------------
    ("POST", r"^/document/[^/]+/edit$"),
    ("GET", r"^/api/document/[^/]+/(status|diagnostics)$"),
    ("POST", r"^/api/document/[^/]+/(retry|deadline-done|paid)$"),
    ("POST", r"^/api/document/[^/]+/receipt/extract$"),
    ("PATCH", r"^/api/document/[^/]+/receipt$"),
    ("POST", r"^/api/bulk/recategorize$"),
    ("GET", r"^/api/receipts/(stats|items)$"),
    # --- shared read-only surface -----------------------------------
    ("GET", r"^/api/(stats|dashboard|activity|pricing)$"),
    ("POST", r"^/api/language/[^/]+$"),
    # --- finance: read + categorise ---------------------------------
    ("GET", r"^/api/ausgaben/data$"),
    ("GET", r"^/api/transactions/search$"),
    ("POST", r"^/api/transactions/categorize$"),
    # field-restricted for non-admins inside the handler: category only
    ("PATCH", r"^/api/transaction/[^/]+$"),
    ("GET", r"^/api/finance/(stats|transactions|categories|unresolved|rules"
            r"|statements|transfer-check|periods|pending|ai-progress)$"),
    ("GET", r"^/api/finance/fixed-costs$"),
    ("GET", r"^/api/finance/fixed-costs/categories$"),
    ("POST", r"^/api/finance/categories$"),
    # Re-checking which booking settled which bill is strictly less than a
    # user may already do by hand: `/api/document/<id>/paid` above lets them
    # link a booking themselves. This only asks the same question for every
    # open bill at once, and writes nothing a person could not write here.
    ("POST", r"^/api/finance/match-deadlines$"),
    # --- own account -------------------------------------------------
    ("GET", r"^/api/me$"),
    ("POST", r"^/api/me/password$"),
)

_USER_ALLOW_COMPILED = tuple((m, re.compile(p)) for m, p in USER_ALLOW)

# Fields a non-admin may change on a transaction. Everything else
# (amount, date, account, counterparty) is the admin's to touch —
# a user categorises, it does not rewrite bookkeeping.
USER_TX_FIELDS = frozenset({"category", "subcategory"})


def is_public_path(path: str) -> bool:
    return any(path == p or path.startswith(p) for p in PUBLIC_PATHS if p.endswith("/")) \
        or path in PUBLIC_PATHS


def may_access(user: User, method: str, path: str) -> bool:
    """Deny by default: admins pass, users need an explicit allow rule."""
    if user.is_admin:
        return True
    method = method.upper()
    if method == "HEAD":
        method = "GET"
    for allowed_method, pattern in _USER_ALLOW_COMPILED:
        if allowed_method == method and pattern.match(path):
            return True
    return False
