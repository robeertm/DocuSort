"""Headless rclone remote setup.

The user pain point: `rclone config` (or any wizard that calls
`webbrowser.open`) doesn't work on a headless VM — there's no display,
nothing pops up, the OAuth flow dies.

The official workaround is `rclone authorize "<type>"` on a machine *with*
a browser (the user's laptop). It runs a one-shot localhost callback,
the user completes OAuth, rclone prints a JSON access token. The user
copies that token over to the headless box and we paste it into a
remote definition in rclone.conf.

This module:
  - lists currently configured remotes (`rclone listremotes`)
  - tests a remote (`rclone lsd remote: --max-depth 1`)
  - writes a remote into rclone.conf (OAuth: token-paste; S3/WebDAV: form fields)
  - removes a remote

We deliberately do NOT shell out to `rclone config create` for OAuth
backends because that command can still try to spawn a browser. We write
the conf entry ourselves — it's just an INI file.
"""

from __future__ import annotations

import configparser
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any


logger = logging.getLogger("docusort.rclone")


# Backends we explicitly support in the wizard. Any rclone backend can still
# be configured by the user via `rclone config` on the machine — this list is
# only for the guided UI.
SUPPORTED_BACKENDS = (
    "drive",      # Google Drive
    "dropbox",
    "onedrive",
    "s3",
    "webdav",
    "sftp",
)

OAUTH_BACKENDS = {"drive", "dropbox", "onedrive"}


def rclone_path() -> str | None:
    return shutil.which("rclone")


def rclone_available() -> bool:
    return rclone_path() is not None


def rclone_version() -> str | None:
    if not rclone_available():
        return None
    try:
        out = subprocess.check_output(
            ["rclone", "version"], text=True, timeout=5,
        )
        first = out.splitlines()[0] if out else ""
        return first.replace("rclone v", "").strip() or None
    except Exception as exc:
        logger.warning("rclone --version failed: %s", exc)
        return None


def conf_path() -> Path:
    """Where rclone keeps its config. Honours RCLONE_CONFIG if set,
    otherwise the documented default per-user location."""
    env = os.environ.get("RCLONE_CONFIG", "").strip()
    if env:
        return Path(env)
    home = Path(os.path.expanduser("~"))
    return home / ".config" / "rclone" / "rclone.conf"


def _read_conf() -> configparser.ConfigParser:
    cp = configparser.ConfigParser()
    cp.optionxform = str  # preserve case for keys like "client_id"
    p = conf_path()
    if p.exists():
        cp.read(p, encoding="utf-8")
    return cp


def _write_conf(cp: configparser.ConfigParser) -> Path:
    p = conf_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        cp.write(f, space_around_delimiters=False)
    try:
        os.chmod(p, 0o600)
    except OSError:
        pass
    return p


def list_remotes() -> list[dict[str, Any]]:
    """Return [{name, type, healthy, problem}] for each remote in rclone.conf.

    `healthy` is a cheap structural check — for OAuth backends we look for a
    non-empty `token` field. It does NOT mean the token isn't expired; the
    `test_remote()` function does the real network probe.
    """
    cp = _read_conf()
    remotes: list[dict[str, Any]] = []
    for section in cp.sections():
        rtype = cp.get(section, "type", fallback="") or ""
        problem = ""
        if rtype in OAUTH_BACKENDS:
            token = cp.get(section, "token", fallback="").strip()
            if not token or token in ("{}", "null"):
                problem = "empty_token"
        remotes.append({
            "name": section, "type": rtype,
            "healthy": not problem, "problem": problem,
        })
    return remotes


def authorize_command(backend: str) -> str:
    """The exact command the user runs on their laptop to mint a token."""
    if backend not in OAUTH_BACKENDS:
        raise ValueError(f"{backend!r} is not OAuth-based — no token needed")
    # Quoting matches the rclone docs verbatim.
    return f'rclone authorize "{backend}"'


def add_oauth_remote(name: str, backend: str, token_json: str) -> Path:
    """Create or overwrite an OAuth remote (drive, dropbox, onedrive) by
    pasting the JSON token returned by `rclone authorize "<backend>"`.

    `token_json` is whatever the user copied from their laptop. Some users
    will paste a single line, others the full multi-line block — we tolerate
    either by re-serializing through json.loads/dumps.
    """
    if backend not in OAUTH_BACKENDS:
        raise ValueError(f"{backend!r} does not use OAuth tokens")
    name = _sanitise_name(name)
    token_json = (token_json or "").strip()

    # Validate JSON; rclone refuses to use a malformed token at sync time
    # and silently writes the wrong thing if we don't normalise here.
    try:
        token_obj = json.loads(token_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Token is not valid JSON: {exc.msg}. Paste the full block printed "
            f"by `rclone authorize \"{backend}\"`."
        ) from exc
    token_min = json.dumps(token_obj, separators=(",", ":"))

    cp = _read_conf()
    if cp.has_section(name):
        cp.remove_section(name)
    cp.add_section(name)
    cp.set(name, "type",  backend)
    cp.set(name, "token", token_min)
    if backend == "drive":
        # Default scope when configured headlessly through us. The user can
        # change this in rclone config later if they want.
        cp.set(name, "scope", "drive")
    return _write_conf(cp)


def add_s3_remote(
    name: str, *, access_key_id: str, secret_access_key: str,
    region: str = "", endpoint: str = "", provider: str = "Other",
) -> Path:
    """Create an S3-compatible remote (AWS, MinIO, Wasabi, R2, ...)."""
    name = _sanitise_name(name)
    if not access_key_id or not secret_access_key:
        raise ValueError("S3 remote requires access_key_id and secret_access_key")
    cp = _read_conf()
    if cp.has_section(name):
        cp.remove_section(name)
    cp.add_section(name)
    cp.set(name, "type",              "s3")
    cp.set(name, "provider",          provider or "Other")
    cp.set(name, "access_key_id",     access_key_id.strip())
    cp.set(name, "secret_access_key", secret_access_key.strip())
    if region:
        cp.set(name, "region", region.strip())
    if endpoint:
        cp.set(name, "endpoint", endpoint.strip())
    return _write_conf(cp)


def add_webdav_remote(
    name: str, *, url: str, user: str = "", password: str = "",
    vendor: str = "other",
) -> Path:
    """Create a WebDAV remote (Nextcloud, ownCloud, generic WebDAV)."""
    name = _sanitise_name(name)
    if not url:
        raise ValueError("WebDAV remote requires a URL")
    cp = _read_conf()
    if cp.has_section(name):
        cp.remove_section(name)
    cp.add_section(name)
    cp.set(name, "type",   "webdav")
    cp.set(name, "url",    url.strip())
    cp.set(name, "vendor", vendor or "other")
    if user:
        cp.set(name, "user", user.strip())
    if password:
        # rclone expects this obscured. Use `rclone obscure` to do it
        # properly so an existing rclone install can decode it.
        cp.set(name, "pass", _obscure(password))
    return _write_conf(cp)


def add_sftp_remote(
    name: str, *, host: str, user: str, port: int = 22,
    password: str = "", key_file: str = "",
) -> Path:
    name = _sanitise_name(name)
    if not host or not user:
        raise ValueError("SFTP remote requires host and user")
    cp = _read_conf()
    if cp.has_section(name):
        cp.remove_section(name)
    cp.add_section(name)
    cp.set(name, "type", "sftp")
    cp.set(name, "host", host.strip())
    cp.set(name, "user", user.strip())
    if port and port != 22:
        cp.set(name, "port", str(port))
    if password:
        cp.set(name, "pass", _obscure(password))
    if key_file:
        cp.set(name, "key_file", key_file.strip())
    return _write_conf(cp)


def remove_remote(name: str) -> bool:
    cp = _read_conf()
    if not cp.has_section(name):
        return False
    cp.remove_section(name)
    _write_conf(cp)
    return True


def test_remote(name: str, *, timeout: int = 30) -> dict[str, Any]:
    """Run `rclone lsd <name>:` and report the outcome. We deliberately
    use lsd (list directories, single level) so a successful auth probe
    doesn't enumerate huge buckets."""
    if not rclone_available():
        return {"ok": False, "error": "rclone is not installed on PATH"}
    if not name:
        return {"ok": False, "error": "no remote name given"}
    try:
        result = subprocess.run(
            ["rclone", "lsd", f"{name}:", "--max-depth", "1"],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "timeout — remote did not respond"}
    if result.returncode == 0:
        # Each line: "  <size> <YYYY-MM-DD HH:MM:SS> <count> <name>"
        # We just count lines for a quick sanity check.
        return {"ok": True, "directories": len(result.stdout.splitlines())}
    return {
        "ok": False,
        "error": (result.stderr or result.stdout or "rclone failed").strip()[-400:],
    }


# ----- internals -------------------------------------------------------------

def _sanitise_name(name: str) -> str:
    """rclone remote names must match `[A-Za-z0-9_.][A-Za-z0-9_. -]*` —
    trim out anything else so a wizard typo can't produce a broken conf."""
    name = (name or "").strip()
    if not name:
        raise ValueError("remote name required")
    safe = "".join(c for c in name if c.isalnum() or c in "-_.")
    if not safe:
        raise ValueError(f"remote name {name!r} has no usable characters")
    return safe


def _obscure(plaintext: str) -> str:
    """Use `rclone obscure` to encode a password the way rclone expects."""
    if not rclone_available():
        # Fallback: store as-is. Sync will fail until the user installs
        # rclone; that's better than silently corrupting the conf.
        return plaintext
    try:
        out = subprocess.check_output(
            ["rclone", "obscure", plaintext], text=True, timeout=5,
        )
        return out.strip()
    except Exception as exc:
        logger.warning("rclone obscure failed: %s", exc)
        return plaintext
