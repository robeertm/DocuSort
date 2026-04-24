"""Self-update from the latest GitHub release.

The updater is intentionally conservative:

1. Ask the GitHub API for the latest release.
2. Compare its tag to `docusort.__version__`.
3. If newer, download the release tarball, extract to a tempdir.
4. Atomically swap the code directories (preserving `.env`, `config/`,
   `docusort-data/`, logs and the virtualenv).
5. Upgrade Python dependencies inside the existing venv.
6. Return a payload the caller can use to trigger a restart.

It does NOT restart the running process itself — on systemd the web handler
invokes `sudo -n systemctl restart docusort` after success; on other platforms
the user sees a "restart required" banner and restarts manually.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from . import __version__


logger = logging.getLogger("docusort.updater")

REPO = os.environ.get("DOCUSORT_UPDATE_REPO", "robeertm/DocuSort")
API_LATEST = f"https://api.github.com/repos/{REPO}/releases/latest"
TARBALL = "https://codeload.github.com/{repo}/tar.gz/refs/tags/{tag}"

# Relative paths inside the install that survive an update. Everything else
# under the project root gets replaced with whatever the tarball contains.
PRESERVE: tuple[str, ...] = (".env", "config", ".venv", "logs")


def project_root() -> Path:
    """The directory that contains the `docusort/` package we're running from."""
    return Path(__file__).resolve().parent.parent


# ---------- version utilities ----------

def _parse(v: str) -> tuple[int, ...]:
    clean = v.lstrip("v").split("-")[0]
    try:
        return tuple(int(x) for x in clean.split("."))
    except ValueError:
        return (0,)


def is_newer(candidate: str, base: str) -> bool:
    return _parse(candidate) > _parse(base)


# ---------- release discovery ----------

def fetch_latest_release() -> dict[str, Any]:
    req = urllib.request.Request(
        API_LATEST,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": f"docusort-updater/{__version__}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"GitHub API returned HTTP {e.code}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Could not reach GitHub: {e.reason}") from e


def version_info() -> dict[str, Any]:
    """Return {current, latest, has_update, tag, html_url, body, published_at}.

    Never raises — on network errors returns `has_update=False` with an
    `error` string the UI can show.
    """
    try:
        rel = fetch_latest_release()
    except Exception as exc:
        return {
            "current": __version__,
            "latest": None,
            "has_update": False,
            "error": str(exc),
        }
    tag = (rel.get("tag_name") or "").strip()
    latest = tag.lstrip("v")
    return {
        "current": __version__,
        "latest": latest,
        "has_update": bool(latest) and is_newer(latest, __version__),
        "tag": tag,
        "html_url": rel.get("html_url"),
        "body": (rel.get("body") or "")[:4000],
        "published_at": rel.get("published_at"),
    }


# ---------- install ----------

def _download_tarball(tag: str, dst: Path) -> None:
    url = TARBALL.format(repo=REPO, tag=tag)
    logger.info("Downloading %s", url)
    req = urllib.request.Request(url, headers={"User-Agent": f"docusort-updater/{__version__}"})
    with urllib.request.urlopen(req, timeout=120) as r, dst.open("wb") as f:
        shutil.copyfileobj(r, f)


def _extract_strip_root(tarball: Path, dest: Path) -> Path:
    """Extract tarball to `dest` and return the single top-level dir it contains."""
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tarball, "r:gz") as tf:
        tf.extractall(dest)
    children = [p for p in dest.iterdir() if p.is_dir()]
    if len(children) != 1:
        raise RuntimeError(f"Unexpected tarball layout: {len(children)} top-level dirs")
    return children[0]


def _atomic_swap(staged_root: Path, live_root: Path) -> None:
    """Copy files from staging into live, replacing directories wholesale,
    but never touching anything in PRESERVE.

    We don't try to prune files that have been removed upstream to avoid
    accidentally deleting something the user placed next to the app.
    """
    for src in staged_root.iterdir():
        if src.name in PRESERVE:
            continue
        dst = live_root / src.name
        if src.is_dir():
            # Replace directory atomically: write sibling .new, swap, delete old.
            new_dst = live_root / f".{src.name}.new"
            if new_dst.exists():
                shutil.rmtree(new_dst)
            shutil.copytree(src, new_dst, symlinks=True)
            old_dst = live_root / f".{src.name}.old"
            if dst.exists():
                if old_dst.exists():
                    shutil.rmtree(old_dst)
                dst.rename(old_dst)
            new_dst.rename(dst)
            if old_dst.exists():
                shutil.rmtree(old_dst)
        else:
            shutil.copy2(src, dst)


def _pip_sync(live_root: Path) -> str:
    venv_pip = live_root / ".venv" / "bin" / "pip"
    if not venv_pip.exists():
        # Windows layout
        venv_pip = live_root / ".venv" / "Scripts" / "pip.exe"
    if not venv_pip.exists():
        return "skipped — no .venv found"
    req = live_root / "requirements.txt"
    if not req.exists():
        return "skipped — no requirements.txt"
    logger.info("pip install -r %s", req)
    result = subprocess.run(
        [str(venv_pip), "install", "-q", "-r", str(req)],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pip install failed: {result.stderr[-800:]}")
    return "ok"


def install_latest(force: bool = False) -> dict[str, Any]:
    info = version_info()
    if info.get("error"):
        raise RuntimeError(f"Cannot check for updates: {info['error']}")
    if not info.get("has_update") and not force:
        return {"updated": False, "reason": "already up to date", **info}

    tag = info.get("tag") or f"v{info.get('latest')}"
    live = project_root()

    with tempfile.TemporaryDirectory(prefix="docusort-update-") as tmp:
        tmp = Path(tmp)
        tarball = tmp / "release.tar.gz"
        _download_tarball(tag, tarball)
        staged = _extract_strip_root(tarball, tmp / "unpacked")
        logger.info("Swapping in %s -> %s", staged, live)
        _atomic_swap(staged, live)

    pip_status = _pip_sync(live)

    return {
        "updated": True,
        "from": __version__,
        "to": info["latest"],
        "tag": tag,
        "pip": pip_status,
        "restart_required": True,
    }


# ---------- restart ----------

def restart_service() -> dict[str, Any]:
    """Try to restart the running service. Works on systemd when a passwordless
    sudo rule exists (see `scripts/install-sudoers-rule.sh`). Returns details
    the UI can use to tell the user what happened.
    """
    if not shutil.which("systemctl"):
        return {"restarted": False, "method": "none", "reason": "not a systemd system"}

    unit_file = Path("/etc/systemd/system/docusort.service")
    if not unit_file.exists():
        return {"restarted": False, "method": "none", "reason": "docusort.service unit not installed"}

    # Schedule the restart AFTER we've returned a response: detach a shell that
    # waits 2s then calls systemctl. Keeps the HTTP response coherent.
    try:
        subprocess.Popen(
            ["sh", "-c", "sleep 2 && sudo -n systemctl restart docusort"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return {"restarted": True, "method": "systemctl (scheduled)", "delay_seconds": 2}
    except Exception as exc:
        return {"restarted": False, "method": "systemctl", "reason": str(exc)}
