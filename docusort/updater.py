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

# The command a container install uses instead of the in-app updater.
CONTAINER_UPDATE_COMMAND = "docker compose pull && docker compose up -d"


class ContainerUpdateError(RuntimeError):
    """Raised when the in-app updater is asked to run inside a container.

    It has to refuse, and the reason is not squeamishness. This updater
    swaps the code directories under the project root and then restarts the
    systemd unit. In a container the code comes from the IMAGE: the swap
    would succeed, there is no systemd to restart, and the next
    `docker compose up` would quietly hand the old code back. The user
    would see "updated", restart, and be on the old version again — the
    worst kind of failure, the silent one.
    """


def in_container() -> bool:
    """True when this process runs inside a container image.

    Checked in order of trustworthiness: our own image sets the variable,
    Docker writes /.dockerenv, Podman writes /run/.containerenv, and the
    cgroup line is the last resort (on cgroup v2 it is often just "0::/",
    so it cannot be the only test).
    """
    flag = os.environ.get("DOCUSORT_IN_DOCKER", "").strip().lower()
    if flag in {"1", "true", "yes", "on"}:
        return True
    if flag in {"0", "false", "no", "off"}:
        return False
    if Path("/.dockerenv").exists() or Path("/run/.containerenv").exists():
        return True
    try:
        cgroup = Path("/proc/1/cgroup").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return any(m in cgroup for m in ("docker", "containerd", "kubepods", "libpod"))

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
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": f"docusort-updater/{__version__}",
    }
    # Optional auth — bumps the unauthenticated 60 req/hour limit to
    # 5000/hour so the upgrade flow doesn't break when /api/version
    # gets polled aggressively. The token is read from env at request
    # time so secrets.yaml-driven setup also works without a restart.
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("DOCUSORT_GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token.strip()}"
    req = urllib.request.Request(API_LATEST, headers=headers)
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
    container = in_container()
    try:
        rel = fetch_latest_release()
    except Exception as exc:
        return {
            "current": __version__,
            "latest": None,
            "has_update": False,
            "error": str(exc),
            "container": container,
            "update_command": CONTAINER_UPDATE_COMMAND if container else "",
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
        # The interface needs to know WHICH update path applies before it
        # offers a button that cannot work here.
        "container": container,
        "update_command": CONTAINER_UPDATE_COMMAND if container else "",
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


def install_latest(force: bool = False, tag: str | None = None) -> dict[str, Any]:
    """Install the latest release. When `tag` is given, skip the GitHub
    release-info lookup and download that exact tag directly from
    codeload.github.com — useful when the (unauthenticated) GitHub
    REST API is rate-limited but the codeload CDN still serves
    tarballs."""
    if in_container():
        raise ContainerUpdateError(
            "This DocuSort runs in a container — the image carries the code, "
            "so swapping files here would be undone by the next restart. "
            "Update the image instead: " + CONTAINER_UPDATE_COMMAND
        )
    if tag:
        # Manual override path: trust the caller, skip the API check.
        latest = tag.lstrip("v")
        info = {
            "current": __version__, "latest": latest, "has_update": True,
            "tag": tag,
        }
    else:
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
