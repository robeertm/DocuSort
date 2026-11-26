"""THIRD-PARTY-NOTICES.md must list every locked package and every vendored
front-end library, and no template may pull a script or stylesheet from an
external host (the browser makes no third-party connection)."""
import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTICES = (ROOT / "THIRD-PARTY-NOTICES.md").read_text(encoding="utf-8")
TEMPLATES = ROOT / "docusort" / "web" / "templates"
VENDOR = ROOT / "docusort" / "web" / "static" / "vendor"

spec = importlib.util.spec_from_file_location("check_pins", ROOT / "scripts" / "check_pins.py")
cp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cp)


def test_every_locked_package_is_in_the_notices():
    entries, problems = cp.lock_entries((ROOT / "requirements.txt").read_text(encoding="utf-8"))
    assert problems == []
    missing = [f"{n} {v}" for n, v in entries.items() if f"- `{n}` {v} - " not in NOTICES]
    assert missing == [], "run scripts/drittlizenzen.py (see its header): " + ", ".join(missing)


def test_notices_do_not_list_packages_that_are_gone():
    entries, _ = cp.lock_entries((ROOT / "requirements.txt").read_text(encoding="utf-8"))
    section = NOTICES.split("## Python packages", 1)[1].split("## Front-end libraries", 1)[0]
    listed = re.findall(r"^- `([^`]+)` (\S+) - ", section, re.M)
    stale = [f"{n} {v}" for n, v in listed if entries.get(n) != v]
    assert stale == []


def test_vendored_scripts_are_listed_and_have_licence_texts():
    base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
    for lib, pat in (("htmx", r"/static/vendor/htmx-([\d.]+)\.min\.js"),
                     ("alpine", r"/static/vendor/alpine-([\d.]+)\.min\.js")):
        m = re.search(pat, base)
        assert m, f"base.html must load {lib} from /static/vendor/"
        assert (VENDOR / f"{lib}-{m.group(1)}.min.js").is_file()
        assert (VENDOR / f"{lib}-LICENSE.txt").is_file()
        assert f"/static/vendor/{lib}-{m.group(1)}.min.js" in NOTICES


def test_licence_files_are_in_the_docker_image():
    docker = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "LICENSE" in docker and "THIRD-PARTY-NOTICES.md" in docker
    ignore = ROOT / ".dockerignore"
    if ignore.is_file():
        lines = [l.strip() for l in ignore.read_text(encoding="utf-8").splitlines()]
        assert "LICENSE" not in lines and "THIRD-PARTY-NOTICES.md" not in lines


EXTERNAL = re.compile(r"""<(?:script|link)\b[^>]*\b(?:src|href)\s*=\s*["']?(?:https?:)?//""", re.I)


def test_no_template_loads_script_or_style_from_an_external_host():
    offenders = [p.name for p in sorted(TEMPLATES.glob("*.html"))
                 if EXTERNAL.search(p.read_text(encoding="utf-8"))]
    assert offenders == []
    for p in TEMPLATES.glob("*.html"):
        text = p.read_text(encoding="utf-8")
        assert not re.search(r"""@import\s+(?:url\()?["']?https?:""", text)
        assert "unpkg.com" not in text and "cdn.jsdelivr" not in text and "fonts.googleapis" not in text
