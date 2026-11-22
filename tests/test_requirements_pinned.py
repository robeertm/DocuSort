"""requirements.txt must be an exact, hash-locked file (reproducible image builds)."""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("check_pins", ROOT / "scripts" / "check_pins.py")
cp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cp)


def test_repo_lock_is_pinned():
    assert cp.check(ROOT) == []


def test_unpinned_line_is_red(tmp_path):
    (tmp_path / "requirements.in").write_text("foo>=1\n")
    (tmp_path / "requirements.txt").write_text("foo>=1,<2\n")
    assert any("not pinned" in p for p in cp.check(tmp_path))


def test_missing_hash_is_red(tmp_path):
    (tmp_path / "requirements.in").write_text("foo\n")
    (tmp_path / "requirements.txt").write_text("foo==1.0\n")
    assert any("no hash" in p for p in cp.check(tmp_path))


def test_missing_direct_dependency_is_red(tmp_path):
    (tmp_path / "requirements.in").write_text("foo\nbar\n")
    (tmp_path / "requirements.txt").write_text("foo==1.0 \\\n    --hash=sha256:ab\n")
    assert any("bar" in p for p in cp.check(tmp_path))
