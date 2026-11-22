#!/usr/bin/env python3
"""Fails (exit 1) when requirements.txt is not a fully pinned, hash-locked file.

    python3 scripts/check_pins.py [repo-root]

Rules: every requirement is `name==version` with at least one `--hash=sha256:`;
every direct dependency of requirements.in is present in the lock; the Dockerfile
installs with `--require-hashes`. Standard library only.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*")


def norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def lock_entries(text: str) -> tuple[dict[str, str], list[str]]:
    """Return ({name: logical line}, problems) for a pip-compile style lock."""
    logical: list[str] = []
    buf = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        buf += " " + line.rstrip("\\").strip()
        if not line.endswith("\\"):
            logical.append(buf.strip())
            buf = ""
    if buf:
        logical.append(buf.strip())
    entries: dict[str, str] = {}
    problems: list[str] = []
    for line in logical:
        if line.startswith("-"):
            if line.startswith(("--index-url", "--extra-index-url", "--find-links", "-i ")):
                problems.append("foreign index/option in lock: " + line.split()[0])
            continue
        spec = line.split(";")[0].split(" --hash")[0].strip()
        m = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9._-]*)(\[[^\]]*\])?==([A-Za-z0-9._+!-]+)", spec)
        if not m:
            problems.append("not pinned with '==': " + spec)
            continue
        if "--hash=sha256:" not in line:
            problems.append("no hash: " + spec)
        entries[norm(m.group(1))] = m.group(3)
    return entries, problems


def check(root: Path) -> list[str]:
    lock, src, dockerfile = root / "requirements.txt", root / "requirements.in", root / "Dockerfile"
    if not lock.is_file():
        return ["requirements.txt missing"]
    entries, problems = lock_entries(lock.read_text(encoding="utf-8"))
    if not entries:
        problems.append("requirements.txt has no requirements")
    if not src.is_file():
        problems.append("requirements.in missing (source of the lock)")
    else:
        for line in src.read_text(encoding="utf-8").splitlines():
            line = line.split("#")[0].strip()
            m = NAME.match(line) if line and not line.startswith("-") else None
            if m and norm(m.group(0)) not in entries:
                problems.append("direct dependency not in lock: " + m.group(0))
    if dockerfile.is_file() and "--require-hashes" not in dockerfile.read_text(encoding="utf-8"):
        problems.append("Dockerfile installs without --require-hashes")
    return problems


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent
    problems = check(root)
    for p in problems:
        print("UNPINNED: " + p)
    print("check_pins: %s" % ("rot (%d)" % len(problems) if problems else "gruen"))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
