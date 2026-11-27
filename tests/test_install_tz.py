"""install.sh must find a sensible time zone, or say why it fell back.

Regression: on systems without /etc/localtime (e.g. Alpine) the installer
ended silently under `set -euo pipefail`.
"""
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

SRC = (Path(__file__).resolve().parent.parent / "deploy" / "install.sh").read_text()
BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(not BASH, reason="bash missing")


def run(tmp_path, env_extra=None):
    m = re.search(r"^tz_valid\(\).*?^TZ_DEFAULT=[^\n]*$", SRC, re.S | re.M)
    assert m, "TZ block not found in install.sh"
    script = (
        "set -euo pipefail\n"
        "warn() { printf '!! %s\\n' \"$*\"; }\n" + m.group(0) +
        "\nprintf 'RESULT=%s\\n' \"$TZ_DEFAULT\"\n"
    )
    empty = tmp_path / "bin"
    empty.mkdir(exist_ok=True)
    for tool in ("head", "tr", "readlink"):
        os.symlink(shutil.which(tool), empty / tool) if not (empty / tool).exists() else None
    env = {
        "PATH": str(empty),  # no timedatectl
        "DOCUSORT_LOCALTIME": str(tmp_path / "missing-localtime"),
        "DOCUSORT_TIMEZONE_FILE": str(tmp_path / "missing-timezone"),
    }
    env.update(env_extra or {})
    return subprocess.run([BASH, "-c", script], env=env, capture_output=True, text=True)


def test_no_localtime_falls_back_to_utc_with_message(tmp_path):
    r = run(tmp_path)
    assert r.returncode == 0, r.stderr
    assert "RESULT=UTC" in r.stdout
    assert "time zone" in r.stderr


def test_tz_env_wins(tmp_path):
    r = run(tmp_path, {"TZ": "Europe/Berlin"})
    assert "RESULT=Europe/Berlin" in r.stdout and not r.stderr


def test_localtime_symlink(tmp_path):
    link = tmp_path / "lt"
    link.symlink_to("/usr/share/zoneinfo/Europe/Paris")
    r = run(tmp_path, {"DOCUSORT_LOCALTIME": str(link)})
    assert "RESULT=Europe/Paris" in r.stdout


def test_etc_timezone_file(tmp_path):
    f = tmp_path / "tzfile"
    f.write_text("America/New_York\n")
    r = run(tmp_path, {"DOCUSORT_TIMEZONE_FILE": str(f)})
    assert "RESULT=America/New_York" in r.stdout
