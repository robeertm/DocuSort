"""Container health probe: `python -m docusort.healthcheck`.

Asks the running web server for `/healthz` on the loopback address and exits
0 (healthy) or 1. Uses only the standard library, because the image carries
no curl or wget. The port comes from `config.yaml`; if the server runs with
TLS the probe retries over https without certificate checks (it only ever
talks to 127.0.0.1).
"""
from __future__ import annotations

import os
import ssl
import sys
import urllib.request
from pathlib import Path


def _port() -> int:
    try:
        from .config import load_config
        cfg_dir = Path(os.environ.get("DOCUSORT_CONFIG_DIR", "/app/config"))
        return int(load_config(cfg_dir).web.port)
    except Exception:           # noqa: BLE001 — fall back to the default port
        return 9876


def probe(port: int, timeout: float = 5.0) -> bool:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    for scheme in ("http", "https"):
        try:
            with urllib.request.urlopen(
                    f"{scheme}://127.0.0.1:{port}/healthz", timeout=timeout,
                    context=ctx if scheme == "https" else None) as r:
                return r.status == 200
        except Exception:       # noqa: BLE001
            continue
    return False


if __name__ == "__main__":
    sys.exit(0 if probe(_port()) else 1)
