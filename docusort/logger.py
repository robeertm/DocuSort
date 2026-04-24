"""Rotating file + console logger for DocuSort."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logger(log_dir: Path, level: str = "INFO") -> logging.Logger:
    """Initialise the root 'docusort' logger.

    Logs to both stdout (for `docker logs`) and a rotating file. The returned
    logger is idempotent – calling setup_logger twice does not duplicate
    handlers.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("docusort")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-7s  %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    rotating = RotatingFileHandler(
        log_dir / "docusort.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    rotating.setFormatter(fmt)
    logger.addHandler(rotating)

    return logger
