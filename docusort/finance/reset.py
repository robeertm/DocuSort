"""One-shot reset of all finance data.

The v0.33.0 redesign moves transaction data from
LLM-extracted-from-PDF to user-uploaded CSV. Existing data from the
old pipeline was never trustworthy (hallucinations, OCR-comma bugs,
duplicate captures across statements) so the safer path is to wipe
the slate clean and let the user re-import CSV exports.

Two callers:

- `main.py._start_web`: runs once per VM, gated by a meta flag, so
  upgrading to v0.33.x clears the legacy data automatically.
- `POST /api/finance/reset`: lets the user wipe + re-import at any
  time without a restart.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("docusort.finance.reset")

# Meta-key the one-time auto-reset uses so we don't blow data away
# on every service restart.
_RESET_FLAG = "finance.reset.v0_33_0_done"


def reset_finance_data(db, *, dry_run: bool = False) -> dict[str, Any]:
    """Delete every row in transactions / statements / accounts and
    wipe any related override tables. Idempotent — running on an
    already-empty DB is a no-op."""
    with db._lock:
        counts = {
            "transactions": db._conn.execute(
                "SELECT COUNT(*) FROM transactions"
            ).fetchone()[0],
            "statements": db._conn.execute(
                "SELECT COUNT(*) FROM statements"
            ).fetchone()[0],
            "accounts": db._conn.execute(
                "SELECT COUNT(*) FROM accounts"
            ).fetchone()[0],
        }
    if dry_run or sum(counts.values()) == 0:
        return {**counts, "dry_run": dry_run, "deleted": 0}
    with db._lock:
        # Order matters: child rows first, parents last.
        db._conn.execute("DELETE FROM transactions")
        db._conn.execute("DELETE FROM statements")
        db._conn.execute("DELETE FROM accounts")
        # Optional override / metadata tables — only drop if they exist.
        for tbl in ("transaction_category_overrides",
                    "account_meta",
                    "statement_meta"):
            try:
                db._conn.execute(f"DELETE FROM {tbl}")
            except Exception:  # noqa: BLE001
                pass
        db._conn.commit()
    logger.warning(
        "Finance reset: dropped %d transactions, %d statements, %d accounts.",
        counts["transactions"], counts["statements"], counts["accounts"],
    )
    return {**counts, "dry_run": False,
            "deleted": sum(counts.values())}


def _meta_get(db, key: str) -> str:
    """Read a meta row, returning '' when missing."""
    try:
        with db._lock:
            r = db._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (key,),
            ).fetchone()
        return (r["value"] if r else "") or ""
    except Exception:  # noqa: BLE001
        return ""


def _meta_set(db, key: str, value: str) -> None:
    try:
        with db._lock:
            db._conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) "
                "VALUES (?, ?)",
                (key, value),
            )
            db._conn.commit()
    except Exception:  # noqa: BLE001
        logger.exception("could not set meta key %r", key)


def maybe_run_one_time_reset(db) -> dict[str, Any] | None:
    """Run the v0.33.0 one-time reset exactly once per DB. Tracks
    state in `meta`. Returns the report when it ran, None
    when already done.
    """
    if _meta_get(db, _RESET_FLAG) == "1":
        return None
    report = reset_finance_data(db, dry_run=False)
    _meta_set(db, _RESET_FLAG, "1")
    return report
