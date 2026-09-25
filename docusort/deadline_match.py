"""Keep the payment deadlines in step with the bookings.

Two steps that always belong together:

1. read the invoice amount out of the document's own text — a bill whose
   amount is unknown can never be matched, because the amount is what the
   comparison is made of;
2. link every open deadline to the booking that settled it.

Why this lives in its own module rather than being called directly in four
places: four callers need it — start-up, the twelve-hour reminder watchdog,
the button on the dashboard, and the pipeline the moment a document is
filed — and two of them must never run at the same time.
`finance_match_due_payments` reads the set of already-linked bookings ONCE
at the start of the pass; two passes running side by side would both see the
same booking as free and hand it to two different bills. `_pass_lock` is
what makes that impossible.

Before this module a freshly filed bill waited for the next twelve-hour
tick before anyone asked whether it had long since been paid — a stack of
invoices dropped in at noon sat on the dashboard as "overdue" until
midnight, although the direct debit had gone out years ago.
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger("docusort.deadlines")

# Seconds of quiet after the last filed document before the pass starts.
# Long enough that a folder full of invoices costs one pass, short enough
# that a single document is checked while the page is still open.
SETTLE_DELAY_S = 20.0

# ... but never postpone longer than this. A steady trickle of arrivals
# (the mail watcher handing over a whole archive, one document every few
# seconds) would otherwise push the timer ahead of itself for ever and the
# pass would never run at all.
MAX_WAIT_S = 120.0

# One pass at a time, process-wide.
_pass_lock = threading.Lock()

_timer_lock = threading.Lock()
_timer: threading.Timer | None = None
_waiting_since: float | None = None


def run_now(db) -> dict[str, int]:
    """Read the missing amounts, then link the deadlines to their bookings.

    Idempotent: a link that already exists is left alone, a link whose
    booking has vanished is dropped, and a bill that nothing settles simply
    stays open. Safe to call as often as you like.

    Returns `filled` (amounts read), `matched`, `checked` and `cleared`.
    """
    with _pass_lock:
        filled = int(db.backfill_due_amounts())
        stats = db.finance_match_due_payments()
    out = {"filled": filled}
    out.update({str(k): int(v) for k, v in stats.items()})
    return out


def schedule(db, delay_s: float = SETTLE_DELAY_S,
             max_wait_s: float = MAX_WAIT_S) -> None:
    """Run a pass shortly after the last document of a batch has landed.

    Documents are filed one at a time but they arrive in handfuls — a folder
    dropped on /upload, or the mail watcher handing over a month of
    invoices. Restarting the timer on every call collapses the whole burst
    into a single pass, which still starts seconds after the last file.
    """
    global _timer, _waiting_since
    now = time.monotonic()
    with _timer_lock:
        if _waiting_since is None:
            _waiting_since = now
        waited = now - _waiting_since
        if waited + delay_s > max_wait_s:
            delay_s = max(0.0, max_wait_s - waited)
        if _timer is not None:
            _timer.cancel()
        _timer = threading.Timer(delay_s, _fire, args=(db,))
        _timer.daemon = True          # never hold up a shutdown
        _timer.start()


def _fire(db) -> None:
    global _timer, _waiting_since
    with _timer_lock:
        _timer = None
        _waiting_since = None
    try:
        out = run_now(db)
        if out["filled"] or out["matched"] or out["cleared"]:
            logger.info(
                "Fristen: %d Betrag/Betraege gelesen, %d von %d offenen "
                "Zahlungen einer Buchung zugeordnet, %d verwaiste "
                "Verknuepfung(en) geloest.",
                out["filled"], out["matched"], out["checked"], out["cleared"],
            )
    except Exception:
        logger.exception("Fristen: Abgleich nach dem Einsortieren fehlgeschlagen")
