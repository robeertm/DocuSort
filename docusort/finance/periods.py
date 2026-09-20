"""Salary-period ("Gehaltsmonat") cashflow.

Robert's bank month doesn't line up with the calendar month: the CSV
statements start around the 23rd/24th with the salary credit from his
employer and the "month" runs until the day before the next salary lands
(≈ the 22nd/23rd). This module turns the flat transaction stream into a
list of such periods and aggregates income / expense / savings for each,
so the /finance tracker can lead with "wie viel kommt rein, wie viel geht
raus, wie viel bleibt" instead of arbitrary Jan–Dec buckets.

Design goals:
- Reuse the data DocuSort already stores (booking_date, amount,
  category, tx_type, counterparty, purpose, and the per-account
  is_savings flag). No new extraction.
- Detect the salary booking robustly: an explicit user-configured match
  string (e.g. "ACME GMBH") wins; otherwise fall back to the
  category/type the CSV importer already tagged as `gehalt`.
- Never crash on messy data — a month with no detectable salary simply
  falls back to a fixed anchor day so the timeline stays contiguous.

Everything here is pure Python on plain dicts; the DB layer feeds it rows
and formats the result. Kept dependency-free on purpose.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Iterable

# A booking only counts as "the salary" when it's an inflow of at least
# this many euros. Guards against a €1,20 interest credit or a tiny
# refund from the employer being mistaken for the paycheck when the user
# relies on category-based detection.
_MIN_SALARY_AMOUNT = 400.0


def _parse(iso: str) -> date | None:
    try:
        return date(int(iso[0:4]), int(iso[5:7]), int(iso[8:10]))
    except (ValueError, TypeError, IndexError):
        return None


def _clamp_day(year: int, month: int, day: int) -> date:
    """Return `day` of (year, month), clamped to the last valid day so
    an anchor of 31 still resolves in February."""
    day = max(1, min(28 if month == 2 else 30 if month in (4, 6, 9, 11) else 31, day))
    # February leap-year correction: allow 29 when valid.
    if month == 2:
        try:
            return date(year, 2, 29 if day >= 29 else day)
        except ValueError:
            return date(year, 2, 28)
    return date(year, month, day)


def is_salary(tx: dict[str, Any], salary_match: str) -> bool:
    """True when this booking looks like the monthly salary credit."""
    try:
        amount = float(tx.get("amount") or 0.0)
    except (TypeError, ValueError):
        return False
    if amount < _MIN_SALARY_AMOUNT:
        return False
    if salary_match:
        blob = f"{tx.get('counterparty') or ''} {tx.get('purpose') or ''}".upper()
        return salary_match.upper() in blob
    # No configured match → trust the tag the CSV importer already set.
    return (tx.get("category") == "gehalt") or (tx.get("tx_type") == "gehalt")


def _salary_starts(
    txs: list[dict[str, Any]], salary_match: str,
) -> list[date]:
    """One period-start date per calendar month that has a salary
    booking. When several salary-like inflows land in the same month we
    take the largest (the real paycheck, not a bonus top-up), and among
    equal amounts the earliest date."""
    by_month: dict[tuple[int, int], tuple[float, date]] = {}
    for tx in txs:
        if tx.get("is_savings"):
            continue  # salary lands on the Girokonto, never a Sparkonto
        if not is_salary(tx, salary_match):
            continue
        d = _parse(str(tx.get("booking_date") or ""))
        if not d:
            continue
        amount = float(tx.get("amount") or 0.0)
        key = (d.year, d.month)
        cur = by_month.get(key)
        if cur is None or amount > cur[0] or (amount == cur[0] and d < cur[1]):
            by_month[key] = (amount, d)
    return sorted(d for _, d in by_month.values())


def compute_boundaries(
    txs: list[dict[str, Any]], *, salary_match: str, anchor_day: int,
) -> list[date]:
    """Ordered list of period-start dates spanning the data.

    Detected salary dates are the primary boundaries. For every calendar
    month in the data range that has NO detected salary, a synthetic
    boundary is inserted at `anchor_day` so the timeline never has a
    multi-month gap. Duplicate/near-duplicate boundaries (a synthetic one
    within a few days of a real salary date) are collapsed to the real
    one."""
    dates = [d for d in (_parse(str(t.get("booking_date") or "")) for t in txs) if d]
    if not dates:
        return []
    first, last = min(dates), max(dates)

    salary_starts = _salary_starts(txs, salary_match)
    have_month = {(d.year, d.month) for d in salary_starts}

    # Walk every calendar month in [first, last] and add an anchor-day
    # boundary where no salary was detected.
    synthetic: list[date] = []
    y, m = first.year, first.month
    while (y, m) <= (last.year, last.month):
        if (y, m) not in have_month:
            synthetic.append(_clamp_day(y, m, anchor_day))
        m += 1
        if m > 12:
            m, y = 1, y + 1

    boundaries = sorted(set(salary_starts) | set(synthetic))

    # Collapse a synthetic anchor that sits within 6 days of a real
    # salary boundary (the salary shifts a day or two around weekends;
    # we don't want two starts for the same real period).
    salary_set = set(salary_starts)
    cleaned: list[date] = []
    for b in boundaries:
        if b in salary_set:
            cleaned.append(b)
            continue
        if any(abs((b - s).days) <= 6 for s in salary_set):
            continue
        cleaned.append(b)
    return sorted(set(cleaned))


def _month_label(d: date) -> str:
    """A short 'YYYY-MM' key for the period, taken from the start date's
    month. The web layer localises it to a month name."""
    return f"{d.year:04d}-{d.month:02d}"


def build_periods(
    txs: Iterable[dict[str, Any]], *,
    salary_match: str = "",
    anchor_day: int = 23,
    monthly_budget: float = 0.0,
    today: str | None = None,
    saving_cats: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Turn the transaction stream into salary periods, newest last.

    Each returned period is a dict:
        start, end        ISO dates (end inclusive; the last period's
                          end is the latest booking date = "current")
        month_key         'YYYY-MM' of the start, for labelling
        income            positive cashflow on spending accounts
                          (salary, refunds, …), transfers excluded
        expense           |negative cashflow| on spending accounts,
                          transfers excluded (>= 0)
        net               income - expense
        saved             net € that moved INTO savings accounts
        salary            the detected salary amount (0 if none)
        budget            the spending budget for this period (fixed
                          `monthly_budget` if set, else `income`)
        remaining         budget - expense (what's left to spend)
        is_current        True for the open/most-recent period
        days_total        length of the period in days
        days_elapsed      days from start..today (current period only)
        days_left         days remaining until the next salary is due
        tx_count          number of bookings in the period
    """
    txs = list(txs)
    saving_set = set(saving_cats or ())
    boundaries = compute_boundaries(
        txs, salary_match=salary_match, anchor_day=anchor_day
    )
    if not boundaries:
        return []

    dates = [d for d in (_parse(str(t.get("booking_date") or "")) for t in txs) if d]
    first_booking = min(dates)
    last_booking = max(dates)
    today_d = _parse(today or "") or last_booking

    # A period can't start in the future: drop any (synthetic anchor)
    # boundary past the newest data / today, otherwise the open period
    # degenerates to end-before-start.
    cutoff = max(last_booking, today_d)
    boundaries = [b for b in boundaries if b <= cutoff]
    if not boundaries:
        boundaries = [first_booking]
    # And no booking may fall before the first boundary (it would be
    # silently dropped): anchor the first period at the earliest booking.
    if boundaries[0] > first_booking:
        boundaries = [first_booking, *boundaries]

    # Period i spans [boundaries[i], boundaries[i+1] - 1 day]; the final
    # period is open and ends at the later of last_booking / today.
    periods: list[dict[str, Any]] = []
    for i, start in enumerate(boundaries):
        if i + 1 < len(boundaries):
            end = boundaries[i + 1] - timedelta(days=1)
            next_start = boundaries[i + 1]
            is_current = False
        else:
            end = max(last_booking, today_d)
            next_start = None
            is_current = True
        periods.append({
            "start": start.isoformat(),
            "end": end.isoformat(),
            "_start": start, "_end": end, "_next": next_start,
            "month_key": _month_label(start),
            "income": 0.0, "expense": 0.0, "saved": 0.0,
            "salary": 0.0, "tx_count": 0,
            "is_current": is_current,
        })

    def _find(d: date) -> dict[str, Any] | None:
        for p in periods:
            if p["_start"] <= d <= p["_end"]:
                return p
        return None

    for tx in txs:
        d = _parse(str(tx.get("booking_date") or ""))
        if not d:
            continue
        p = _find(d)
        if p is None:
            continue
        p["tx_count"] += 1
        try:
            amount = float(tx.get("amount") or 0.0)
        except (TypeError, ValueError):
            continue
        if tx.get("is_savings"):
            p["saved"] += amount
            continue
        if tx.get("category") == "uebertrag":
            continue  # internal move between own accounts, not cashflow
        if tx.get("category") in saving_set:
            # Fund plan, endowment insurance …: the money is parked, not
            # spent — counts as saved (a payout comes back as negative).
            p["saved"] += -amount
            continue
        if amount > 0:
            p["income"] += amount
            if is_salary(tx, salary_match) and amount > p["salary"]:
                p["salary"] = amount
        else:
            p["expense"] += -amount

    for p in periods:
        p["income"] = round(p["income"], 2)
        p["expense"] = round(p["expense"], 2)
        p["saved"] = round(p["saved"], 2)
        p["salary"] = round(p["salary"], 2)
        p["net"] = round(p["income"] - p["expense"], 2)
        budget = monthly_budget if monthly_budget and monthly_budget > 0 else p["income"]
        p["budget"] = round(budget, 2)
        p["remaining"] = round(budget - p["expense"], 2)
        p["days_total"] = (p["_end"] - p["_start"]).days + 1
        if p["is_current"]:
            # Days until the next salary is due. If a real next boundary
            # is unknown (it always is for the open period), estimate it
            # as start + typical period length (30 days) or the anchor of
            # next month, whichever we can compute.
            due = p["_start"] + timedelta(days=30)
            p["_due"] = due
            elapsed = (today_d - p["_start"]).days
            p["days_elapsed"] = max(0, elapsed)
            p["days_left"] = max(0, (due - today_d).days)
            p["due_date"] = due.isoformat()
        else:
            p["days_elapsed"] = p["days_total"]
            p["days_left"] = 0
            p["due_date"] = ""
        # Strip the private date objects before returning.
        for k in ("_start", "_end", "_next", "_due"):
            p.pop(k, None)

    return periods
