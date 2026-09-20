"""Das Spar-Spiel: Punkte, Serien, Ränge und Abzeichen für eine Tagesreihe.

🔴 Diese Datei ist die EINZIGE Stelle, an der die Spielregeln stehen. Die
Karte auf /ausgaben zeigt einen Zeitraum, die Bestenliste vergleicht alle —
beide rechnen mit dieser Funktion. Eine zweite Umsetzung in JavaScript
(so lief 0.52.0) hätte bei der ersten Regeländerung zwei verschiedene
Wahrheiten auf derselben Seite ergeben.

Die Regeln, in Worten:

* Messlatte ist der **Median der Tage mit Ausgaben** des Zeitraums — nicht
  der Durchschnitt: ein einziger teurer Tag würde sonst die Latte so weit
  heben, dass fast jeder Tag darunter liegt, und das Spiel verschenken.
* Ein Tag ohne Ausgaben bringt 10 Punkte, **am Wochenende nur 5**: Karten-
  und Lastschriftbuchungen kommen erst am nächsten Bankarbeitstag an, ein
  leerer Sonntag ist also oft nur ein Buchungsloch und keine Leistung.
* Darunter gestaffelt: bis zu einem Viertel der Messlatte 6, bis zur
  Hälfte 4, bis zur Messlatte 2, darüber nichts. Keine Minuspunkte.
* Tage, die noch kommen (`future`) oder für die noch keine Buchung
  vorliegt (`pending`), zählen **gar nicht** — weder im Zähler noch im
  Nenner. Eine Lücke in den Daten ist kein Sparerfolg.
* Unter drei gewerteten Tagen gibt es **keinen Rang** (`rated = False`),
  sonst stünde am ersten Tag eines Gehaltsmonats eine Wertung über einem
  Zeitraum, den es noch gar nicht gab.
* Zusätzlich zählt das **Monatsergebnis** (Wunsch: „beim spiel sollte auch
  noch gewichtet werden wenn saldo am ende des monats positiv war"):
  Wer mehr eingenommen als ausgegeben hat, bekommt einen Bonus von bis zu
  20 % der Tagespunkte — voll ab einer Sparquote von 30 %. Der Bonus
  wächst mit der Länge des Zeitraums mit, damit ein kurzer Monat ihn nicht
  überproportional gewinnt, und er steckt auch im Nenner: die Punktequote
  bleibt vergleichbar.
"""

from __future__ import annotations

from datetime import date
from typing import Any

ZERO_WEEKDAY = 10
ZERO_WEEKEND = 5          # die Bank bucht am Wochenende nicht
QUARTER_BAR = 6
HALF_BAR = 4
AT_BAR = 2
MAX_PER_DAY = ZERO_WEEKDAY
MIN_RATED_DAYS = 3
BONUS_SHARE = 0.2        # Bonus = höchstens 20 % der möglichen Tagespunkte
FULL_BONUS_RATE = 0.30   # volle Punkte ab 30 % Sparquote (Saldo ÷ Einnahmen)

# Anteil erreichter Punkte → Rang. Von oben nach unten geprüft.
RANKS: tuple[tuple[float, str, str], ...] = (
    (60.0, "rank_fox", "🦊"),
    (45.0, "rank_hero", "🛡️"),
    (30.0, "rank_solid", "⚖️"),
    (15.0, "rank_room", "🌱"),
    (0.0, "rank_spend", "🎈"),
)


def _is_weekend(iso_day: str) -> bool:
    try:
        return date.fromisoformat(iso_day[:10]).weekday() >= 5
    except ValueError:
        return False


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def day_points(spend: float, bar: float, weekend: bool) -> int:
    """Punkte eines einzelnen gewerteten Tages."""
    if spend <= 0:
        return ZERO_WEEKEND if weekend else ZERO_WEEKDAY
    if bar <= 0:
        return 0
    if spend <= bar * 0.25:
        return QUARTER_BAR
    if spend <= bar * 0.5:
        return HALF_BAR
    if spend <= bar:
        return AT_BAR
    return 0


def score_days(days: list[dict[str, Any]],
               income_total: float = 0.0) -> dict[str, Any]:
    """Bewertet eine Tagesreihe aus `_day_series`.

    `income_total` sind die Einnahmen desselben Zeitraums; daraus entsteht
    der Bonus fürs Monatsergebnis. Ohne Einnahmen gibt es keinen Bonus —
    und keinen aufgeblähten Nenner.
    """
    counted = [d for d in days
               if not d.get("future") and not d.get("pending")]
    spends = [float(d.get("spend") or 0.0) for d in counted]
    bar = _median([v for v in spends if v > 0])

    points = zero_days = under_bar = best = run = 0
    spend_total = 0.0
    for d in counted:
        v = float(d.get("spend") or 0.0)
        spend_total += v
        p = day_points(v, bar, _is_weekend(str(d.get("date") or "")))
        if v <= 0:
            zero_days += 1
        if v <= bar:
            under_bar += 1
        points += p
        if p > 0:
            run += 1
            best = max(best, run)
        else:
            run = 0

    n = len(counted)
    day_max = n * MAX_PER_DAY

    # Monatsergebnis: nur ein positiver Saldo bringt etwas, und er bringt
    # umso mehr, je größer der Teil der Einnahmen ist, der übrig blieb.
    income = round(float(income_total or 0.0), 2)
    net = round(income - round(spend_total, 2), 2)
    rate = (net / income) if income > 0 and net > 0 else 0.0
    bonus_max = int(round(day_max * BONUS_SHARE)) if income > 0 else 0
    bonus = int(round(bonus_max * min(1.0, rate / FULL_BONUS_RATE))) if bonus_max else 0

    points_days = points
    points += bonus
    max_points = day_max + bonus_max
    share = (points / max_points * 100) if max_points else 0.0
    rated = n >= MIN_RATED_DAYS

    idx = next((i for i, r in enumerate(RANKS) if share >= r[0]), len(RANKS) - 1)
    rank_key, rank_icon = RANKS[idx][1], RANKS[idx][2]
    next_rank: dict[str, Any] | None = None
    if rated and idx > 0:
        up = RANKS[idx - 1]
        next_rank = {"key": up[1], "icon": up[2],
                     "missing": max(1, int(round(up[0] / 100 * max_points - points)))}

    badges = [
        {"key": "plus", "icon": "💰", "done": net > 0,
         "rate": round(rate * 100, 1), "net": net},
        {"key": "zero", "icon": "🎯", "done": zero_days >= 1, "count": zero_days},
        {"key": "three", "icon": "🔥", "done": best >= 3, "missing": max(0, 3 - best)},
        {"key": "week", "icon": "🏆", "done": best >= 7, "missing": max(0, 7 - best)},
        {"key": "half", "icon": "🌗", "done": n > 0 and under_bar > n / 2,
         "count": under_bar, "of": n},
    ]

    return {
        "bar": round(bar, 2),
        "points": points,
        "points_days": points_days,
        "day_max": day_max,
        "bonus": bonus,
        "bonus_max": bonus_max,
        "income_total": income,
        "net": net,
        "savings_rate": round(rate * 100, 1),
        "max_points": max_points,
        "share": round(share, 1),
        "zero_days": zero_days,
        "counted_days": n,
        "total_days": len(days),
        "open_days": len(days) - n,
        "streak": run,            # Serie bis zum letzten gewerteten Tag
        "best_streak": best,
        "under_bar": under_bar,
        "spend_total": round(spend_total, 2),
        "rated": rated,
        "rank_key": rank_key,
        "rank_icon": rank_icon,
        "next_rank": next_rank,
        "badges": badges,
    }
