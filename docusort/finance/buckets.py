"""Everyday-spending buckets for the Monats-Ausgaben dashboard.

The /finance tracker answers "rein / raus / übrig" per Gehaltsmonat. This
module answers a narrower, very concrete question Robert asked for: *wofür*
ging das Geld diesen Monat drauf, in den paar Alltags-Töpfen, die er wirklich
im Blick behalten will —

    Lebensmittel   (Aldi, Lidl, Edeka, Rewe, …)
    Drogerie       (dm, Rossmann, Müller, …)
    Essen gehen    (Restaurant, Lieferando, Bäckerei-Imbiss, Soul Food, …)
    Auto           (Tankstelle, Werkstatt, Reifen, KFZ-Zubehör, …)
    Baumarkt       (Obi, Bauhaus, Hornbach, Toom, Baustoffe, …)
    Amazon         (alles über Amazon, egal welche Warengruppe)
    Kleidung       (Zalando, H&M, C&A, Deichmann, …)

plus ein Sammel-Topf „Sonstiges" für alles andere, damit die Summen aufgehen.

Es wird bewusst **keine neue Extraktion** gebaut: jede Buchung, die der
CSV-Import bzw. die Kontoauszug-Analyse ohnehin schon mit `category` und
`counterparty`/`purpose` abgelegt hat, wird hier nur einem Topf zugeordnet.
Zuordnung passiert in drei Stufen:
  1. **Händlername** (robuster als die LLM-Kategorie — „REWE" ist eindeutig)
  2. ein **KI-Override** aus `merchant_buckets` — die lokale KI (dieselbe, die
     alle Dokumente prüft) hat für exotische / englische Händlernamen wie
     „Soul Food" (Suppenbar) einmalig einen Topf bestimmt, der hier gecacht
     nachgeschlagen wird
  3. Rückfall auf die grobe **Buchungs-Kategorie**

so dass am Ende möglichst wenig im Sammel-Topf „Sonstiges" landet.

Reines Python auf schlichten Dicts, ohne Provider-/DB-Abhängigkeiten, damit die
Web- und DB-Schicht es frei importieren und testen können.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

# Canonical bucket order — this is the order tiles, legend and donut segments
# render in. `sonstiges` is always last (the catch-all).
BUCKET_ORDER = (
    "lebensmittel", "drogerie", "essen", "auto", "baumarkt",
    "amazon", "kleidung", "sonstiges",
)

# Per-bucket display metadata. Colours are the validated data-viz categorical
# palette (light / dark step); the template turns them into CSS custom
# properties so a theme toggle swaps both. `icon` is an inline-SVG path set
# rendered by the template.
BUCKET_META: dict[str, dict[str, str]] = {
    "lebensmittel": {"color_light": "#2a78d6", "color_dark": "#3987e5", "icon": "cart"},
    "drogerie":     {"color_light": "#eb6834", "color_dark": "#d95926", "icon": "sparkles"},
    "essen":        {"color_light": "#1baf7a", "color_dark": "#199e70", "icon": "utensils"},
    "auto":         {"color_light": "#7c5cd4", "color_dark": "#8f74e0", "icon": "car"},
    "baumarkt":     {"color_light": "#97633a", "color_dark": "#ac764a", "icon": "hammer"},
    "amazon":       {"color_light": "#eda100", "color_dark": "#c98500", "icon": "box"},
    "kleidung":     {"color_light": "#e87ba4", "color_dark": "#d55181", "icon": "shirt"},
    "sonstiges":    {"color_light": "#898781", "color_dark": "#898781", "icon": "dots"},
}

# Merchant keyword lists. Matching is whole-token (see `_matches`), so short
# distinctive names like "dm" or "amzn" are safe from firing on substrings of
# unrelated words. Keep everything lower-case; umlauts are normalised away.
_AMAZON = (
    "amazon", "amzn", "audible", "amazon prime", "amazon payments",
)
_DROGERIE = (
    "dm", "dm drogeriemarkt", "rossmann", "mueller", "muller", "budni",
    "budnikowsky", "drogerie", "drogeriemarkt",
)
_KLEIDUNG = (
    "zalando", "aboutyou", "about you", "zara", "primark", "deichmann",
    "tk maxx", "tkmaxx", "tk max", "hennes", "mauritz", "esprit",
    "new yorker", "snipes", "foot locker", "footlocker", "reno",
    "gerry weber", "bonprix", "peek", "cloppenburg", "engelhorn",
    "breuninger", "uniqlo", "mango", "bershka", "ernsting", "ernstings",
    "kik", "takko", "nkd", "s oliver", "soliver", "vero moda", "veromoda",
    "jack jones", "jackjones", "adidas", "nike", "puma", "jd sports",
    "intersport", "sportscheck", "schuhe", "modehaus", "bekleidung",
)
_SUPERMARKET = (
    "aldi", "lidl", "edeka", "rewe", "penny", "kaufland", "netto",
    "norma", "real", "tegut", "globus", "nahkauf", "marktkauf", "famila",
    "combi", "denns", "denn s", "alnatura", "e center", "wasgau",
    "konsum", "feneberg", "trinkgut", "getraenke", "getranke",
    "metzgerei", "baeckerei", "backerei", "supermarkt", "lebensmittel",
    "v markt", "kaufhof lebensmittel",
)
_ESSEN = (
    "restaurant", "ristorante", "pizzeria", "pizza", "kebab", "doener",
    "doner", "mcdonald", "mcdonalds", "burger king", "burgerking", "kfc",
    "subway", "lieferando", "wolt", "uber eats", "ubereats", "starbucks",
    "gastro", "gaststaette", "gaststatte", "imbiss", "sushi", "trattoria",
    "bistro", "vapiano", "nordsee", "backwerk", "coffee fellows",
    "gasthaus", "gasthof", "brauhaus", "eiscafe", "eiscafé", "cafe",
    "caf", "l osteria", "losteria", "hans im glueck", "peter pane",
    # English / anglicised names — häufig bei modernen Läden, die die reine
    # Keyword-Liste sonst nicht fängt (Robert: „soul food ist ne suppenbar").
    "soul food", "soulfood", "food", "streetfood", "street food", "foodtruck",
    "kitchen", "grill", "steakhouse", "steak house", "diner", "deli",
    "canteen", "kantine", "mensa", "bowl", "poke", "ramen", "noodle",
    "noodles", "wok", "asia", "asiate", "thai", "curry", "sushi bar",
    "taco", "burrito", "mexican", "mexikaner", "vietnam", "pho", "dim sum",
    "tapas", "kaffeehaus", "kaffee", "coffee", "espresso", "roastery",
    "roesterei", "baecker cafe", "konditorei", "eisdiele", "eismanufaktur",
    "gelateria", "pub", "bar ", "biergarten", "brewery", "brauerei",
    "pommes", "currywurst", "wurstbraterei", "food court", "foodcourt",
    "bringdienst", "lieferservice", "just eat", "flink", "gorillas",
    "smoothie", "juice", "burger", "chicken", "fish", "seafood",
    "brunch", "breakfast", "lunch", "restaurant gmbh",
)
# Auto — Tankstelle, Werkstatt, Reifen, KFZ-Zubehör, Autohaus, Parken, Maut.
# Absichtlich VOR Lebensmittel geprüft (Tankstellen-Shops verkaufen auch
# Lebensmittel, sollen aber als Auto-Ausgabe zählen).
_AUTO = (
    "aral", "shell", "esso", "total", "totalenergies", "jet", "star tankstelle", "orlen",
    "bft", "hem", "agip", "avia", "westfalen", "raststaette", "raststätte",
    "tankstelle", "tank", "sprit", "kraftstoff", "benzin", "diesel", "e10",
    "e5", "adblue", "ladepark", "ladesaeule", "ladesäule", "ionity", " enbw ",
    "wallbox", "ladestrom", "charge bill", "giro e de", "ladevorgang", "ph centrum galerie", "centrum galerie", "kfz", "werkstatt", "autowerkstatt", "freie werkstatt",
    "reifen", "reifenservice", "reifendienst", "atu", "a t u", "pit stop",
    "pitstop", "vergoelst", "vergölst", "premio", "euromaster", "first stop",
    "bosch service", "dekra", "tuev", "tüv", "hauptuntersuchung", "adac",
    "autoteile", "kfz teile", "autohaus", "autohandel", "autozubehoer",
    "carglass", "autoglas", "waschstrasse", "waschstraße", "autowaesche",
    "autowäsche", "carwash", "parkhaus", "parkplatz", "apcoa", "contipark",
    "park and", "parkgebuehr", "maut", "toll collect", "go maxx", "leasing",
    "autovermietung", "sixt", "europcar", "hertz", "car2go", "share now",
    "opel", "volkswagen", "vw autohaus", "mercedes", "bmw autohaus", "audi zentrum",
    # Autohaus-/Händler-Namen, die weder „Autohaus" noch eine Marke im Klartext
    # führen — z. B. „AUTOMOBILE MUSTERSTADT". „automobile"/„automobil" ist als
    # ganzes Token eindeutig ein Fahrzeug-Händler.
    "automobile", "automobil", "autozentrum", "autocenter", "autopark",
    "autogalerie", "automobilhandel", "automobile gmbh", "car center",
    # Autobanken / Fahrzeug-Finanzierung — die Rate für Auto/Leasing läuft über
    # eine markengebundene Bank. Robert will diese Raten im Auto-Topf sehen,
    # nicht in „Sonstiges" (die reine KI schiebt „…Bank…" sonst zu sonstiges).
    "hyundai capital", "vw bank", "volkswagen bank", "vw financial",
    "vw leasing", "volkswagen leasing", "vw financial services",
    "mercedes benz bank", "mercedes-benz bank", "daimler", "bmw bank",
    "bmw financial", "toyota kreditbank", "toyota financial", "rci banque",
    "ford bank", "opel bank", "psa bank", "renault bank", "kia finance",
    "hyundai finance", "audi bank", "porsche financial", "fca bank",
    # Fahrzeug-Marken als eigenständiges Token — im Zahlungsempfänger eines
    # Kontos praktisch immer Autohaus/Werkstatt/Finanzierung dieser Marke.
    "hyundai", "kia", "toyota", "renault", "peugeot", "citroen", "skoda",
    "seat", "dacia", "mazda", "nissan", "fiat", "volvo", "tesla", "porsche",
    "suzuki", "mitsubishi", "subaru", "jeep", "cupra",
)
# Baumarkt / Bauen — Heimwerker, Baustoffe, Garten, Sanitär, Holz, Farben.
_BAUMARKT = (
    "obi", "bauhaus", "hornbach", "hagebau", "hagebaumarkt", "toom",
    "hellweg", "globus baumarkt", "baywa", "raiffeisen markt", "praktiker",
    "max bahr", "baumarkt", "baustoffe", "baustoff", "baumarkt gmbh",
    "holz", "holzhandel", "sanitaer", "sanitär", "fliesen", "farben",
    "malerbedarf", "baubedarf", "gartencenter", "gartenmarkt", "pflanzen",
    "dehner", "gartenland", "elektrogrosshandel", "elektro grosshandel",
    "werkzeug", "eisenwaren", "schrauben", "beschlag", "dachdecker",
    "heizung sanitaer", "installateur", "klempner", "estrich", "beton",
    "kies", "schotter", "zaun", "zaunbau", "rollrasen", "hagebau markt",
)

# Bank-statement `category` values → bucket, as a fallback when no merchant
# keyword hits. Deliberately conservative: `haushalt` is NOT mapped to
# Drogerie because it also covers general home goods; Drogerie stays
# merchant-driven so it doesn't swallow unrelated household spend.
_CATEGORY_BUCKET: dict[str, str] = {
    "lebensmittel": "lebensmittel",
    "essen-ausser-haus": "essen",
    "bekleidung": "kleidung",
    "mobilitaet": "auto",
}


def _norm(text: str | None) -> str:
    """Lower-case, strip umlauts/punctuation to single spaces. Turns
    'dm-drogerie márkt' → 'dm drogerie markt' so keyword tokens line up."""
    s = (text or "").lower()
    s = (s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
           .replace("ß", "ss"))
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return f" {s.strip()} "


def _matches(blob: str, keywords: Iterable[str]) -> bool:
    """True when any keyword appears as a whole token / phrase inside the
    already-normalised, space-padded `blob`. Whole-token matching keeps
    'dm' from firing on 'admin' while still catching 'dm drogeriemarkt'."""
    for kw in keywords:
        k = _norm(kw).strip()
        if k and f" {k} " in blob:
            return True
    return False


_KEY_TAIL = re.compile(r"\s{3,}.*$|//.*$|/.*$")


def merchant_key(counterparty: str | None) -> str:
    """Normalised lookup key for a merchant, used both to store and to read
    KI overrides in `merchant_buckets`. Empty string when there is no usable
    counterparty name. Keeping this in one place means the DB cache and the
    classifier always agree on the key.

    The bank appends the payee's address in two shapes — a run of spaces
    („DekaBank … Girozentrale        Grosse Gallusstrasse 14", older
    Sparkasse exports) or slashes („Supercell//Helsinki/FI/1",
    „ZIEGELWIRTSCHAFT/An der Ziegelei 2/Musterstadt/DE", card terminals). Both
    are cut off so one payee stays one payee across export formats;
    otherwise a contract counts twice on /fixkosten."""
    return _norm(_KEY_TAIL.sub("", (counterparty or "").strip())).strip()


def classify(tx: dict[str, Any], overrides: dict[str, str] | None = None) -> str:
    """Return the bucket id for one booking.

    Order: merchant keyword → KI-override (cached, for exotic/English names)
    → grobe Buchungs-Kategorie → Sonstiges. Amazon and Auto are checked
    early so an Amazon grocery order lands in Amazon and a Tankstellen-Shop
    counts as Auto rather than Lebensmittel — the way Robert asked.

    `overrides` maps `merchant_key(counterparty)` → bucket id (from the
    lokale-KI cache). Only consulted when the keyword lists don't fire, so a
    clear name like „REWE" always wins over a stale cache entry.
    """
    blob = _norm(f"{tx.get('counterparty') or ''} {tx.get('purpose') or ''}")
    if _matches(blob, _AMAZON):
        return "amazon"
    if _matches(blob, _AUTO):
        return "auto"
    if _matches(blob, _BAUMARKT):
        return "baumarkt"
    if _matches(blob, _DROGERIE):
        return "drogerie"
    if _matches(blob, _KLEIDUNG):
        return "kleidung"
    if _matches(blob, _SUPERMARKET):
        return "lebensmittel"
    if _matches(blob, _ESSEN):
        return "essen"
    if overrides:
        b = overrides.get(merchant_key(tx.get("counterparty")))
        if b in BUCKET_ORDER:
            return b
    cat = (tx.get("category") or "").strip()
    return _CATEGORY_BUCKET.get(cat, "sonstiges")


def summarise(txs: Iterable[dict[str, Any]],
              overrides: dict[str, str] | None = None) -> dict[str, Any]:
    """Aggregate a stream of *expense* bookings (amount < 0) into buckets.

    Callers pass only the rows they want counted (one month, spending
    accounts, transfers already excluded). Returns, per bucket in
    `BUCKET_ORDER`: total spend (positive €), booking count, and the top
    merchants inside it. Also the grand total and a per-day spend series
    (all buckets combined) for the trend strip.
    """
    buckets: dict[str, dict[str, Any]] = {
        b: {"id": b, "total": 0.0, "count": 0, "_merchants": {}}
        for b in BUCKET_ORDER
    }
    per_day: dict[str, float] = {}
    grand_total = 0.0

    for tx in txs:
        try:
            amount = float(tx.get("amount") or 0.0)
        except (TypeError, ValueError):
            continue
        if amount >= 0:
            continue  # income / refunds aren't spend
        spend = -amount
        b = classify(tx, overrides)
        entry = buckets[b]
        entry["total"] += spend
        entry["count"] += 1
        merchant = (tx.get("counterparty") or "").strip() or "—"
        m = entry["_merchants"]
        m[merchant] = m.get(merchant, 0.0) + spend
        grand_total += spend
        day = str(tx.get("booking_date") or "")[:10]
        if day:
            per_day[day] = per_day.get(day, 0.0) + spend

    out_buckets = []
    for b in BUCKET_ORDER:
        e = buckets[b]
        merchants = sorted(
            ({"name": n, "total": round(v, 2)} for n, v in e["_merchants"].items()),
            key=lambda r: r["total"], reverse=True,
        )[:5]
        out_buckets.append({
            "id": b,
            "total": round(e["total"], 2),
            "count": e["count"],
            "share": round(100.0 * e["total"] / grand_total, 1) if grand_total > 0 else 0.0,
            "top_merchants": merchants,
            **BUCKET_META[b],
        })

    days = [{"date": d, "spend": round(per_day[d], 2)} for d in sorted(per_day)]
    return {
        "buckets": out_buckets,
        "total": round(grand_total, 2),
        "days": days,
    }
