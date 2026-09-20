"""Eine Buchung → Kategorie **mit Begründung**.

Robert: „es gibt immer eine kategorie sonstige … zuweisen und das system
lernt dann dabei … jede einzelne buchung muss nachvollziehbar sein."

Deshalb liefert dieser Klassifikator nie nur ein Wort, sondern eine
`Decision(category, source, reason)`, die an der Buchung gespeichert wird
(`transactions.category_source` / `category_reason`) und im Explorer als
Abzeichen + Tooltip erscheint. Die Stufen, von stark nach schwach:

    manual    – von Hand an DIESER Buchung gesetzt (Pin über tx_hash)
    transfer  – Gegen-IBAN ist ein eigenes Konto → Umbuchung
    rule      – gelernte Regel für diesen Empfänger (IBAN oder Name),
                entstanden aus einer Hand-Zuweisung
    ai        – dieselbe Regel, aber von der lokalen KI vorgeschlagen
    keyword   – eingebaute Erkennung (Händlerlisten aus `buckets.py`,
                Muster für Miete/Versicherung/Gehalt/…)
    bank      – nur der Buchungstyp der Bank (Gehalt, Zinsen, Gebühr, Bargeld)
    none      – nichts erkannt → „sonstiges", wartet auf Zuweisung

Die Händler-Wortlisten werden aus `buckets.py` wiederverwendet, damit die
Töpfe auf /ausgaben und die Kategorien hier nie auseinanderlaufen. Reines
Python auf Dicts; die DB-Schicht liefert Regeln und eigene IBANs.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

from .buckets import (
    _AMAZON, _AUTO, _BAUMARKT, _DROGERIE, _ESSEN, _KLEIDUNG, _SUPERMARKET,
    _matches, _norm, merchant_key,
)
from .categories import TX_CATEGORIES, INCOME_CATEGORIES

SOURCE_MANUAL = "manual"
SOURCE_TRANSFER = "transfer"
SOURCE_RULE = "rule"
SOURCE_AI = "ai"
SOURCE_KEYWORD = "keyword"
SOURCE_BANK = "bank"
SOURCE_IMPORT = "import"     # Kategorie stammt aus der Kontoauszug-Analyse (LLM), nicht reproduzierbar
SOURCE_NONE = "none"

# Anzeige-Kurzformen für das Abzeichen im Explorer (deutsch wie die Seite).
SOURCE_LABELS: dict[str, str] = {
    SOURCE_MANUAL: "Hand",
    SOURCE_TRANSFER: "Umbuchung",
    SOURCE_RULE: "gelernt",
    SOURCE_AI: "KI",
    SOURCE_KEYWORD: "erkannt",
    SOURCE_BANK: "Bank",
    SOURCE_IMPORT: "Auszug",
    SOURCE_NONE: "offen",
}


@dataclass(frozen=True)
class Decision:
    category: str
    source: str
    reason: str


class RuleIndex:
    """Gelernte Regeln, nachschlagbar von speziell nach allgemein:

        merchant_amount  – Empfänger + Betrag (±1 %): „Wohnbau, 20 €“ = Garage,
                           „Wohnbau, 770 €“ = Miete (zwei Verträge, ein Empfänger)
        iban             – Gegen-IBAN (fängt Namensvarianten je Bank/Export)
        merchant         – normalisierter Name (Kartenzahlungen haben keine IBAN)

    Jede Regel ist ein Dict mit `match_kind`, `match_value`, `category`,
    `source` ('user' | 'ai'), `sample_name` und `direction` ('' = beide,
    'in' = nur Gutschriften, 'out' = nur Abbuchungen — gelernt aus dem
    Vorzeichen der Buchung, damit eine Deka-Erstattung nicht den
    Deka-Sparplan zur Erstattung macht). `match_value` bei
    merchant_amount = "<merchant_key>|<betrag mit 2 Nachkommastellen>".
    """

    def __init__(self, rules: Iterable[dict[str, Any]] = ()):
        self.by_iban: dict[str, list[dict[str, Any]]] = {}
        self.by_merchant: dict[str, list[dict[str, Any]]] = {}
        self.by_merchant_amount: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        for r in rules:
            kind = (r.get("match_kind") or "").strip()
            val = (r.get("match_value") or "").strip()
            if not val or not (r.get("category") or "").strip():
                continue   # categories are validated on write (built-in or user-defined)
            if kind == "iban":
                self.by_iban.setdefault(val.replace(" ", "").upper(), []).append(dict(r))
            elif kind == "merchant":
                self.by_merchant.setdefault(val, []).append(dict(r))
            elif kind == "merchant_amount" and "|" in val:
                key, amt = val.rsplit("|", 1)
                try:
                    self.by_merchant_amount.setdefault(key, []).append((abs(float(amt)), dict(r)))
                except ValueError:
                    continue

    def __len__(self) -> int:
        return sum(len(v) for v in self.by_iban.values()) + sum(len(v) for v in self.by_merchant.values()) \
            + sum(len(v) for v in self.by_merchant_amount.values())

    @staticmethod
    def _pick(rules: list[dict[str, Any]], sign: str) -> dict[str, Any] | None:
        """A rule for exactly this direction beats a both-ways rule."""
        for r in rules:
            if (r.get("direction") or "").strip() == sign:
                return r
        for r in rules:
            if not (r.get("direction") or "").strip():
                return r
        return None

    def lookup(self, tx: dict[str, Any]) -> dict[str, Any] | None:
        key = merchant_key(tx.get("counterparty"))
        try:
            raw = float(tx.get("amount") or 0.0)
        except (TypeError, ValueError):
            raw = 0.0
        sign = "in" if raw > 0 else "out"
        amt = abs(raw)
        if key and key in self.by_merchant_amount:
            hit = self._pick([rule for ra, rule in self.by_merchant_amount[key] if abs(ra - amt) <= max(0.01 * ra, 0.01)], sign)
            if hit:
                return hit
        iban = (tx.get("counterparty_iban") or "").replace(" ", "").upper()
        hit = self._pick(self.by_iban.get(iban, []), sign) if iban else None
        if hit:
            return hit
        return self._pick(self.by_merchant.get(key, []), sign) if key else None


# ---------------------------------------------------------------------------
# Eingebaute Erkennung
# ---------------------------------------------------------------------------

# Drogerie ohne das nackte „mueller": das trifft sonst jeden Herrn Müller,
# dem man Geld überweist. Die Kette schreibt sich selbst immer mit Zusatz.
_DROGERIE_SAFE = tuple(k for k in _DROGERIE if k not in ("mueller", "muller")) + (
    "mueller ltd", "mueller sagt danke", "mueller drogerie", "drogerie mueller",
    "mueller handels", "muller ltd",
)

# Händlerlisten (ganze Wörter, aus buckets.py) → Kategorie. Reihenfolge wie
# in buckets.classify: Amazon und Auto zuerst (Tankstellen-Shop = Auto).
# Gilt nur für Ausgaben — eine Gutschrift von Amazon ist eine Erstattung.
# Ergänzungen, die in den Topf-Listen fehlen (Bäcker als ein Wort, Kantinen-
# Betreiber, Imbiss-Namen) — hier statt in buckets.py, damit die Töpfe auf
# /ausgaben unverändert bleiben.
_LEBENSMITTEL_EXTRA = (
    "muehlenbaecker", "baecker", "baeckerei", "backhaus", "backerei", "fleischerei", "hofladen", "sternenbaeck",
    "getraenkemarkt", "obsthof", "biomarkt", "bio company", "landmarkt", "wochenmarkt",
)
_ESSEN_EXTRA = (
    "aramark", "sodexo", "eurest", "apetito", "compass group", "kantine", "snack", "snackpoint",
    "snack point", "suppenbar", "gasthaus", "wirtshaus", "schnellrestaurant", "lieferando",
    "rest", "gastronomie", "gastronomiebetriebe", "gastronomiebetrieb", "gastst", "wirtschaft", "eatery",
    "berggasthof", "landgasthof", "vietnamese", "italian", "greek", "indian", "bakery", "cafeteria",
    "brasserie", "weinstube", "schaenke", "schenke", "hofcafe", "backstube", "baeckerei cafe", "violino", "marcolinihaus",
    "systemgastronomie", "freddy fresh", "amrest",
)
_HAUSHALT_EXTRA = (
    "ikea", "tedi", "action", "woolworth", "kodi", "nanu nana", "butlers", "moebel", "moebelhaus", "pfennigpfeiffer", "tedox",
    "roller", "poco", "hoeffner", "xxxlutz", "xxxl", "moemax", "segmueller", "porta", "sconto",
    "depot deutschland", "leonardo", "wmf", "tchibo", "kaufhof", "galeria", "karstadt", "real markt",
    "haushaltswaren", "einrichtungshaus", "matratzen", "betten", "dekoration",
)
_KLEIDUNG_EXTRA = ("newyorker", "h m", "c a onlineshop", "c a mode", "ca mode", "tk maxx", "tjx europe", "sportshop", "sport shop",
                   "engbers", "olymp", "marc o polo", "tom tailor", "jack wolfskin", "hunkemoller", "calzedonia")
_MERCHANT_GROUPS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("online-shopping", _AMAZON, "Amazon"),
    ("mobilitaet", _AUTO, "Tankstelle / Auto / Fahrzeugfinanzierung"),
    ("haushalt", _BAUMARKT, "Baumarkt"),
    ("haushalt", _DROGERIE_SAFE, "Drogerie"),
    ("haushalt", _HAUSHALT_EXTRA, "Möbel / Haushaltswaren"),
    ("bekleidung", _KLEIDUNG + _KLEIDUNG_EXTRA, "Mode / Schuhe"),
    ("lebensmittel", _SUPERMARKET + _LEBENSMITTEL_EXTRA, "Supermarkt / Bäcker"),
    ("essen-ausser-haus", _ESSEN + _ESSEN_EXTRA, "Restaurant / Imbiss / Kantine"),
)


def _rx(*words: str) -> re.Pattern[str]:
    return re.compile(r"\b(" + "|".join(words) + r")\b", re.I)


# Muster auf dem normalisierten Text (Umlaute → ae/oe/ue, alles klein, nur
# a-z0-9). Reihenfolge = Priorität: das erste Muster gewinnt.
_OUT_PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("kreditkarte", _rx("kreditkartenabrechnung", "kreditkarten abrechnung", "visa abr", "mastercard abr",
                        "kreditkarte abrechnung", "kk abrechnung"),
     "Kreditkarten-Abrechnung (die Kartenumsätze selbst sind nicht importiert)"),
    ("bargeld", _rx("bargeldauszahlung", "bargeld", "geldautomat", "gaa", "ga nr", "auszahlung geldautomat", "atm", "cash"),
     "Bargeld am Automaten"),
    # Risk cover sold by a life insurer (BU, Unfall, Hausrat at the
    # „Sparkassen-Versicherung … Lebensversicherung") is insurance, not
    # saving — the purpose says so, check it before the insurer's name.
    ("versicherung", _rx("bu vers", "berufsunfaehigkeit", "berufsunfaehigkeits", "unfallvers", "unfallversicherung", "direct line",
                         "hausratvers", "hausrat", "haftpflicht", "haftpflichtvers", "kfz vers", "kfz versicherung",
                         "rechtsschutz", "risikolebens", "risiko lv", "wohngebaeude", "gebaeudevers", "tierhalter",
                         "zahnzusatz", "krankenzusatz", "pflegezusatz"),
     "Versicherungsbeitrag (Risikoschutz)"),
    ("miete", _rx("miete", "mietzahlung", "kaltmiete", "warmmiete", "wohnbau", "wohnungsbau", "wohnungsgenossenschaft",
                  "wohnungsbaugesellschaft", "hausverwaltung", "wohnungsgesellschaft", "vonovia", "wbg", "gwg", "gewobag",
                  "hausgeld", "nebenkostenvorauszahlung"),
     "Miete / Wohnungsgesellschaft"),
    ("sparversicherung", _rx("lebensvers", "lebensversicherung", "lebensversicherungs", "kapitallebensversicherung",
                             "rentenversicherung", "rentenvers", "riester", "ruerup", "basisrente", "altersvorsorge",
                             "sparversicherung", "fondsgebunden", "fondsgebundene", "pensionskasse", "direktversicherung",
                             "sparvertrag", "vorsorgevertrag", "rentenvertrag", "neue leben", "vorsorge"),
     "Sparversicherung / Altersvorsorge — Geld ist angelegt, nicht ausgegeben"),
    ("versicherung", _rx("versicherung", "versicherungs", "vers ag", "allianz", "huk",
                         "huk coburg", "generali", "devk", "r v", "ergo", "axa", "signal iduna", "debeka", "wgv", "lvm",
                         "gothaer", "nuernberger", "zurich", "cosmosdirekt", "hannoversche", "provinzial", "svs",
                         "sparkassen versicherung", "hdi", "vhv", "adac schutzbrief", "krankenversicherung",
                         "haftpflicht", "hausrat", "kfz versicherung", "rechtsschutz", "riester", "ruerup"),
     "Versicherungsbeitrag"),
    ("nebenkosten", _rx("stadtwerke", "vattenfall", "enbw", "e on", "eon", "sachsenenergie", "drewag", "enso", "eins energie",
                        "energie", "strom", "gas", "wasser", "abwasser", "fernwaerme", "heizkosten", "vodafone", "telekom",
                        "o2", "telefonica", "1 1", "1und1", "pyur", "unitymedia", "glasfaser", "internet", "rundfunk",
                        "ard zdf", "beitragsservice", "gez", "abfall", "muell", "entsorgung", "schornsteinfeger",
                        "grundsteuer", "hausstrom"),
     "Strom / Gas / Wasser / Telefon / Rundfunk"),
    ("elektronik", _rx("mediamarkt", "media markt", "saturn", "cyberport", "notebooksbilliger", "alternate", "conrad",
                       "reichelt", "apple store", "gravis", "expert", "euronics", "medimax", "coolblue", "computer",
                       "asgoodasnew", "refurbed", "rebuy", "back market", "tube amp doctor", "musikhaus",
                       "elektronik", "shelly", "berrybase"),
     "Elektronik-Händler"),
    ("abonnement", _rx("netflix", "spotify", "disney", "dazn", "sky", "amazon prime", "prime video", "youtube", "google one",
                       "icloud", "apple com bill", "apple com", "itunes", "apple services", "microsoft", "adobe", "dropbox", "playstation", "xbox",
                       "nintendo", "supercell", "steam", "epic games", "anthropic", "claude sub", "nextdns", "github",
                       "tailscale", "cloudflare", "1password", "jetbrains", "google cloud", "aws", "hetzner cloud",
                       "patreon", "twitch", "audible", "kindle unlimited", "fitness", "gym", "mcfit", "clever fit", "fitx", "abo",
                       "abonnement", "mitgliedsbeitrag", "mitgliedschaft", "verein", "zeitung", "zeitschrift",
                       "strato", "ionos", "hetzner", "domain", "hosting", "chatgpt", "openai"),
     "Abo / Mitgliedschaft"),
    ("gesundheit", _rx("apotheke", "arzt", "aerzte", "praxis", "zahnarzt", "kieferorthop", "krankenhaus", "klinik",
                       "klinikum", "physiotherapie", "therapie", "optiker", "fielmann", "apollo", "brille", "hoergeraete",
                       "sanitaetshaus", "rezept", "zuzahlung", "medizin", "labor", "heilpraktiker", "osteopath"),
     "Arzt / Apotheke / Klinik"),
    ("kinder", _rx("kita", "kindergarten", "kinderkrippe", "krippe", "hort", "schule", "grundschule", "gymnasium",
                   "schulverein", "foerderverein", "elternbeitrag", "essengeld", "schulessen", "mittagessen",
                   "volkssolidaritaet", "volkssolidaritat", "awo", "diakonie", "caritas", "taschengeld", "klassenfahrt", "musikschule",
                   "playport", "kidsplanet", "oskarshausen", "indoorspielplatz",
                   "nachhilfe", "sportverein", "spielzeug", "smyths", "toys", "babyone", "ernstings family",
                   "jako o", "vertbaudet"),
     "Kita / Schule / Kinder"),
    ("reisen", _rx("hotel", "hostel", "booking com", "booking", "airbnb", "expedia", "check24 reise", "tui", "fti",
                   "alltours", "dertour", "eurotours", "reise", "reisen", "reisebuero", "reiseverkehr", "kurtaxe",
                   "kurverwaltung", "kurbetrieb", "lufthansa", "eurowings",
                   "ryanair", "easyjet", "condor", "flug", "flughafen", "airport", "fewo", "ferienwohnung",
                   "campingplatz", "camping", "kreuzfahrt", "aida", "center parcs", "urlaub"),
     "Reise / Hotel / Flug"),
    ("freizeit", _rx("kino", "kinowelt", "uci", "cinemaxx", "cinestar", "cineplex", "rundkino", "ufa", "theater", "oper", "konzert", "eventim",
                     "ticket", "tickets", "museum", "zoo", "tierpark", "schwimmbad", "freibad", "therme", "sauna",
                     "freizeitpark", "funpark", "kletterwald", "bowling", "minigolf", "kletter", "escape", "spielhalle",
                     "bibliothek", "burg", "schloss", "festung",
                     "thalia", "hugendubel", "buecher", "buch", "musik", "instrument", "hobby", "angeln", "fahrrad",
                     "bike", "decathlon", "sport", "verlag"),
     "Kino / Kultur / Sport / Bücher"),
    ("kapital", _rx("wertpapier", "dekabank", "deka", "depot", "aktien", "fonds", "etf", "sparplan", "sparrate", "trade republic",
                    "scalable", "flatex", "comdirect depot", "bausparkasse", "bausparen", "lbs", "wuestenrot",
                    "schwaebisch hall", "bitvavo", "coinbase", "kraken", "bison", "krypto", "bitcoin", "vl sparen",
                    "vermoegenswirksame", "union investment"),
     "Sparen / Wertpapiere / Krypto"),
    ("steuer", _rx("finanzamt", "stadtkasse", "landesjustizkasse", "justizkasse", "bundeskasse", "kreiskasse", "zoll",
                   "hauptzollamt", "steuer", "kfz steuer", "gemeindekasse", "landeshauptkasse", "gebuehrenbescheid",
                   "bussgeld", "ordnungsamt", "landratsamt", "buergeramt", "stadtverwaltung", "gemeinde", "rathaus",
                   "zweckverband", "amtsgericht"),
     "Steuern / Behörden / Gebührenbescheid"),
    ("spende", _rx("spende", "spenden", "unicef", "aerzte ohne grenzen", "drk", "rotes kreuz", "brot fuer die welt",
                   "misereor", "wwf", "greenpeace", "kirche", "kirchensteuer", "patenschaft", "foerderbeitrag"),
     "Spende"),
    ("bildung", _rx("kurs", "seminar", "weiterbildung", "fortbildung", "vhs", "volkshochschule", "udemy", "coursera",
                    "sprachschule", "fahrschule", "universitaet", "hochschule", "semesterbeitrag", "studiengebuehr",
                    "lernen", "schulung"),
     "Kurs / Weiterbildung"),
    ("kredit", _rx("darlehen", "darl leistung", "darlehensleistung", "annuitaet", "tilgung", "rate annuitaet",
                   "kredit", "ratenkredit", "baufinanzierung", "konsumentenkredit", "kreditrate", "darlehensrate",
                   "finanzierung", "santander consumer", "targobank", "creditplus", "easycredit", "bank11",
                   "ratenzahlung", "zins und tilgung"),
     "Kredit-/Darlehensrate"),
    ("online-shopping", _rx("paypal", "klarna", "ebay", "otto gmbh", "otto de", "otto versand", "zalando", "bonprix", "sheego", "shein", "temu",
                            "aliexpress", "wish", "etsy", "onlineshop", "online shop", "shop", "mytoys", "home24",
                            "wayfair", "westwing", "thomann", "galaxus", "digitec", "limango", "baur", "witt",
                            "c a onlineshop", "h m online", "about you", "payone", "mollie", "stripe", "adyen",
                            "shopify"),
     "Online-Händler / Zahlungsdienst"),
    ("mobilitaet", _rx("db vertrieb", "db automat", "db bahn", "deutsche bahn", "bahn", "flixbus", "flixtrain", "dvb",
                       "vvo", "mvg", "bvg", "hvv", "rmv", "vrr", "vrs", "mvv", "vbb", "oepnv", "verkehrsbetriebe",
                       "verkehrsverbund", "fahrkarte", "deutschlandticket", "d ticket", "taxi", "uber", "bolt", "free now",
                       "parken", "parkhaus", "parkschein", "q park", "apcoa", "contipark", "wemolo", "parkservice",
                       "easypark", "parkster", "paybyphone", "peter park", "tanken", "tankstelle",
                       "waschanlage", "fahrrad", "e scooter", "tier", "lime", "voi"),
     "Bahn / ÖPNV / Parken"),
    ("gebuehr", _rx("kontofuehrung", "kontofuehrungsgebuehr", "entgelt", "gebuehr", "gebuehren", "kontogebuehr",
                    "kartengebuehr", "jahresgebuehr", "abrechnung siehe anlage", "entgeltabrechnung", "sollzinsen",
                    "dispozinsen", "ueberziehungszinsen"),
     "Bankgebühr / Sollzinsen"),
)

_IN_PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("gehalt", _rx("lohn", "gehalt", "gehaltszahlung", "lohn gehalt", "bezuege", "besoldung", "verguetung",
                   "entgelt", "hauptkasse", "landeshauptkasse", "bezuegestelle", "landesamt fuer besoldung",
                   "landesamt fuer steuern und finanzen", "bundesverwaltungsamt", "tantieme", "bonus", "praemie",
                   "urlaubsgeld", "weihnachtsgeld", "reisekosten", "spesen"),
     "Gehalt / Bezüge vom Arbeitgeber"),
    ("rente-zuschuss", _rx("kindergeld", "familienkasse", "elterngeld", "erziehungsgeld", "rente", "rentenversicherung",
                           "deutsche rentenversicherung", "drv", "pension", "krankengeld", "ikk", "aok", "tk",
                           "techniker krankenkasse", "barmer", "dak", "kkh", "knappschaft", "krankenkasse",
                           "arbeitslosengeld", "bundesagentur fuer arbeit", "arbeitsagentur", "jobcenter",
                           "wohngeld", "bafoeg", "pflegegeld", "pflegekasse", "sozialamt", "jugendamt",
                           "unterhaltsvorschuss", "mutterschaftsgeld", "energiepreispauschale"),
     "Staatliche Leistung / Kasse"),
    ("steuer", _rx("finanzamt", "steuererstattung", "einkommensteuer", "lohnsteuer", "steuer"),
     "Steuererstattung vom Finanzamt"),
    ("kredit", _rx("darlehen auszahlung", "darlehensauszahlung", "kreditauszahlung", "auszahlung darlehen"),
     "Auszahlung eines Darlehens — kein Einkommen, eine Schuld"),
    ("kapital", _rx("kontoaufloesung", "sparbuch", "sparkonto", "sparguthaben", "bausparguthaben", "lbs ost"),
     "Aufgelöstes Spar-/Bausparguthaben kommt zurück"),
    ("zins-dividende", _rx("zinsen", "habenzinsen", "zinsgutschrift", "dividende", "ausschuettung", "abschluss",
                           "zinsertrag", "kapitalertrag", "ertrag"),
     "Zinsen / Ausschüttung"),
    ("erstattung", _rx("erstattung", "rueckerstattung", "rueckzahlung", "gutschrift", "retoure", "storno",
                       "rueckueberweisung", "refund", "rueckverguetung", "guthaben", "korrektur", "kulanz",
                       "amazon", "amzn", "paypal", "zalando", "otto gmbh", "ebay", "klarna", "bonprix", "sheego",
                       "c a mode", "h m", "ikea", "mediamarkt", "saturn", "deutsche bahn", "db vertrieb", "lidl", "aldi",
                       "rewe", "edeka", "rossmann", "dm", "vodafone", "telekom", "sachsenenergie", "enbw",
                       "stadtwerke", "versicherung", "allianz", "huk"),
     "Rückzahlung / Gutschrift eines Händlers oder Versorgers"),
)

# Buchungstyp der Bank → Kategorie, wenn sonst nichts greift.
_BANK_TYPE_CATEGORY: dict[str, tuple[str, str]] = {
    "gehalt": ("gehalt", "Bank bucht als Lohn/Gehalt"),
    "zinsen": ("zins-dividende", "Bank bucht als Zinsen/Abschluss"),
    "gebuehr": ("gebuehr", "Bank bucht als Entgelt/Gebühr"),
    "bargeld": ("bargeld", "Bank bucht als Bargeldauszahlung"),
}


# Wallet-Namen im Kartenzahlungs-Text („… Apple Pay", „… Google Pay") sagen
# nichts über den Händler und würden sonst jedes Café zum Apple-Abo machen.
_WALLET_RE = re.compile(r"\b(apple pay|google pay|samsung pay|garmin pay)\b")


_DIGIT_EDGE = re.compile(r"(?<=[a-z])(?=\d)|(?<=\d)(?=[a-z])")


def _blob(tx: dict[str, Any]) -> str:
    """Normalised text of the booking, twice: as is, and with letter/digit
    boundaries split („NEWYORKER28083" → „newyorker 28083", „H+M DE0197" →
    „h m de 0197") so shop names glued to a branch number still match."""
    b = _norm(f"{tx.get('counterparty') or ''} {tx.get('purpose') or ''} {tx.get('booking_text') or ''}")
    b = _WALLET_RE.sub(" ", b)
    return b + " | " + _DIGIT_EDGE.sub(" ", b)


def builtin_decision(tx: dict[str, Any]) -> Decision | None:
    """Nur die eingebaute Erkennung (Stufen keyword + bank), ohne Regeln
    und ohne Umbuchungs-Logik. None, wenn nichts greift."""
    try:
        amount = float(tx.get("amount") or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    blob = _blob(tx)
    name = (tx.get("counterparty") or "").strip()

    if amount < 0:
        # Kreditkarte + Bargeld vor den Händlerlisten: „VISA" steht auch in
        # der Kreditkarten-Abrechnung, „Aral" auch im Bargeld-am-Automaten.
        for cat, rx, label in _OUT_PATTERNS[:3]:
            m = rx.search(blob)
            if m:
                return Decision(cat, SOURCE_KEYWORD, f"{label} — Treffer „{m.group(1)}“")
        for cat, words, label in _MERCHANT_GROUPS:
            if _matches(blob, words):
                hit = next((w for w in words if f" {_norm(w).strip()} " in blob), "")
                return Decision(cat, SOURCE_KEYWORD, f"{label} — Händler „{hit}“ in „{name or '—'}“")
        for cat, rx, label in _OUT_PATTERNS[3:]:
            m = rx.search(blob)
            if m:
                return Decision(cat, SOURCE_KEYWORD, f"{label} — Treffer „{m.group(1)}“")
    elif amount > 0:
        for cat, rx, label in _IN_PATTERNS:
            m = rx.search(blob)
            if m:
                return Decision(cat, SOURCE_KEYWORD, f"{label} — Treffer „{m.group(1)}“")

    tt = (tx.get("tx_type") or "").strip()
    if tt in _BANK_TYPE_CATEGORY:
        cat, label = _BANK_TYPE_CATEGORY[tt]
        if cat == "gehalt" and amount <= 0:
            return None
        if tt == "zinsen" and amount < 0:
            return Decision("gebuehr", SOURCE_BANK, "Bank bucht als Sollzinsen/Abschluss (Ausgang)")
        return Decision(cat, SOURCE_BANK, label)
    return None


# Kreditkarten: die Karte selbst bucht den Ausgleich vom Giro als Eingang,
# das Giro bucht die Abrechnung mit der maskierten Kartennummer im Text.
_CARD_SETTLE_IN = re.compile(r"\b(ausgleich kreditkarte|kreditkarteneinzug|rechnungsausgleich|lastschrift kreditkarte|"
                             r"kartenabrechnung|abrechnung kreditkarte)\b")
_CARD_SETTLE_OUT = re.compile(r"\b(kreditkartenabrechnung|kreditkarten abrechnung|visa abr|mastercard abr|kk abrechnung|"
                              r"kreditkarte abrechnung|abrechnung visa|abrechnung mastercard)\b")


def card_settlement_last4(tx: dict[str, Any], own_cards: Iterable[str]) -> str:
    """Last four digits of the own card this booking settles, or ''."""
    cards = {c for c in own_cards if c}
    if not cards:
        return ""
    blob = _blob(tx)
    if not _CARD_SETTLE_OUT.search(blob):
        return ""
    raw = f"{tx.get('purpose') or ''} {tx.get('counterparty') or ''}"
    for last4 in cards:
        if re.search(r"[0-9xX•*]{4,}\s?" + re.escape(last4) + r"\b", raw):
            return last4
    return ""


def is_holder_name(counterparty: str, holder_tokens: Iterable[str]) -> bool:
    """True when the counterparty is nothing but the account holders' own
    names (also broken in two: „ERIKA MUSTER MANN")."""
    toks = sorted({t for t in holder_tokens if t}, key=len, reverse=True)
    if not toks:
        return False
    t = re.sub(r"[^a-zäöüß]", "", (counterparty or "").lower())
    if not t:
        return False
    while t:
        for tok in toks:
            if t.startswith(tok):
                t = t[len(tok):]
                break
        else:
            return False
    return True


def classify(tx: dict[str, Any], *, rules: RuleIndex | None = None,
             own_ibans: Iterable[str] = (), pinned: dict[str, str] | None = None,
             own_cards: Iterable[str] = (), holders: Iterable[str] = ()) -> Decision:
    """Vollständige Entscheidung für eine Buchung.

    `pinned` = {tx_hash → category} der Hand-Zuweisungen, `own_ibans` = die
    IBANs aller eigenen Konten (Karten als "CARD-…"), `own_cards` = letzte
    vier Ziffern der eigenen Kreditkarten, `rules` = gelernte Regeln.
    """
    tx_hash = tx.get("tx_hash") or ""
    try:
        _amt = float(tx.get("amount") or 0.0)
    except (TypeError, ValueError):
        _amt = 0.0
    # Hard rule (Robert: „Ausgaben sind nie Erstattungen!"): a debit can
    # never carry an income category — not by hand, not by rule, not by AI.
    debit = _amt < 0
    if pinned and tx_hash in pinned and (pinned[tx_hash] or "").strip() \
            and not (debit and pinned[tx_hash] in INCOME_CATEGORIES):
        return Decision(pinned[tx_hash], SOURCE_MANUAL, "von Hand für genau diese Buchung gesetzt")

    own = {i.replace(" ", "").upper() for i in own_ibans if i}
    cp_iban = (tx.get("counterparty_iban") or "").replace(" ", "").upper()
    own_iban = (tx.get("account_iban") or "").replace(" ", "").upper()
    if cp_iban and cp_iban in own and cp_iban != own_iban:
        return Decision("uebertrag", SOURCE_TRANSFER,
                        f"Gegenkonto …{cp_iban[-4:]} ist ein eigenes Konto — zählt weder als Einnahme noch als Ausgabe")
    try:
        amt = float(tx.get("amount") or 0.0)
    except (TypeError, ValueError):
        amt = 0.0
    if own_iban.startswith("CARD-") and amt > 0 and _CARD_SETTLE_IN.search(_blob(tx)):
        return Decision("uebertrag", SOURCE_TRANSFER,
                        "Ausgleich der Kartenabrechnung vom Girokonto — die Ausgaben stehen bereits einzeln auf der Karte")
    settled = card_settlement_last4(tx, own_cards)
    if settled and amt < 0:
        return Decision("uebertrag", SOURCE_TRANSFER,
                        f"Abrechnung der eigenen Kreditkarte …{settled} — deren Umsätze sind einzeln importiert, "
                        "die Abrechnung zählt nicht noch einmal")
    # The account holder as payee/payer without any IBAN: money moved to or
    # from an own account whose statements are not imported (a closed
    # Tagesgeld, a Sparbuch, a loan payout). Neither income nor expense.
    self_named = is_holder_name(tx.get("counterparty") or "", holders)
    if self_named and not cp_iban and (tx.get("tx_type") or "") not in ("kartenzahlung", "bargeld"):
        return Decision("uebertrag", SOURCE_TRANSFER,
                        "Empfänger/Zahler ist der Kontoinhaber selbst — Umbuchung auf ein eigenes Konto, "
                        "dessen Auszug nicht vorliegt")

    if rules:
        r = rules.lookup(tx)
        if r and self_named and r.get("match_kind") != "iban":
            r = None   # the holder's own name is not a merchant — only an IBAN rule may decide
        if r and debit and r.get("category") in INCOME_CATEGORIES:
            r = None   # a refund rule never turns a debit into a refund
        if r:
            src = SOURCE_AI if (r.get("source") or "") == "ai" else SOURCE_RULE
            kind = r.get("match_kind")
            how = "IBAN" if kind == "iban" else ("Name + Betrag" if kind == "merchant_amount" else "Name")
            who = (r.get("sample_name") or r.get("match_value") or "").strip()
            origin = "KI-Vorschlag" if src == SOURCE_AI else "aus deiner Zuweisung gelernt"
            return Decision(r["category"], src, f"Regel für Empfänger „{who}“ ({how}) — {origin}")

    d = builtin_decision(tx)
    if d:
        return d
    return Decision("sonstiges", SOURCE_NONE,
                    "kein Muster erkannt — Kategorie zuweisen, DocuSort merkt sie sich für diesen Empfänger")


def amount_rule_value(tx: dict[str, Any]) -> str:
    """match_value einer Empfänger+Betrag-Regel für diese Buchung."""
    key = merchant_key(tx.get("counterparty"))
    try:
        amt = abs(float(tx.get("amount") or 0.0))
    except (TypeError, ValueError):
        amt = 0.0
    return f"{key}|{amt:.2f}" if key else ""


_CARD_TEXT = re.compile(r"\b(DIG\.?\s*KARTE|KARTENZAHLUNG|GIRO\s*E-?COM|E-?COM\s*\(|APPLE PAY|GOOGLE PAY|Debitk\.)", re.I)


def is_card_payment(tx: dict[str, Any]) -> bool:
    """Debit-card / wallet payment at a terminal or online — recognisable
    by tx_type or by the bank's booking text."""
    if (tx.get("tx_type") or "") == "kartenzahlung":
        return True
    return bool(_CARD_TEXT.search(f"{tx.get('booking_text') or ''} {tx.get('purpose') or ''}"))


def rule_targets(tx: dict[str, Any], *, scope: str = "merchant") -> list[tuple[str, str]]:
    """Welche Regel(n) eine Hand-Zuweisung an dieser Buchung erzeugen soll:
    (match_kind, match_value). scope="merchant": IBAN wenn vorhanden UND ein
    Name — die IBAN fängt Namensvarianten, der Name fängt Buchungen ohne IBAN
    (Kartenzahlungen). scope="amount": nur Empfänger + dieser Betrag."""
    out: list[tuple[str, str]] = []
    if scope == "amount":
        v = amount_rule_value(tx)
        return [("merchant_amount", v)] if v else []
    iban = (tx.get("counterparty_iban") or "").replace(" ", "").upper()
    # A card payment carries the IBAN of the payment processor, which dozens
    # of shops share — learning it would tag TK Maxx as a restaurant because
    # the Ziegelwirtschaft uses the same acquirer. Cards learn by name only.
    if iban and len(iban) >= 8 and iban != "0000000000" and not is_card_payment(tx):
        out.append(("iban", iban))
    key = merchant_key(tx.get("counterparty"))
    if key:
        out.append(("merchant", key))
    return out
