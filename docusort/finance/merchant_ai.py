"""KI-gestützte Händler→Topf-Zuordnung für das Monats-Ausgaben-Dashboard.

Die reine Keyword-Liste in `buckets.py` fängt die üblichen Verdächtigen
(REWE, dm, Aral, Obi …). Sie scheitert aber an exotischen oder englischen
Namen — der Nutzer nennt „Soul Food", das eine Suppenbar ist und ins „Essen
gehen" gehört, obwohl der Name das nirgends verrät.

Dieses Modul lässt die **lokale KI** — dieselbe, die ohnehin jedes Dokument
klassifiziert — für einen Stapel unbekannter Händlernamen jeweils einen Topf
bestimmen. Das Ergebnis wird in `merchant_buckets` gecacht, also fällt die
Kosten/Latenz pro Händler genau einmal an; danach ist die Zuordnung eine
reine Dict-Suche in `buckets.classify`.

Bewusst schlank gehalten: ein Batch-Prompt für viele Händler auf einmal
(der lokale Bridge/Ollama-Provider ist langsam pro Call), Rückgabe als
JSON-Array `[{"n": <index>, "bucket": "<id>"}]`, defensiv geparst.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from .buckets import BUCKET_ORDER
from ..providers import Provider, ProviderError

logger = logging.getLogger("docusort.finance.merchant_ai")

# Only these buckets are eligible for a KI decision. „sonstiges" bleibt der
# Rückfall, wenn die KI unsicher ist — das listen wir zwar als erlaubt, aber
# es ist auch der Default bei jeder ungültigen Antwort.
_ALLOWED = set(BUCKET_ORDER)

# Wieviele Händler pro Call. Klein genug, dass der lokale Provider in seinem
# Antwort-Token-Budget bleibt, groß genug, dass ein Monat meist in 1–2 Calls
# durch ist.
BATCH_SIZE = 25

SYSTEM_PROMPT = """Du ordnest Namen von Händlern / Zahlungsempfängern aus einem deutschen Bankkonto jeweils einem Ausgaben-Topf zu. Du bekommst eine nummerierte Liste und antwortest mit GENAU EINEM JSON-Array, ohne Prosa, ohne Markdown-Zäune.

Format der Antwort:
[{"n": 1, "bucket": "essen"}, {"n": 2, "bucket": "auto"}, ...]
- Ein Objekt pro Eingabezeile, mit derselben Nummer "n".
- "bucket" ist GENAU einer dieser Werte:

  lebensmittel  — Supermarkt / Lebensmittel-Einkauf (Aldi, Lidl, Edeka, Rewe, Penny, Bäckerei zum Mitnehmen, Getränkemarkt, Metzgerei, Hofladen, Bio-Laden)
  drogerie      — Drogeriemarkt / Körperpflege (dm, Rossmann, Müller, Budni)
  essen         — Essen & Trinken auswärts: Restaurant, Imbiss, Lieferdienst, Café, Bar, Kantine. AUCH englische/kreative Namen: "Soul Food" (Suppenbar), "Bowl", "Kitchen", "Grill", "Ramen", "Poke", "Coffee", "Deli", "Streetfood" usw.
  auto          — Rund ums Fahrzeug: Tankstelle, Ladesäule, Werkstatt, Reifen, KFZ-Teile, Autowäsche, Parken, Maut, TÜV, Autovermietung, Autohaus / Händler (auch Namen wie "Automobile ...", "Autozentrum ..."), UND Fahrzeug-Finanzierung / Autobanken bzw. Leasing einer Marke: "Hyundai Capital Bank", "VW Bank", "Mercedes-Benz Bank", "BMW Bank", "Toyota Kreditbank", "RCI Banque". Auch ein Markenname allein (Hyundai, Kia, Toyota, Skoda, Renault, Peugeot …) im Zahlungsempfänger meint fast immer das Autohaus/die Finanzierung dieser Marke → auto.
  baumarkt      — Baumarkt / Heimwerker / Bauen / Garten: Obi, Bauhaus, Hornbach, Toom, Baustoffe, Sanitär, Holz, Farben, Gartencenter
  amazon        — alles über Amazon / Amazon-Marktplatz (egal welche Ware)
  kleidung      — Kleidung & Schuhe: Zalando, H&M, C&A, Deichmann, Modeläden, Sportbekleidung
  sonstiges     — alles andere ODER wenn du dir wirklich unsicher bist (Miete, Versicherung, Strom, Telekom, Abos, allgemeine Bank/Kredit, Bargeld, Arzt, Möbel, Elektronik, Reisen, Behörden …). ABER: eine Autobank / Fahrzeugfinanzierung (Name enthält eine Automarke, z. B. "Hyundai Capital Bank") gehört zu auto, NICHT hierher.

Regeln:
- Wähle den TREFFENDSTEN Topf. Nur bei echter Unsicherheit "sonstiges".
- Nutze deine Weltkenntnis über bekannte Marken/Ketten und typische Namensmuster (englische Food-Konzepte, Endungen wie "-grill", "-kitchen", "-bar").
- Der Verwendungszweck in Klammern ist ein Hinweis, oft aber Rauschen (Kartennummer, Datum). Verlass dich primär auf den Namen.
- Antworte NUR mit dem JSON-Array."""


def _parse_array(raw: str) -> list[dict[str, Any]]:
    """Erstes gültiges JSON-Array aus der Modell-Antwort ziehen."""
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].lstrip()
    decoder = json.JSONDecoder()
    for i, ch in enumerate(raw):
        if ch != "[":
            continue
        try:
            obj, _ = decoder.raw_decode(raw[i:])
            if isinstance(obj, list):
                return obj
        except json.JSONDecodeError:
            continue
    raise ValueError(f"Keine JSON-Liste in der Modell-Antwort: {raw[:200]!r}")


def _build_user_message(batch: list[dict[str, str]]) -> str:
    lines = ["Ordne jeden dieser Händler einem Topf zu:", ""]
    for idx, m in enumerate(batch, start=1):
        name = m.get("name") or "—"
        sample = (m.get("sample") or "").strip()
        if sample and sample.lower() != name.lower():
            lines.append(f"{idx}. {name}  ({sample[:80]})")
        else:
            lines.append(f"{idx}. {name}")
    lines.append("")
    lines.append("Gib jetzt das JSON-Array zurück.")
    return "\n".join(lines)


def classify_batch(provider: Provider, model: str,
                   batch: list[dict[str, str]],
                   *, max_output_tokens: int = 1200) -> dict[int, str]:
    """Einen Stapel Händler klassifizieren.

    `batch` ist eine Liste von {"name": <Händlername>, "sample": <Beispiel-
    Verwendungszweck>}. Rückgabe: {index_in_batch (0-basiert) → bucket_id}
    nur für Zeilen mit gültigem Topf. Fehlt eine Zeile oder ist der Topf
    ungültig, taucht der Index nicht auf (Aufrufer behandelt das als
    „sonstiges"/übersprungen)."""
    if not batch:
        return {}
    user = _build_user_message(batch)
    resp = provider.classify(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user,
        model=model,
        max_output_tokens=max_output_tokens,
    )
    arr = _parse_array(resp.raw_text)
    out: dict[int, str] = {}
    for item in arr:
        if not isinstance(item, dict):
            continue
        try:
            n = int(item.get("n"))
        except (TypeError, ValueError):
            continue
        bucket = str(item.get("bucket", "") or "").strip().lower()
        pos = n - 1  # prompt is 1-basiert
        if 0 <= pos < len(batch) and bucket in _ALLOWED:
            out[pos] = bucket
    return out


def classify_merchants(provider: Provider, model: str,
                       merchants: list[dict[str, str]],
                       *, progress=None) -> dict[str, str]:
    """Viele Händler in Batches klassifizieren.

    `merchants`: Liste von {"key": <norm. Schlüssel>, "name": <Anzeigename>,
    "sample": <Beispielzweck>}. Rückgabe: {key → bucket_id}, nur für Händler,
    denen die KI einen konkreten (Nicht-„sonstiges") Topf zugewiesen hat —
    „sonstiges" muss nicht gecacht werden, das ist ohnehin der Default.

    `progress(done, total)` wird nach jedem Batch aufgerufen, falls gesetzt.
    Provider-Fehler in einem Batch werden geloggt und übersprungen, damit ein
    einzelner Ausfall nicht den ganzen Lauf killt."""
    result: dict[str, str] = {}
    total = len(merchants)
    done = 0
    for start in range(0, total, BATCH_SIZE):
        batch = merchants[start:start + BATCH_SIZE]
        try:
            mapping = classify_batch(provider, model, batch)
        except (ProviderError, ValueError) as exc:
            logger.warning("merchant_ai batch %d failed: %s", start // BATCH_SIZE, exc)
            mapping = {}
        for pos, bucket in mapping.items():
            if bucket == "sonstiges":
                continue
            key = batch[pos].get("key") or ""
            if key:
                result[key] = bucket
        done += len(batch)
        if progress:
            try:
                progress(done, total)
            except Exception:  # noqa: BLE001
                pass
    return result


# ---------------------------------------------------------------------------
# Zweiter Einsatz derselben Maschine: Empfänger → Buchungs-KATEGORIE
# (v0.43, „Sonstiges aufdröseln"). Gleiches Batch-/JSON-Protokoll, andere
# Zielmenge — die 27 Kategorien aus finance.categories statt der 8 Töpfe.
# Das Ergebnis wird als Regel (`category_rules`, source='ai') gespeichert;
# eine Hand-Zuweisung von Hand überschreibt sie jederzeit.
# ---------------------------------------------------------------------------

from .categories import TX_CATEGORIES as _TX_CATEGORIES  # noqa: E402

_CATEGORY_HELP = """  miete             — Miete, Wohnungsgesellschaft, Hausverwaltung, Hausgeld
  nebenkosten       — Strom, Gas, Wasser, Müll, Telefon/Internet, Rundfunkbeitrag
  lebensmittel      — Supermarkt, Bäcker, Metzger, Getränkemarkt
  essen-ausser-haus — Restaurant, Imbiss, Kantine, Café, Lieferdienst
  mobilitaet        — Tankstelle, Bahn, ÖPNV, Parken, Werkstatt, Autofinanzierung/Leasing
  versicherung      — jede Versicherung (Leben, KFZ, Haftpflicht, Hausrat …)
  abonnement        — Streaming, Software, Fitnessstudio, Vereinsbeitrag, Hosting, Spiele-Apps
  gesundheit        — Arzt, Apotheke, Klinik, Optiker, Physio
  freizeit          — Kino, Kultur, Sport, Bücher, Hobby
  bekleidung        — Kleidung, Schuhe
  elektronik        — Elektronik-Händler
  haushalt          — Drogerie, Möbel, Baumarkt, Haushaltswaren
  online-shopping   — Amazon, PayPal, eBay, Klarna, Otto … (Ware unbekannt)
  kinder            — Kita, Schule, Hort, Taschengeld, Spielzeug, Kinder-Vereine
  reisen            — Hotel, Flug, Pauschalreise, Ferienwohnung
  bildung           — Kurse, Weiterbildung, Fahrschule
  spende            — Spenden, Kirche, Patenschaften
  gehalt            — Lohn/Gehalt/Bezüge vom Arbeitgeber (Eingang)
  rente-zuschuss    — Rente, Kindergeld, Elterngeld, Krankengeld, Arbeitsagentur (Eingang)
  erstattung        — Rückzahlung/Gutschrift eines Händlers oder Versorgers (Eingang)
  zins-dividende    — Zinsen, Dividenden (Eingang)
  bargeld           — Geldautomat
  gebuehr           — Bankgebühren, Sollzinsen
  steuer            — Finanzamt, Stadtkasse, Behörden-Gebühren, Bußgeld
  kapital           — Sparplan, Depot, Bausparen, Krypto
  kreditkarte       — Kreditkarten-Abrechnung
  kredit            — Darlehens-/Kreditrate, Tilgung
  sonstiges         — wirklich unklar (z. B. Überweisung an eine Privatperson)"""

CATEGORY_SYSTEM_PROMPT = """Du ordnest Empfänger/Absender von Buchungen eines deutschen Bankkontos jeweils EINER Kategorie zu. Du bekommst eine nummerierte Liste (Name, Beispiel-Verwendungszweck, Richtung „Ausgang" oder „Eingang", typischer Betrag) und antwortest mit GENAU EINEM JSON-Array, ohne Prosa, ohne Markdown-Zäune.

Format: [{"n": 1, "category": "miete"}, {"n": 2, "category": "versicherung"}, ...]
- Ein Objekt pro Eingabezeile, mit derselben Nummer "n".
- "category" ist GENAU einer dieser Schlüssel:

""" + _CATEGORY_HELP + """

Regeln:
- Wähle die TREFFENDSTE Kategorie; nutze dein Wissen über deutsche Firmen, Behörden und Kassen (z. B. „Hauptkasse des Freistaates Sachsen" = Gehalt/Bezüge, „Familienkasse" = rente-zuschuss, „Wohnbau …" = miete, „… Lebensversicherung" = versicherung, „Hyundai Capital Bank" = mobilitaet).
- Eingänge bekommen Eingangs-Kategorien (gehalt, rente-zuschuss, erstattung, zins-dividende, steuer).
- Überweisungen an oder von Privatpersonen (Vor- und Nachname) → sonstiges, außer der Verwendungszweck sagt klar etwas (z. B. „Taschengeld" → kinder, „Miete" → miete).
- Antworte NUR mit dem JSON-Array."""

_ALLOWED_CATEGORIES = set(_TX_CATEGORIES)


def _build_category_message(batch: list[dict[str, Any]]) -> str:
    lines = ["Ordne jeden dieser Empfänger einer Kategorie zu:", ""]
    for idx, m in enumerate(batch, start=1):
        name = m.get("name") or "—"
        sample = (m.get("sample") or "").strip()[:80]
        direction = "Eingang" if float(m.get("sum_in") or 0) > float(m.get("sum_out") or 0) else "Ausgang"
        typ = float(m.get("total") or 0) / max(1, int(m.get("count") or 1))
        extra = f"[{direction}, ~{typ:.0f} €, {m.get('count') or 1}×]"
        lines.append(f"{idx}. {name}  ({sample}) {extra}" if sample else f"{idx}. {name} {extra}")
    lines.append("")
    lines.append("Gib jetzt das JSON-Array zurück.")
    return "\n".join(lines)


def classify_categories_batch(provider: Provider, model: str, batch: list[dict[str, Any]],
                              *, max_output_tokens: int = 1500) -> dict[int, str]:
    if not batch:
        return {}
    resp = provider.classify(
        system_prompt=CATEGORY_SYSTEM_PROMPT,
        user_prompt=_build_category_message(batch),
        model=model,
        max_output_tokens=max_output_tokens,
    )
    arr = _parse_array(resp.raw_text)
    out: dict[int, str] = {}
    for item in arr:
        if not isinstance(item, dict):
            continue
        try:
            n = int(item.get("n"))
        except (TypeError, ValueError):
            continue
        cat = str(item.get("category", "") or "").strip().lower()
        pos = n - 1
        if 0 <= pos < len(batch) and cat in _ALLOWED_CATEGORIES:
            out[pos] = cat
    return out


def classify_categories(provider: Provider, model: str, merchants: list[dict[str, Any]],
                        *, progress=None) -> dict[str, str]:
    """{merchant key → category} für alle Empfänger, denen die KI eine
    konkrete (Nicht-„sonstiges") Kategorie gibt. Fehler je Batch werden
    geloggt und übersprungen."""
    result: dict[str, str] = {}
    total = len(merchants)
    done = 0
    for start in range(0, total, BATCH_SIZE):
        batch = merchants[start:start + BATCH_SIZE]
        try:
            mapping = classify_categories_batch(provider, model, batch)
        except (ProviderError, ValueError) as exc:
            logger.warning("merchant_ai category batch %d failed: %s", start // BATCH_SIZE, exc)
            mapping = {}
        for pos, cat in mapping.items():
            if cat == "sonstiges":
                continue
            key = batch[pos].get("key") or ""
            if key:
                result[key] = cat
        done += len(batch)
        if progress:
            try:
                progress(done, total)
            except Exception:  # noqa: BLE001
                pass
    return result
