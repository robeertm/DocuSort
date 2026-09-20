"""Kontoauszug-PDF → Buchungen (Sparkasse), ohne LLM.

Ein Sparkassen-Kontoauszug ist eine Tabelle: Kopfzeile je Buchung
(Datum [Wertstellung] Buchungsart), darunter Text (Empfänger + Verwendungs-
zweck), darunter der Betrag allein in einer Zeile. Davor „Kontostand am …,
Auszug Nr. N", dahinter „Kontostand am … um … Uhr". Der Text kommt aus
`ocr.extract_text` (pypdf) — derselbe Text, den die Pipeline ohnehin für
die Klassifizierung liest.

Die Kontostände sind die Wahrheit: Anfangssaldo + Σ Buchungen muss den
Endsaldo ergeben. Stimmt das nicht, wird der Auszug NICHT importiert,
sondern gemeldet — lieber eine Lücke als eine falsche Zahl.

Zwei Layouts:
  neu (ab ~2024)  ``01.08.2025 Lastschrift`` … ``-249,64``
  alt             ``02.06.202002.06.2020Lastschrift`` … ``86,41-``
                  (Buchungs- und Wertstellungsdatum ohne Trenner, Vorzeichen
                  hinten, Text zweispaltig mit 35-Zeichen-Feldern)
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from .csv_import import ImportRow
from .dates import normalise_date, normalise_iban

logger = logging.getLogger("docusort.finance.pdf")

_DATE = r"\d{2}\.\d{2}\.\d{4}"
_AMT = r"-?\d{1,3}(?:\.\d{3})*,\d{2}"

_RE_STMT_NO = re.compile(r"Kontoauszug\s+(\d{1,2})\s*/\s*(\d{4})")
_RE_ACCOUNT = re.compile(
    r"^\s*([A-Za-zÄÖÜäöü][A-Za-zÄÖÜäöü ]*konto)\s+(\d{6,12}),\s*"
    r"((?:DE|AT|CH)\d{2}(?:\s?[0-9A-Z]{4}){3,6}\s?[0-9A-Z]{0,4})",
    re.M,
)
_RE_OPENING = re.compile(
    rf"Kontostand am ({_DATE}),\s*Auszug Nr\.\s*(\d+)\s+({_AMT})([+-])?"
)
_RE_CLOSING = re.compile(
    rf"Kontostand am ({_DATE}) um (\d{{1,2}}:\d{{2}}) Uhr\s+({_AMT})([+-])?"
)
_RE_TABLE_HEAD = re.compile(r"^\s*Datum\s+(?:Wert\s+)?Erläuterung\s+Betrag")
_RE_AMOUNT_LINE = re.compile(rf"^\s*({_AMT})([+-])?\s*$")
_RE_HEADER = re.compile(
    rf"^\s*({_DATE})\s?({_DATE})?\s*([A-Za-zÄÖÜäöüß][^\n]*?)"
    rf"(?:\s*/\s*Wert:\s*({_DATE}))?\s*$"
)
_RE_ABRECHNUNG = re.compile(rf"^Abrechnung\s+{_DATE}$")
_RE_IBAN_IN_TEXT = re.compile(r"\b((?:DE|AT|CH|NL|FR|IT|ES|LU|IE|BE|GB|PL|CZ)\d{2}(?:\s?[0-9A-Z]{4}){2,7}\s?[0-9A-Z]{0,4})")
_RE_CARD_PLACE = re.compile(r"^(.*?//[^/]+/[A-Z]{2})(?:/\S*)?\b")
_RE_HOLDERS = re.compile(r"^\s*Herrn(?: und Frau)?|^\s*Frau(?: und Herrn)?", re.M)

# Buchungsarten, wie die Sparkasse sie druckt. Eine Kopfzeile mit anderem Text
# wird nur als Buchung genommen, wenn gerade keine Buchung offen ist — sonst
# ist es eine Verwendungszweck-Zeile, die zufällig mit einem Datum beginnt.
_KNOWN_TYPES = {
    "lastschrift", "kartenzahlung", "digitale karte", "gutschrift", "überweisung online",
    "ausf. dauerauftrag", "ga-verfügungen", "ga-verfügung fremd", "geldautomat",
    "sonst. gutschrift", "sonst. buchung", "sonstige buchung", "gehalt/renten",
    "zahlungseingang", "entgeltabrechnung", "entgeltfreie buchung", "buchung entgeltfrei",
    "gebf.buchung soll", "überw. echtz. online", "zahlg.eing.sonst.", "buchung darlehen",
    "echtzeitüberweisung", "bareinzahlung sb", "bargeldeinz. sb", "kartenbel.-entgelt",
    "giro e-com- apple pay", "laden prepaid", "sonstige entgelte", "überw./umb. echtz.",
    "überweisung m. b.", "scheck e.v.", "rechnung", "lastschr. sparen", "gutschr. institut",
    "berichtigung", "dauerauftrag", "überweisung", "umbuchung", "storno", "rücklastschrift",
    "abschluss", "zinsen/abschluss", "kartenzahlung ausland", "bargeldauszahlung",
}

# Sparkassen-Buchungsart → DocuSort tx_type (wie beim CSV-Import) und ein
# Buchungstext, den `classify.is_card_payment` und die Muster wiedererkennen.
_TYPE_MAP: tuple[tuple[str, str, str], ...] = (
    ("digitale karte", "kartenzahlung", "DIG. KARTE (APPLE PAY)"),
    ("giro e-com", "kartenzahlung", "GIRO E-COM (APPLE PAY)"),
    ("kartenzahlung", "kartenzahlung", "KARTENZAHLUNG"),
    ("kartenbel", "gebuehr", "KARTENBELASTUNGSENTGELT"),
    ("ga-verfüg", "bargeld", "BARGELDAUSZAHLUNG"),
    ("geldautomat", "bargeld", "BARGELDAUSZAHLUNG"),
    ("bargeldausz", "bargeld", "BARGELDAUSZAHLUNG"),
    ("bareinzahlung", "ueberweisung", "BARGELDEINZAHLUNG"),
    ("bargeldeinz", "ueberweisung", "BARGELDEINZAHLUNG"),
    ("laden prepaid", "kartenzahlung", "LADEN PREPAID"),
    ("entgeltfrei", "ueberweisung", "UEBERWEISUNG"),
    ("lastschr", "lastschrift", "FOLGELASTSCHRIFT"),
    ("dauerauftrag", "dauerauftrag", "DAUERAUFTRAG"),
    ("gehalt", "gehalt", "LOHN GEHALT"),
    ("entgelt", "gebuehr", "ENTGELTABSCHLUSS"),
    ("abrechnung", "zinsen", "ABSCHLUSS"),
    ("abschluss", "zinsen", "ABSCHLUSS"),
    ("zinsen", "zinsen", "ABSCHLUSS"),
    ("darlehen", "sonstiges", "DARLEHEN"),
    ("gutschrift", "ueberweisung", "GUTSCHRIFT UEBERWEISUNG"),
    ("zahlungseingang", "ueberweisung", "GUTSCHRIFT UEBERWEISUNG"),
    ("zahlg.eing", "ueberweisung", "GUTSCHRIFT UEBERWEISUNG"),
    ("überw", "ueberweisung", "ONLINE-UEBERWEISUNG"),
    ("echtzeit", "ueberweisung", "ECHTZEITUEBERWEISUNG"),
    ("umb", "ueberweisung", "UMBUCHUNG"),
    ("scheck", "sonstiges", "SCHECK"),
    ("berichtigung", "sonstiges", "BERICHTIGUNG"),
    ("storno", "sonstiges", "STORNO"),
    ("rechnung", "gebuehr", "RECHNUNG"),
)

# Seitenmöbel, die pypdf zwischen die Buchungen streut (Fußzeile, Kopf der
# Folgeseite, Strichcode). Alles, was innerhalb einer offenen Buchung steht
# und hierauf passt, ist kein Verwendungszweck.
_FURNITURE = re.compile(
    r"^\s*(?:S\s+)?Ostsächsische Sparkasse(?:\s+Musterstadt)?\s*$|^\s*Musterstadt\s*$|^\s*Güntzplatz|^\s*01307 Musterstadt|"
    r"^\s*Anstalt des öffentlichen Rechts|^\s*Sparkassen-Finanzgruppe|^\s*Vorstand:|^\s*HR Nr\.|^\s*USt-IdNr|"
    r"^\s*Telefon 0351|^\s*Fax\b|^\s*www\.|^\s*e-mail@|^\s*SWIFT-Adresse|^\s*BLZ:|^\s*\*\d{10,}\*\s*$|^\s*\d{16,}\s*$|"
    r"^\s*\d+\.\d+\.\d+\.\d+\.\d+\.\d+\s*$|^\s*Seite \d+ von \d+|^\s*Kontoauszug \d{1,2}/\d{4}\s*$|"
    r"^\s*[A-Za-zÄÖÜäöü ]*konto \d{6,12}, (?:DE|AT|CH)\d{2}[0-9A-Z ]+,?\s*$|^\s*Datum\s+(?:Wert\s+)?Erläuterung|"
    r"^\s*\d{3} \d{2} \d{2} \d{2}\s*$|^\s*\.\s*$|^\s*©|^\s*\d{3} \d{3}\.\d{3} VF|^\s*(?:Ulrich Franzen|Heiko Lachmann|Joachim Hoof|Petra von Crailsheim)\s*$|"
    r"^\s*Herrn(?: und Frau)?\s*$|^\s*Frau(?: und Herrn)?\s*$|^\s*Zeppelinstr|^\s*04910 Musterstadt|^\s*\d{1,2}\. [A-ZÄÖÜ][a-zäöü]+ \d{4}\s*$"
)


@dataclass
class ParsedStatement:
    bank: str = "Sparkasse"
    bank_name: str = ""             # „Ostsächsische Sparkasse Musterstadt" (Absender in der Bibliothek)
    account_kind: str = ""          # „Privatgirokonto", „Tagesgeldkonto", …
    account_no: str = ""
    account_iban: str = ""
    holder: str = ""                # „Max Mustermann Erika Mustermann"
    statement_no: str = ""          # „8/2025"
    opening_date: str = ""          # ISO
    opening_balance: float | None = None
    closing_date: str = ""
    closing_balance: float | None = None
    rows: list[ImportRow] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def total(self) -> float:
        return round(sum(r.amount for r in self.rows), 2)

    @property
    def balanced(self) -> bool | None:
        """True/False wenn beide Salden bekannt sind, sonst None."""
        if self.opening_balance is None or self.closing_balance is None:
            return None
        return abs(round(self.opening_balance + self.total, 2) - self.closing_balance) < 0.005

    @property
    def period_start(self) -> str:
        return min((r.booking_date for r in self.rows), default=self.opening_date)

    @property
    def period_end(self) -> str:
        return max((r.booking_date for r in self.rows), default=self.closing_date)

    @property
    def is_savings(self) -> bool:
        k = self.account_kind.lower()
        return "tagesgeld" in k or "spar" in k or "zinsaktiv" in k


def looks_like_statement(text: str) -> bool:
    """Billige Vorprüfung, bevor der Parser läuft."""
    if not text or "Kontoauszug" not in text:
        return False
    return bool(_RE_ACCOUNT.search(text) and _RE_OPENING.search(text))


def _amount(num: str, sign: str | None) -> float:
    v = float(num.replace(".", "").replace(",", "."))
    if sign == "-":
        v = -abs(v)
    return round(v, 2)


def _squeeze(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _map_type(kind: str) -> tuple[str, str]:
    k = kind.lower()
    for needle, tx_type, text in _TYPE_MAP:
        if needle in k:
            return tx_type, text
    return "sonstiges", kind.upper()


class _Known:
    """Bekannte Empfängernamen, nach dem ersten normalisierten Wort
    indiziert, damit je Buchung nur wenige Kandidaten geprüft werden."""

    def __init__(self, names: tuple[str, ...]):
        from .buckets import _norm
        self._norm = _norm
        self.by_first: dict[str, list[tuple[str, str]]] = {}
        for n in names:
            nn = _norm(n).strip()
            if len(nn) < 5:
                continue
            first = nn.split(" ", 1)[0]
            self.by_first.setdefault(first, []).append((nn, n))
        for lst in self.by_first.values():
            lst.sort(key=lambda t: len(t[0]), reverse=True)

    def match(self, flat: str) -> tuple[str, str] | None:
        """(Name in bekannter Schreibweise, Rest) — wenn der Text mit einem
        bekannten Namen beginnt, oder sein 35-Zeichen-Kopf (die Bank kürzt
        den Namen im Auszug) der Anfang eines bekannten Namens ist."""
        fn = self._norm(flat).strip()
        if not fn:
            return None
        cands = self.by_first.get(fn.split(" ", 1)[0]) or []
        if not cands:
            return None
        # Wortgrenzen im Original, längste zuerst (max. 10 Wörter)
        cuts = [m.start() for m in re.finditer(r" ", flat)][:10] + [len(flat)]
        heads = [(flat[:c], self._norm(flat[:c]).strip()) for c in sorted(set(cuts), reverse=True)]
        # gekürzter Kopf (35 Zeichen): die Bank schneidet lange Namen ab
        hn35 = ""
        if len(flat) > 36 and flat[35] == " " and flat[34] != " ":
            hn35 = self._norm(flat[:35]).strip()
            if len(hn35) < 12:
                hn35 = ""
        # längster bekannter Name zuerst — „Wohnbau Musterstadt Kommunale
        # Wohnungsbaugesellschaft" schlägt „WOHNBAU MUSTERSTADT"
        for nn, original in cands:
            for head, hn in heads:
                if hn == nn:
                    return original, flat[len(head):].strip()
            if hn35 and len(nn) > len(hn35) and nn.startswith(hn35):
                return original, flat[36:].strip()
        return None


def _split_name(detail_lines: list[str], kind: str, known: "_Known | tuple[str, ...]") -> tuple[str, str, str]:
    """(Empfänger, Verwendungszweck, Gegen-IBAN) aus dem Buchungstext.

    Die Sparkasse druckt den Namen (max. 35 Zeichen) und dahinter den
    Verwendungszweck in einem Fluss. Reihenfolge der Erkennung: alte
    zweispaltige Form (Spaltenlücke), bekannter Name aus früheren Importen
    (auch wenn der Auszug ihn auf 35 Zeichen kürzt), Kartenterminal
    („…//Stadt/DE"), Name doppelt („SVS Allgemeine Vers. AG SVS Allgemeine
    Vers. AG Kfz-Vers."), exakt 35 Zeichen (abgeschnittener Name), sonst bis
    zum ersten Wort mit Ziffer bzw. „DATUM"/„BIC".
    """
    if not detail_lines:
        return "", "", ""
    if not isinstance(known, _Known):
        known = _Known(tuple(known))
    first = detail_lines[0].rstrip()
    rest_lines = detail_lines[1:]
    iban = ""
    blob = " ".join(_squeeze(x) for x in detail_lines)
    m = re.search(r"BIC\s*/\s*IBAN:\s*[A-Z0-9]{8,11}\s+(" + _RE_IBAN_IN_TEXT.pattern[3:-1] + ")", blob)
    if m:
        iban = normalise_iban(m.group(1))
    else:
        m = _RE_IBAN_IN_TEXT.search(blob)
        if m and "IBAN" in blob.upper():
            iban = normalise_iban(m.group(1))

    flat = _squeeze(blob)
    # Entgelte: „siehe Anlage Nr. 1" — die Bank selbst ist der Empfänger
    if flat.lower().startswith("siehe anlage"):
        return "Ostsächsische Sparkasse Musterstadt", f"{kind} {flat}".strip(), iban

    # 1) alte Form: Spaltenlücke (≥ 3 Leerzeichen) in der ersten Zeile
    m = re.match(r"^(.{3,40}?)\s{3,}(\S.*)$", first)
    if m and not first.startswith("DANKE"):
        name = m.group(1).strip()
        first_rest = m.group(2)
        purpose = _squeeze(" ".join([first_rest, *rest_lines]))
        hit = known.match(name)
        if hit and not hit[1]:
            name = hit[0]
        return name, purpose, iban

    # 2) Kartenterminal: Name bis „//Stadt/DE" — so steht er auch im CSV
    kl = kind.lower()
    if "//" in flat and ("karte" in kl or "e-com" in kl or "ga-verf" in kl or "geldautomat" in kl or "prepaid" in kl):
        m = _RE_CARD_PLACE.match(flat)
        if m:
            return m.group(1).strip(), flat[m.end():].strip(), iban
    # 3) bekannter Name (in der Schreibweise des CSV-Exports)
    hit = known.match(flat)
    if hit:
        return hit[0], hit[1], iban
    # 4) Name doppelt
    words = flat.split(" ")
    for n in range(min(8, len(words) // 2), 1, -1):
        if words[:n] == words[n:2 * n]:
            return " ".join(words[:n]), " ".join(words[n:]).strip(), iban
    # 5) exakt 35 Zeichen, gefolgt von Leerzeichen → abgeschnittener Name
    if len(flat) > 36 and flat[35] == " " and flat[34] != " " and re.search(r"[A-Za-z]", flat[:35]):
        head = flat[:35]
        if not re.search(r"\d{4,}", head):
            return head.strip(), flat[36:].strip(), iban
    # 6) bis zum ersten Wort mit Ziffer / Schlüsselwort
    stop = re.compile(r"\s(?=\S*\d|DATUM\b|BIC\b|IBAN\b|Gläubiger-ID|ELV\d|Debitk\.)")
    m = stop.search(flat)
    if m and m.start() >= 3:
        return flat[:m.start()].strip(" ,"), flat[m.start():].strip(), iban
    return flat[:35].strip(), flat[35:].strip(), iban


def parse_statement_text(text: str, *, known_names: tuple[str, ...] = ()) -> ParsedStatement | None:
    """Parst den Text eines Sparkassen-Kontoauszugs. None, wenn es keiner ist."""
    if not looks_like_statement(text):
        return None
    st = ParsedStatement()
    m = _RE_ACCOUNT.search(text)
    if m:
        st.account_kind = _squeeze(m.group(1)).replace(" ", "") if len(m.group(1).split()) > 1 and len(m.group(1).split()[0]) == 1 else _squeeze(m.group(1))
        st.account_no = m.group(2)
        st.account_iban = normalise_iban(m.group(3))
    m = _RE_STMT_NO.search(text)
    if m:
        st.statement_no = f"{int(m.group(1))}/{m.group(2)}"
    # Bank as it names itself: the most frequent line „… Sparkasse …" (the
    # footer repeats it on every page), longest wins on a tie; „ae" → „ä".
    names: dict[str, int] = {}
    for bm in re.finditer(r"^\s*(?:S\s+)?([A-ZÄÖÜ][A-Za-zÄÖÜäöü\-]* )?Sparkasse( [A-ZÄÖÜ][A-Za-zÄÖÜäöü\-]+)?\s*$", text, re.M):
        n = re.sub(r"\s+", " ", bm.group(0).strip()).removeprefix("S ").strip()
        n = n.replace("Ostsaechsische", "Ostsächsische")
        names[n] = names.get(n, 0) + 1
    if names:
        st.bank_name = max(names, key=lambda k: (names[k], len(k)))
    m = _RE_OPENING.search(text)
    if m:
        st.opening_date = normalise_date(m.group(1))
        st.opening_balance = _amount(m.group(3), m.group(4))
    m = _RE_CLOSING.search(text)
    if m:
        st.closing_date = normalise_date(m.group(1))
        st.closing_balance = _amount(m.group(3), m.group(4))
    # Kontoinhaber: die Zeilen zwischen „Herrn …" und der Straße
    hm = _RE_HOLDERS.search(text)
    if hm:
        names = []
        for ln in text[hm.end():].splitlines()[1:5]:
            ln = ln.strip()
            if not ln or re.search(r"\d", ln):
                break
            names.append(ln)
        st.holder = " ".join(names)

    known = _Known(tuple(k for k in known_names if k))
    lines = text.splitlines()
    in_table = False
    cur: dict | None = None
    seen_closing = False

    def _finish(amount: float) -> None:
        nonlocal cur
        if cur is None:
            return
        kind = cur["kind"]
        tx_type, booking_text = _map_type(kind)
        name, purpose, iban = _split_name(cur["detail"], kind, known)
        st.rows.append(ImportRow(
            account_iban=st.account_iban,
            booking_date=cur["date"],
            value_date=cur["value"] or cur["date"],
            booking_text=booking_text,
            purpose=purpose,
            amount=amount,
            currency="EUR",
            counterparty=name,
            counterparty_iban=iban,
            sammlerreferenz="",
            mandatsreferenz="",
            glaeubiger_id="",
            info=f"Kontoauszug {st.statement_no}".strip(),
            tx_type=tx_type,
        ))
        cur = None

    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            continue
        if _RE_TABLE_HEAD.match(line):
            in_table = True
            continue
        if not in_table:
            continue
        if _RE_CLOSING.search(line):
            seen_closing = True
            if cur is not None:
                st.warnings.append(f"Buchung ohne Betrag vor dem Endsaldo: {cur['date']} {cur['kind']}")
                cur = None
            continue
        if seen_closing:
            # Danach folgen nur noch Entgeltaufstellung und Hinweise.
            continue
        if _RE_OPENING.search(line):
            continue
        am = _RE_AMOUNT_LINE.match(line)
        if am and cur is not None:
            _finish(_amount(am.group(1), am.group(2)))
            continue
        hm2 = _RE_HEADER.match(line)
        if hm2:
            kind = _squeeze(hm2.group(3))
            kind_key = kind.lower()
            is_known = kind_key in _KNOWN_TYPES or bool(_RE_ABRECHNUNG.match(kind)) or (
                not re.search(r"\d", kind) and len(kind) <= 30 and cur is None
            )
            if is_known and not (cur is not None and kind_key not in _KNOWN_TYPES and not _RE_ABRECHNUNG.match(kind)):
                if cur is not None:
                    st.warnings.append(f"Buchung ohne Betrag: {cur['date']} {cur['kind']}")
                cur = {
                    "date": normalise_date(hm2.group(1)),
                    "value": normalise_date(hm2.group(4) or hm2.group(2) or ""),
                    "kind": kind,
                    "detail": [],
                }
                continue
        if cur is not None:
            if _FURNITURE.match(line):
                continue
            cur["detail"].append(line)
    if cur is not None:
        st.warnings.append(f"Buchung ohne Betrag am Ende: {cur['date']} {cur['kind']}")
    if st.balanced is False:
        st.warnings.append(
            f"Salden passen nicht: {st.opening_balance:.2f} + {st.total:.2f} ≠ {st.closing_balance:.2f} "
            f"(Differenz {round(st.opening_balance + st.total - st.closing_balance, 2):+.2f})"
        )
    return st


# ---------------------------------------------------------------------------
# Import: Text → Buchungen in der Datenbank
# ---------------------------------------------------------------------------

def known_counterparties(db, limit: int = 4000) -> tuple[str, ...]:
    """Empfängernamen, die DocuSort schon kennt (aus CSV-Importen und
    Hand-Zuweisungen) — damit der Auszug dieselben Namen erhält wie der
    CSV-Export und gelernte Regeln greifen."""
    try:
        with db._lock:
            rows = db._conn.execute(
                "SELECT counterparty, COUNT(*) n FROM transactions "
                "WHERE counterparty IS NOT NULL AND LENGTH(counterparty) >= 5 AND COALESCE(synthetic, 0) = 0 "
                "GROUP BY counterparty ORDER BY n DESC LIMIT ?", (limit,)
            ).fetchall()
    except Exception:  # noqa: BLE001
        return ()
    names = []
    for r in rows:
        n = re.sub(r"\s{2,}.*$", "", str(r["counterparty"]).strip())   # alter Export: Adresse nach Leerraum
        if len(n) >= 5 and not re.search(r"^\d", n):
            names.append(n)
    return tuple(dict.fromkeys(names))


def import_statement_text(db, text: str, *, doc_id: int, file_hash: str, file_label: str = "",
                          known_names: tuple[str, ...] | None = None, finalize: bool = True):
    """Einen Kontoauszug (Text) in die Finanzen übernehmen.

    Nur ein Auszug, dessen Salden aufgehen, wird importiert. Ein Auszug, der
    schon drin ist (gleiche Datei oder gleicher Zeitraum desselben Kontos),
    wird übersprungen. Rückgabe: ImportReport wie beim CSV-Import."""
    from .csv_import import ImportReport, import_rows
    rep = ImportReport(file_label=file_label, bank="Sparkasse")
    st = parse_statement_text(text, known_names=known_names if known_names is not None else known_counterparties(db))
    if st is None:
        rep.errors.append("kein Kontoauszug erkannt")
        return rep, None
    rep.rows_seen = len(st.rows)
    if not st.account_iban:
        rep.errors.append("Kontoauszug ohne IBAN")
        rep.statements_skipped += 1
        return rep, st
    if st.balanced is not True:
        rep.errors.append("Salden gehen nicht auf — Auszug nicht importiert: " + "; ".join(st.warnings[-1:]))
        rep.statements_skipped += 1
        return rep, st
    with db._lock:
        dup = db._conn.execute("SELECT id FROM statements WHERE file_hash = ?", (file_hash,)).fetchone()
        same = None
        if not dup:
            acc = db._conn.execute("SELECT id FROM accounts WHERE iban = ?", (st.account_iban,)).fetchone()
            if acc:
                same = db._conn.execute(
                    "SELECT id FROM statements WHERE account_id = ? AND period_start = ? AND period_end = ? "
                    "  AND opening_balance IS NOT NULL",
                    (int(acc["id"]), st.opening_date, st.closing_date),
                ).fetchone()
    if dup or same:
        rep.statements_skipped += 1
        rep.rows_duplicate = len(st.rows)
        return rep, st
    meta = {
        "doc_id": int(doc_id), "file_hash": file_hash, "statement_no": st.statement_no,
        "opening_date": st.opening_date, "opening_balance": st.opening_balance,
        "closing_date": st.closing_date, "closing_balance": st.closing_balance,
        "is_savings": st.is_savings, "holder": st.holder, "finalize": finalize,
        "extra_json": __import__("json").dumps({"bank_name": st.bank_name, "account_kind": st.account_kind}),
    }
    if not st.rows:
        # Ein Auszug ohne Buchungen (Tagesgeld, ruhiger Monat) zählt trotzdem
        # als Auszug: er belegt den Kontostand für die Lückenprüfung.
        from .csv_import import _ensure_pdf_statement
        from .dates import iban_hash
        account_id = db.upsert_account(bank_name="Sparkasse", iban=st.account_iban, iban_last4=st.account_iban[-4:],
                                       iban_hash=iban_hash(st.account_iban), account_holder=st.holder)
        _ensure_pdf_statement(db, account_id, meta)
        rep.accounts_touched.append(st.account_iban)
        rep.statements += 1
        return rep, st
    import_rows(db, st.rows, rep, statement=meta)
    rep.statements += 1
    return rep, st


def import_statement_file(db, path, *, doc_id: int, file_hash: str = "", known_names=None, finalize: bool = True):
    """Kontoauszug-PDF von der Platte lesen (voller Text, nicht der in der DB
    gekürzte) und importieren."""
    import hashlib
    from pathlib import Path
    from ..ocr import _extract_pdf
    p = Path(path)
    if not file_hash:
        file_hash = hashlib.sha256(p.read_bytes()).hexdigest()
    text, _pages = _extract_pdf(p)
    return import_statement_text(db, text, doc_id=doc_id, file_hash=file_hash, file_label=p.name,
                                 known_names=known_names, finalize=finalize)


def statement_subject(st: "ParsedStatement") -> str:
    kind = "Tagesgeld" if st.is_savings else "Girokonto"
    return f"Kontoauszug {st.statement_no} {kind} …{st.account_iban[-4:]}".strip()


def tidy_statement_documents(db, settings, *, log=None, only_doc_ids: set[int] | None = None) -> dict:
    """Give every recognised Kontoauszug in the library the metadata the
    parser knows for certain — the LLM had filed many as „Klassifizierung
    fehlgeschlagen" / „Unbekannt" / without a date (Robert: „ganz viele
    klassifizierung fehlgeschlagen, bitte aufräumen und einsortieren").
    Category Kontoauszug, subcategory Girokonto/Tagesgeld, date = closing
    day, sender = the bank, subject „Kontoauszug 8/2025 Girokonto …8621";
    the file is moved to its canonical place the same way a manual edit
    on /document/<id> does (organizer.target_path + db.update_metadata).
    Only documents whose metadata differs are touched."""
    import json as _json
    import shutil
    from pathlib import Path
    from ..ocr import _extract_pdf
    from ..organizer import target_path
    log = log or logger
    out = {"checked": 0, "changed": 0, "moved": 0, "errors": []}
    if settings is None:
        return out
    with db._lock:
        rows = [dict(r) for r in db._conn.execute(
            "SELECT s.id AS stmt_id, s.doc_id, s.statement_no, s.period_end, s.extra_json, a.iban, a.is_savings, a.bank_name, "
            "       d.category, d.subcategory, d.doc_date, d.sender, d.subject, d.library_path, d.status, d.tags "
            "FROM statements s JOIN documents d ON d.id = s.doc_id LEFT JOIN accounts a ON a.id = s.account_id "
            "WHERE COALESCE(s.file_hash, '') NOT LIKE 'csv-import:%' AND s.opening_balance IS NOT NULL "
            "  AND d.deleted_at IS NULL"
        ).fetchall()]
    for r in rows:
        if only_doc_ids is not None and int(r["doc_id"]) not in only_doc_ids:
            continue
        out["checked"] += 1
        p = Path(r["library_path"] or "")
        if not p.exists():
            continue
        kind = "Tagesgeld" if int(r["is_savings"] or 0) else "Girokonto"
        last4 = (r["iban"] or "")[-4:]
        subject = f"Kontoauszug {r['statement_no']} {kind} …{last4}".strip()
        date = r["period_end"] or r["doc_date"] or ""
        # Sender = the bank as it names itself on the statement. Read once per
        # statement and remembered in statements.extra_json, so the LLM's
        # variants („Sparkasse Musterstadt", „Unbekannt") converge on one name.
        try:
            extra = _json.loads(r["extra_json"] or "{}") if r["extra_json"] else {}
        except ValueError:
            extra = {}
        sender = (extra.get("bank_name") or "").strip()
        if not sender:
            try:
                text, _n = _extract_pdf(p)
                st = parse_statement_text(text)
                sender = (st.bank_name if st else "") or ""
            except Exception:  # noqa: BLE001
                sender = ""
            sender = sender or (r["sender"] if "sparkasse" in (r["sender"] or "").lower() else "") or r["bank_name"] or "Sparkasse"
            extra["bank_name"] = sender
            with db._lock:
                db._conn.execute("UPDATE statements SET extra_json = ? WHERE id = ?", (_json.dumps(extra), int(r["stmt_id"])))
                db._conn.commit()
        same = (r["category"] == "Kontoauszug" and (r["subcategory"] or "") == kind and (r["doc_date"] or "") == date
                and (r["subject"] or "") == subject and (r["sender"] or "") == sender and r["status"] == "filed")
        if same:
            continue
        try:
            new_path = target_path(settings.paths.library, date, "Kontoauszug", sender, subject,
                                   settings.filename_template, settings.max_filename_length, p.suffix,
                                   subcategory=kind, current_path=p)
            if new_path != p:
                shutil.move(str(p), str(new_path))
                out["moved"] += 1
            try:
                tags = list(_json.loads(r["tags"] or "[]"))
            except ValueError:
                tags = []
            if "kontoauszug" not in tags:
                tags.append("kontoauszug")
            db.update_metadata(int(r["doc_id"]), category="Kontoauszug", subcategory=kind, tags=tags[:8],
                               doc_date=date, sender=sender, subject=subject, filename=new_path.name,
                               library_path=str(new_path), status="filed")
            out["changed"] += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("Kontoauszug-Dokument %s aufräumen: %s", p.name, exc)
            out["errors"].append({"doc_id": r["doc_id"], "error": str(exc)})
    if out["changed"]:
        log.info("Kontoauszug-Dokumente aufgeräumt: %d geändert, %d verschoben.", out["changed"], out["moved"])
    return out


def backfill_library_statements(db, *, log=None, settings=None) -> dict:
    """Alle Kontoauszug-PDFs der Bibliothek einlesen, die noch nicht in den
    Finanzen sind. Läuft beim Start einmal im Hintergrund und auf Knopfdruck
    von /finance. Rückgabe: Zusammenfassung mit je Datei Ergebnis."""
    from pathlib import Path
    from .csv_import import finalize_import
    log = log or logger
    with db._lock:
        docs = [dict(r) for r in db._conn.execute(
            "SELECT id, library_path, content_hash, category, subject, original_name FROM documents "
            "WHERE deleted_at IS NULL AND category != '_csv_container' AND library_path != '' "
            "  AND LOWER(library_path) LIKE '%.pdf' ORDER BY doc_date, id"
        ).fetchall()]
        done = {str(r["file_hash"]) for r in db._conn.execute(
            "SELECT file_hash FROM statements WHERE file_hash IS NOT NULL"
        ).fetchall()}
        done_docs = {int(r["doc_id"]) for r in db._conn.execute(
            "SELECT doc_id FROM statements WHERE COALESCE(file_hash, '') NOT LIKE 'csv-import:%'"
        ).fetchall()}
    known = known_counterparties(db)
    out = {"files": [], "statements": 0, "skipped": 0, "rows_inserted": 0, "rows_overlap": 0,
           "unbalanced": [], "missing_files": 0, "not_statement": 0}
    for d in docs:
        p = Path(d["library_path"])
        if int(d["id"]) in done_docs:
            continue
        if not p.exists():
            out["missing_files"] += 1
            continue
        cat = (d.get("category") or "").lower()
        subj = (d.get("subject") or "").lower()
        name = (d.get("original_name") or "").lower()
        # Only look inside documents that could be a statement — the text of
        # every other PDF in the library is not worth reading again.
        if not ("kontoauszug" in cat or "bank" in cat or "kontoauszug" in subj or "auszug" in name
                or "sonstiges" in cat or "fehlgeschlagen" in name):
            continue
        import hashlib
        fh = d.get("content_hash") or hashlib.sha256(p.read_bytes()).hexdigest()
        if fh in done:
            continue
        try:
            rep, st = import_statement_file(db, p, doc_id=int(d["id"]), file_hash=fh, known_names=known, finalize=False)
        except Exception as exc:  # noqa: BLE001
            log.warning("Kontoauszug %s: %s", p.name, exc)
            out["files"].append({"doc_id": d["id"], "file": p.name, "error": str(exc)})
            continue
        if st is None:
            out["not_statement"] += 1
            continue
        entry = {"doc_id": d["id"], "file": p.name, "account": st.account_iban[-4:] if st.account_iban else "",
                 "statement_no": st.statement_no, "period": f"{st.opening_date}..{st.closing_date}",
                 "rows": len(st.rows), "inserted": rep.rows_inserted, "overlap": rep.rows_overlap,
                 "balanced": st.balanced, "errors": rep.errors}
        out["files"].append(entry)
        out["statements"] += rep.statements
        out["skipped"] += rep.statements_skipped
        out["rows_inserted"] += rep.rows_inserted
        out["rows_overlap"] += rep.rows_overlap
        if st.balanced is False:
            out["unbalanced"].append(entry)
    finalize_import(db)
    try:
        db.finance_reclassify(force=False)
    except Exception as exc:  # noqa: BLE001
        log.warning("reclassify after statement backfill failed: %s", exc)
    out["overview"] = db.finance_statement_overview()
    try:
        out["tidy"] = tidy_statement_documents(db, settings, log=log)
    except Exception as exc:  # noqa: BLE001
        log.warning("Kontoauszug-Dokumente aufräumen fehlgeschlagen: %s", exc)
        out["tidy"] = {"checked": 0, "changed": 0, "moved": 0, "errors": [str(exc)]}
    log.info("Kontoauszüge: %d eingelesen, %d übersprungen, %d Buchungen neu, %d Überschneidungen mit CSV.",
             out["statements"], out["skipped"], out["rows_inserted"], out["rows_overlap"])
    return out
