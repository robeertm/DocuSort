"""SQLite storage for document metadata, token usage, and costs.

The database lives alongside the library (path comes from config) and is
created on first access. Callers should use `open_db()` to get a `Database`
instance — it owns a single connection in WAL mode so the watcher thread and
the web server can read concurrently while writes are serialized.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


logger = logging.getLogger("docusort.db")

# Bumped whenever finance.classify changes its built-in patterns, so the
# start-up hook re-runs over old bookings exactly once.
FINANCE_CLASSIFIER_VERSION = "2026-09-20.6"
LEGACY_CSV_REASON = "vom CSV-Import nach Stichwort gesetzt"


# Cost calculation lives in providers.pricing now (so each provider can
# contribute its own model table). We keep these names exported as legacy
# aliases for the dashboard UI, retry.py, etc.
from .providers.pricing import (  # noqa: E402
    all_pricing as _all_pricing,
    calculate_cost as _provider_calc_cost,
)

CACHE_WRITE_MULTIPLIER = 1.25
CACHE_READ_MULTIPLIER  = 0.10


def _provider_for_model(model: str) -> str:
    """Infer the provider from a model string for legacy callers that only
    have the model name available (e.g. dashboard cost recompute)."""
    m = (model or "").lower()
    if m.startswith("claude"):
        return "anthropic"
    if m.startswith(("gpt", "o1", "o3", "o4", "chatgpt")):
        return "openai"
    if m.startswith("gemini"):
        return "gemini"
    return "openai_compat"


def calculate_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    *,
    cache_write: int = 0,
    cache_read: int = 0,
) -> float:
    return _provider_calc_cost(
        _provider_for_model(model), model, input_tokens, output_tokens,
        cache_write=cache_write, cache_read=cache_read,
    )


# Flat dict for the /api/pricing endpoint — flattens all provider tables
# into one map so the JS frontend keeps working unchanged.
def _flatten_pricing() -> dict[str, tuple[float, float]]:
    flat: dict[str, tuple[float, float]] = {}
    for table in _all_pricing().values():
        flat.update(table)
    return flat


MODEL_PRICING: dict[str, tuple[float, float]] = _flatten_pricing()


@dataclass
class DocumentRecord:
    filename: str
    original_name: str
    category: str
    doc_date: str
    sender: str
    subject: str
    confidence: float
    reasoning: str
    library_path: str
    processed_path: str
    file_size: int
    page_count: int | None
    ocr_used: bool
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    status: str  # 'filed' | 'review' | 'failed' | 'duplicate'
    content_hash: str = ""
    cache_creation_tokens: int = 0
    cache_read_tokens: int = 0
    extracted_text: str = ""
    subcategory: str = ""
    tags: str = "[]"  # JSON array of lowercase short labels
    due_date: str = ""            # ISO YYYY-MM-DD deadline, or ""
    due_kind: str = ""            # 'zahlung' | 'kuendigung' | ''
    deadline_notified_at: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))


SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filename        TEXT NOT NULL,
    original_name   TEXT NOT NULL,
    category        TEXT NOT NULL,
    doc_date        TEXT,
    sender          TEXT,
    subject         TEXT,
    confidence      REAL,
    reasoning       TEXT,
    library_path    TEXT NOT NULL,
    processed_path  TEXT,
    file_size       INTEGER,
    page_count      INTEGER,
    ocr_used        INTEGER,
    model           TEXT,
    input_tokens    INTEGER,
    output_tokens   INTEGER,
    cache_creation_tokens INTEGER DEFAULT 0,
    cache_read_tokens     INTEGER DEFAULT 0,
    content_hash    TEXT,
    cost_usd        REAL,
    status          TEXT NOT NULL,
    extracted_text  TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_documents_category ON documents(category);
CREATE INDEX IF NOT EXISTS idx_documents_doc_date ON documents(doc_date);
CREATE INDEX IF NOT EXISTS idx_documents_status   ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_created  ON documents(created_at);
-- idx_documents_hash is created in _migrate() after the column has been
-- added to pre-v0.3 databases.

CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    filename, sender, subject, reasoning, extracted_text,
    content='documents', content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS documents_ai AFTER INSERT ON documents BEGIN
    INSERT INTO documents_fts(rowid, filename, sender, subject, reasoning, extracted_text)
    VALUES (new.id, new.filename, new.sender, new.subject, new.reasoning, new.extracted_text);
END;

CREATE TRIGGER IF NOT EXISTS documents_ad AFTER DELETE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, filename, sender, subject, reasoning, extracted_text)
    VALUES ('delete', old.id, old.filename, old.sender, old.subject, old.reasoning, old.extracted_text);
END;

CREATE TRIGGER IF NOT EXISTS documents_au AFTER UPDATE ON documents BEGIN
    INSERT INTO documents_fts(documents_fts, rowid, filename, sender, subject, reasoning, extracted_text)
    VALUES ('delete', old.id, old.filename, old.sender, old.subject, old.reasoning, old.extracted_text);
    INSERT INTO documents_fts(rowid, filename, sender, subject, reasoning, extracted_text)
    VALUES (new.id, new.filename, new.sender, new.subject, new.reasoning, new.extracted_text);
END;

-- Receipts (Kassenzettel) — one row per document classified as Kassenzettel.
-- Line items live in receipt_items, FK-linked. Both cascade on document
-- deletion via the trigger below (SQLite FKs only fire on direct deletes
-- of the parent table, but we delete via the documents row, so we use a
-- trigger to keep things in sync).
CREATE TABLE IF NOT EXISTS receipts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id        INTEGER NOT NULL UNIQUE,
    shop_name     TEXT,
    shop_type     TEXT,         -- supermarkt | drogerie | restaurant | tankstelle | ...
    payment_method TEXT,        -- bar | girocard | kreditkarte | paypal | sonstiges
    total_amount  REAL,
    currency      TEXT DEFAULT 'EUR',
    receipt_date  TEXT,         -- ISO date; usually mirrors documents.doc_date
    extra_json    TEXT,         -- raw extractor output for debugging
    created_at    TEXT NOT NULL,
    FOREIGN KEY (doc_id) REFERENCES documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_receipts_doc        ON receipts(doc_id);
CREATE INDEX IF NOT EXISTS idx_receipts_shop_type  ON receipts(shop_type);
CREATE INDEX IF NOT EXISTS idx_receipts_date       ON receipts(receipt_date);

CREATE TABLE IF NOT EXISTS receipt_items (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id    INTEGER NOT NULL,
    name          TEXT NOT NULL,
    quantity      REAL,
    unit_price    REAL,
    total_price   REAL,
    item_category TEXT,         -- lebensmittel | getraenke | haushalt | ...
    line_no       INTEGER,      -- preserve original ordering on the receipt
    FOREIGN KEY (receipt_id) REFERENCES receipts(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_receipt_items_receipt  ON receipt_items(receipt_id);
CREATE INDEX IF NOT EXISTS idx_receipt_items_name     ON receipt_items(name);
CREATE INDEX IF NOT EXISTS idx_receipt_items_category ON receipt_items(item_category);

-- Cascade receipts when a document is deleted. SQLite enforces FK cascades
-- only when PRAGMA foreign_keys=ON (we do that on connect), but the trigger
-- guards against the rare case of a soft-delete without a real DELETE.
CREATE TRIGGER IF NOT EXISTS receipts_cascade_on_doc_delete
AFTER DELETE ON documents BEGIN
    DELETE FROM receipts WHERE doc_id = old.id;
END;

-- Finance: bank accounts, statements (Kontoauszüge), and transactions.
-- An account is identified by iban_hash (SHA256 of normalised IBAN) so
-- two statements from the same account auto-merge even if one was
-- pseudonymised before extraction.
CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bank_name       TEXT NOT NULL,
    iban            TEXT,                  -- nullable if user opts out of storing
    iban_last4      TEXT,                  -- 'DE89...0123'  for display
    iban_hash       TEXT UNIQUE,           -- SHA256 of normalised IBAN; dedup key
    account_holder  TEXT,
    currency        TEXT DEFAULT 'EUR',
    -- is_savings: 1 = Sparkonto / Tagesgeld / Depot the user parks money
    -- in. These accounts are summed into "Vermögen" but their inflows
    -- are NOT counted as income in the salary-period cashflow (the money
    -- just moved between the user's own pockets).
    is_savings      INTEGER DEFAULT 0,
    -- start_balance: the account's balance BEFORE the first imported
    -- booking. Sparkasse CSV exports carry no running balance, so the
    -- user sets this once and every later balance is start_balance +
    -- SUM(amount). 0 is the safe default (net worth then reads as the
    -- cumulative cashflow since the first import).
    start_balance   REAL DEFAULT 0,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_accounts_iban_hash ON accounts(iban_hash);

CREATE TABLE IF NOT EXISTS statements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id          INTEGER NOT NULL UNIQUE,
    account_id      INTEGER,
    period_start    TEXT,                   -- ISO date inclusive
    period_end      TEXT,                   -- ISO date inclusive
    statement_no    TEXT,                   -- bank-assigned statement number
    opening_balance REAL,
    closing_balance REAL,
    currency        TEXT DEFAULT 'EUR',
    file_hash       TEXT,                   -- SHA256 of the PDF; identical files dedup
    privacy_mode    TEXT,                   -- 'pseudonymize' | 'local' | 'plain'
    extra_json      TEXT,                   -- raw extractor output for debugging
    created_at      TEXT NOT NULL,
    FOREIGN KEY (doc_id)     REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts(id)  ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_statements_doc       ON statements(doc_id);
CREATE INDEX IF NOT EXISTS idx_statements_account   ON statements(account_id);
CREATE INDEX IF NOT EXISTS idx_statements_period    ON statements(period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_statements_file_hash ON statements(file_hash);

CREATE TABLE IF NOT EXISTS transactions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    statement_id    INTEGER NOT NULL,
    account_id      INTEGER,
    booking_date    TEXT,
    value_date      TEXT,
    amount          REAL NOT NULL,           -- negative = outgoing
    currency        TEXT DEFAULT 'EUR',
    counterparty    TEXT,
    counterparty_iban TEXT,                 -- masked or empty after pseudonymisation
    purpose         TEXT,
    tx_type         TEXT,                   -- ueberweisung | lastschrift | gehalt | ...
    category        TEXT,                   -- miete | lebensmittel | mobilitaet | ...
    tx_hash         TEXT UNIQUE,            -- account+date+amount+purpose hash for dedup
    line_no         INTEGER,
    FOREIGN KEY (statement_id) REFERENCES statements(id) ON DELETE CASCADE,
    FOREIGN KEY (account_id)   REFERENCES accounts(id)   ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_stmt     ON transactions(statement_id);
CREATE INDEX IF NOT EXISTS idx_transactions_account  ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date     ON transactions(booking_date);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category);

CREATE TRIGGER IF NOT EXISTS statements_cascade_on_doc_delete
AFTER DELETE ON documents BEGIN
    DELETE FROM statements WHERE doc_id = old.id;
END;
"""


def _day_series(range_start: str, range_end: str,
                per_day: dict[str, float],
                data_through: str = "") -> list[dict[str, Any]]:
    """Lückenlose Tagesreihe über den GANZEN Zeitraum.

    Zwei Dinge, die `per_day` nicht kann:
    * Ein Tag ohne Buchung fehlte dort ganz — dabei ist „nichts ausgegeben"
      eine Aussage und gehört angezeigt (Wunsch: „es soll auch tage
      anzeigen wo man nichts ausgegeben hat").
    * Tage, die noch kommen, gehören ebenfalls ins Bild — am ersten Tag
      eines Gehaltsmonats soll der ganze Monat zu sehen sein. Sie werden
      mit `future: True` markiert: 🔴 ein Tag, der noch nicht stattgefunden
      hat, ist KEIN Tag ohne Ausgaben und darf im Spiel nichts zählen.
    * Dasselbe gilt für Tage, die zwar vorbei sind, für die aber noch keine
      Buchung vorliegt (`pending: True`, alles nach `data_through`, dem
      jüngsten bekannten Buchungstag). Sonst würde ein Konto, dessen Auszug
      seit einer Woche nicht importiert wurde, sieben „Nullrunden"
      verschenken — eine Lücke in den Daten ist kein Sparerfolg.
    """
    if not range_start or not range_end:
        return [{"date": d, "spend": round(v, 2), "future": False, "pending": False}
                for d, v in sorted(per_day.items())]
    try:
        start = date.fromisoformat(range_start)
        end = date.fromisoformat(range_end)
    except ValueError:
        return [{"date": d, "spend": round(v, 2), "future": False, "pending": False}
                for d, v in sorted(per_day.items())]
    today = date.today()
    # Buchungen außerhalb des Zeitraums (sollte nicht vorkommen) gehen nicht verloren.
    for d in per_day:
        try:
            dd = date.fromisoformat(d)
        except ValueError:
            continue
        if dd < start:
            start = dd
        if dd > end:
            end = dd
    if end < start:
        return []
    out: list[dict[str, Any]] = []
    cur = start
    while cur <= end:
        key = cur.isoformat()
        future = cur > today
        out.append({"date": key, "spend": round(per_day.get(key, 0.0), 2),
                    "future": future,
                    "pending": bool(not future and data_through and key > data_through
                                    and per_day.get(key) is None)})
        cur += timedelta(days=1)
    return out


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._migrate()
        # v0.41: transfers between own accounts were never tagged — catch up
        # for databases filled by earlier versions (no-op afterwards).
        try:
            self.finance_retag_transfers()
        except Exception as exc:  # noqa: BLE001 — never block start-up on this
            logger.warning("Finance: transfer re-tag at start-up failed: %s", exc)
        try:
            self.finance_fill_transfer_gaps()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Finance: gap fill at start-up failed: %s", exc)
        # v0.43: run the explainable classifier over the existing bookings
        # once per classifier version — fills category_source/reason for
        # old rows and resolves what the new patterns recognise.
        try:
            if (self.meta_get("finance.classifier_version") or "") != FINANCE_CLASSIFIER_VERSION:
                self._finance_rekey_rules()
                self._finance_drop_pending_placeholders()
                self._finance_drop_income_pins_on_debits()
                self._finance_drop_acquirer_iban_rules()
                stats = self.finance_reclassify(force=False)
                self.meta_set("finance.classifier_version", FINANCE_CLASSIFIER_VERSION)
                logger.info("Finance: classifier %s applied at start-up: %s", FINANCE_CLASSIFIER_VERSION, stats)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Finance: reclassify at start-up failed: %s", exc)
        logger.info("Database ready at %s", path)

    def _finance_rekey_rules(self) -> None:
        """v0.45: `merchant_key` cuts address tails („//Musterstadt/DE", padded
        street) — learned merchant rules keep working by recomputing their
        key from the remembered sample name. A rule whose new key collides
        with an existing one is dropped (the surviving rule wins)."""
        from .finance.buckets import merchant_key
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, match_kind, match_value, sample_name FROM category_rules "
                "WHERE match_kind IN ('merchant', 'merchant_amount') AND sample_name != ''"
            ).fetchall()
            for r in rows:
                key = merchant_key(r["sample_name"])
                if not key:
                    continue
                old = r["match_value"]
                new = key if r["match_kind"] == "merchant" else key + "|" + old.rsplit("|", 1)[-1]
                if new == old:
                    continue
                try:
                    self._conn.execute("UPDATE category_rules SET match_value = ? WHERE id = ?", (new, int(r["id"])))
                except sqlite3.IntegrityError:
                    self._conn.execute("DELETE FROM category_rules WHERE id = ?", (int(r["id"]),))
            self._conn.commit()

    def _finance_drop_acquirer_iban_rules(self) -> None:
        """v0.45.4: IBAN rules learned from card payments point at the
        payment processor's account, shared by many shops (11 payees on one
        IBAN) — every rule on an IBAN that card payments of ≥ 2 different
        payees carry is removed; the merchant-name rules stay."""
        from .finance.buckets import merchant_key
        with self._lock:
            rows = self._conn.execute(
                "SELECT counterparty_iban AS iban, counterparty FROM transactions "
                "WHERE tx_type = 'kartenzahlung' AND COALESCE(counterparty_iban, '') != ''"
            ).fetchall()
            payees: dict[str, set[str]] = {}
            for r in rows:
                payees.setdefault(r["iban"], set()).add(merchant_key(r["counterparty"]))
            shared = [i for i, ks in payees.items() if len(ks) > 1]
            n = 0
            for iban in shared:
                n += self._conn.execute(
                    "DELETE FROM category_rules WHERE match_kind = 'iban' AND match_value = ?", (iban,)
                ).rowcount
            if n:
                logger.info("Finance: %d IBAN rules on shared card-acquirer accounts removed", n)
            self._conn.commit()

    def _finance_drop_income_pins_on_debits(self) -> None:
        """v0.45.3: hand-pins that put a debit into an income category are
        void (Wunsch: „Ausgaben sind nie Erstattungen") — dropped so the
        reclassify can decide afresh."""
        from .finance.categories import INCOME_CATEGORIES
        marks = ",".join("?" * len(INCOME_CATEGORIES))
        with self._lock:
            cur = self._conn.execute(
                f"DELETE FROM transaction_category_overrides WHERE category IN ({marks}) AND tx_hash IN "
                "(SELECT tx_hash FROM transactions WHERE amount < 0)", tuple(INCOME_CATEGORIES),
            )
            self._conn.execute(
                f"UPDATE category_rules SET direction = 'in' WHERE category IN ({marks}) AND direction != 'in'",
                tuple(INCOME_CATEGORIES),
            )
            if cur.rowcount:
                logger.info("Finance: %d income pins on debits dropped", cur.rowcount)
            self._conn.commit()

    def _finance_drop_pending_placeholders(self) -> None:
        """v0.45: Sparkasse exports list pending card payments as
        „SONSTIGER EINZUG" with a terminal number („F050569390480") as payee.
        Earlier versions imported them as bookings; the booked version came
        later under the merchant's name, so both were counted. Pending rows
        now live in `pending_transactions`; the placeholders are removed."""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM transactions WHERE counterparty GLOB 'F[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' "
                "  AND (purpose LIKE 'SONSTIGER EINZUG%' OR purpose GLOB '[EM][CO] [0-9]*')"
            )
            if cur.rowcount:
                logger.info("Finance: %d pending card placeholders removed", cur.rowcount)
            self._conn.commit()

    def _migrate(self) -> None:
        """Idempotent column adds for databases created by older versions."""
        cols = {row["name"] for row in self._conn.execute("PRAGMA table_info(documents)")}
        migrations = [
            ("cache_creation_tokens", "INTEGER DEFAULT 0"),
            ("cache_read_tokens",     "INTEGER DEFAULT 0"),
            ("content_hash",          "TEXT"),
            ("deleted_at",            "TEXT"),
            ("subcategory",           "TEXT DEFAULT ''"),
            ("tags",                  "TEXT DEFAULT '[]'"),
            # Deadline reminders (v0.36): a document can carry a due /
            # cancellation date the KI pulls out. due_kind is 'zahlung'
            # (Rechnung fällig), 'kuendigung' (Vertrag kündbar bis) or ''.
            ("due_date",              "TEXT DEFAULT ''"),
            ("due_kind",              "TEXT DEFAULT ''"),
            # ISO timestamp of the last deadline notification for this row;
            # reset to '' whenever due_date changes so a corrected date
            # re-arms the reminder.
            ("deadline_notified_at",  "TEXT DEFAULT ''"),
            # ISO timestamp of when the user ticked this deadline off as
            # "erledigt" on the dashboard. Non-empty => hidden from the
            # 'Fällig demnächst' card. The reminder is otherwise untouched.
            ("deadline_done_at",      "TEXT DEFAULT ''"),
            # v0.49: what an invoice asks for, read out of its own text
            # (no KI call), plus the booking that settled it. paid_source
            # is 'auto' when the matcher found it and 'manual' when the
            # user linked it by hand, so an automatic link can be undone
            # without touching a manual one.
            ("due_amount",            "REAL"),
            ("due_amount_src",        "TEXT DEFAULT ''"),
            ("paid_tx_id",            "INTEGER"),
            ("paid_at",               "TEXT DEFAULT ''"),
            ("paid_source",           "TEXT DEFAULT ''"),
        ]
        for name, decl in migrations:
            if name not in cols:
                self._conn.execute(f"ALTER TABLE documents ADD COLUMN {name} {decl}")
                logger.info("DB migration: added column %s", name)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_hash    ON documents(content_hash)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_deleted ON documents(deleted_at)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_subcat  ON documents(subcategory)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_due     ON documents(due_date)")

        # statements: opt-in column to mark a row as "no transactions
        # ever, stop nagging about it". Used by the diag banner so the
        # user can hide individual statements that genuinely have
        # nothing to extract (e.g. a Tagesgeldkonto cover page).
        stmt_cols = {row["name"] for row in self._conn.execute("PRAGMA table_info(statements)")}
        if "acknowledged_empty" not in stmt_cols:
            self._conn.execute(
                "ALTER TABLE statements ADD COLUMN acknowledged_empty INTEGER DEFAULT 0"
            )
            logger.info("DB migration: added statements.acknowledged_empty")
        if "extraction_warning" not in stmt_cols:
            self._conn.execute(
                "ALTER TABLE statements ADD COLUMN extraction_warning TEXT DEFAULT ''"
            )
            logger.info("DB migration: added statements.extraction_warning")

        # receipts: a receipt that never shows up on a statement was paid
        # in cash — and cash spending is invisible in the bank data. The
        # user can put such a receipt into a category here (v0.55).
        rec_cols = {row["name"] for row in self._conn.execute("PRAGMA table_info(receipts)")}
        if "cash_category" not in rec_cols:
            self._conn.execute(
                "ALTER TABLE receipts ADD COLUMN cash_category TEXT DEFAULT ''"
            )
            logger.info("DB migration: added receipts.cash_category")
        if "cash_confirmed" not in rec_cols:
            # 1 = the user confirmed „this was cash", 0 = only inferred.
            self._conn.execute(
                "ALTER TABLE receipts ADD COLUMN cash_confirmed INTEGER DEFAULT 0"
            )
            logger.info("DB migration: added receipts.cash_confirmed")

        # accounts: the savings flag + opening balance that power the
        # net-worth ("Gesamtvermögen") view and the salary-period
        # cashflow tracker (v0.37). Added here so databases that predate
        # the feature pick them up without a reset.
        acct_cols = {row["name"] for row in self._conn.execute("PRAGMA table_info(accounts)")}
        if "is_savings" not in acct_cols:
            self._conn.execute(
                "ALTER TABLE accounts ADD COLUMN is_savings INTEGER DEFAULT 0"
            )
            logger.info("DB migration: added accounts.is_savings")
        if "start_balance" not in acct_cols:
            self._conn.execute(
                "ALTER TABLE accounts ADD COLUMN start_balance REAL DEFAULT 0"
            )
            logger.info("DB migration: added accounts.start_balance")

        # Per-tx category overrides — set when the user manually
        # recategorises a booking via /transactions. Survives the
        # delete-and-reinsert that upsert_statement performs on
        # re-extraction, so an upgrade-time bulk reanalysis doesn't
        # silently throw away the user's manual labels.
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS transaction_category_overrides (
                   tx_hash  TEXT PRIMARY KEY,
                   category TEXT NOT NULL,
                   set_at   TEXT NOT NULL
               )"""
        )

        # KI-Zuordnung Händler → Ausgaben-Topf (v0.39). Die lokale KI
        # bestimmt für exotische/englische Händlernamen ("Soul Food" =
        # Suppenbar → essen) einmalig einen Topf; das Ergebnis wird hier
        # gecacht, damit die /ausgaben-Klassifizierung eine reine Dict-Suche
        # bleibt. `merchant_key` ist der normalisierte Schlüssel aus
        # finance.buckets.merchant_key. `source` = 'ai' | 'manual'.
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS merchant_buckets (
                   merchant_key  TEXT PRIMARY KEY,
                   sample_name   TEXT NOT NULL DEFAULT '',
                   bucket        TEXT NOT NULL,
                   source        TEXT NOT NULL DEFAULT 'ai',
                   updated_at    TEXT NOT NULL
               )"""
        )

        # v0.43: every booking carries WHY it has its category
        # (`category_source` = manual|transfer|rule|ai|keyword|bank|import|none,
        # `category_reason` = one human sentence) — Wunsch: „jede einzelne
        # Buchung muss nachvollziehbar sein".
        tx_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(transactions)")}
        if "category_source" not in tx_cols:
            self._conn.execute("ALTER TABLE transactions ADD COLUMN category_source TEXT DEFAULT ''")
            logger.info("DB migration: added transactions.category_source")
        if "category_reason" not in tx_cols:
            self._conn.execute("ALTER TABLE transactions ADD COLUMN category_reason TEXT DEFAULT ''")
            logger.info("DB migration: added transactions.category_reason")
        if "synthetic" not in tx_cols:
            # v0.44: counter-legs DocuSort added itself to close a gap in an
            # export (see finance_fill_transfer_gaps). Replaced by the real
            # booking when it is imported.
            self._conn.execute("ALTER TABLE transactions ADD COLUMN synthetic INTEGER DEFAULT 0")
            logger.info("DB migration: added transactions.synthetic")

        # v0.43: learned category rules. A manual assignment on one booking
        # teaches a rule for its counterparty (IBAN and/or normalised name);
        # the local AI may propose rules too (`source = 'ai'`), which a user
        # rule overrides. Consulted by the CSV importer and the reclassifier.
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS category_rules (
                   id           INTEGER PRIMARY KEY AUTOINCREMENT,
                   match_kind   TEXT NOT NULL,            -- 'iban' | 'merchant'
                   match_value  TEXT NOT NULL,            -- IBAN or finance.buckets.merchant_key
                   category     TEXT NOT NULL,
                   source       TEXT NOT NULL DEFAULT 'user',   -- 'user' | 'ai'
                   sample_name  TEXT NOT NULL DEFAULT '',
                   hits         INTEGER NOT NULL DEFAULT 0,
                   created_at   TEXT NOT NULL,
                   updated_at   TEXT NOT NULL,
                   UNIQUE (match_kind, match_value)
               )"""
        )

        # v0.45: bookings the bank lists as „vorgemerkt" (pending). They are
        # not transactions yet — the merchant name, date and even the amount
        # may change when they book — so they live apart and count nowhere.
        # Every import of an account replaces its pending set with the
        # file's (a newer export is the truth about what is still pending).
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS pending_transactions (
                   id            INTEGER PRIMARY KEY AUTOINCREMENT,
                   account_id    INTEGER NOT NULL,
                   booking_date  TEXT NOT NULL,
                   amount        REAL NOT NULL,
                   currency      TEXT NOT NULL DEFAULT 'EUR',
                   counterparty  TEXT NOT NULL DEFAULT '',
                   purpose       TEXT NOT NULL DEFAULT '',
                   tx_hash       TEXT NOT NULL,
                   as_of         TEXT NOT NULL,      -- newest booked date in the export that listed it
                   imported_at   TEXT NOT NULL,
                   UNIQUE (account_id, tx_hash)
               )"""
        )

        # v0.45.2: rules carry a direction ('' | 'in' | 'out') and the unique
        # key includes it — SQLite cannot change a constraint, so rebuild once.
        cols = [r[1] for r in self._conn.execute("PRAGMA table_info(category_rules)").fetchall()]
        if "direction" not in cols:
            self._conn.executescript(
                """CREATE TABLE category_rules_v2 (
                       id           INTEGER PRIMARY KEY AUTOINCREMENT,
                       match_kind   TEXT NOT NULL,
                       match_value  TEXT NOT NULL,
                       category     TEXT NOT NULL,
                       source       TEXT NOT NULL DEFAULT 'user',
                       sample_name  TEXT NOT NULL DEFAULT '',
                       direction    TEXT NOT NULL DEFAULT '',
                       hits         INTEGER NOT NULL DEFAULT 0,
                       created_at   TEXT NOT NULL,
                       updated_at   TEXT NOT NULL,
                       UNIQUE (match_kind, match_value, direction)
                   );
                   INSERT INTO category_rules_v2 (id, match_kind, match_value, category, source, sample_name, direction, hits, created_at, updated_at)
                       SELECT id, match_kind, match_value, category, source, sample_name,
                              CASE WHEN category IN ('erstattung', 'gehalt', 'zins-dividende', 'rente-zuschuss') THEN 'in' ELSE '' END,
                              hits, created_at, updated_at FROM category_rules;
                   DROP TABLE category_rules;
                   ALTER TABLE category_rules_v2 RENAME TO category_rules;"""
            )
            logger.info("Finance: category_rules rebuilt with direction (income rules → credits only)")

        # v0.44: user-defined categories next to the built-in TX_CATEGORIES.
        # `key` is the slug used in transactions.category / category_rules,
        # `label` what the UI shows, `is_fixed` feeds the Fixkosten page.
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS custom_categories (
                   key        TEXT PRIMARY KEY,
                   label      TEXT NOT NULL,
                   is_fixed   INTEGER NOT NULL DEFAULT 0,
                   created_at TEXT NOT NULL
               )"""
        )
        cc_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(custom_categories)")}
        if "is_saving" not in cc_cols:
            self._conn.execute("ALTER TABLE custom_categories ADD COLUMN is_saving INTEGER NOT NULL DEFAULT 0")

        # Generic key-value meta table. First customer is
        # last_reanalyzed_version so the auto-reanalyse-on-upgrade hook
        # can tell what version it last ran for, but anything else that
        # needs a single durable string belongs here too instead of
        # growing the schema.
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS meta (
                   key   TEXT PRIMARY KEY,
                   value TEXT NOT NULL
               )"""
        )

        # 0.33.2: revive CSV-container stub documents that 0.33.x
        # incorrectly created with `deleted_at = now`. With deleted_at
        # set, every `JOIN documents d WHERE d.deleted_at IS NULL`
        # finance query silently filtered out the imported
        # transactions, so /finance kept showing the empty-state even
        # after a successful CSV import. We tag the stubs with the
        # sentinel category `_csv_container` so the library list
        # queries can skip them without touching deleted_at.
        try:
            self._conn.execute(
                "UPDATE documents "
                "SET deleted_at = NULL, "
                "    category   = '_csv_container', "
                "    status     = 'csv_container' "
                "WHERE original_name LIKE '.csv-container-account-%' "
                "  AND (category != '_csv_container' "
                "       OR deleted_at IS NOT NULL)"
            )
        except Exception:  # noqa: BLE001
            logger.exception("DB migration: revive csv-container stubs failed")

        # NOTE: 0.15.1 shipped a startup cleanup that deleted every
        # `statements` row whose document didn't have category =
        # 'Kontoauszug'. That was destructive — pre-0.13 installs
        # legitimately stored statements under the legacy `Bank`
        # category (`backfill_statements()` still handles that case),
        # so the cleanup wiped real transactions. The cleanup has
        # been removed; re-classification cascade still happens
        # inline in `update_metadata()` for the original use case
        # (user moves a non-statement away from Kontoauszug).


        # 0.48.0: users and sessions. DocuSort had no accounts at all
        # until now; an existing install therefore has zero rows here
        # and the web layer treats "no admin yet" as the claim state
        # (see web/app.py `/setup/admin`) rather than locking the owner
        # out of his own archive.
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS users (
                   id            INTEGER PRIMARY KEY AUTOINCREMENT,
                   username      TEXT NOT NULL UNIQUE COLLATE NOCASE,
                   display_name  TEXT NOT NULL DEFAULT '',
                   password_hash TEXT NOT NULL DEFAULT '',
                   role          TEXT NOT NULL DEFAULT 'user',
                   is_active     INTEGER NOT NULL DEFAULT 1,
                   must_change_password INTEGER NOT NULL DEFAULT 0,
                   created_at    TEXT NOT NULL,
                   last_login    TEXT
               )"""
        )
        # Sessions are rows, not signed cookies, so that "log this user
        # out" and "disable this account" take effect immediately.
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS sessions (
                   token_hash TEXT PRIMARY KEY,
                   user_id    INTEGER NOT NULL,
                   created_at TEXT NOT NULL,
                   expires_at TEXT NOT NULL,
                   last_seen  TEXT,
                   user_agent TEXT NOT NULL DEFAULT '',
                   FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
               )"""
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)"
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- Meta key/value ----------

    def meta_get(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (key,)
            ).fetchone()
        return row["value"] if row else None

    def meta_set(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO meta (key, value) VALUES (?, ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
                (key, value),
            )
            self._conn.commit()


    # ---------- Users & sessions (0.48.0) ----------

    def user_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()
        return int(row["n"] or 0)

    def admin_count(self) -> int:
        """Active admins. The web layer refuses to delete, demote or
        disable the last one — an archive with no admin is unreachable."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM users "
                "WHERE role = 'admin' AND is_active = 1 AND password_hash != ''"
            ).fetchone()
        return int(row["n"] or 0)

    def user_get(self, user_id: int) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM users WHERE id = ?", (user_id,)
            ).fetchone()
        return dict(row) if row else None

    def user_by_name(self, username: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM users WHERE username = ? COLLATE NOCASE",
                (username.strip(),),
            ).fetchone()
        return dict(row) if row else None

    def user_list(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, username, display_name, role, is_active, "
                "       must_change_password, created_at, last_login "
                "FROM users ORDER BY role = 'admin' DESC, username COLLATE NOCASE"
            ).fetchall()
        return [dict(r) for r in rows]

    def user_create(self, username: str, password_hash: str, role: str,
                    display_name: str = "", must_change: bool = False) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO users (username, display_name, password_hash,
                                      role, is_active, must_change_password, created_at)
                   VALUES (?, ?, ?, ?, 1, ?, ?)""",
                (username.strip(), display_name.strip(), password_hash,
                 role, 1 if must_change else 0, now),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def user_update(self, user_id: int, **fields) -> None:
        allowed = {"display_name", "password_hash", "role",
                   "is_active", "must_change_password", "username"}
        sets, vals = [], []
        for key, value in fields.items():
            if key not in allowed:
                continue
            sets.append(f"{key} = ?")
            vals.append(value)
        if not sets:
            return
        vals.append(user_id)
        with self._lock:
            self._conn.execute(
                f"UPDATE users SET {', '.join(sets)} WHERE id = ?", vals
            )
            self._conn.commit()

    def user_delete(self, user_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
            self._conn.commit()

    def user_touch_login(self, user_id: int) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            self._conn.execute(
                "UPDATE users SET last_login = ? WHERE id = ?", (now, user_id)
            )
            self._conn.commit()

    def session_create(self, token_hash: str, user_id: int,
                       days: int, user_agent: str = "") -> None:
        now = datetime.now()
        with self._lock:
            self._conn.execute(
                """INSERT INTO sessions (token_hash, user_id, created_at,
                                         expires_at, last_seen, user_agent)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (token_hash, user_id, now.isoformat(timespec="seconds"),
                 (now + timedelta(days=days)).isoformat(timespec="seconds"),
                 now.isoformat(timespec="seconds"), (user_agent or "")[:200]),
            )
            self._conn.commit()

    def session_user(self, token_hash: str) -> dict | None:
        """Resolve a session to its user, or None when the session is
        expired, unknown, or belongs to a disabled account."""
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            row = self._conn.execute(
                """SELECT u.* FROM sessions s
                     JOIN users u ON u.id = s.user_id
                    WHERE s.token_hash = ? AND s.expires_at > ? AND u.is_active = 1""",
                (token_hash, now),
            ).fetchone()
            if row is not None:
                self._conn.execute(
                    "UPDATE sessions SET last_seen = ? WHERE token_hash = ?",
                    (now, token_hash),
                )
                self._conn.commit()
        return dict(row) if row else None

    def session_delete(self, token_hash: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM sessions WHERE token_hash = ?", (token_hash,)
            )
            self._conn.commit()

    def session_delete_for_user(self, user_id: int) -> None:
        """Used when a password changes, a role changes, or an account is
        disabled — the old cookies must stop working at that moment."""
        with self._lock:
            self._conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            self._conn.commit()

    def session_prune(self) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM sessions WHERE expires_at <= ?", (now,)
            )
            self._conn.commit()
            return cur.rowcount or 0

    # ---------- Transaction category overrides ----------

    def tx_override_set(self, tx_hash: str, category: str) -> None:
        """Record that the user manually pinned this booking to `category`.
        Re-extraction will restore this label after wiping the row.
        No-op when tx_hash is empty (legacy rows can lack the hash)."""
        if not tx_hash:
            return
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            self._conn.execute(
                """INSERT INTO transaction_category_overrides (tx_hash, category, set_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(tx_hash) DO UPDATE SET
                     category = excluded.category,
                     set_at   = excluded.set_at""",
                (tx_hash, category, now),
            )
            self._conn.commit()

    def _apply_overrides_to_statement(self, stmt_id: int) -> int:
        """Re-stamp manual categories onto the freshly inserted rows of a
        re-extracted statement. Called from inside upsert_statement under
        the existing lock — does not commit on its own. Returns the row
        count that was actually rewritten."""
        cur = self._conn.execute(
            """UPDATE transactions
                  SET category = (
                    SELECT o.category
                    FROM transaction_category_overrides o
                    WHERE o.tx_hash = transactions.tx_hash
                  )
                WHERE statement_id = ?
                  AND tx_hash IS NOT NULL AND tx_hash != ''
                  AND tx_hash IN (SELECT tx_hash FROM transaction_category_overrides)""",
            (stmt_id,),
        )
        return cur.rowcount or 0

    def insert_document(self, rec: DocumentRecord) -> int:
        data = asdict(rec)
        data["ocr_used"] = 1 if rec.ocr_used else 0
        cols = ", ".join(data.keys())
        placeholders = ", ".join(f":{k}" for k in data.keys())
        sql = f"INSERT INTO documents ({cols}) VALUES ({placeholders})"
        with self._lock:
            cur = self._conn.execute(sql, data)
            return cur.lastrowid or 0

    def update_category(self, doc_id: int, category: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE documents SET category = ?, status = 'filed' WHERE id = ?",
                (category, doc_id),
            )

    def update_classification(
        self,
        doc_id: int,
        cls,
        *,
        library_path: str,
        filename: str,
        status: str,
        extracted_text: str | None = None,
    ) -> None:
        """Apply a fresh Classification to an existing row. Token counts and
        cost accumulate on top of the previous run (if any), so the history of
        retries is visible in the per-document figures."""
        import json as _json
        fields = {
            "category":      cls.category,
            "subcategory":   getattr(cls, "subcategory", "") or "",
            "tags":          _json.dumps(getattr(cls, "tags", []) or []),
            "doc_date":      cls.date,
            "sender":        cls.sender,
            "subject":       cls.subject,
            "confidence":    cls.confidence,
            "reasoning":     cls.reasoning,
            "library_path":  library_path,
            "filename":      filename,
            "model":         cls.model,
            "status":        status,
            "due_date":      getattr(cls, "due_date", "") or "",
            "due_kind":      getattr(cls, "due_kind", "") or "",
            # A re-classification can change the deadline; re-arm the
            # reminder so the new date gets its own notification.
            "deadline_notified_at": "",
        }
        sql_sets = ", ".join(f"{k} = :{k}" for k in fields)
        accum = (
            "input_tokens = input_tokens + :input_tokens, "
            "output_tokens = output_tokens + :output_tokens, "
            "cache_creation_tokens = cache_creation_tokens + :cache_creation_tokens, "
            "cache_read_tokens = cache_read_tokens + :cache_read_tokens, "
            "cost_usd = cost_usd + :cost_usd"
        )
        params = {
            **fields,
            "input_tokens": cls.input_tokens,
            "output_tokens": cls.output_tokens,
            "cache_creation_tokens": cls.cache_creation_tokens,
            "cache_read_tokens": cls.cache_read_tokens,
            "cost_usd": cls.cost_usd,
            "doc_id": doc_id,
        }
        text_sql = ""
        if extracted_text is not None:
            text_sql = ", extracted_text = :extracted_text"
            params["extracted_text"] = extracted_text
        sql = f"UPDATE documents SET {sql_sets}, {accum}{text_sql} WHERE id = :doc_id"
        with self._lock:
            self._conn.execute(sql, params)

    def update_paths(self, doc_id: int, library_path: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE documents SET library_path = ? WHERE id = ?",
                (library_path, doc_id),
            )

    def update_metadata(
        self,
        doc_id: int,
        *,
        category: str,
        subcategory: str,
        tags: list[str],
        doc_date: str,
        sender: str,
        subject: str,
        filename: str,
        library_path: str,
        status: str = "filed",
        due_date: str | None = None,
        due_kind: str | None = None,
    ) -> None:
        """Apply a manual metadata edit. Token counts and confidence are
        preserved; status defaults to 'filed' since a human just verified it.

        due_date/due_kind are only touched when explicitly passed (None =
        leave as-is). Changing the due date re-arms the reminder."""
        import json as _json
        with self._lock:
            self._conn.execute(
                """UPDATE documents SET
                   category = ?, subcategory = ?, tags = ?,
                   doc_date = ?, sender = ?, subject = ?,
                   filename = ?, library_path = ?, status = ?
                   WHERE id = ?""",
                (category, subcategory, _json.dumps(tags),
                 doc_date, sender, subject, filename,
                 library_path, status, doc_id),
            )
            if due_date is not None or due_kind is not None:
                prev = self._conn.execute(
                    "SELECT due_date FROM documents WHERE id = ?", (doc_id,)
                ).fetchone()
                new_due = (due_date if due_date is not None else (prev["due_date"] if prev else "")) or ""
                new_kind = (due_kind if due_kind is not None else "") or ""
                notified_reset = "" if (not prev or (prev["due_date"] or "") != new_due) else None
                if notified_reset is not None:
                    self._conn.execute(
                        "UPDATE documents SET due_date = ?, due_kind = ?, "
                        "deadline_notified_at = ? WHERE id = ?",
                        (new_due, new_kind, notified_reset, doc_id),
                    )
                else:
                    self._conn.execute(
                        "UPDATE documents SET due_date = ?, due_kind = ? WHERE id = ?",
                        (new_due, new_kind, doc_id),
                    )
            # If the user re-classified a doc away from Kontoauszug /
            # Kassenzettel, drop the corresponding extractor row so the
            # /finance "needs review" banner and the receipts dashboard
            # don't keep flagging an orphan that no longer belongs to
            # them. ON DELETE CASCADE on transactions / receipt_items
            # cleans up the children.
            # v0.47.5: a Kontoauszug that the parser verified (balances add up)
            # keeps its statement and bookings whatever the category says — a
            # hand edit must never wipe a month of finances via the cascade.
            # The tidy step files it back under Kontoauszug on the next run.
            if category != "Kontoauszug":
                self._conn.execute(
                    "DELETE FROM statements WHERE doc_id = ? AND opening_balance IS NULL "
                    "  AND COALESCE(file_hash, '') NOT LIKE 'csv-import:%'", (doc_id,)
                )
            if category != "Kassenzettel":
                self._conn.execute(
                    "DELETE FROM receipts WHERE doc_id = ?", (doc_id,)
                )

    def get(self, doc_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM documents WHERE id = ?", (doc_id,)
            ).fetchone()
        return dict(row) if row else None

    def find_by_original_name(self, original_name: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM documents WHERE original_name = ? "
                "ORDER BY id DESC LIMIT 1",
                (original_name,),
            ).fetchone()
        return dict(row) if row else None

    def find_by_hash(self, content_hash: str) -> dict[str, Any] | None:
        """Return the most recent non-deleted doc with this content hash."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM documents WHERE content_hash = ? "
                "AND deleted_at IS NULL ORDER BY id DESC LIMIT 1",
                (content_hash,),
            ).fetchone()
        return dict(row) if row else None

    # ---------- Deadlines (due / cancellation reminders) ----------

    def upcoming_deadlines(
        self, *, within_days: int = 30, overdue_grace_days: int = 21,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Non-deleted documents whose due_date falls in
        [today - overdue_grace_days, today + within_days], soonest first.
        Feeds the dashboard 'Fällig demnächst' card."""
        from datetime import date as _date, timedelta as _td
        today = _date.today()
        lo = (today - _td(days=overdue_grace_days)).isoformat()
        hi = (today + _td(days=within_days)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                # Paid rows stay in the list on purpose: the owner asked to
                # SEE that a bill is settled, not for it to quietly vanish.
                # Only a hand-tick ("erledigt") removes a row.
                "SELECT id, filename, category, subcategory, sender, subject, "
                "       doc_date, due_date, due_kind, status, "
                "       due_amount, paid_tx_id, paid_at, paid_source "
                "FROM documents "
                "WHERE deleted_at IS NULL AND category != '_csv_container' "
                "  AND due_date IS NOT NULL AND due_date != '' "
                "  AND COALESCE(deadline_done_at, '') = '' "
                "  AND due_date BETWEEN ? AND ? "
                "ORDER BY (COALESCE(paid_at,'') != '') ASC, due_date ASC, id DESC LIMIT ?",
                (lo, hi, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def deadlines_needing_notice(self, *, within_days: int = 14) -> list[dict[str, Any]]:
        """Upcoming (not yet past) deadlines within `within_days` that haven't
        been notified yet. Used by the reminder watchdog."""
        from datetime import date as _date, timedelta as _td
        today = _date.today().isoformat()
        hi = (_date.today() + _td(days=within_days)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, filename, category, sender, subject, due_date, due_kind "
                "FROM documents "
                "WHERE deleted_at IS NULL AND category != '_csv_container' "
                "  AND due_date IS NOT NULL AND due_date != '' "
                "  AND due_date BETWEEN ? AND ? "
                "  AND COALESCE(deadline_notified_at, '') = '' "
                # A bill that is already paid has nothing to remind about.
                "  AND COALESCE(paid_at, '') = '' "
                "ORDER BY due_date ASC LIMIT 200",
                (today, hi),
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_deadline_notified(self, doc_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE documents SET deadline_notified_at = ? WHERE id = ?",
                (datetime.now().isoformat(timespec="seconds"), doc_id),
            )

    def set_deadline_done(self, doc_id: int, done: bool = True) -> bool:
        """Tick a deadline off as erledigt (done=True) — it then drops out
        of the dashboard 'Fällig demnächst' card — or un-tick it (done=False).
        Returns True if a matching document existed. The due_date itself is
        left intact so the info stays on the document page."""
        stamp = datetime.now().isoformat(timespec="seconds") if done else ""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE documents SET deadline_done_at = ? WHERE id = ?",
                (stamp, doc_id),
            )
            return cur.rowcount > 0


    # ---------- Deadlines ↔ payments (0.49.0) ----------
    #
    # A bill on the dashboard and the booking that settles it were two
    # unrelated worlds until now. Linking them has one hard rule: a wrong
    # "paid" mark is worse than no mark at all, because the owner would stop
    # looking at a bill that is still open. So a match needs THREE things
    # to agree — the amount to the cent, the payee's name, and a
    # plausible date. Real data proved why: the archive holds a booking
    # of exactly -54,95 € (Amazon) while a Telekom invoice asks for
    # 54,95 €, and a -7,86 € Rossmann card payment from 2021 next to a
    # 7,86 € fee notice. Amount alone would have "settled" both.

    # Company-form and title noise that must not count as a name match.
    _PAYEE_STOP = frozenset({
        "gmbh", "ag", "kg", "mbh", "co", "ohg", "gbr", "ug", "se", "ev",
        "deutschland", "deutsche", "und", "der", "die", "das", "den",
        "dr", "med", "dent", "prof", "herr", "frau", "firma",
    })

    @staticmethod
    def _payee_tokens(text: str | None) -> set[str]:
        from .finance.buckets import merchant_key
        return {
            w for w in merchant_key(text or "").split()
            if len(w) > 2 and w not in Database._PAYEE_STOP
        }

    def backfill_due_amounts(self, *, force: bool = False) -> int:
        """Read the amount out of each payment deadline's own text.

        Documents never stored one, and re-running the KI over the archive
        would cost money for something the text already says. Returns how
        many rows were filled.
        """
        from .finance.invoice_amount import find_invoice_amount
        where = "" if force else " AND due_amount IS NULL"
        # 🔴 Ein Kassenzettel trägt seine Summe in der eigenen Zeile — die
        # aus dem OCR-Text zu raten ist unnötig und schlägt oft fehl (kein
        # „Summe"-Etikett, kein €-Zeichen, Punkt statt Komma). Damit ein
        # Einkauf überhaupt auf einer Buchung wiedergefunden werden kann,
        # kommt der Betrag zuerst von dort.
        filled_receipts = 0
        with self._lock:
            rrows = self._conn.execute(
                "SELECT r.doc_id AS doc_id, r.total_amount AS total "
                "FROM receipts r JOIN documents d ON d.id = r.doc_id "
                "WHERE d.deleted_at IS NULL AND COALESCE(r.total_amount, 0) > 0"
                + ("" if force else " AND d.due_amount IS NULL")
            ).fetchall()
            for r in rrows:
                self._conn.execute(
                    "UPDATE documents SET due_amount = ?, due_amount_src = ? WHERE id = ?",
                    (round(float(r["total"]), 2), "receipt-total", int(r["doc_id"])),
                )
                filled_receipts += 1
            if filled_receipts:
                self._conn.commit()
        with self._lock:
            # Not just documents with a deadline: the owner wants the payment
            # shown on ANY scanned document a booking belongs to. Statements
            # and CSV containers are excluded — they list payments, they are
            # not themselves something that gets paid.
            rows = self._conn.execute(
                "SELECT id, extracted_text FROM documents "
                "WHERE deleted_at IS NULL "
                "  AND category NOT IN ('_csv_container', 'Kontoauszug') "
                "  AND COALESCE(extracted_text,'') != '' "
                "  AND (COALESCE(doc_date,'') != '' OR COALESCE(due_date,'') != '')"
                + where
            ).fetchall()
        filled = 0
        for row in rows:
            found = find_invoice_amount(row["extracted_text"] or "")
            if not found:
                continue
            amount, evidence = found
            with self._lock:
                self._conn.execute(
                    "UPDATE documents SET due_amount = ?, due_amount_src = ? WHERE id = ?",
                    (round(amount, 2), evidence, row["id"]),
                )
                self._conn.commit()
            filled += 1
        if filled or filled_receipts:
            logger.info("Deadlines: amount known for %d of %d documents "
                        "(+ %d receipt totals)", filled, len(rows), filled_receipts)
        return filled + filled_receipts

    def finance_match_due_payments(
        self, *, days_before: int = 5, days_after: int = 60,
    ) -> dict[str, int]:
        """Link open payment deadlines to the booking that settled them.

        Window: from `days_before` before the invoice date to `days_after`
        after the due date — bills get paid early and late, but not years
        apart. Credit notes (negative amount) are skipped: nothing is owed,
        so nothing can settle them.
        """
        from datetime import date as _date, timedelta as _td

        stats = {"matched": 0, "cleared": 0, "checked": 0}

        # First drop automatic links whose booking no longer exists — a
        # re-import rebuilds transaction rows, and a link pointing at a
        # deleted row would show a payment that is no longer there.
        with self._lock:
            stale = self._conn.execute(
                "SELECT d.id FROM documents d "
                " LEFT JOIN transactions t ON t.id = d.paid_tx_id "
                "WHERE d.paid_source = 'auto' AND d.paid_tx_id IS NOT NULL "
                "  AND t.id IS NULL"
            ).fetchall()
            for row in stale:
                self._conn.execute(
                    "UPDATE documents SET paid_tx_id = NULL, paid_at = '', "
                    "       paid_source = '' WHERE id = ?", (row["id"],)
                )
            if stale:
                self._conn.commit()
        stats["cleared"] = len(stale)

        with self._lock:
            # A bill without a formal deadline is still a bill. Anchor those
            # on their document date instead and allow a longer tail, since
            # nothing declares when they were due.
            docs = self._conn.execute(
                "SELECT id, sender, doc_date, due_date, due_amount FROM documents "
                "WHERE deleted_at IS NULL "
                "  AND category NOT IN ('_csv_container', 'Kontoauszug') "
                "  AND due_amount IS NOT NULL AND due_amount > 0 "
                "  AND COALESCE(paid_at,'') = '' "
                "  AND (COALESCE(due_date,'') != '' OR COALESCE(doc_date,'') != '')"
            ).fetchall()
        stats["checked"] = len(docs)

        # Collect EVERY plausible (bill, booking) pair first, then hand out
        # the closest ones. Taking the earliest candidate per bill looked
        # right until eight identical Telekom invoices arrived: the March
        # bill grabbed the April debit, the May bill took the July one, and
        # every later bill shifted along. A direct debit lands on the due
        # date, so the pair with the smallest date distance is the true one.
        with self._lock:
            taken_tx = {
                int(r["paid_tx_id"]) for r in self._conn.execute(
                    "SELECT paid_tx_id FROM documents WHERE paid_tx_id IS NOT NULL"
                ).fetchall()
            }

        pairs: list[tuple[int, int, int, int, str]] = []   # distance, doc, tx, ...
        for doc in docs:
            amount = round(float(doc["due_amount"]), 2)
            has_due = bool(doc["due_date"])
            try:
                due = _date.fromisoformat(doc["due_date"] if has_due else doc["doc_date"])
            except (TypeError, ValueError):
                continue
            try:
                start_d = _date.fromisoformat(doc["doc_date"]) if doc["doc_date"] else due
            except ValueError:
                start_d = due
            # The invoice date can sit after the due date in the archive (the
            # KI mixes the two up on some layouts), so anchor the window on
            # whichever comes first and still allow the usual lateness.
            anchor = min(start_d, due)
            lo = (anchor - _td(days=days_before)).isoformat()
            # Without a stated deadline there is nothing to be late against,
            # so allow the longer tail an undated invoice needs.
            tail = days_after if has_due else max(days_after, 90)
            hi = (due + _td(days=tail)).isoformat()

            wanted = self._payee_tokens(doc["sender"])
            if not wanted:
                # Without a payee to compare we would be matching on the
                # amount alone — exactly the mistake that turns an Amazon
                # purchase into a paid phone bill.
                continue

            with self._lock:
                cands = self._conn.execute(
                    "SELECT id, booking_date, amount, counterparty, purpose "
                    "FROM transactions "
                    "WHERE COALESCE(synthetic,0) = 0 "
                    "  AND ROUND(amount, 2) = ? "
                    "  AND booking_date BETWEEN ? AND ? ",
                    (-amount, lo, hi),
                ).fetchall()
            for tx in cands:
                if int(tx["id"]) in taken_tx:
                    continue
                have = self._payee_tokens(tx["counterparty"]) | self._payee_tokens(tx["purpose"])
                if not (wanted & have):
                    continue
                try:
                    bd = _date.fromisoformat(tx["booking_date"])
                except (TypeError, ValueError):
                    continue
                pairs.append((abs((bd - due).days), int(doc["id"]), int(tx["id"]),
                              0, tx["booking_date"]))

        # Closest first; ties resolved deterministically by document then
        # booking id, so a re-run produces the same links.
        pairs.sort(key=lambda p: (p[0], p[1], p[2]))
        used_docs: set[int] = set()
        for distance, doc_id, tx_id, _unused, booking_date in pairs:
            if doc_id in used_docs or tx_id in taken_tx:
                continue
            used_docs.add(doc_id)
            taken_tx.add(tx_id)
            with self._lock:
                self._conn.execute(
                    "UPDATE documents SET paid_tx_id = ?, paid_at = ?, paid_source = 'auto' "
                    "WHERE id = ?", (tx_id, booking_date, doc_id),
                )
                self._conn.commit()
            stats["matched"] += 1
            logger.info("Deadline #%s settled by booking #%s on %s (%d day(s) from the due date)",
                        doc_id, tx_id, booking_date, distance)
        return stats

    def deadline_payment(self, doc_id: int) -> dict[str, Any] | None:
        """The booking that settled this document, for the detail page."""
        with self._lock:
            row = self._conn.execute(
                "SELECT t.id, t.booking_date, t.amount, t.counterparty, t.purpose, "
                "       d.paid_source, d.due_amount, d.due_amount_src "
                "FROM documents d JOIN transactions t ON t.id = d.paid_tx_id "
                "WHERE d.id = ?", (doc_id,),
            ).fetchone()
        return dict(row) if row else None

    def set_deadline_paid(self, doc_id: int, tx_id: int | None) -> bool:
        """Link or unlink a payment by hand. `tx_id=None` clears the link."""
        with self._lock:
            if tx_id is None:
                cur = self._conn.execute(
                    "UPDATE documents SET paid_tx_id = NULL, paid_at = '', "
                    "       paid_source = '' WHERE id = ?", (doc_id,)
                )
                self._conn.commit()
                return cur.rowcount > 0
            tx = self._conn.execute(
                "SELECT booking_date FROM transactions WHERE id = ?", (tx_id,)
            ).fetchone()
            if tx is None:
                return False
            cur = self._conn.execute(
                "UPDATE documents SET paid_tx_id = ?, paid_at = ?, "
                "       paid_source = 'manual' WHERE id = ?",
                (tx_id, tx["booking_date"], doc_id),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def siblings_of(
        self,
        doc_id: int,
        *,
        category: str | None = None,
        subcategory: str | None = None,
        tag: str | None = None,
        status: str | None = None,
        year: str | None = None,
        query: str | None = None,
        trash: bool = False,
    ) -> dict[str, Any]:
        """Return prev / next document IDs for `doc_id` inside the same
        filtered listing — used by the document-detail keyboard nav so
        ←/→ jump to the previous / next document in the same Kategorie
        (and Jahr / Tag / etc.) ordering.

        Sort order matches `list_documents` default: doc_date DESC,
        falling back to created_at when doc_date is null. Returns
        `position` (1-based), `total`, `prev_id`, `next_id`. Position
        and prev/next are null when the doc isn't in the filter result
        — happens when the user navigates directly via URL with a
        filter the doc doesn't match."""
        # Reuse list_documents to share the WHERE-clause logic. Pull a
        # generous slice; finance / library accounts max out around a
        # few thousand docs in any one filter, and we only need IDs.
        rows = self.list_documents(
            category=category, subcategory=subcategory, tag=tag,
            status=status, year=year, query=query, trash=trash,
            limit=5000,
        )
        ids = [int(r["id"]) for r in rows]
        try:
            idx = ids.index(int(doc_id))
        except ValueError:
            return {
                "position": None, "total": len(ids),
                "prev_id": None, "next_id": None,
            }
        # Older entries are LATER in the list (DESC sort). For UX we
        # treat ←/→ as natural reading order: ← = older, → = newer.
        prev_id = ids[idx + 1] if idx + 1 < len(ids) else None
        next_id = ids[idx - 1] if idx - 1 >= 0           else None
        return {
            "position": idx + 1, "total": len(ids),
            "prev_id":  prev_id, "next_id": next_id,
        }

    # Whitelisted sort columns. Keys are the public sort names accepted
    # from the URL; values are the SQL expressions (NEVER interpolate raw
    # user input into ORDER BY). `relevance` is only meaningful on the
    # FTS path — it falls back to doc_date elsewhere.
    _SORT_EXPR = {
        "doc_date":   "COALESCE(NULLIF({p}doc_date, ''), {p}created_at)",
        "created_at": "{p}created_at",
        "sender":     "{p}sender COLLATE NOCASE",
        "subject":    "{p}subject COLLATE NOCASE",
        "category":   "{p}category COLLATE NOCASE",
        "file_size":  "{p}file_size",
        "confidence": "{p}confidence",
        "page_count": "{p}page_count",
        "relevance":  "rank",
    }

    @staticmethod
    def _fts_match_query(raw: str) -> str | None:
        """Turn free user input into a safe FTS5 MATCH expression.

        FTS5 treats `" * ( ) : . - + ^ AND OR NOT` as syntax — a raw
        user string like "e.on-2024" or an unbalanced quote throws
        `fts5: syntax error`. We extract alphanumeric runs (incl.
        German umlauts / Latin-1 accents), append `*` to each for
        prefix matching ("rechn" → matches "Rechnung"), and AND them
        together implicitly. Bareword prefix tokens can't contain
        special chars, so this can never produce invalid FTS5.
        Returns None when nothing searchable remains.
        """
        import re as _re
        if not raw:
            return None
        parts = _re.findall(r"[0-9A-Za-zÀ-ÿ_]+", raw)
        if not parts:
            return None
        # Quote each token then append '*'. A bare `AND*` / `OR*` /
        # `NOT*` is parsed as an FTS5 operator and throws "syntax
        # error near AND"; the quoted form `"and"*` is treated as a
        # literal prefix term and is operator-safe (verified against
        # sqlite fts5). Tokens are already special-char-free from the
        # regex above, so there are no embedded quotes to escape.
        return " ".join(f'"{p}"*' for p in parts)

    def _order_clause(self, order_by: str, sort_dir: str, *, fts: bool) -> str:
        expr_tmpl = self._SORT_EXPR.get(order_by)
        if expr_tmpl is None or (order_by == "relevance" and not fts):
            # Default: newest document date first.
            expr_tmpl = self._SORT_EXPR["doc_date"]
        prefix = "d." if fts and order_by != "relevance" else ""
        expr = expr_tmpl.format(p=prefix)
        direction = "ASC" if str(sort_dir).lower() == "asc" else "DESC"
        if order_by == "relevance":
            # FTS5 `rank` is most-relevant-first when ascending.
            direction = "ASC" if direction == "DESC" else "DESC"
        # Stable tie-breaker so pagination is deterministic.
        tie = "d.id" if fts else "id"
        return f"ORDER BY {expr} {direction}, {tie} DESC"

    def list_documents(
        self,
        *,
        category: str | None = None,
        subcategory: str | None = None,
        tag: str | None = None,
        status: str | None = None,
        year: str | None = None,
        query: str | None = None,
        trash: bool = False,
        order_by: str = "doc_date",
        sort_dir: str = "desc",
        doc_from: str | None = None,   # ISO date, filter doc_date >=
        doc_to: str | None = None,     # ISO date, filter doc_date <=
        scan_from: str | None = None,  # ISO date, filter created_at day >=
        scan_to: str | None = None,    # ISO date, filter created_at day <=
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        # `_csv_container` is the sentinel category used by CSV-import
        # stub documents — they're never user-visible and must not
        # show up in the library or trash.
        trash_clause = (
            "(d.deleted_at IS NOT NULL AND d.category != '_csv_container')"
            if trash else
            "(d.deleted_at IS NULL AND d.category != '_csv_container')"
        )
        trash_clause_plain = trash_clause.replace("d.", "")
        tag_like = f'%"{tag}"%' if tag else None
        match_query = self._fts_match_query(query) if query else None

        def _common_filters(prefix: str) -> tuple[list[str], list[Any]]:
            """Filter clauses shared by the FTS and non-FTS paths.
            `prefix` is 'd.' on the FTS join, '' otherwise."""
            cl: list[str] = []
            pr: list[Any] = []
            if category:
                cl.append(f"{prefix}category = ?"); pr.append(category)
            if subcategory:
                cl.append(f"{prefix}subcategory = ?"); pr.append(subcategory)
            if tag_like:
                cl.append(f"{prefix}tags LIKE ?"); pr.append(tag_like)
            if status:
                cl.append(f"{prefix}status = ?"); pr.append(status)
            else:
                # A `duplicate` row is not a document — it is the note
                # "you uploaded this file again", pointing at the copy that
                # is already filed. Re-uploading a Sammeldownload of 130
                # statements produced 116 of them and buried the library.
                # They stay reachable via ?status=duplicate.
                cl.append(f"{prefix}status != 'duplicate'")
            if year == "unknown":
                cl.append(f"({prefix}doc_date IS NULL OR {prefix}doc_date = '')")
            elif year:
                cl.append(f"substr({prefix}doc_date, 1, 4) = ?"); pr.append(year)
            if doc_from:
                cl.append(f"{prefix}doc_date >= ?"); pr.append(doc_from)
            if doc_to:
                cl.append(f"{prefix}doc_date <= ?"); pr.append(doc_to)
            if scan_from:
                cl.append(f"substr({prefix}created_at, 1, 10) >= ?")
                pr.append(scan_from)
            if scan_to:
                cl.append(f"substr({prefix}created_at, 1, 10) <= ?")
                pr.append(scan_to)
            return cl, pr

        if match_query:
            sql = (
                "SELECT d.* FROM documents d "
                "JOIN documents_fts f ON f.rowid = d.id "
                f"WHERE documents_fts MATCH ? AND {trash_clause}"
            )
            params.append(match_query)
            extra, extra_p = _common_filters("d.")
            for c in extra:
                sql += f" AND {c}"
            params += extra_p
            sql += " " + self._order_clause(order_by, sort_dir, fts=True)
            sql += " LIMIT ? OFFSET ?"
            params += [limit, offset]
        else:
            where = [trash_clause_plain]
            extra, extra_p = _common_filters("")
            where += extra
            params += extra_p
            where_sql = " WHERE " + " AND ".join(where)
            order_sql = self._order_clause(order_by, sort_dir, fts=False)
            sql = (
                "SELECT * FROM documents" + where_sql
                + f" {order_sql} LIMIT ? OFFSET ?"
            )
            params += [limit, offset]

        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def all_tags(self, trash: bool = False) -> list[tuple[str, int]]:
        """Return distinct tags with their occurrence count (excl. trash)."""
        import json as _json
        trash_clause = (
            "(deleted_at IS NOT NULL AND category != '_csv_container')"
            if trash else
            "(deleted_at IS NULL AND category != '_csv_container')"
        )
        with self._lock:
            rows = self._conn.execute(
                f"SELECT tags FROM documents WHERE {trash_clause} AND tags IS NOT NULL AND tags != '[]'"
            ).fetchall()
        counts: dict[str, int] = {}
        for r in rows:
            try:
                for t in _json.loads(r["tags"] or "[]"):
                    if t:
                        counts[t] = counts.get(t, 0) + 1
            except Exception:
                continue
        return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))

    def count_documents(
        self, *, category: str | None = None, status: str | None = None,
        trash: bool = False,
    ) -> int:
        where = [
            "(deleted_at IS NOT NULL AND category != '_csv_container')"
            if trash else
            "(deleted_at IS NULL AND category != '_csv_container')"
        ]
        params: list[Any] = []
        if category:
            where.append("category = ?")
            params.append(category)
        if status:
            where.append("status = ?")
            params.append(status)
        where_sql = " WHERE " + " AND ".join(where)
        with self._lock:
            row = self._conn.execute(
                f"SELECT COUNT(*) FROM documents{where_sql}", params
            ).fetchone()
        return int(row[0]) if row else 0

    def mark_deleted(self, doc_id: int, new_library_path: str) -> None:
        """Flag a document as deleted and update its on-disk location."""
        with self._lock:
            self._conn.execute(
                "UPDATE documents SET deleted_at = ?, library_path = ? WHERE id = ?",
                (datetime.now().isoformat(timespec="seconds"), new_library_path, doc_id),
            )

    def mark_restored(self, doc_id: int, new_library_path: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE documents SET deleted_at = NULL, library_path = ? WHERE id = ?",
                (new_library_path, doc_id),
            )

    def purge(self, doc_id: int) -> None:
        """Permanent delete — row gone, FTS index cleaned via trigger."""
        with self._lock:
            self._conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))

    def stats(self) -> dict[str, Any]:
        """Aggregate stats. Excludes trash from all counts and sums."""
        with self._lock:
            totals = self._conn.execute("""
                SELECT COUNT(*) AS n,
                       COALESCE(SUM(input_tokens), 0)            AS input_tokens,
                       COALESCE(SUM(output_tokens), 0)           AS output_tokens,
                       COALESCE(SUM(cache_creation_tokens), 0)   AS cache_creation_tokens,
                       COALESCE(SUM(cache_read_tokens), 0)       AS cache_read_tokens,
                       COALESCE(SUM(cost_usd), 0)                AS cost_usd,
                       SUM(CASE WHEN status='duplicate' THEN 1 ELSE 0 END) AS duplicates
                FROM documents WHERE deleted_at IS NULL AND category != '_csv_container'
            """).fetchone()
            by_cat = self._conn.execute("""
                SELECT category, COUNT(*) AS n, COALESCE(SUM(cost_usd),0) AS cost_usd
                FROM documents WHERE deleted_at IS NULL AND category != '_csv_container'
                GROUP BY category ORDER BY n DESC
            """).fetchall()
            by_status = self._conn.execute("""
                SELECT status, COUNT(*) AS n FROM documents
                WHERE deleted_at IS NULL AND category != '_csv_container'
                GROUP BY status
            """).fetchall()
            by_month = self._conn.execute("""
                SELECT substr(created_at,1,7) AS month,
                       COUNT(*) AS n,
                       COALESCE(SUM(cost_usd),0) AS cost_usd
                FROM documents WHERE deleted_at IS NULL AND category != '_csv_container'
                GROUP BY month ORDER BY month DESC LIMIT 12
            """).fetchall()
            trash_count = self._conn.execute(
                "SELECT COUNT(*) FROM documents "
                "WHERE deleted_at IS NOT NULL AND category != '_csv_container'"
            ).fetchone()[0]
        return {
            "totals": dict(totals) if totals else {},
            "by_category": [dict(r) for r in by_cat],
            "by_status": [dict(r) for r in by_status],
            "by_month": [dict(r) for r in by_month],
            "trash_count": int(trash_count),
        }

    def distinct_years(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT substr(doc_date,1,4) AS y FROM documents "
                "WHERE doc_date IS NOT NULL AND doc_date != '' "
                "AND deleted_at IS NULL AND category != '_csv_container' "
                "ORDER BY y DESC"
            ).fetchall()
        return [r["y"] for r in rows if r["y"]]

    def tree(self) -> dict[str, Any]:
        """Build a year -> category aggregation for the library tree view.

        Documents without a doc_date fall into a '—' year bucket so they stay
        reachable. Review/failed status buckets are returned separately so the
        UI can show them as quick-filters next to the tree. Trash is not
        included — it lives in its own view.
        """
        with self._lock:
            # `duplicate` rows are excluded here for the same reason the
            # list excludes them: the counter in the sidebar has to agree
            # with the number of cards the user actually sees. They are
            # counted on their own quick-filter below.
            total_row = self._conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(cost_usd),0) AS cost_usd "
                "FROM documents WHERE deleted_at IS NULL AND category != '_csv_container' "
                "  AND status != 'duplicate'"
            ).fetchone()
            rows = self._conn.execute("""
                SELECT COALESCE(NULLIF(substr(doc_date,1,4), ''), '—') AS year,
                       category,
                       COUNT(*) AS n,
                       COALESCE(SUM(cost_usd), 0) AS cost_usd
                FROM documents WHERE deleted_at IS NULL AND category != '_csv_container'
                  AND status != 'duplicate'
                GROUP BY year, category
                ORDER BY year DESC, n DESC
            """).fetchall()
            status_rows = self._conn.execute("""
                SELECT status, COUNT(*) AS n FROM documents
                WHERE deleted_at IS NULL AND category != '_csv_container'
                  AND status IN ('review', 'failed', 'duplicate')
                GROUP BY status
            """).fetchall()
            trash_row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM documents "
                "WHERE deleted_at IS NOT NULL AND category != '_csv_container'"
            ).fetchone()

        by_year: dict[str, dict[str, Any]] = {}
        for r in rows:
            y = r["year"] or "—"
            key = "unknown" if y == "—" else y
            bucket = by_year.setdefault(
                y, {"year": y, "key": key, "count": 0, "cost_usd": 0.0, "categories": []}
            )
            bucket["count"] += int(r["n"])
            bucket["cost_usd"] += float(r["cost_usd"] or 0)
            bucket["categories"].append({
                "name": r["category"], "count": int(r["n"]),
                "cost_usd": float(r["cost_usd"] or 0),
            })

        years = sorted(by_year.values(), key=lambda b: (b["year"] == "—", b["year"]), reverse=True)
        return {
            "total": dict(total_row) if total_row else {"n": 0, "cost_usd": 0.0},
            "years": years,
            "statuses": {r["status"]: int(r["n"]) for r in status_rows},
            "trash": int(trash_row["n"]) if trash_row else 0,
        }

    # ---------- Receipts (Kassenzettel) ----------

    def upsert_receipt(
        self,
        doc_id: int,
        *,
        shop_name: str = "",
        shop_type: str = "",
        payment_method: str = "",
        total_amount: float | None = None,
        currency: str = "EUR",
        receipt_date: str = "",
        items: list[dict] | None = None,
        extra_json: str = "",
    ) -> int:
        """Create or replace the receipt + line items for a document.

        Existing items get wiped and re-inserted (callers pass the full new
        list). Returns the receipt row id."""
        import json as _json
        items = items or []
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            existing = self._conn.execute(
                "SELECT id FROM receipts WHERE doc_id = ?", (doc_id,)
            ).fetchone()
            if existing:
                receipt_id = int(existing["id"])
                self._conn.execute(
                    """UPDATE receipts SET shop_name=?, shop_type=?, payment_method=?,
                       total_amount=?, currency=?, receipt_date=?, extra_json=?
                       WHERE id=?""",
                    (shop_name, shop_type, payment_method, total_amount,
                     currency, receipt_date, extra_json, receipt_id),
                )
                self._conn.execute(
                    "DELETE FROM receipt_items WHERE receipt_id = ?", (receipt_id,)
                )
            else:
                cur = self._conn.execute(
                    """INSERT INTO receipts
                       (doc_id, shop_name, shop_type, payment_method,
                        total_amount, currency, receipt_date, extra_json, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (doc_id, shop_name, shop_type, payment_method,
                     total_amount, currency, receipt_date, extra_json, now),
                )
                receipt_id = cur.lastrowid or 0
            for i, it in enumerate(items):
                self._conn.execute(
                    """INSERT INTO receipt_items
                       (receipt_id, name, quantity, unit_price, total_price,
                        item_category, line_no)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (receipt_id, it.get("name") or "", it.get("quantity"),
                     it.get("unit_price"), it.get("total_price"),
                     it.get("item_category") or "", i),
                )
        return receipt_id

    def get_receipt(self, doc_id: int) -> dict[str, Any] | None:
        """Return the receipt + items for a document, or None."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM receipts WHERE doc_id = ?", (doc_id,)
            ).fetchone()
            if not row:
                return None
            items = self._conn.execute(
                "SELECT * FROM receipt_items WHERE receipt_id = ? ORDER BY line_no, id",
                (row["id"],),
            ).fetchall()
        receipt = dict(row)
        receipt["items"] = [dict(i) for i in items]
        return receipt

    def delete_receipt(self, doc_id: int) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM receipts WHERE doc_id = ?", (doc_id,))
            return cur.rowcount > 0

    def receipt_summary(self) -> dict[str, Any]:
        """Top-level numbers for the analytics dashboard."""
        with self._lock:
            tot = self._conn.execute(
                """SELECT COUNT(*) AS n, COALESCE(SUM(total_amount), 0) AS total
                   FROM receipts r
                   JOIN documents d ON d.id = r.doc_id
                   WHERE d.deleted_at IS NULL"""
            ).fetchone()
            item_count = self._conn.execute(
                """SELECT COUNT(*) AS n FROM receipt_items i
                   JOIN receipts r ON r.id = i.receipt_id
                   JOIN documents d ON d.id = r.doc_id
                   WHERE d.deleted_at IS NULL"""
            ).fetchone()
            shops = self._conn.execute(
                """SELECT shop_type AS type, COUNT(*) AS n,
                          COALESCE(SUM(total_amount), 0) AS total
                   FROM receipts r
                   JOIN documents d ON d.id = r.doc_id
                   WHERE d.deleted_at IS NULL AND shop_type != ''
                   GROUP BY shop_type ORDER BY total DESC"""
            ).fetchall()
            cats = self._conn.execute(
                """SELECT item_category AS category, COUNT(*) AS n,
                          COALESCE(SUM(total_price), 0) AS total
                   FROM receipt_items i
                   JOIN receipts r ON r.id = i.receipt_id
                   JOIN documents d ON d.id = r.doc_id
                   WHERE d.deleted_at IS NULL AND item_category != ''
                   GROUP BY item_category ORDER BY total DESC"""
            ).fetchall()
        return {
            "receipt_count": int(tot["n"]) if tot else 0,
            "item_count":    int(item_count["n"]) if item_count else 0,
            "total_spent":   float(tot["total"]) if tot else 0.0,
            "by_shop_type":  [dict(r) for r in shops],
            "by_item_category": [dict(r) for r in cats],
        }

    def receipt_monthly(self, months: int = 12) -> list[dict[str, Any]]:
        """Spend per month for the last N months, oldest first."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT substr(receipt_date, 1, 7) AS month,
                          COUNT(*) AS receipts,
                          COALESCE(SUM(total_amount), 0) AS total
                   FROM receipts r
                   JOIN documents d ON d.id = r.doc_id
                   WHERE d.deleted_at IS NULL
                     AND receipt_date IS NOT NULL AND receipt_date != ''
                   GROUP BY month
                   ORDER BY month DESC
                   LIMIT ?""",
                (months,),
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def receipts_list(
        self,
        *,
        shop_type: str | None = None,
        start: str | None = None,    # ISO date inclusive
        end: str | None = None,      # ISO date inclusive
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        where = ["d.deleted_at IS NULL"]
        params: list[Any] = []
        if shop_type:
            where.append("r.shop_type = ?")
            params.append(shop_type)
        if start:
            where.append("r.receipt_date >= ?")
            params.append(start)
        if end:
            where.append("r.receipt_date <= ?")
            params.append(end)
        sql = (
            "SELECT r.*, d.subject AS doc_subject, d.library_path, "
            "       d.paid_tx_id, d.paid_at, d.paid_source, "
            "       t.booking_date AS booking_date, t.amount AS booking_amount, "
            "       t.counterparty AS booking_counterparty, "
            "       a.bank_name AS booking_bank, a.iban_last4 AS booking_iban_last4 "
            "FROM receipts r JOIN documents d ON d.id = r.doc_id "
            "LEFT JOIN transactions t ON t.id = d.paid_tx_id "
            "LEFT JOIN accounts a ON a.id = t.account_id "
            "WHERE " + " AND ".join(where) +
            " ORDER BY r.receipt_date DESC, r.id DESC LIMIT ?"
        )
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        out = [dict(r) for r in rows]
        for r in out:
            r["pay_state"] = self._receipt_pay_state(r)
        return out

    def _statement_covers(self, day: str) -> bool:
        """Liegt dieser Tag in einem eingelesenen Kontoauszug-Zeitraum?

        🔴 Das ist der Unterschied zwischen „bar bezahlt" und „der Auszug
        fehlt noch". Ohne diese Prüfung würde jeder Kassenzettel aus einem
        nicht importierten Monat als Barzahlung gelten.
        """
        if not day:
            return False
        cache = getattr(self, "_cover_cache", None)
        if cache is None:
            cache = self._cover_cache = {}
        if day in cache:
            return cache[day]
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM statements s "
                "LEFT JOIN accounts a ON a.id = s.account_id "
                "WHERE COALESCE(a.is_savings, 0) = 0 "
                "  AND COALESCE(s.period_start,'') != '' AND COALESCE(s.period_end,'') != '' "
                "  AND s.period_start <= ? AND s.period_end >= ? LIMIT 1",
                (day, day),
            ).fetchone()
            if row is None:
                # 🔴 Ein CSV-Import legt KEINEN Auszugszeitraum an (nur
                # Kontoauszug-PDFs tun das). Für solche Konten zählt daher,
                # ob rund um den Tag überhaupt gebucht wurde: liegen im
                # Fenster von ±10 Tagen Buchungen, ist der Zeitraum
                # eingelesen — fehlt dort alles, ist nichts bewiesen.
                try:
                    d0 = date.fromisoformat(day)
                except ValueError:
                    cache[day] = False
                    return False
                lo = (d0 - timedelta(days=10)).isoformat()
                hi = (d0 + timedelta(days=10)).isoformat()
                row = self._conn.execute(
                    "SELECT 1 FROM transactions t "
                    "LEFT JOIN accounts a ON a.id = t.account_id "
                    "WHERE COALESCE(a.is_savings, 0) = 0 "
                    "  AND COALESCE(t.synthetic, 0) = 0 "
                    "  AND t.booking_date BETWEEN ? AND ? LIMIT 1",
                    (lo, hi),
                ).fetchone()
        cache[day] = row is not None
        return cache[day]

    def _receipt_pay_state(self, r: dict[str, Any]) -> str:
        """`matched` · `cash` · `open` — woran die Zahlung hängt.

        Reihenfolge der Beweise: eine gefundene Buchung schlägt alles;
        sonst zählt, was der Kassenzettel selbst über die Zahlart sagt;
        erst danach wird aus „keine Buchung, obwohl der Zeitraum
        eingelesen ist" auf Bargeld geschlossen.
        """
        if r.get("paid_tx_id"):
            return "matched"
        if (r.get("payment_method") or "").lower() == "bar" or int(r.get("cash_confirmed") or 0):
            return "cash"
        if self._statement_covers(str(r.get("receipt_date") or "")[:10]):
            return "cash"
        return "open"

    def receipt_set_cash_category(self, receipt_id: int, category: str,
                                  *, confirm_cash: bool = True) -> bool:
        """Kategorie für einen bar bezahlten Kassenzettel setzen."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE receipts SET cash_category = ?, cash_confirmed = ? WHERE id = ?",
                (category or "", 1 if (confirm_cash and category) else 0, int(receipt_id)),
            )
            self._conn.commit()
        return cur.rowcount > 0

    def receipt_payment_overview(self, *, start: str = "", end: str = "") -> dict[str, Any]:
        """Wie viel hing am Konto, wie viel war Bargeld, was ist offen —
        plus die Bargeld-Summen je Kategorie."""
        where = ["d.deleted_at IS NULL"]
        params: list[Any] = []
        if start:
            where.append("r.receipt_date >= ?")
            params.append(start)
        if end:
            where.append("r.receipt_date <= ?")
            params.append(end)
        with self._lock:
            rows = self._conn.execute(
                "SELECT r.id, r.receipt_date, r.total_amount, r.payment_method, "
                "       r.cash_category, r.cash_confirmed, r.shop_name, r.shop_type, "
                "       d.paid_tx_id "
                "FROM receipts r JOIN documents d ON d.id = r.doc_id "
                "WHERE " + " AND ".join(where), params,
            ).fetchall()
        out = {"matched": {"count": 0, "total": 0.0},
               "cash": {"count": 0, "total": 0.0},
               "open": {"count": 0, "total": 0.0},
               "cash_uncategorised": 0,
               "by_category": []}
        cats: dict[str, dict[str, Any]] = {}
        for raw in rows:
            r = dict(raw)
            state = self._receipt_pay_state(r)
            amount = float(r.get("total_amount") or 0.0)
            if amount < 0:            # Pfandrückgabe & Co. sind keine Ausgabe
                amount = 0.0
            out[state]["count"] += 1
            out[state]["total"] = round(out[state]["total"] + amount, 2)
            if state == "cash":
                cat = (r.get("cash_category") or "").strip()
                if not cat:
                    out["cash_uncategorised"] += 1
                    continue
                e = cats.setdefault(cat, {"category": cat, "total": 0.0, "count": 0})
                e["total"] = round(e["total"] + amount, 2)
                e["count"] += 1
        out["by_category"] = sorted(cats.values(), key=lambda x: -x["total"])
        return out

    def receipt_items_search(
        self,
        *,
        query: str | None = None,
        item_category: str | None = None,
        shop_type: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        where = ["d.deleted_at IS NULL"]
        params: list[Any] = []
        if query:
            where.append("i.name LIKE ?")
            params.append(f"%{query}%")
        if item_category:
            where.append("i.item_category = ?")
            params.append(item_category)
        if shop_type:
            where.append("r.shop_type = ?")
            params.append(shop_type)
        if start:
            where.append("r.receipt_date >= ?")
            params.append(start)
        if end:
            where.append("r.receipt_date <= ?")
            params.append(end)
        sql = (
            "SELECT i.*, r.shop_name, r.shop_type, r.receipt_date, r.doc_id "
            "FROM receipt_items i "
            "JOIN receipts r ON r.id = i.receipt_id "
            "JOIN documents d ON d.id = r.doc_id "
            "WHERE " + " AND ".join(where) +
            " ORDER BY r.receipt_date DESC, i.line_no LIMIT ?"
        )
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    # ---------- Finance (bank statements / Kontoauszüge) ----------

    def upsert_account(
        self,
        *,
        bank_name: str,
        iban_hash: str,
        iban: str = "",
        iban_last4: str = "",
        account_holder: str = "",
        currency: str = "EUR",
    ) -> int:
        """Idempotent: returns id, creating the row if needed. iban_hash is
        the dedup key — same IBAN seen via two different statements lands on
        the same account row even if one was pseudonymised."""
        if not iban_hash:
            raise ValueError("iban_hash is required")
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            row = self._conn.execute(
                "SELECT id FROM accounts WHERE iban_hash = ?", (iban_hash,)
            ).fetchone()
            if row:
                acct_id = int(row["id"])
                # Backfill missing display fields if a later, less-redacted
                # statement gives us better data.
                self._conn.execute(
                    """UPDATE accounts SET
                         bank_name      = COALESCE(NULLIF(?, ''), bank_name),
                         iban           = COALESCE(NULLIF(?, ''), iban),
                         iban_last4     = COALESCE(NULLIF(?, ''), iban_last4),
                         account_holder = COALESCE(NULLIF(?, ''), account_holder),
                         currency       = COALESCE(NULLIF(?, ''), currency)
                       WHERE id = ?""",
                    (bank_name, iban, iban_last4, account_holder, currency, acct_id),
                )
                return acct_id
            cur = self._conn.execute(
                """INSERT INTO accounts
                     (bank_name, iban, iban_last4, iban_hash, account_holder, currency, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (bank_name, iban, iban_last4, iban_hash, account_holder, currency, now),
            )
            return cur.lastrowid or 0

    def list_accounts(self) -> list[dict[str, Any]]:
        """All accounts with a derived current `balance` and `net`.

        `balance` = start_balance + SUM(all booked amounts). Because CSV
        exports carry no running balance, this is the only balance we can
        offer — accurate iff start_balance was set (or the CSV covers the
        account's whole life, where 0 is correct). `net` is the pure
        cashflow (SUM of amounts) so the accounts card can show both the
        movement and the resulting balance."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT a.*,
                          COALESCE(a.is_savings, 0)    AS is_savings,
                          COALESCE(a.start_balance, 0) AS start_balance,
                          (SELECT COUNT(*) FROM statements   WHERE account_id = a.id) AS statement_count,
                          (SELECT COUNT(*) FROM transactions WHERE account_id = a.id) AS tx_count,
                          (SELECT COALESCE(SUM(amount), 0) FROM transactions
                             WHERE account_id = a.id) AS net
                   FROM accounts a
                   ORDER BY COALESCE(a.is_savings, 0) ASC, a.bank_name, a.id"""
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["balance"] = round(float(d.get("start_balance") or 0.0)
                                 + float(d.get("net") or 0.0), 2)
            d["is_savings"] = int(d.get("is_savings") or 0)
            out.append(d)
        return out

    def finance_game_ranking(self, *, periods: list[dict[str, Any]] | None = None,
                             limit: int = 24,
                             account_ids: list[int] | None = None) -> list[dict[str, Any]]:
        """Bestenliste des Spar-Spiels über ALLE Zeiträume.

        Wunsch: „nun brauche ich noch eine übersicht welcher monat am
        erfolgreichsten war, wo man alle sehen kann." Gewertet wird mit
        derselben Funktion wie die Karte auf /ausgaben
        (`finance.game.score_days`) — es gibt nur EINEN Regelsatz.

        Eine Abfrage über alle Buchungen, danach je Zeitraum eine
        Tagesreihe; die Buchungsfilter sind dieselben wie in
        `finance_spend_by_category` (keine Sparkonten, keine Umbuchungen,
        keine Sparkategorien, nur Abgänge).
        """
        from .finance.game import score_days
        saving = sorted(self.finance_saving_categories()) or ["__none__"]
        acc_sql = ""
        acc_args: tuple[Any, ...] = ()
        if account_ids:
            acc_sql = " AND t.account_id IN (" + ",".join("?" * len(account_ids)) + ")"
            acc_args = tuple(account_ids)
        with self._lock:
            rows = self._conn.execute(
                """SELECT t.booking_date AS day, SUM(-t.amount) AS spend
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  doc ON doc.id = s.doc_id
                   LEFT JOIN accounts a ON a.id = t.account_id
                   WHERE doc.deleted_at IS NULL
                     AND COALESCE(a.is_savings, 0) = 0
                     AND COALESCE(t.category, '') != 'uebertrag'
                     AND COALESCE(t.category, '') NOT IN (""" + ",".join("?" * len(saving)) + """)
                     AND t.amount < 0""" + acc_sql + """
                   GROUP BY t.booking_date""",
                (*saving, *acc_args),
            ).fetchall()
        per_day = {str(r["day"] or "")[:10]: float(r["spend"] or 0.0) for r in rows}
        # Einnahmen desselben Zuschnitts: sie entscheiden über den Bonus
        # fürs Monatsergebnis (Saldo am Ende des Zeitraums).
        with self._lock:
            in_rows = self._conn.execute(
                """SELECT t.booking_date AS day, SUM(t.amount) AS income
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  doc ON doc.id = s.doc_id
                   LEFT JOIN accounts a ON a.id = t.account_id
                   WHERE doc.deleted_at IS NULL
                     AND COALESCE(a.is_savings, 0) = 0
                     AND COALESCE(t.category, '') != 'uebertrag'
                     AND COALESCE(t.category, '') NOT IN (""" + ",".join("?" * len(saving)) + """)
                     AND t.amount > 0""" + acc_sql + """
                   GROUP BY t.booking_date""",
                (*saving, *acc_args),
            ).fetchall()
        per_day_in = {str(r["day"] or "")[:10]: float(r["income"] or 0.0) for r in in_rows}
        data_through = self._last_booking_date()
        today_iso = date.today().isoformat()

        plist: list[dict[str, Any]] = []
        if periods:
            for p in reversed(periods):          # neueste zuerst
                plist.append({"key": p["start"], "start": p["start"], "end": p["end"],
                              "is_current": bool(p.get("is_current"))})
        else:
            for m in self.finance_available_periods()["months"]:
                y, mo = int(m[:4]), int(m[5:7])
                last = 31 if mo in (1, 3, 5, 7, 8, 10, 12) else 30 if mo != 2 else (
                    29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28)
                plist.append({"key": m, "start": f"{m}-01", "end": f"{m}-{last:02d}",
                              "is_current": m == today_iso[:7]})

        out: list[dict[str, Any]] = []
        for p in plist[:limit]:
            slice_ = {k: v for k, v in per_day.items() if p["start"] <= k <= p["end"]}
            income = sum(v for k, v in per_day_in.items() if p["start"] <= k <= p["end"])
            days = _day_series(p["start"], p["end"], slice_, data_through)
            out.append({**p, **score_days(days, income)})

        # Bestenliste: nach Punktequote, nicht nach Punkten — ein kurzer
        # Monat (oder der laufende) hat sonst nie eine Chance. Zeiträume
        # ohne Wertung stehen hinten, chronologisch.
        rated = [x for x in out if x["rated"]]
        unrated = [x for x in out if not x["rated"]]
        rated.sort(key=lambda x: (-x["share"], -x["points"], x["start"]))
        for i, x in enumerate(rated):
            x["place"] = i + 1
        for x in unrated:
            x["place"] = 0
        return rated + unrated

    def _last_booking_date(self) -> str:
        """Jüngster Buchungstag im Bestand — bis hierhin sind die Konten
        eingelesen. Alles danach ist „noch nichts da", nicht „nichts
        ausgegeben"."""
        try:
            with self._lock:
                row = self._conn.execute(
                    "SELECT MAX(booking_date) AS d FROM transactions"
                ).fetchone()
            return str(row["d"] or "") if row else ""
        except Exception:  # noqa: BLE001
            return ""

    def finance_drop_obsolete_transfer_overrides(self) -> int:
        """Release hand-set categories on bookings that turned out to be
        transfers between the user's own accounts.

        A pinned category is a statement about a PAYEE: "money to this
        recipient is Nebenkosten". When the recipient later turns out to be
        another of your own accounts — normally because that account was
        imported afterwards — the statement is about something that is not
        an expense at all, and keeping it double-counts the money (once
        leaving the Giro, once as the real payment from the other account).

        Worse, it was not even stable: `finance_retag_transfers()` set the
        booking to `uebertrag`, the next `finance_reclassify()` restored the
        pin, so the category depended on which background job ran last.

        The previous value is kept in `meta` under
        `finance.transfer_override.<tx_hash>` so nothing is lost.
        Returns the number of pins released.
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT o.tx_hash, o.category
                     FROM transaction_category_overrides o
                     JOIN transactions t ON t.tx_hash = o.tx_hash
                    WHERE o.category != 'uebertrag'
                      AND t.counterparty_iban IS NOT NULL AND t.counterparty_iban != ''
                      AND t.counterparty_iban IN (SELECT iban FROM accounts
                                                   WHERE iban IS NOT NULL AND iban != '')
                      AND t.counterparty_iban != (SELECT COALESCE(a.iban,'') FROM accounts a
                                                   WHERE a.id = t.account_id)"""
            ).fetchall()
        for row in rows:
            self.meta_set(f"finance.transfer_override.{row['tx_hash']}", row["category"])
            with self._lock:
                self._conn.execute(
                    "DELETE FROM transaction_category_overrides WHERE tx_hash = ?",
                    (row["tx_hash"],),
                )
                self._conn.commit()
            logger.info("Finance: released the hand-set category %r — the counterpart "
                        "turned out to be one of your own accounts (booking %s…)",
                        row["category"], row["tx_hash"][:10])
        return len(rows)

    def finance_retag_transfers(self) -> int:
        """Mark bookings between the user's OWN accounts as `uebertrag`.

        Every finance view already excludes `category = 'uebertrag'` (it is
        not income, not expense, not a bucket) — but until v0.41 nothing ever
        SET it. A CSV import of the Giro account booked "5.000 € to the
        Tagesgeld" as an expense, the Tagesgeld's CSV booked the same money
        as saved: counted twice, and the Giro's cashflow was wrong by every
        transfer. The only honest signal is the IBAN: a booking whose
        counterparty IBAN belongs to another of our accounts is a transfer,
        whatever its text says. Idempotent; re-run after every import (a
        newly imported account makes older bookings on the other accounts
        recognisable) and once at start-up for existing databases. Returns
        the number of rows changed."""
        # A pin on a booking that is really an own-account transfer would
        # be restored by the next reclassify and undo what we do here.
        try:
            self.finance_drop_obsolete_transfer_overrides()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Finance: releasing obsolete transfer pins failed: %s", exc)
        with self._lock:
            cur = self._conn.execute(
                """UPDATE transactions
                      SET category = 'uebertrag',
                          category_source = 'transfer',
                          category_reason = 'Gegenkonto …' || substr(counterparty_iban, -4) ||
                                            ' ist ein eigenes Konto — zählt weder als Einnahme noch als Ausgabe'
                    WHERE COALESCE(category, '') != 'uebertrag'
                      AND counterparty_iban IS NOT NULL AND counterparty_iban != ''
                      AND counterparty_iban IN (SELECT iban FROM accounts
                                                 WHERE iban IS NOT NULL AND iban != '')
                      AND counterparty_iban != (SELECT COALESCE(a.iban, '') FROM accounts a
                                                 WHERE a.id = transactions.account_id)"""
            )
            n = cur.rowcount if cur.rowcount is not None else 0
            self._conn.commit()
        # Credit cards (v0.43.1): the Giro's "KREDITKARTENABRECHNUNG … 0424"
        # and the card's "Ausgleich Kreditkarte" are the same money once
        # the card's own export is imported.
        try:
            n += self._retag_card_settlements()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Finance: card settlement re-tag failed: %s", exc)
        if n:
            logger.info("Finance: %d booking(s) between own accounts marked as transfer.", n)
        return int(n)

    def _retag_card_settlements(self) -> int:
        from .finance.csv_import import is_card_account, _cards_covering, cards_for_day
        from .finance.classify import classify
        own = [a.get("iban") for a in self.list_accounts() if a.get("iban")]
        cards = {i[-4:] for i in own if is_card_account(i)}
        if not cards:
            return 0
        cov = _cards_covering(self, set(own))
        holders = self.finance_holder_tokens()
        rows = self._tx_rows_for_classify(
            "COALESCE(t.category,'') != 'uebertrag' AND (a.iban LIKE 'CARD-%' OR t.purpose LIKE '%ABRECHNUNG%' "
            "OR t.purpose LIKE '%abrechnung%' OR t.purpose LIKE '%Abrechnung%' OR t.purpose LIKE '%ABR.%')"
        )
        updates = []
        for tx in rows:
            d = classify(tx, own_ibans=own, own_cards=cards_for_day(cov, tx.get("booking_date") or ""), holders=holders)
            if d.category == "uebertrag":
                updates.append((d.reason[:300], int(tx["id"])))
        if updates:
            with self._lock:
                self._conn.executemany(
                    "UPDATE transactions SET category = 'uebertrag', category_source = 'transfer', category_reason = ? WHERE id = ?",
                    updates,
                )
                self._conn.commit()
        return len(updates)



    # ---------- Finance: user-defined categories (v0.44) ----------

    def finance_custom_categories(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, label, is_fixed, is_saving, created_at FROM custom_categories ORDER BY label COLLATE NOCASE"
            ).fetchall()
        return [dict(r) for r in rows]

    def finance_category_keys(self) -> list[str]:
        """Built-in categories followed by the user's own ones; `sonstiges`
        always last so selects read naturally."""
        from .finance.categories import TX_CATEGORIES
        builtin = [c for c in TX_CATEGORIES if c != "sonstiges"]
        custom = [c["key"] for c in self.finance_custom_categories()]
        return builtin + custom + ["sonstiges"]

    @staticmethod
    def _category_slug(label: str) -> str:
        import re as _re
        s = (label or "").strip().lower()
        s = (s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss"))
        s = _re.sub(r"[^a-z0-9]+", "-", s).strip("-")
        return s[:40]

    def finance_saving_categories(self) -> set[str]:
        """Categories whose outflow is 'gespart / angelegt', not spending."""
        from .finance.categories import SAVING_CATEGORIES
        return set(SAVING_CATEGORIES) | {c["key"] for c in self.finance_custom_categories() if c.get("is_saving")}

    def finance_saved_external(self) -> dict[str, Any]:
        """Money paid into savings products held elsewhere (fund plans,
        endowment insurance) since the data begins — payments, not market
        value, which DocuSort cannot know."""
        cats = sorted(self.finance_saving_categories())
        if not cats:
            return {"total": 0.0, "by_category": [], "first": "", "last": ""}
        ph = ",".join("?" * len(cats))
        with self._lock:
            rows = self._conn.execute(
                "SELECT t.category, COALESCE(-SUM(t.amount), 0) AS paid, COUNT(*) AS n, MIN(t.booking_date) a, MAX(t.booking_date) b "
                "FROM transactions t JOIN statements s ON s.id = t.statement_id JOIN documents d ON d.id = s.doc_id "
                "LEFT JOIN accounts a ON a.id = t.account_id "
                f"WHERE d.deleted_at IS NULL AND COALESCE(a.is_savings, 0) = 0 AND t.category IN ({ph}) "
                "GROUP BY t.category ORDER BY paid DESC", cats,
            ).fetchall()
        by = [dict(r) for r in rows]
        return {"total": round(sum(float(r["paid"]) for r in by), 2), "by_category": by,
                "first": min((r["a"] for r in by if r["a"]), default=""), "last": max((r["b"] for r in by if r["b"]), default="")}

    def finance_category_add(self, label: str, *, is_fixed: bool = False, is_saving: bool = False) -> dict[str, Any]:
        from .finance.categories import TX_CATEGORIES
        label = (label or "").strip()[:60]
        key = self._category_slug(label)
        if not label or not key:
            raise ValueError("Bitte einen Namen angeben.")
        if key in TX_CATEGORIES:
            raise ValueError(f"„{label}“ gibt es schon als eingebaute Kategorie.")
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            exists = self._conn.execute("SELECT key FROM custom_categories WHERE key = ?", (key,)).fetchone()
            if exists:
                raise ValueError(f"„{label}“ gibt es schon.")
            self._conn.execute(
                "INSERT INTO custom_categories (key, label, is_fixed, is_saving, created_at) VALUES (?, ?, ?, ?, ?)",
                (key, label, 1 if is_fixed else 0, 1 if is_saving else 0, now),
            )
            self._conn.commit()
        return {"key": key, "label": label, "is_fixed": 1 if is_fixed else 0, "is_saving": 1 if is_saving else 0, "created_at": now}

    def finance_category_update(self, key: str, *, label: str | None = None,
                                is_fixed: bool | None = None, is_saving: bool | None = None) -> bool:
        sets, params = [], []
        if label is not None and label.strip():
            sets.append("label = ?"); params.append(label.strip()[:60])
        if is_fixed is not None:
            sets.append("is_fixed = ?"); params.append(1 if is_fixed else 0)
        if is_saving is not None:
            sets.append("is_saving = ?"); params.append(1 if is_saving else 0)
        if not sets:
            return False
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE custom_categories SET {', '.join(sets)} WHERE key = ?", [*params, key]
            )
            self._conn.commit()
        return bool(cur.rowcount)

    def finance_category_delete(self, key: str) -> dict[str, int]:
        """Remove a user category. Its bookings fall back to „sonstiges"
        (unpinned, so the next assignment re-teaches them), its rules and
        pins go with it."""
        with self._lock:
            if not self._conn.execute("SELECT 1 FROM custom_categories WHERE key = ?", (key,)).fetchone():
                return {"deleted": 0, "bookings": 0, "rules": 0}
            b = self._conn.execute(
                "UPDATE transactions SET category = 'sonstiges', category_source = 'none', "
                "       category_reason = 'Kategorie wurde gelöscht — bitte neu zuweisen' WHERE category = ?", (key,)
            ).rowcount
            r = self._conn.execute("DELETE FROM category_rules WHERE category = ?", (key,)).rowcount
            self._conn.execute("DELETE FROM transaction_category_overrides WHERE category = ?", (key,))
            self._conn.execute("DELETE FROM custom_categories WHERE key = ?", (key,))
            self._conn.commit()
        return {"deleted": 1, "bookings": int(b or 0), "rules": int(r or 0)}

    # ---------- Finance: explainable categories & learned rules (v0.43) ----------

    def tx_overrides_map(self) -> dict[str, str]:
        """{tx_hash → category} of every manual per-booking assignment."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT tx_hash, category FROM transaction_category_overrides"
            ).fetchall()
        return {r["tx_hash"]: r["category"] for r in rows if r["tx_hash"]}

    def finance_rules_list(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM category_rules ORDER BY updated_at DESC, id DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def finance_rule_upsert(self, match_kind: str, match_value: str, category: str, *,
                            source: str = "user", sample_name: str = "", direction: str = "") -> int:
        """Create/replace one rule. A user rule always wins over an AI rule
        for the same key; an AI proposal never overwrites a user rule.
        `direction`: '' both ways, 'in' only credits, 'out' only debits —
        a rule learned from one booking carries that booking's sign."""
        if match_kind not in ("iban", "merchant", "merchant_amount") or not match_value \
                or category not in self.finance_category_keys():
            return 0
        direction = direction if direction in ("in", "out") else ""
        from .finance.categories import INCOME_CATEGORIES
        if category in INCOME_CATEGORIES:
            direction = "in"    # Ausgaben sind nie Erstattungen
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            existing = self._conn.execute(
                "SELECT id, source FROM category_rules WHERE match_kind = ? AND match_value = ? AND direction = ?",
                (match_kind, match_value, direction),
            ).fetchone()
            if existing and existing["source"] == "user" and source == "ai":
                return int(existing["id"])
            # A directed rule supersedes an undirected one of the same key for
            # that direction: „Deka credits = Erstattung" must not leave a
            # both-ways „Deka = Erstattung" behind that still catches debits.
            # The newer intent wins: a directed rule replaces the both-ways
            # rule of that key, a both-ways rule replaces the directed ones.
            if direction:
                self._conn.execute(
                    "DELETE FROM category_rules WHERE match_kind = ? AND match_value = ? AND direction = ''",
                    (match_kind, match_value),
                )
            else:
                self._conn.execute(
                    "DELETE FROM category_rules WHERE match_kind = ? AND match_value = ? AND direction != ''",
                    (match_kind, match_value),
                )
            self._conn.execute(
                """INSERT INTO category_rules
                       (match_kind, match_value, category, source, sample_name, direction, hits, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
                   ON CONFLICT(match_kind, match_value, direction) DO UPDATE SET
                       category = excluded.category, source = excluded.source,
                       sample_name = CASE WHEN excluded.sample_name != '' THEN excluded.sample_name
                                          ELSE category_rules.sample_name END,
                       updated_at = excluded.updated_at""",
                (match_kind, match_value, category, source, (sample_name or "")[:200], direction, now, now),
            )
            rid = self._conn.execute(
                "SELECT id FROM category_rules WHERE match_kind = ? AND match_value = ? AND direction = ?",
                (match_kind, match_value, direction),
            ).fetchone()["id"]
            self._conn.commit()
        return int(rid)

    def finance_rule_delete(self, rule_id: int) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM category_rules WHERE id = ?", (int(rule_id),))
            self._conn.commit()
        return bool(cur.rowcount)

    def _tx_rows_for_classify(self, where: str = "", params: list[Any] | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT t.id, t.tx_hash, t.amount, t.counterparty, t.counterparty_iban, t.purpose, "
                "       t.tx_type, t.category, t.category_source, t.category_reason, t.booking_date, "
                "       a.iban AS account_iban, s.file_hash AS stmt_file_hash "
                "FROM transactions t "
                "JOIN statements s ON s.id = t.statement_id "
                "LEFT JOIN accounts a ON a.id = t.account_id "
                + (("WHERE " + where) if where else ""),
                params or [],
            ).fetchall()
        return [dict(r) for r in rows]

    def finance_reclassify(self, *, force: bool = False, where: str = "",
                           params: list[Any] | None = None) -> dict[str, int]:
        """Run the explainable classifier over bookings and store the
        decision (category + source + reason).

        force=False (start-up, after an import): a booking keeps a concrete
        category it already has unless the classifier is *more* certain
        (transfer, rule, AI) — we never downgrade a Kontoauszug-LLM label to
        „sonstiges", but we do resolve „sonstiges" and correct plain keyword
        hits (the old importer filed Aral under „freizeit").
        force=True (after a rule changed, scoped by `where`): the new
        decision always wins, except for manual pins and transfers.
        """
        from .finance.classify import (
            classify, RuleIndex, SOURCE_IMPORT, SOURCE_KEYWORD, SOURCE_BANK, SOURCE_NONE,
            SOURCE_MANUAL, SOURCE_TRANSFER, SOURCE_RULE, SOURCE_AI,
        )
        from .finance.categories import INCOME_CATEGORIES
        from .finance.csv_import import _cards_covering, cards_for_day
        rules = RuleIndex(self.finance_rules_list())
        own = {a.get("iban") for a in self.list_accounts() if a.get("iban")}
        card_cov = _cards_covering(self, own)
        pinned = self.tx_overrides_map()
        holders = self.finance_holder_tokens()
        # Synthetic rows (gap fillers, counter-legs) carry their own label.
        where = "COALESCE(t.synthetic, 0) = 0" + (f" AND ({where})" if where else "")
        rows = self._tx_rows_for_classify(where, params)
        strong = {SOURCE_MANUAL, SOURCE_TRANSFER, SOURCE_RULE, SOURCE_AI}
        stats = {"seen": len(rows), "changed": 0, "explained": 0, "kept": 0}
        updates: list[tuple[str, str, str, int]] = []
        for tx in rows:
            old_cat = (tx.get("category") or "").strip() or "sonstiges"
            old_src = (tx.get("category_source") or "").strip()
            d = classify(tx, rules=rules, own_ibans=own, pinned=pinned,
                         own_cards=cards_for_day(card_cov, tx.get("booking_date") or ""), holders=holders)
            new_cat, new_src, reason = d.category, d.source, d.reason
            try:
                _debit = float(tx.get("amount") or 0.0) < 0
            except (TypeError, ValueError):
                _debit = False
            if _debit and old_cat in INCOME_CATEGORIES:
                pass   # Ausgaben sind nie Erstattungen — the old label is void, take the fresh decision
            elif old_cat == "uebertrag" and new_src != SOURCE_MANUAL:
                # transfers are owned by finance_retag_transfers
                new_cat, new_src = "uebertrag", SOURCE_TRANSFER
                reason = tx.get("category_reason") or "Umbuchung zwischen eigenen Konten"
            elif not force and old_cat != "sonstiges" and new_src not in strong:
                # Keep the existing concrete label. Explain it: reproducible
                # by our patterns → that source; otherwise it came from the
                # Kontoauszug analysis (LLM) or an older importer.
                if new_cat == old_cat and new_src in (SOURCE_KEYWORD, SOURCE_BANK):
                    pass  # same result, take the fresh reason
                elif old_src in (SOURCE_MANUAL,):
                    new_cat, new_src, reason = old_cat, old_src, tx.get("category_reason") or reason
                elif old_src == SOURCE_RULE:
                    pass  # the rule that set this no longer applies (deleted, re-keyed, directed) → fresh decision
                elif new_src in (SOURCE_KEYWORD,) and new_cat != old_cat and old_src in ("", SOURCE_KEYWORD, SOURCE_BANK, SOURCE_NONE):
                    pass  # a built-in pattern now knows better than the old importer's guess
                else:
                    is_csv = str(tx.get("stmt_file_hash") or "").startswith("csv-import:")
                    legacy_guess = is_csv and old_src in ("", SOURCE_KEYWORD, SOURCE_NONE) and (
                        (tx.get("category_reason") or "") in ("", LEGACY_CSV_REASON)
                    )
                    if legacy_guess:
                        # The pre-0.43 importer's keyword guess („APPLE" in
                        # „DIG. KARTE (APPLE PAY)" → Abo, „MUELLER" → Haushalt)
                        # is not evidence: when no current pattern reproduces
                        # the label, the booking is honestly open again.
                        pass
                    else:
                        new_cat, new_src = old_cat, (old_src or (SOURCE_KEYWORD if is_csv else SOURCE_IMPORT))
                        reason = tx.get("category_reason") or (
                            LEGACY_CSV_REASON if is_csv
                            else "aus der Kontoauszug-Analyse übernommen (kein eingebautes Muster trifft)"
                        )
            if new_cat != old_cat:
                stats["changed"] += 1
            elif new_src != old_src or reason != (tx.get("category_reason") or ""):
                stats["explained"] += 1
            else:
                stats["kept"] += 1
                continue
            updates.append((new_cat, new_src, reason[:300], int(tx["id"])))
        if updates:
            with self._lock:
                self._conn.executemany(
                    "UPDATE transactions SET category = ?, category_source = ?, category_reason = ? WHERE id = ?",
                    updates,
                )
                self._conn.commit()
        return stats

    @staticmethod
    def _amount_clusters(amounts: list[float]) -> list[list[float]]:
        """Split absolute amounts into groups whose members lie within ~25 %
        of their neighbours: [20, 20, 770, 770] → [[20, 20], [770, 770]]."""
        vals = sorted(abs(float(a)) for a in amounts)
        clusters: list[list[float]] = []
        for v in vals:
            if clusters and (v <= clusters[-1][-1] * 1.25 + 1.0):
                clusters[-1].append(v)
            else:
                clusters.append([v])
        return clusters

    def finance_merchant_amount_profile(self, counterparty: str) -> dict[str, Any]:
        """How many distinct amount classes a counterparty has — decides
        whether a manual assignment should learn for the whole merchant or
        only for this amount (two contracts at one landlord)."""
        from .finance.buckets import merchant_key
        key = merchant_key(counterparty)
        if not key:
            return {"key": "", "clusters": 0, "count": 0}
        rows = self._tx_rows_for_classify("t.counterparty IS NOT NULL AND t.counterparty != ''")
        amounts = [float(r["amount"] or 0.0) for r in rows if merchant_key(r.get("counterparty")) == key]
        clusters = self._amount_clusters(amounts)
        # "Contracts" = amount classes that repeat (same amount at least twice,
        # within 1 %). Pocket money in varying amounts is one merchant, not
        # several contracts.
        rep = [c for c in clusters if len(c) >= 2 and (c[-1] - c[0]) <= max(0.01 * c[-1], 0.01)]
        covered = sum(len(c) for c in rep)
        contract_like = len(rep) >= 2 and covered >= 0.8 * max(1, len(amounts))
        return {"key": key, "clusters": len(clusters), "repeated_clusters": len(rep),
                "contract_like": contract_like, "count": len(amounts)}

    def transactions_set_category_learn(self, tx_ids: list[int], category: str, *,
                                        learn: bool = True, scope: str = "auto") -> dict[str, Any]:
        """Manual assignment: pin the chosen category on these bookings and —
        if `learn` — teach a rule for each booking's counterparty (IBAN and
        name) and apply it to every other unpinned booking of that
        counterparty. Returns counts so the UI can say what happened."""
        from .finance.classify import rule_targets
        from .finance.buckets import merchant_key as _mk
        n = self.transactions_set_category(tx_ids, category)
        with self._lock:
            self._conn.executemany(
                "UPDATE transactions SET category_source = 'manual', "
                "       category_reason = 'von Hand für genau diese Buchung gesetzt' WHERE id = ?",
                [(int(i),) for i in tx_ids],
            )
            self._conn.commit()
        out = {"updated": n, "rules": 0, "applied": 0, "merchants": [], "scope": "merchant", "amount": None}
        if not learn:
            return out
        rows = self._tx_rows_for_classify(
            "t.id IN (" + ",".join("?" * len(tx_ids)) + ")", [int(i) for i in tx_ids]
        )
        # scope: 'merchant' (all bookings of the counterparty), 'amount'
        # (only this amount class), 'auto' = amount when the counterparty
        # has several amount classes (two contracts, one landlord).
        targets: dict[tuple[str, str], str] = {}
        signs: dict[tuple[str, str], set[str]] = {}
        for tx in rows:
            sc = scope
            if sc == "auto":
                prof = self.finance_merchant_amount_profile(tx.get("counterparty") or "")
                sc = "amount" if prof["contract_like"] else "merchant"
            sign = "in" if float(tx.get("amount") or 0.0) > 0 else "out"
            for kind, val in rule_targets(tx, scope=sc):
                targets[(kind, val)] = (tx.get("counterparty") or "").strip()
                signs.setdefault((kind, val), set()).add(sign)
            if sc == "amount":
                out["scope"] = "amount"
                out["amount"] = abs(float(tx.get("amount") or 0.0))
        from .finance.categories import INCOME_CATEGORIES
        for (kind, val), name in targets.items():
            # Direction of a learned rule (Wunsch: „Ausgaben sind nie
            # Erstattungen"): an income category → credits only; an expense
            # category learned from debits → debits only, so a credit from
            # that shop stays a refund; learned from a credit (pocket money
            # back from Oskar → Kinder) or mixed → both ways.
            sg = signs.get((kind, val), set())
            if category in INCOME_CATEGORIES:
                direction = "in"
            elif sg == {"out"}:
                direction = "out"
            else:
                direction = ""
            if self.finance_rule_upsert(kind, val, category, source="user", sample_name=name, direction=direction):
                out["rules"] += 1
                if name and name not in out["merchants"]:
                    out["merchants"].append(name)
        if targets:
            out["applied"] = self._apply_rules_to_merchants(list(targets))
        return out

    def _apply_rules_to_merchants(self, targets: list[tuple[str, str]]) -> int:
        """Re-run the classifier (force) over every booking that matches one
        of the (kind, value) targets. Manual pins and transfers stay."""
        from .finance.buckets import merchant_key
        ibans = [v for k, v in targets if k == "iban"]
        keys = {v for k, v in targets if k == "merchant"} | {v.rsplit("|", 1)[0] for k, v in targets if k == "merchant_amount"}
        where_parts, params = [], []
        if ibans:
            where_parts.append("t.counterparty_iban IN (" + ",".join("?" * len(ibans)) + ")")
            params += ibans
        if keys:
            where_parts.append("t.counterparty IS NOT NULL AND t.counterparty != ''")
        if not where_parts:
            return 0
        rows = self._tx_rows_for_classify("(" + " OR ".join(where_parts) + ")", params)
        ids = [int(r["id"]) for r in rows
               if (r.get("counterparty_iban") in ibans) or (merchant_key(r.get("counterparty")) in keys)]
        if not ids:
            return 0
        changed = 0
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            st = self.finance_reclassify(
                force=True, where="t.id IN (" + ",".join("?" * len(chunk)) + ")", params=chunk
            )
            changed += st.get("changed", 0)
        return changed

    def finance_unresolved_merchants(self, *, limit: int = 300, category: str = "sonstiges",
                                     start: str | None = None, end: str | None = None
                                     ) -> list[dict[str, Any]]:
        """Distinct counterparties whose bookings sit in `category` (default
        „sonstiges"), biggest money first — the work list for „Sonstiges
        aufdröseln". Each entry knows its IBAN/name key so one assignment
        teaches a rule for the whole group."""
        from .finance.buckets import merchant_key
        where = ["d.deleted_at IS NULL", "COALESCE(NULLIF(t.category, ''), 'sonstiges') = ?"]
        params: list[Any] = [category]
        if start:
            where.append("t.booking_date >= ?"); params.append(start)
        if end:
            where.append("t.booking_date <= ?"); params.append(end)
        with self._lock:
            rows = self._conn.execute(
                "SELECT t.id, t.amount, t.counterparty, t.counterparty_iban, t.purpose, t.booking_date, "
                "       t.category_source, t.category_reason "
                "FROM transactions t JOIN statements s ON s.id = t.statement_id "
                "JOIN documents d ON d.id = s.doc_id "
                "WHERE " + " AND ".join(where),
                params,
            ).fetchall()
        agg: dict[str, dict[str, Any]] = {}
        for r in rows:
            name = (r["counterparty"] or "").strip()
            key = merchant_key(name) or f"iban:{r['counterparty_iban'] or ''}" or "—"
            if key == "—" or key == "iban:":
                key = "purpose:" + (r["purpose"] or "")[:40].lower()
            e = agg.get(key)
            if e is None:
                e = agg[key] = {
                    "key": key, "name": name or ((r["purpose"] or "")[:60] or "—"),
                    "iban": r["counterparty_iban"] or "", "count": 0, "sum_out": 0.0, "sum_in": 0.0,
                    "sample": (r["purpose"] or "")[:120], "last_date": "", "ids": [],
                    "source": r["category_source"] or "", "reason": r["category_reason"] or "",
                }
            amt = float(r["amount"] or 0.0)
            e["count"] += 1
            if amt < 0:
                e["sum_out"] += -amt
            else:
                e["sum_in"] += amt
            if (r["booking_date"] or "") > e["last_date"]:
                e["last_date"] = r["booking_date"] or ""
            if len(e["ids"]) < 2000:
                e["ids"].append(int(r["id"]))
        out = sorted(agg.values(), key=lambda x: (x["sum_out"] + x["sum_in"]), reverse=True)
        for e in out:
            e["sum_out"] = round(e["sum_out"], 2)
            e["sum_in"] = round(e["sum_in"], 2)
            e["total"] = round(e["sum_out"] + e["sum_in"], 2)
        return out[:limit]

    def finance_transfer_check(self, *, tolerance_days: int = 5, include_synthetic: bool = True) -> dict[str, Any]:
        """Pair every transfer leg with its counter-leg on the other own
        account (same amount, opposite sign, within `tolerance_days`) and
        report the legs that have NO counterpart although the other
        account's export covers that date — those are gaps in the exports
        (a missing month), and they silently skew that account's balance."""
        from datetime import date as _d
        accounts = {a["id"]: a for a in self.list_accounts()}
        by_iban = {a["iban"]: a["id"] for a in accounts.values() if a.get("iban")}
        from .finance.classify import card_settlement_last4
        cards_by_last4 = {a["iban"][-4:]: a["id"] for a in accounts.values()
                          if (a.get("iban") or "").startswith("CARD-")}
        card_ids = set(cards_by_last4.values())
        with self._lock:
            legs = [dict(r) for r in self._conn.execute(
                "SELECT t.id, t.account_id, t.booking_date, t.amount, t.counterparty_iban, t.purpose, t.counterparty "
                "FROM transactions t WHERE t.category = 'uebertrag' AND (t.counterparty_iban IN "
                "  (SELECT iban FROM accounts WHERE iban IS NOT NULL AND iban != '') "
                "  OR t.account_id IN (SELECT id FROM accounts WHERE iban LIKE 'CARD-%') "
                "  OR t.purpose LIKE '%ABRECHNUNG%' OR t.purpose LIKE '%ABR.%') "
                + ("" if include_synthetic else "AND COALESCE(t.synthetic, 0) = 0 ")
                + "ORDER BY t.booking_date"
            ).fetchall()]
        # Give card legs a virtual counterparty: the card's own settlement
        # booking points at "the giro" (unknown → any non-card account), the
        # giro's settlement booking points at the card by its last four digits.
        for lg in legs:
            if lg["account_id"] in card_ids:
                lg["_other"] = None          # any non-card account
                lg["_card_leg"] = True
            else:
                last4 = card_settlement_last4(lg, cards_by_last4.keys())
                if last4:
                    lg["_other"] = cards_by_last4[last4]
                    lg["_card_leg"] = True
                else:
                    lg["_other"] = by_iban.get(lg["counterparty_iban"])
                    lg["_card_leg"] = False
        # Once, not per leg (v0.47.1: this GROUP BY sat inside the loop above
        # and ran 1 000× over 13 000 rows — 16 s per /finance).
        with self._lock:
            ranges = {int(r["account_id"]): (r["a"], r["b"]) for r in self._conn.execute(
                "SELECT account_id, MIN(booking_date) a, MAX(booking_date) b FROM transactions "
                "WHERE booking_date IS NOT NULL AND booking_date != '' GROUP BY account_id"
            ).fetchall()}

        def _p(s: str) -> _d | None:
            try:
                return _d.fromisoformat((s or "")[:10])
            except ValueError:
                return None

        # Candidates by absolute amount: a counter-leg has the same amount
        # with the opposite sign, so only that bucket needs scanning.
        by_amount: dict[int, list[dict[str, Any]]] = {}
        for lg in legs:
            lg["_day"] = _p(lg["booking_date"])
            by_amount.setdefault(int(round(abs(float(lg["amount"])) * 100)), []).append(lg)

        used: set[int] = set()
        pairs = 0
        unpaired: list[dict[str, Any]] = []
        for x in legs:
            if x["id"] in used:
                continue
            other_acc = x.get("_other")
            dx = x["_day"]
            match = None
            if (other_acc is not None or x.get("_card_leg")) and dx:
                for y in by_amount.get(int(round(abs(float(x["amount"])) * 100)), ()):
                    if y["id"] in used or y["id"] == x["id"]:
                        continue
                    if x.get("_card_leg"):
                        # card ↔ giro: the other leg must point back at us
                        if x["account_id"] in card_ids:
                            if y["account_id"] in card_ids or y.get("_other") != x["account_id"]:
                                continue
                        else:
                            if y["account_id"] != other_acc:
                                continue
                    else:
                        if y["account_id"] != other_acc or by_iban.get(y["counterparty_iban"]) != x["account_id"]:
                            continue
                    if abs(float(x["amount"]) + float(y["amount"])) > 0.005:
                        continue
                    dy = y["_day"]
                    # Card settlements take longer around holidays (20.12. → 27.12.).
                    tol = max(tolerance_days, 10) if x.get("_card_leg") else tolerance_days
                    if dy and abs((dx - dy).days) <= tol:
                        match = y
                        break
            if match:
                used.add(x["id"]); used.add(match["id"]); pairs += 1
                continue
            used.add(x["id"])
            if x.get("_card_leg") and x["account_id"] in card_ids:
                # The card's own settlement: we don't know which giro pays it; the giro
                # may simply not be imported. Never a "gap".
                other_acc = None
            # Unpaired: only a finding when the other account's data covers the date.
            rng = ranges.get(other_acc) if other_acc is not None else None
            covered = bool(rng and rng[0] <= (x["booking_date"] or "") <= rng[1])
            other = accounts.get(other_acc) if other_acc is not None else None
            unpaired.append({
                "id": x["id"], "account_id": x["account_id"],
                "account": f"{accounts[x['account_id']]['bank_name']} …{accounts[x['account_id']].get('iban_last4') or ''}"
                           if x["account_id"] in accounts else "?",
                "date": x["booking_date"], "amount": float(x["amount"]),
                "other_account_id": other_acc,
                "other_account": f"{other['bank_name']} …{other.get('iban_last4') or ''}" if other else "?",
                "other_covered": covered,
                "expected_amount": -float(x["amount"]),
            })
        gaps = [u for u in unpaired if u["other_covered"]]
        return {"pairs": pairs, "unpaired": unpaired, "gaps": gaps,
                "gap_sum_by_account": self._sum_by(gaps)}


    # ---------- Finance: Kontoauszug-PDFs (v0.47) ----------

    @staticmethod
    def _name_tokens(s: str) -> set[str]:
        import re as _re
        return {w for w in _re.sub(r"[^a-zäöüß ]", " ", (s or "").lower()).split() if len(w) > 1}

    @staticmethod
    def _is_own_name(counterparty: str, holders: set[str]) -> bool:
        """True when the counterparty is nothing but the account holders'
        names — in any order, and even with a name broken in two („ERIKA
        MUSTER MANN", a kerning artefact of the PDF text)."""
        import re as _re
        if not holders:
            return False
        t = _re.sub(r"[^a-zäöüß]", "", (counterparty or "").lower())
        if not t:
            return False
        toks = sorted(holders, key=len, reverse=True)
        while t:
            for tok in toks:
                if t.startswith(tok):
                    t = t[len(tok):]
                    break
            else:
                return False
        return True

    def finance_holder_tokens(self) -> set[str]:
        """Name tokens of all account holders („max", „erika", „mustermann")."""
        out: set[str] = set()
        with self._lock:
            rows = self._conn.execute("SELECT account_holder FROM accounts").fetchall()
        for r in rows:
            out |= self._name_tokens(r["account_holder"] or "")
        return out

    def finance_pair_name_transfers(self, *, tolerance_days: int = 3) -> int:
        """Give name-only transfer legs their counterparty IBAN.

        A Kontoauszug prints an incoming move from the own Tagesgeld as
        „Sonst. Gutschrift — Max Mustermann Erika Mustermann" without an
        IBAN; the other account's leg carries ours. Two passes, both
        one-to-one: first every IBAN leg is matched with its IBAN
        counterpart (same amount, opposite sign, ≤ `tolerance_days` apart),
        then the IBAN legs still alone are matched with name-only rows
        (counterparty = the account holders' names, no IBAN) on the account
        they point at, and finally name-only rows with each other. The IBAN
        is written so `finance_retag_transfers` sees a transfer instead of
        income. Returns rows updated."""
        from datetime import date as _d
        accounts = {int(a["id"]): a for a in self.list_accounts() if a.get("iban") and not str(a["iban"]).startswith("CARD-")}
        if len(accounts) < 2:
            return 0
        by_iban = {a["iban"]: aid for aid, a in accounts.items()}
        holders: set[str] = set()
        for a in accounts.values():
            holders |= self._name_tokens(a.get("account_holder") or "")
        with self._lock:
            rows = [dict(r) for r in self._conn.execute(
                "SELECT id, account_id, booking_date, amount, counterparty, counterparty_iban, category "
                "FROM transactions WHERE COALESCE(synthetic, 0) = 0 ORDER BY booking_date, id"
            ).fetchall()]

        def _day(r):
            try:
                return _d.fromisoformat(str(r["booking_date"])[:10])
            except ValueError:
                return None

        legs = []      # IBAN legs: point at another own account
        names = []     # name-only rows: no IBAN, counterparty = the holders
        for r in rows:
            aid = int(r["account_id"])
            if aid not in accounts:
                continue
            r["_day"] = _day(r)
            if r["_day"] is None:
                continue
            ib = (r.get("counterparty_iban") or "").replace(" ", "").upper()
            if ib and ib in by_iban and by_iban[ib] != aid:
                r["_other"] = by_iban[ib]
                legs.append(r)
            elif not ib and self._is_own_name(r.get("counterparty") or "", holders):
                names.append(r)
        if not names:
            return 0

        def _match(x, pool, *, need_iban_back: bool) -> dict | None:
            best = None
            for y in pool:
                if y.get("_used") or int(y["account_id"]) != x["_other"]:
                    continue
                if need_iban_back and y.get("_other") != int(x["account_id"]):
                    continue
                if abs(float(x["amount"]) + float(y["amount"])) > 0.005:
                    continue
                gap = abs((y["_day"] - x["_day"]).days)
                if gap > tolerance_days:
                    continue
                if best is None or gap < best[0]:
                    best = (gap, y)
            return best[1] if best else None

        # pass 1: IBAN leg ↔ IBAN leg
        for x in legs:
            if x.get("_used"):
                continue
            y = _match(x, legs, need_iban_back=True)
            if y is not None:
                x["_used"] = y["_used"] = True
        # pass 2: lonely IBAN leg ↔ name-only row on the account it points at
        updates: list[tuple[str, int]] = []
        for x in legs:
            if x.get("_used"):
                continue
            y = _match(x, names, need_iban_back=False)
            if y is not None:
                x["_used"] = y["_used"] = True
                updates.append((accounts[int(x["account_id"])]["iban"], int(y["id"])))
        # pass 3: name-only ↔ name-only on different accounts
        for x in names:
            if x.get("_used"):
                continue
            best = None
            for y in names:
                if y is x or y.get("_used") or int(y["account_id"]) == int(x["account_id"]):
                    continue
                if abs(float(x["amount"]) + float(y["amount"])) > 0.005:
                    continue
                gap = abs((y["_day"] - x["_day"]).days)
                if gap <= tolerance_days and (best is None or gap < best[0]):
                    best = (gap, y)
            if best:
                y = best[1]
                x["_used"] = y["_used"] = True
                updates.append((accounts[int(y["account_id"])]["iban"], int(x["id"])))
                updates.append((accounts[int(x["account_id"])]["iban"], int(y["id"])))
        if updates:
            with self._lock:
                self._conn.executemany("UPDATE transactions SET counterparty_iban = ? WHERE id = ?", updates)
                self._conn.commit()
            logger.info("Finance: %d name-only transfer leg(s) got their counterpart IBAN.", len(updates))
        return len(updates)

    def finance_statement_overview(self) -> list[dict[str, Any]]:
        """Per account: the imported Kontoauszüge in order plus the holes
        between them (a statement missing, or its opening balance not
        matching the previous closing balance)."""
        out: list[dict[str, Any]] = []
        with self._lock:
            accts = [dict(r) for r in self._conn.execute(
                "SELECT id, bank_name, iban, iban_last4, is_savings, start_balance FROM accounts ORDER BY is_savings, id"
            ).fetchall()]
            stmts = [dict(r) for r in self._conn.execute(
                "SELECT s.id, s.doc_id, s.account_id, s.period_start, s.period_end, s.statement_no, "
                "       s.opening_balance, s.closing_balance, s.file_hash, "
                "       (SELECT COUNT(*) FROM transactions t WHERE t.statement_id = s.id) AS n_rows, "
                "       d.subject, d.original_name "
                "FROM statements s LEFT JOIN documents d ON d.id = s.doc_id "
                "WHERE COALESCE(s.file_hash, '') NOT LIKE 'csv-import:%' AND s.opening_balance IS NOT NULL "
                "ORDER BY s.account_id, s.period_start, s.period_end"
            ).fetchall()]
            csv_range = {int(r["account_id"]): (r["lo"], r["hi"]) for r in self._conn.execute(
                "SELECT t.account_id, MIN(t.booking_date) lo, MAX(t.booking_date) hi FROM transactions t "
                "JOIN statements s ON s.id = t.statement_id WHERE s.file_hash LIKE 'csv-import:%' "
                "  AND COALESCE(t.synthetic, 0) = 0 GROUP BY t.account_id"
            ).fetchall()}
        for a in accts:
            mine = [s for s in stmts if int(s["account_id"]) == int(a["id"])]
            gaps: list[dict[str, Any]] = []
            prev = None
            for s in mine:
                if prev is not None and prev["closing_balance"] is not None and s["opening_balance"] is not None:
                    if abs(float(prev["closing_balance"]) - float(s["opening_balance"])) > 0.005 or prev["period_end"] != s["period_start"]:
                        lo, hi = csv_range.get(int(a["id"]), ("", ""))
                        covered = bool(lo and hi and lo <= prev["period_end"] and hi >= s["period_start"])
                        # A statement the bank itself no longer has can be
                        # ticked off; the synthetic bridging booking stays,
                        # so the balance history remains correct.
                        ack = self.meta_get(
                            f"finance.gap_ack.{a['id']}|{prev['period_end']}|{s['period_start']}"
                        )
                        gaps.append({
                            "after": prev["period_end"], "before": s["period_start"],
                            "after_no": prev["statement_no"], "before_no": s["statement_no"],
                            "expected": round(float(s["opening_balance"]) - float(prev["closing_balance"]), 2),
                            "csv_covered": covered,
                            "acknowledged": bool(ack),
                            "acknowledged_at": ack or "",
                        })
                prev = s
            out.append({
                "account_id": int(a["id"]), "bank_name": a["bank_name"], "iban_last4": a["iban_last4"],
                "is_savings": int(a.get("is_savings") or 0),
                "statements": mine, "gaps": gaps,
                "first": mine[0]["period_start"] if mine else "", "last": mine[-1]["period_end"] if mine else "",
                "csv_from": csv_range.get(int(a["id"]), ("", ""))[0], "csv_to": csv_range.get(int(a["id"]), ("", ""))[1],
            })
        return out

    def finance_gap_acknowledge(self, account_id: int, after: str, before: str,
                                acknowledged: bool = True) -> bool:
        """Mark a hole in the statement chain as "this one does not exist".

        Some statements simply cannot be obtained any more — the owner's bank
        has no 5/2016 and no 12/2019 for the Giro, and he has no copy. The
        chain would otherwise ask for them forever. Ticking the gap off
        changes nothing about the money: the synthetic booking that bridges
        it stays, so every later balance still adds up. It only stops the
        nagging.
        """
        key = f"finance.gap_ack.{int(account_id)}|{after}|{before}"
        if acknowledged:
            self.meta_set(key, datetime.now().isoformat(timespec="seconds"))
        else:
            with self._lock:
                self._conn.execute("DELETE FROM meta WHERE key = ?", (key,))
                self._conn.commit()
        return True

    def finance_reconcile_statements(self) -> dict[str, Any]:
        """Make the balance history agree with the bank's own statements.

        1. Start balance: the earliest Kontoauszug states the balance at its
           opening day; start = that balance − everything booked up to that
           day (normally nothing). Written whenever a statement exists — the
           bank's number outranks a typed guess; the previous value is kept
           in meta so it can be shown.
        2. Holes: where the next statement opens with a different balance
           than the previous one closed, and no other source (CSV) explains
           the difference, a synthetic „Lücke" row carries the difference so
           every later balance is right. Re-run after each import; the row is
           updated or removed as real bookings arrive. Idempotent."""
        import json as _json
        summary = {"start_set": [], "gap_rows": 0, "gap_removed": 0}
        # Every gap row this run still wants. Anything else that carries
        # `category_source = 'gap'` is a leftover and gets removed below.
        wanted_gap_hashes: set[str] = set()
        overview = self.finance_statement_overview()
        from .finance.csv_import import _ensure_csv_container_statement
        for acc in overview:
            aid = int(acc["account_id"])
            st = acc["statements"]
            if not st:
                continue
            first = st[0]
            with self._lock:
                booked = float(self._conn.execute(
                    "SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE account_id = ? AND booking_date <= ?",
                    (aid, first["period_start"]),
                ).fetchone()[0] or 0.0)
                cur_start = float(self._conn.execute(
                    "SELECT COALESCE(start_balance, 0) FROM accounts WHERE id = ?", (aid,)
                ).fetchone()[0] or 0.0)
            implied = round(float(first["opening_balance"]) - booked, 2)
            if abs(cur_start - implied) >= 0.005:
                key = f"finance.statement_start.{aid}"
                self.meta_set(key, _json.dumps({"previous": cur_start, "set": implied,
                                                "date": first["period_start"], "statement": first["statement_no"]}))
                self.set_account_meta(aid, start_balance=implied)
                summary["start_set"].append({"account_id": aid, "previous": cur_start, "start": implied,
                                             "date": first["period_start"]})
            # holes between consecutive statements
            prev = None
            for s in st:
                if prev is not None:
                    key = f"stmt-gap|{aid}|{prev['period_end']}|{s['period_start']}"
                    tx_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
                    wanted_gap_hashes.add(tx_hash)
                    with self._lock:
                        between = float(self._conn.execute(
                            "SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE account_id = ? "
                            "  AND booking_date > ? AND booking_date <= ? AND tx_hash != ?",
                            (aid, prev["period_end"], s["period_start"], tx_hash),
                        ).fetchone()[0] or 0.0)
                        existing = self._conn.execute(
                            "SELECT id, amount FROM transactions WHERE tx_hash = ?", (tx_hash,)
                        ).fetchone()
                    residual = round(float(s["opening_balance"]) - float(prev["closing_balance"]) - between, 2)
                    if abs(residual) < 0.005:
                        if existing:
                            with self._lock:
                                self._conn.execute("DELETE FROM transactions WHERE id = ?", (int(existing["id"]),))
                                self._conn.commit()
                            summary["gap_removed"] += 1
                    else:
                        d_after = prev["period_end"]
                        d_before = s["period_start"]
                        de = lambda x: f"{x[8:10]}.{x[5:7]}.{x[0:4]}"
                        reason = (f"Kontoauszug fehlt zwischen {de(d_after)} (Auszug {prev['statement_no']}) und "
                                  f"{de(d_before)} (Auszug {s['statement_no']}): laut Bank änderte sich der Kontostand um "
                                  f"{residual:+.2f} €, die einzelnen Buchungen fehlen. Diese Zeile hält den Kontostand richtig; "
                                  f"sie verschwindet, sobald der fehlende Auszug oder ein CSV-Export für den Zeitraum importiert ist.")
                        if existing:
                            if abs(float(existing["amount"]) - residual) >= 0.005:
                                with self._lock:
                                    self._conn.execute("UPDATE transactions SET amount = ?, category_reason = ? WHERE id = ?",
                                                       (residual, reason, int(existing["id"])))
                                    self._conn.commit()
                        else:
                            stmt_id = _ensure_csv_container_statement(self, aid)
                            with self._lock:
                                self._conn.execute(
                                    "INSERT INTO transactions (statement_id, account_id, booking_date, value_date, amount, "
                                    "  currency, counterparty, counterparty_iban, purpose, tx_type, category, tx_hash, "
                                    "  category_source, category_reason, synthetic) "
                                    "VALUES (?, ?, ?, ?, ?, 'EUR', ?, '', ?, 'sonstiges', 'sonstiges', ?, 'gap', ?, 1)",
                                    (stmt_id, aid, d_before, d_before, residual,
                                     "Lücke: Kontoauszug fehlt",
                                     f"LUECKE Kontoauszug {prev['statement_no']} → {s['statement_no']} ({de(d_after)} – {de(d_before)})",
                                     tx_hash, reason),
                                )
                                self._conn.commit()
                        summary["gap_rows"] += 1
                prev = s

        # 🔴 The loop above only ever visits gaps that exist BETWEEN two
        # consecutive statements. When the missing statement finally
        # arrives, its old gap key is never revisited — so the synthetic
        # row that bridged it stayed behind and the account counted the
        # same money twice (live: Tagesgeld …9990 booked -849,94 € for
        # 12/2019 both as the real statement and as the old filler).
        # A leftover is anything tagged 'gap' that this run did not ask for.
        with self._lock:
            leftovers = self._conn.execute(
                "SELECT id, tx_hash, account_id, booking_date, amount FROM transactions "
                "WHERE COALESCE(synthetic,0) = 1 AND category_source = 'gap'"
            ).fetchall()
            for row in leftovers:
                if row["tx_hash"] in wanted_gap_hashes:
                    continue
                self._conn.execute("DELETE FROM transactions WHERE id = ?", (int(row["id"]),))
                summary["gap_removed"] += 1
                logger.info("Finance: removed the leftover gap booking of %.2f € on "
                            "%s (account %s) — the statement it stood in for is here now.",
                            float(row["amount"] or 0), row["booking_date"], row["account_id"])
            if summary["gap_removed"]:
                self._conn.commit()
        return summary

    def finance_fill_transfer_gaps(self) -> dict[str, Any]:
        """Close export gaps with synthetic counter-legs.

        A transfer leg on account A whose counterpart on our own account B is
        missing — although B's export covers that day — means B's export
        skipped a period. Without the counter-leg B's balance is off by that
        amount and the transfer looks one-sided. We add the missing leg on B
        ourselves: category `uebertrag` (never income/expense), flagged
        `synthetic = 1`, text says where it came from. When the real booking
        arrives in a later import, the importer deletes the synthetic twin
        (same account, day, amount). Idempotent: re-running adds nothing
        that already exists."""
        from .finance.csv_import import _ensure_csv_container_statement
        # Judge the gaps WITHOUT our own counter-legs — with them every filled
        # gap looks closed, the leg would be dropped as stale and re-added on
        # the next run (flip-flop).
        check = self.finance_transfer_check(include_synthetic=False)
        added = []
        now = datetime.now().isoformat(timespec="seconds")
        for g in check.get("gaps", []):
            other = g.get("other_account_id")
            if not other:
                continue
            key = f"gap-fill|{other}|{g['date']}|{g['expected_amount']:.2f}|{g['id']}"
            tx_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
            with self._lock:
                dup = self._conn.execute("SELECT 1 FROM transactions WHERE tx_hash = ?", (tx_hash,)).fetchone()
            if dup:
                continue
            stmt_id = _ensure_csv_container_statement(self, int(other))
            src = self._conn.execute(
                "SELECT t.counterparty, t.purpose, a.iban FROM transactions t LEFT JOIN accounts a ON a.id = t.account_id "
                "WHERE t.id = ?", (int(g["id"]),)
            ).fetchone()
            src_iban = (src["iban"] if src else "") or ""
            reason = (f"Von DocuSort ergänzt: Gegenstück zur Umbuchung vom {g['date'][8:10]}.{g['date'][5:7]}.{g['date'][0:4]} "
                      f"auf {g['account']} — im Export dieses Kontos fehlte der Zeitraum. Wird durch die echte Buchung ersetzt, "
                      "sobald sie importiert wird.")
            with self._lock:
                self._conn.execute(
                    "INSERT INTO transactions (statement_id, account_id, booking_date, value_date, amount, currency, "
                    "  counterparty, counterparty_iban, purpose, tx_type, category, tx_hash, category_source, "
                    "  category_reason, synthetic) VALUES (?, ?, ?, ?, ?, 'EUR', ?, ?, ?, 'uebertrag', 'uebertrag', ?, "
                    "  'transfer', ?, 1)",
                    (stmt_id, int(other), g["date"], g["date"], float(g["expected_amount"]),
                     f"Ergänzt: Gegenbuchung zu {g['account']}", src_iban,
                     f"ERGAENZT (Export-Lücke) Gegenstück zu Buchung #{g['id']} auf {g['account']}", tx_hash, reason),
                )
                self._conn.commit()
            added.append({"account_id": other, "account": g["other_account"], "date": g["date"],
                          "amount": float(g["expected_amount"])})
        # A synthetic leg whose gap has since closed (the real counterpart
        # arrived, or a name-only leg got its IBAN) must go again — otherwise
        # the transfer is counted twice on that account.
        wanted = set()
        for g in check.get("gaps", []):
            if g.get("other_account_id"):
                key = f"gap-fill|{g['other_account_id']}|{g['date']}|{g['expected_amount']:.2f}|{g['id']}"
                wanted.add(hashlib.sha256(key.encode("utf-8")).hexdigest())
        with self._lock:
            stale = [int(r["id"]) for r in self._conn.execute(
                "SELECT id, tx_hash FROM transactions WHERE synthetic = 1 AND COALESCE(category_source, '') = 'transfer'"
            ).fetchall() if r["tx_hash"] not in wanted]
            if stale:
                self._conn.executemany("DELETE FROM transactions WHERE id = ?", [(i,) for i in stale])
                self._conn.commit()
                logger.info("Finance: %d synthetic counter-leg(s) removed — their gap has closed.", len(stale))
            total = int(self._conn.execute("SELECT COUNT(*) FROM transactions WHERE synthetic = 1").fetchone()[0])
        if added:
            logger.info("Finance: %d transfer gap(s) closed with synthetic counter-legs.", len(added))
        return {"added": added, "synthetic_total": total, "added_at": now}

    def finance_synthetic_legs(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT t.id, t.booking_date, t.amount, t.counterparty, t.purpose, t.category_reason, "
                "       a.bank_name, a.iban_last4 FROM transactions t LEFT JOIN accounts a ON a.id = t.account_id "
                "WHERE t.synthetic = 1 AND COALESCE(t.category_source, '') = 'transfer' ORDER BY t.booking_date"
            ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _sum_by(gaps: list[dict[str, Any]]) -> dict[str, float]:
        out: dict[str, float] = {}
        for g in gaps:
            k = g["other_account"]
            out[k] = round(out.get(k, 0.0) + g["expected_amount"], 2)
        return out

    # Wunsch (2026-09-19): „fixkosten sind miete, nebenkosten, auto, kredite,
    # telefon internet, versicherungen" — plus what runs on contract anyway
    # (Abos, Kita, Sparraten), each of which he can untick on /fixkosten.
    DEFAULT_FIXED_CATEGORIES = ("miete", "nebenkosten", "mobilitaet", "kredit", "versicherung",
                                "sparversicherung", "kapital", "abonnement", "kinder", "steuer", "gebuehr")
    # Card payments are never fixed costs in these categories (fuel, toys,
    # a one-off tax) — a contract is paid by Lastschrift/Dauerauftrag.
    _FIXED_NO_CARD = {"mobilitaet", "kinder", "steuer", "kapital", "gebuehr"}
    # Amounts may vary ±20 % and still be one contract (utility Abschlag
    # steps, phone bill extras); every other category must debit the exact
    # amount to count as fixed.
    _FIXED_LOOSE = {"miete", "nebenkosten", "kredit", "versicherung", "sparversicherung", "kapital", "steuer", "gebuehr", "kinder"}
    # One booking in the window is enough only here (a yearly premium, a tax).
    _FIXED_SINGLE_OK = {"versicherung", "steuer"}

    def finance_fixed_categories(self) -> list[str]:
        import json as _json
        try:
            v = _json.loads(self.meta_get("finance.fixed_categories") or "null")
        except ValueError:
            v = None
        if not isinstance(v, list):
            v = list(self.DEFAULT_FIXED_CATEGORIES)
        custom = {c["key"] for c in self.finance_custom_categories() if c.get("is_fixed")}
        keys = set(self.finance_category_keys())
        return [c for c in dict.fromkeys([*v, *sorted(custom)]) if c in keys]

    def finance_set_fixed_categories(self, cats: list[str]) -> list[str]:
        import json as _json
        keys = set(self.finance_category_keys())
        clean = [c for c in dict.fromkeys(cats) if c in keys and c not in ("uebertrag", "sonstiges")]
        self.meta_set("finance.fixed_categories", _json.dumps(clean))
        return self.finance_fixed_categories()

    def account_picks(self, account_ids: list[int] | None = None) -> list[dict[str, Any]]:
        """Die Konten, aus denen die Kontenauswahl besteht — je Konto ein
        Name und ob es gerade eingerechnet wird.

        🔴 EINE Stelle für /ausgaben und /fixkosten: beide Seiten zeigen
        denselben Knopf mit denselben Namen, und eine leere Auswahl heißt
        auf beiden „alle Konten". Sparkonten stehen nicht zur Wahl — sie
        sind aus den Ausgabenzahlen ohnehin ausgenommen.
        """
        picked = set(account_ids or [])
        return [
            {"id": int(a["id"]),
             "label": (a.get("bank_name") or "?") + (" \u00b7\u00b7\u00b7" + str(a["iban_last4"]) if a.get("iban_last4") else ""),
             "is_savings": int(a.get("is_savings") or 0),
             "selected": (not picked) or int(a["id"]) in picked}
            for a in self.list_accounts() if not int(a.get("is_savings") or 0)
        ]

    def finance_fixed_costs(self, *, months_back: int = 24, min_hits: int = 2,
                            account_ids: list[int] | None = None) -> dict[str, Any]:
        """Fixkostenrechner: recurring outgoing bookings per counterparty,
        their rhythm (monatlich / vierteljährlich / halbjährlich / jährlich),
        and what that means per month and per year.

        Rhythm = median gap between consecutive bookings of the same
        counterparty with amounts within ±20 % of their median. Everything
        else (irregular shopping) is not a fixed cost and is left out. The
        user can pin/unpin via `fixed_cost_overrides` (meta key) — a map of
        item uids (`<merchant_key>#<cluster>`; bare merchant keys from older
        versions still count for every cluster) forced in or out.

        Returns three lists: `items` (counted), `candidates` (regular but not
        counted — a tick takes them in) and `others` (every remaining payee
        with outgoing bookings, for the search box: anything can be taken
        into the fixed costs by hand, however irregular it looks).

        `account_ids` narrows every figure to those accounts (Wunsch: „bei
        den fixkosten will ich auch die konten auswählen können") — the
        same selection as on /ausgaben, and like there an empty list means
        all accounts. The filter sits in the ONE query this function reads
        from, so contracts, the per-category averages and the totals can
        never disagree about which accounts they cover.
        """
        from statistics import median
        from datetime import date as _d, timedelta
        from .finance.buckets import merchant_key
        import json as _json
        since = (_d.today() - timedelta(days=30 * months_back)).isoformat()
        acc_sql = ""
        acc_args: tuple[Any, ...] = ()
        if account_ids:
            acc_sql = " AND t.account_id IN (" + ",".join("?" * len(account_ids)) + ")"
            acc_args = tuple(account_ids)
        with self._lock:
            rows = [dict(r) for r in self._conn.execute(
                "SELECT t.id, t.booking_date, t.amount, t.counterparty, t.category, t.tx_type, t.purpose "
                "FROM transactions t JOIN statements s ON s.id = t.statement_id "
                "JOIN documents d ON d.id = s.doc_id LEFT JOIN accounts a ON a.id = t.account_id "
                "WHERE d.deleted_at IS NULL AND t.amount < 0 AND COALESCE(a.is_savings, 0) = 0 "
                "  AND COALESCE(t.category, '') NOT IN ('uebertrag', 'bargeld', 'kreditkarte') "
                "  AND t.booking_date >= ? AND t.counterparty IS NOT NULL AND TRIM(t.counterparty) != '' "
                + acc_sql +
                " ORDER BY t.booking_date", (since,) + acc_args,
            ).fetchall()]
        try:
            overrides = _json.loads(self.meta_get("finance.fixed_cost_overrides") or "{}")
        except ValueError:
            overrides = {}
        groups: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            k = merchant_key(r["counterparty"])
            if k:
                groups.setdefault(k, []).append(r)
        # Categories that are fixed by nature — a single yearly insurance
        # premium is a fixed cost even with only one booking in range.
        fixed_cats = set(self.finance_fixed_categories())
        items: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []   # regular, but variable by nature (weekly Aldi) — pinnable
        others: list[dict[str, Any]] = []       # everything else — reachable through the search box
        expired: list[dict[str, Any]] = []      # fixed by kind, but the debits stopped — not summed
        # One counterparty may carry several contracts (rent 770 € + garage
        # 20 €, same landlord, same day): split its bookings into amount
        # clusters and judge each cluster as its own item.
        cluster_groups: list[tuple[str, int, int, list[dict[str, Any]]]] = []
        for k, g in groups.items():
            cl = self._amount_clusters([-float(x["amount"]) for x in g])
            if len(cl) <= 1:
                cluster_groups.append((k, 0, 1, g))
                continue
            bounds = [(c[0], c[-1]) for c in cl]
            for ci, (lo, hi) in enumerate(bounds):
                members = [x for x in g if lo - 0.005 <= -float(x["amount"]) <= hi + 0.005]
                if members:
                    cluster_groups.append((k, ci, len(bounds), members))
        today = _d.today()
        for k, ci, ncl, g in cluster_groups:
            uid = f"{k}#{ci}"
            forced = overrides.get(uid, overrides.get(k))
            # Several bookings on one day are ONE instalment (a court fee paid
            # in two transfers, a split direct debit) — judge per day.
            by_day: dict[str, float] = {}
            last_row: dict[str, dict[str, Any]] = {}
            for x in g:
                day = x["booking_date"][:10]
                by_day[day] = by_day.get(day, 0.0) - float(x["amount"])
                last_row[day] = x
            events = [(day, by_day[day]) for day in sorted(by_day)]
            med = median(a for _, a in events)
            steady = [(day, a) for day, a in events if abs(a - med) <= max(0.2 * med, 1.0)]
            if not steady:
                continue
            ref = last_row[steady[-1][0]]
            dates = [_d.fromisoformat(day) for day, _ in steady]
            gaps = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
            gaps = [gp for gp in gaps if gp >= 20]   # two instalments within days are one
            cat = (ref.get("category") or "sonstiges")
            card = (ref.get("tx_type") or "") == "kartenzahlung"
            # A contract debits the same amount to the cent; a utility
            # Abschlag may step, a phone bill may carry extras — those
            # categories get ±20 %, everything else (subscriptions, car,
            # own categories) must be exact, or it is regular shopping.
            # ±3 %: a card subscription billed in dollars moves with the rate.
            exact = sum(1 for _, a in steady if abs(a - med) <= max(0.03 * med, 0.10))
            exact_share = exact / len(steady)
            exact_ok = cat in self._FIXED_LOOSE or exact_share >= 0.8
            # A single yearly booking is a fixed cost only where that is the
            # rule (insurance premium, tax) and never for a card payment.
            single_ok = cat in fixed_cats and cat in self._FIXED_SINGLE_OK and not card
            need = 3 if card else min_hits
            if len(steady) < need and not (single_ok or forced == "in"):
                continue
            if gaps:
                mg = median(gaps)
                if mg <= 45:
                    rhythm, per_year = "monatlich", 12
                elif mg <= 75:
                    rhythm, per_year = "zweimonatlich", 6
                elif mg <= 120:
                    rhythm, per_year = "vierteljährlich", 4
                elif mg <= 240:
                    rhythm, per_year = "halbjährlich", 2
                else:
                    rhythm, per_year = "jährlich", 1
            else:
                # One booking counts once a year when its kind is fixed
                # (yearly insurance) or when the user took it in by hand.
                rhythm, per_year = ("jährlich", 1) if (single_ok or forced == "in") else ("einmalig", 0)
            # Fixed = the category is one the owner calls fixed (configurable on
            # /fixkosten) AND the booking runs on contract (not a card
            # payment for fuel/toys) — regular eating out is never a fixed cost,
            # however regular.
            regular = len(steady) >= need and per_year >= 1
            is_fixed = cat in fixed_cats and per_year > 0 and (regular or single_ok) and exact_ok \
                and not (card and cat in self._FIXED_NO_CARD)
            if forced == "in":
                is_fixed = True
            elif forced == "out":
                is_fixed = False
            last = steady[-1][0]
            days_since = (today - _d.fromisoformat(last)).days
            # A contract that stopped debiting is over: nothing for 1.6
            # intervals (monthly → ~7 weeks, yearly → ~19 months) means it no
            # longer costs anything. Shown separately, not summed — a tick
            # brings it back if the bank is just late.
            expected_gap = 365 / per_year if per_year else 0
            active = per_year == 0 or days_since <= expected_gap * 1.6 + 7
            regular3 = per_year > 0 and len(steady) >= 3
            amount = round(median(a for _, a in steady), 2)
            next_due = ""
            if per_year:
                next_due = (_d.fromisoformat(last) + timedelta(days=round(365 / per_year))).isoformat()
            entry = {
                "key": k, "uid": uid, "cluster": ci, "clusters": ncl,
                "amount_rule": f"{k}|{amount:.2f}",
                "name": (ref["counterparty"] or "").strip(), "category": cat,
                "rhythm": rhythm, "per_year": per_year, "amount": amount,
                "monthly": round(amount * per_year / 12, 2), "yearly": round(amount * per_year, 2),
                "hits": len(steady), "first": steady[0][0], "last": last, "days_since": days_since,
                "next_due": next_due, "forced": forced or "", "card": card,
                "exact_share": round(exact_share, 2),
                "sample": (ref.get("purpose") or "")[:100],
            }
            if is_fixed and (active or forced == "in"):
                items.append(entry)
            elif is_fixed:
                expired.append(entry)
            elif regular3 or forced == "out":
                candidates.append(entry)
            else:
                others.append(entry)
        # Everything in a fixed category that is NOT a recognised contract
        # still costs money every month — groceries have no contract amount,
        # a car category holds fuel next to the loan. That remainder counts
        # as a monthly average over the last months with data, as its own
        # line per category (Wunsch: ticked Lebensmittel, got 4,66 €).
        contract_ids: set[int] = set()
        contract_rows: dict[str, list[dict[str, Any]]] = {}
        for k, ci, ncl, g in cluster_groups:
            contract_rows[f"{k}#{ci}"] = g
        for it in items + expired:
            for x in contract_rows.get(it["uid"], []):
                contract_ids.add(int(x["id"]))
        avg_window = min(6, max(1, months_back))   # the Ø follows a shorter window
        avg_since = (today - timedelta(days=30 * avg_window + 5)).isoformat()
        months_with_data = {r["booking_date"][:7] for r in rows if r["booking_date"] >= avg_since}
        divisor = max(1, min(avg_window, len(months_with_data)))
        avg_items: list[dict[str, Any]] = []
        for cat in sorted(fixed_cats):
            rest = [r for r in rows if (r.get("category") or "sonstiges") == cat
                    and int(r["id"]) not in contract_ids and r["booking_date"] >= avg_since]
            if not rest:
                continue
            # A one-off far above the usual (the garage bill under Mobilität)
            # is not a running cost: payees seen at most twice in the window
            # whose booking is > 5× the category's median (and ≥ 250 €) stay
            # out of the average.
            payee_n: dict[str, int] = {}
            for r in rest:
                k2 = merchant_key(r["counterparty"])
                payee_n[k2] = payee_n.get(k2, 0) + 1
            med_cat = median(-float(r["amount"]) for r in rest)
            one_offs = [r for r in rest if payee_n.get(merchant_key(r["counterparty"]), 0) <= 2
                        and -float(r["amount"]) > 5 * med_cat and -float(r["amount"]) >= 250]
            one_off_ids = {int(r["id"]) for r in one_offs}
            rest = [r for r in rest if int(r["id"]) not in one_off_ids]
            if not rest:
                continue
            total = round(sum(-float(r["amount"]) for r in rest), 2)
            monthly = round(total / divisor, 2)
            uid = f"avg:{cat}#0"
            forced = overrides.get(uid)
            if monthly < 0.5 or forced == "out":
                if forced == "out" and monthly >= 0.5:
                    candidates.append({
                        "key": f"avg:{cat}", "uid": uid, "cluster": 0, "clusters": 1, "amount_rule": "",
                        "name": "", "category": cat, "rhythm": "Ø monatlich", "per_year": 12,
                        "amount": monthly, "monthly": monthly, "yearly": round(monthly * 12, 2),
                        "hits": len(rest), "first": rest[0]["booking_date"][:10], "last": rest[-1]["booking_date"][:10],
                        "days_since": 0, "next_due": "", "forced": "out", "card": False, "exact_share": 0.0,
                        "sample": "", "is_avg": True, "avg_months": divisor,
                    })
                continue
            avg_items.append({
                "key": f"avg:{cat}", "uid": uid, "cluster": 0, "clusters": 1, "amount_rule": "",
                "name": "", "category": cat, "rhythm": "Ø monatlich", "per_year": 12,
                "amount": monthly, "monthly": monthly, "yearly": round(monthly * 12, 2),
                "hits": len(rest), "first": rest[0]["booking_date"][:10], "last": rest[-1]["booking_date"][:10],
                "days_since": 0, "next_due": "", "forced": forced or "", "card": False, "exact_share": 0.0,
                "sample": "", "is_avg": True, "avg_months": divisor,
                "one_offs": [{"name": (r["counterparty"] or "").strip(), "date": r["booking_date"][:10],
                              "amount": round(-float(r["amount"]), 2)} for r in one_offs],
            })
        items.extend(avg_items)
        saving = self.finance_saving_categories()
        for it in items:
            it["is_saving"] = it["category"] in saving
            it.setdefault("is_avg", False)
        for it in expired + candidates + others:
            it.setdefault("is_avg", False)
        items.sort(key=lambda x: x["monthly"], reverse=True)
        candidates.sort(key=lambda x: x["monthly"], reverse=True)
        others.sort(key=lambda x: (-x["hits"], x["name"].casefold()))
        expired.sort(key=lambda x: x["monthly"], reverse=True)
        by_cat: dict[str, dict[str, float]] = {}
        for it in items:
            e = by_cat.setdefault(it["category"], {"monthly": 0.0, "yearly": 0.0, "n": 0})
            e["monthly"] = round(e["monthly"] + it["monthly"], 2)
            e["yearly"] = round(e["yearly"] + it["yearly"], 2)
            e["n"] += 1
        # Candidates the user may want to pin: recurring but not counted.
        picks = self.account_picks(account_ids)
        return {
            "items": items,
            "candidates": candidates[:80],
            "expired": expired,
            "others": others,
            "saving_monthly": round(sum(i["monthly"] for i in items if i["is_saving"]), 2),
            "saving_yearly": round(sum(i["yearly"] for i in items if i["is_saving"]), 2),
            "total_monthly": round(sum(i["monthly"] for i in items), 2),
            "total_yearly": round(sum(i["yearly"] for i in items), 2),
            "by_category": [{"category": c, **v} for c, v in sorted(by_cat.items(), key=lambda kv: -kv[1]["monthly"])],
            "months_back": months_back,
            "since": since,
            "fixed_categories": sorted(fixed_cats),
            "accounts": picks,
            "accounts_filtered": bool(account_ids) and any(not a["selected"] for a in picks),
        }

    # ---------- Pending (vorgemerkte) bookings ----------

    def pending_replace(self, account_id: int, as_of: str, rows: list[dict[str, Any]]) -> dict[str, int]:
        """Replace an account's pending set with what the newest export
        says. An export older than what we already know (its newest booked
        date lies before the stored `as_of`) is ignored — it would
        resurrect authorisations that have long since booked. A pending
        row that an earlier version imported as a real booking (same hash)
        is moved out of `transactions`."""
        from datetime import datetime as _dt
        now = _dt.now().isoformat(timespec="seconds")
        with self._lock:
            known = self._conn.execute(
                "SELECT MAX(as_of) FROM pending_transactions WHERE account_id = ?", (account_id,)
            ).fetchone()[0] or ""
            if as_of and known and as_of < known:
                return {"added": 0, "cleared": 0, "moved": 0, "ignored": len(rows)}
            cleared = self._conn.execute(
                "DELETE FROM pending_transactions WHERE account_id = ?", (account_id,)
            ).rowcount
            moved = 0
            for r in rows:
                moved += self._conn.execute(
                    "DELETE FROM transactions WHERE account_id = ? AND tx_hash = ? AND synthetic = 0",
                    (account_id, r["tx_hash"]),
                ).rowcount
                self._conn.execute(
                    "INSERT OR IGNORE INTO pending_transactions "
                    "  (account_id, booking_date, amount, currency, counterparty, purpose, tx_hash, as_of, imported_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (account_id, r["booking_date"], float(r["amount"]), r.get("currency") or "EUR",
                     r.get("counterparty") or "", r.get("purpose") or "", r["tx_hash"], as_of or now[:10], now),
                )
            self._conn.commit()
        return {"added": len(rows), "cleared": int(cleared), "moved": int(moved), "ignored": 0}

    def pending_list(self, account_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            sql = ("SELECT p.*, a.bank_name, a.iban_last4 FROM pending_transactions p "
                   "LEFT JOIN accounts a ON a.id = p.account_id ")
            params: list[Any] = []
            if account_id:
                sql += "WHERE p.account_id = ? "
                params.append(account_id)
            sql += "ORDER BY p.booking_date DESC, p.id DESC"
            return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def finance_fixed_cost_override(self, key: str, mode: str) -> dict[str, str]:
        """mode: 'in' | 'out' | '' (clear)."""
        import json as _json
        try:
            overrides = _json.loads(self.meta_get("finance.fixed_cost_overrides") or "{}")
        except ValueError:
            overrides = {}
        if mode in ("in", "out"):
            overrides[key] = mode
        else:
            overrides.pop(key, None)
        self.meta_set("finance.fixed_cost_overrides", _json.dumps(overrides))
        return overrides

    def set_account_meta(
        self, account_id: int, *,
        is_savings: bool | None = None,
        start_balance: float | None = None,
        bank_name: str | None = None,
    ) -> dict[str, Any] | None:
        """Update the user-editable account attributes (savings flag,
        opening balance, display name). Only the fields that are not
        None get written. Returns the refreshed account row or None if
        the id doesn't exist."""
        sets: list[str] = []
        params: list[Any] = []
        if is_savings is not None:
            sets.append("is_savings = ?")
            params.append(1 if is_savings else 0)
        if start_balance is not None:
            sets.append("start_balance = ?")
            params.append(round(float(start_balance), 2))
        if bank_name is not None:
            sets.append("bank_name = ?")
            params.append(str(bank_name).strip()[:120] or "—")
        with self._lock:
            exists = self._conn.execute(
                "SELECT id FROM accounts WHERE id = ?", (account_id,)
            ).fetchone()
            if not exists:
                return None
            if sets:
                self._conn.execute(
                    f"UPDATE accounts SET {', '.join(sets)} WHERE id = ?",
                    (*params, account_id),
                )
                self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM accounts WHERE id = ?", (account_id,)
            ).fetchone()
        return dict(row) if row else None

    def finance_net_worth(self, as_of: str | None = None) -> dict[str, Any]:
        """Total wealth across all accounts, split into spending
        (Girokonten) and savings (Sparkonten/Tagesgeld/Depot).

        Per-account balance = start_balance + SUM(amount) up to and
        including `as_of` (ISO YYYY-MM-DD; None = latest). This is the
        "Gesamtvermögen zu Zeit x" the user asked for — pass a date to
        rewind the whole portfolio to that day."""
        clause = ""
        params: list[Any] = []
        if as_of:
            clause = " AND t.booking_date <= ?"
            params = [as_of[:10]]
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT a.id, a.bank_name, a.iban_last4,
                           COALESCE(a.is_savings, 0)    AS is_savings,
                           COALESCE(a.start_balance, 0) AS start_balance,
                           COALESCE((SELECT SUM(t.amount) FROM transactions t
                                     WHERE t.account_id = a.id{clause}), 0) AS net
                    FROM accounts a
                    ORDER BY COALESCE(a.is_savings, 0) ASC, a.bank_name""",
                params,
            ).fetchall()
        accounts: list[dict[str, Any]] = []
        spending_total = 0.0
        savings_total = 0.0
        for r in rows:
            bal = round(float(r["start_balance"] or 0.0) + float(r["net"] or 0.0), 2)
            is_sav = int(r["is_savings"] or 0)
            accounts.append({
                "id": r["id"], "bank_name": r["bank_name"],
                "iban_last4": r["iban_last4"], "is_savings": is_sav,
                "balance": bal,
            })
            if is_sav:
                savings_total += bal
            else:
                spending_total += bal
        return {
            "as_of": as_of or "",
            "total": round(spending_total + savings_total, 2),
            "spending_total": round(spending_total, 2),
            "savings_total": round(savings_total, 2),
            "accounts": accounts,
            "has_savings": any(a["is_savings"] for a in accounts),
        }

    def find_statement_by_file_hash(self, file_hash: str) -> dict[str, Any] | None:
        if not file_hash:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM statements WHERE file_hash = ? LIMIT 1", (file_hash,)
            ).fetchone()
        return dict(row) if row else None

    def upsert_statement(
        self,
        doc_id: int,
        *,
        account_id: int | None,
        period_start: str = "",
        period_end: str = "",
        statement_no: str = "",
        opening_balance: float | None = None,
        closing_balance: float | None = None,
        currency: str = "EUR",
        file_hash: str = "",
        privacy_mode: str = "",
        transactions: list[dict] | None = None,
        extra_json: str = "",
        extraction_warning: str = "",
    ) -> int:
        """Replace the statement + transactions for a document.

        Transactions get inserted with INSERT OR IGNORE on tx_hash so that
        a second statement covering an overlapping period does not create
        duplicate rows for the same booking. Returns the statement row id.

        `extraction_warning` flags a result the extractor doesn't trust
        (e.g. balances differ but no transactions were returned). The
        analyse-all selector picks these up on the next sweep so the
        user doesn't have to chase them manually."""
        transactions = transactions or []
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            existing = self._conn.execute(
                "SELECT id FROM statements WHERE doc_id = ?", (doc_id,)
            ).fetchone()
            if existing:
                stmt_id = int(existing["id"])
                self._conn.execute(
                    """UPDATE statements SET account_id=?, period_start=?, period_end=?,
                         statement_no=?, opening_balance=?, closing_balance=?,
                         currency=?, file_hash=?, privacy_mode=?, extra_json=?,
                         extraction_warning=?
                       WHERE id=?""",
                    (account_id, period_start, period_end, statement_no,
                     opening_balance, closing_balance, currency, file_hash,
                     privacy_mode, extra_json, extraction_warning, stmt_id),
                )
                self._conn.execute(
                    "DELETE FROM transactions WHERE statement_id = ?", (stmt_id,)
                )
            else:
                cur = self._conn.execute(
                    """INSERT INTO statements
                         (doc_id, account_id, period_start, period_end,
                          statement_no, opening_balance, closing_balance,
                          currency, file_hash, privacy_mode, extra_json,
                          extraction_warning, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (doc_id, account_id, period_start, period_end,
                     statement_no, opening_balance, closing_balance,
                     currency, file_hash, privacy_mode, extra_json,
                     extraction_warning, now),
                )
                stmt_id = cur.lastrowid or 0
            for i, tx in enumerate(transactions):
                # INSERT OR IGNORE: if a tx with the same tx_hash already
                # exists (= same booking from an overlapping statement),
                # skip silently. The dedup is still account-scoped because
                # tx_hash is computed from the IBAN hash + booking line.
                self._conn.execute(
                    """INSERT OR IGNORE INTO transactions
                         (statement_id, account_id, booking_date, value_date,
                          amount, currency, counterparty, counterparty_iban,
                          purpose, tx_type, category, tx_hash, line_no)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (stmt_id, account_id, tx.get("booking_date") or "",
                     tx.get("value_date") or "",
                     float(tx.get("amount") or 0.0), tx.get("currency") or currency,
                     tx.get("counterparty") or "", tx.get("counterparty_iban") or "",
                     tx.get("purpose") or "", tx.get("tx_type") or "",
                     tx.get("category") or "", tx.get("tx_hash") or "", i),
                )
            # Re-stamp manual category overrides over whatever the
            # extractor produced. Without this an upgrade-time bulk
            # reanalysis would silently undo the user's `Sonstige`
            # tagging across thousands of bookings.
            self._apply_overrides_to_statement(stmt_id)
        return stmt_id

    def get_statement(self, doc_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT s.*, a.bank_name, a.iban_last4, a.account_holder
                   FROM statements s
                   LEFT JOIN accounts a ON a.id = s.account_id
                   WHERE s.doc_id = ?""", (doc_id,)
            ).fetchone()
            if not row:
                return None
            txs = self._conn.execute(
                """SELECT * FROM transactions
                   WHERE statement_id = ?
                   ORDER BY booking_date, line_no, id""",
                (row["id"],),
            ).fetchall()
        out = dict(row)
        out["transactions"] = [dict(t) for t in txs]
        return out

    def finance_summary(self) -> dict[str, Any]:
        """Top-level cashflow numbers across all non-deleted statements.

        Internal transfers (category=uebertrag) — money the user moves
        between their own accounts — are excluded from the headline
        income / expense numbers since they otherwise dominate the
        chart with figures that aren't real cashflow ("€90,000 income"
        from closing a Tagesgeld and crediting the Girokonto). The
        full numbers including transfers stay accessible via the
        category breakdown."""
        with self._lock:
            tot = self._conn.execute(
                """SELECT
                     COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS income,
                     COALESCE(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END), 0) AS expense,
                     COUNT(*) AS tx_count
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'"""
            ).fetchone()
            # Separate "transfers" total so the UI can show it as a
            # neutral chip ("€110,000 zwischen eigenen Konten verschoben").
            transfers = self._conn.execute(
                """SELECT
                     COALESCE(SUM(amount), 0) AS net,
                     COUNT(*) AS n
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category = 'uebertrag'"""
            ).fetchone()
            stmt_count = self._conn.execute(
                """SELECT COUNT(*) AS n FROM statements s
                   JOIN documents d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL"""
            ).fetchone()
            acct_count = self._conn.execute(
                "SELECT COUNT(*) AS n FROM accounts"
            ).fetchone()
            cats = self._conn.execute(
                """SELECT t.category AS category,
                          COALESCE(SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END), 0) AS income,
                          COALESCE(SUM(CASE WHEN t.amount < 0 THEN t.amount ELSE 0 END), 0) AS expense,
                          COUNT(*) AS n
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL AND t.category != ''
                   GROUP BY t.category
                   ORDER BY expense ASC, income DESC"""
            ).fetchall()
        return {
            "income":          float(tot["income"]) if tot else 0.0,
            "expense":         float(tot["expense"]) if tot else 0.0,
            "net":             float((tot["income"] if tot else 0) + (tot["expense"] if tot else 0)),
            "tx_count":        int(tot["tx_count"]) if tot else 0,
            "statement_count": int(stmt_count["n"]) if stmt_count else 0,
            "account_count":   int(acct_count["n"]) if acct_count else 0,
            "by_category":     [dict(r) for r in cats],
            "transfer_count":  int(transfers["n"]) if transfers else 0,
            "transfer_volume": float(transfers["net"]) if transfers else 0.0,
        }

    def finance_monthly(self, months: int | None = None) -> list[dict[str, Any]]:
        """Income + expense per month, oldest first.

        Defaults to ALL months that contain bookings — earlier versions
        capped at the most recent 12 months, which silently dropped
        years of history when the user uploaded statements going back
        to 2016. Pass `months=N` to limit explicitly.

        Gaps within the visible range are filled with zero-rows so the
        chart shows a contiguous timeline instead of compressing months
        with no bookings out of existence (which made it look like
        statements were missing).

        Excludes internal transfers (category=uebertrag) for the same
        reason as `finance_summary`: a single big move between own
        accounts would dwarf every other month and make the chart
        useless."""
        params: list[Any] = []
        sql = (
            "SELECT substr(t.booking_date, 1, 7) AS month, "
            "       COALESCE(SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END), 0) AS income, "
            "       COALESCE(SUM(CASE WHEN t.amount < 0 THEN t.amount ELSE 0 END), 0) AS expense, "
            "       COUNT(*) AS n "
            "FROM transactions t "
            "JOIN statements   s ON s.id = t.statement_id "
            "JOIN documents    d ON d.id = s.doc_id "
            "WHERE d.deleted_at IS NULL "
            "  AND t.booking_date IS NOT NULL AND t.booking_date != '' "
            "  AND t.category != 'uebertrag' "
            "GROUP BY month "
            "ORDER BY month DESC "
        )
        if months is not None:
            sql += "LIMIT ? "
            params.append(months)
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        results = [dict(r) for r in reversed(rows)]
        if not results:
            return results
        # Fill gaps so the chart x-axis stays contiguous. Without this,
        # a year with only Jan + Dec bookings rendered as two adjacent
        # bars and looked like "everything in between is missing".
        try:
            from datetime import date as _date
            def _ym(s: str) -> tuple[int, int]:
                return int(s[:4]), int(s[5:7])
            first_y, first_m = _ym(results[0]["month"])
            last_y, last_m   = _ym(results[-1]["month"])
            filled: list[dict[str, Any]] = []
            existing = {r["month"]: r for r in results}
            cur_y, cur_m = first_y, first_m
            while (cur_y, cur_m) <= (last_y, last_m):
                key = f"{cur_y:04d}-{cur_m:02d}"
                if key in existing:
                    filled.append(existing[key])
                else:
                    filled.append({
                        "month": key, "income": 0.0,
                        "expense": 0.0, "n": 0,
                    })
                cur_m += 1
                if cur_m > 12:
                    cur_m = 1
                    cur_y += 1
            return filled
        except Exception:
            return results

    def finance_top_counterparties(
        self, *, direction: str = "expense", limit: int = 15,
    ) -> list[dict[str, Any]]:
        """Top counterparties by total spend or income.

        direction='expense' returns the largest outflows (most negative
        sum first); 'income' the largest inflows."""
        op = "<" if direction == "expense" else ">"
        order = "ASC" if direction == "expense" else "DESC"
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT t.counterparty AS counterparty,
                           COUNT(*) AS times,
                           COALESCE(SUM(t.amount), 0) AS total
                    FROM transactions t
                    JOIN statements   s ON s.id = t.statement_id
                    JOIN documents    d ON d.id = s.doc_id
                    WHERE d.deleted_at IS NULL
                      AND t.amount {op} 0
                      AND t.counterparty != ''
                      AND t.category != 'uebertrag'
                    GROUP BY LOWER(t.counterparty)
                    ORDER BY total {order}
                    LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def finance_recurring(self, *, min_months: int = 3, limit: int = 30) -> list[dict[str, Any]]:
        """Counterparties that show up in at least N distinct months with
        amounts within ±15% of each other — typical for subscriptions,
        rent, insurance, gym memberships, etc."""
        with self._lock:
            rows = self._conn.execute(
                """WITH cp_monthly AS (
                       SELECT LOWER(t.counterparty) AS cp_key,
                              t.counterparty AS counterparty,
                              substr(t.booking_date, 1, 7) AS month,
                              AVG(t.amount) AS avg_amount,
                              COUNT(*) AS n
                       FROM transactions t
                       JOIN statements   s ON s.id = t.statement_id
                       JOIN documents    d ON d.id = s.doc_id
                       WHERE d.deleted_at IS NULL
                         AND t.counterparty != ''
                         AND t.booking_date IS NOT NULL AND t.booking_date != ''
                       GROUP BY cp_key, month
                   )
                   SELECT counterparty,
                          COUNT(DISTINCT month) AS months,
                          AVG(avg_amount) AS amount,
                          MIN(avg_amount) AS min_amount,
                          MAX(avg_amount) AS max_amount
                   FROM cp_monthly
                   GROUP BY cp_key
                   HAVING months >= ?
                          AND ABS(MAX(avg_amount) - MIN(avg_amount)) <=
                              ABS(AVG(avg_amount)) * 0.15
                   ORDER BY months DESC, ABS(amount) DESC
                   LIMIT ?""",
                (min_months, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def _build_tx_filter(
        self,
        *,
        account_id: int | None = None,
        category: str | None = None,
        direction: str | None = None,   # 'income' | 'expense' | None
        start: str | None = None,
        end: str | None = None,
        query: str | None = None,
        amount_min: float | None = None,
        amount_max: float | None = None,
        exclude_uebertrag: bool = False,
    ) -> tuple[list[str], list[Any]]:
        """Shared WHERE-builder for the transactions explorer.

        `query` is split on commas — each non-empty token becomes a
        substring match against `counterparty OR purpose`, and the tokens
        themselves are OR'd together. So "rossmann, dm" returns rows that
        mention *either* Rossmann or DM, which is what the user types
        when comparing several merchants in one go.

        `amount_min` / `amount_max` are matched against the absolute
        amount, so the user doesn't have to flip signs depending on
        income vs expense — "show me everything between 50 € and 500 €"
        works regardless of direction.
        """
        where = ["d.deleted_at IS NULL"]
        params: list[Any] = []
        if account_id is not None:
            where.append("t.account_id = ?")
            params.append(account_id)
        if category:
            where.append("t.category = ?")
            params.append(category)
        if direction == "income":
            where.append("t.amount > 0")
        elif direction == "expense":
            where.append("t.amount < 0")
        if start:
            where.append("t.booking_date >= ?")
            params.append(start)
        if end:
            where.append("t.booking_date <= ?")
            params.append(end)
        if amount_min is not None:
            where.append("ABS(t.amount) >= ?")
            params.append(float(amount_min))
        if amount_max is not None:
            where.append("ABS(t.amount) <= ?")
            params.append(float(amount_max))
        if exclude_uebertrag:
            where.append("(t.category IS NULL OR t.category != 'uebertrag')")
        if query:
            tokens = [t.strip() for t in str(query).split(",") if t.strip()]
            if tokens:
                or_clauses = []
                for tok in tokens:
                    or_clauses.append("(t.counterparty LIKE ? OR t.purpose LIKE ?)")
                    like = f"%{tok}%"
                    params += [like, like]
                where.append("(" + " OR ".join(or_clauses) + ")")
        return where, params

    def transactions_list(
        self,
        *,
        account_id: int | None = None,
        category: str | None = None,
        direction: str | None = None,
        start: str | None = None,
        end: str | None = None,
        query: str | None = None,
        amount_min: float | None = None,
        amount_max: float | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        where, params = self._build_tx_filter(
            account_id=account_id, category=category, direction=direction,
            start=start, end=end, query=query,
            amount_min=amount_min, amount_max=amount_max,
        )
        sql = (
            "SELECT t.*, s.doc_id, a.bank_name, a.iban_last4, COALESCE(a.is_savings, 0) AS is_savings, "
            "       CASE WHEN d.category = '_csv_container' THEN 0 ELSE 1 END AS has_doc "
            "FROM transactions t "
            "JOIN statements   s ON s.id = t.statement_id "
            "JOIN documents    d ON d.id = s.doc_id "
            "LEFT JOIN accounts a ON a.id = t.account_id "
            "WHERE " + " AND ".join(where) +
            " ORDER BY t.booking_date DESC, t.id DESC LIMIT ? OFFSET ?"
        )
        params += [int(limit), int(offset)]
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def transactions_aggregate(
        self,
        *,
        account_id: int | None = None,
        category: str | None = None,
        direction: str | None = None,
        start: str | None = None,
        end: str | None = None,
        query: str | None = None,
        amount_min: float | None = None,
        amount_max: float | None = None,
        top_n: int = 10,
        monthly_limit: int = 36,
    ) -> dict[str, Any]:
        """Aggregations over the same filter that drives transactions_list.

        Returns counters + sums + top counterparties + a per-month
        breakdown so the explorer page can render KPI cards and a small
        trend chart from a single round trip. Internal transfers are
        excluded from the totals because they'd otherwise double-count
        spend that just moved between the user's own accounts.
        """
        where, params = self._build_tx_filter(
            account_id=account_id, category=category, direction=direction,
            start=start, end=end, query=query,
            amount_min=amount_min, amount_max=amount_max,
            # Asking explicitly for transfers must not yield an empty sum.
            exclude_uebertrag=(category != "uebertrag"),
        )
        join = (
            "FROM transactions t "
            "JOIN statements   s ON s.id = t.statement_id "
            "JOIN documents    d ON d.id = s.doc_id "
            "LEFT JOIN accounts a ON a.id = t.account_id "
        )
        where_sql = "WHERE " + " AND ".join(where)
        # Fetched BEFORE taking the lock — it takes the (non-reentrant) lock itself.
        saving = sorted(self.finance_saving_categories())
        sv_ph = ",".join("?" * len(saving)) if saving else "''"
        with self._lock:
            totals = self._conn.execute(
                "SELECT "
                "  COUNT(*)                                            AS n, "
                f"  COALESCE(SUM(CASE WHEN t.amount > 0 AND t.category NOT IN ({sv_ph}) THEN t.amount END), 0) AS sum_in, "
                f"  COALESCE(SUM(CASE WHEN t.amount < 0 AND t.category NOT IN ({sv_ph}) THEN t.amount END), 0) AS sum_out, "
                f"  COALESCE(SUM(CASE WHEN t.category IN ({sv_ph}) THEN -t.amount END), 0) AS saved, "
                "  COALESCE(SUM(t.amount), 0)                          AS sum_net, "
                "  COALESCE(MIN(t.booking_date), '')                   AS first_date, "
                "  COALESCE(MAX(t.booking_date), '')                   AS last_date "
                + join + where_sql,
                [*saving, *saving, *saving, *params],
            ).fetchone()
            # Top counterparties (by absolute spend, expense side first
            # since users care most about where the money went; income
            # gets its own list below for symmetry).
            top_expense = self._conn.execute(
                "SELECT COALESCE(NULLIF(t.counterparty, ''), '—') AS counterparty, "
                "       COUNT(*) AS times, "
                "       COALESCE(SUM(t.amount), 0) AS total "
                + join + where_sql + " AND t.amount < 0 "
                "GROUP BY LOWER(COALESCE(NULLIF(t.counterparty, ''), '—')) "
                "ORDER BY total ASC LIMIT ?",
                params + [int(top_n)],
            ).fetchall()
            top_income = self._conn.execute(
                "SELECT COALESCE(NULLIF(t.counterparty, ''), '—') AS counterparty, "
                "       COUNT(*) AS times, "
                "       COALESCE(SUM(t.amount), 0) AS total "
                + join + where_sql + " AND t.amount > 0 "
                "GROUP BY LOWER(COALESCE(NULLIF(t.counterparty, ''), '—')) "
                "ORDER BY total DESC LIMIT ?",
                params + [int(top_n)],
            ).fetchall()
            monthly = self._conn.execute(
                "SELECT substr(t.booking_date, 1, 7) AS month, "
                "       COALESCE(SUM(CASE WHEN t.amount > 0 THEN t.amount END), 0) AS sum_in, "
                "       COALESCE(SUM(CASE WHEN t.amount < 0 THEN t.amount END), 0) AS sum_out, "
                "       COUNT(*) AS n "
                + join + where_sql +
                " AND t.booking_date IS NOT NULL AND t.booking_date != '' "
                "GROUP BY month ORDER BY month DESC LIMIT ?",
                params + [int(monthly_limit)],
            ).fetchall()
            by_category = self._conn.execute(
                "SELECT COALESCE(NULLIF(t.category, ''), 'sonstiges') AS cat, "
                "       COUNT(*) AS n, "
                "       COALESCE(SUM(t.amount), 0) AS total "
                + join + where_sql +
                " GROUP BY cat ORDER BY total ASC",
                params,
            ).fetchall()
            # Transfers between own accounts for the SAME filter — listed in
            # the table but not in the sums; shown so the user sees why the
            # list is longer than the count.
            tw, tp = self._build_tx_filter(
                account_id=account_id, category=category, direction=direction,
                start=start, end=end, query=query,
                amount_min=amount_min, amount_max=amount_max,
                exclude_uebertrag=False,
            )
            transfers = self._conn.execute(
                "SELECT COUNT(*) AS n, "
                "  COALESCE(SUM(CASE WHEN t.amount > 0 THEN t.amount END), 0) AS sum_in, "
                "  COALESCE(SUM(CASE WHEN t.amount < 0 THEN t.amount END), 0) AS sum_out "
                + join + "WHERE " + " AND ".join(tw) + " AND t.category = 'uebertrag'",
                tp,
            ).fetchone()
            savings = self._conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(t.amount), 0) AS net "
                + join + where_sql + " AND COALESCE(a.is_savings, 0) = 1",
                params,
            ).fetchone()
        return {
            "transfer_count": int(transfers["n"] or 0),
            "transfer_in":    float(transfers["sum_in"] or 0.0),
            "transfer_out":   float(transfers["sum_out"] or 0.0),
            "savings_count":  int(savings["n"] or 0),
            "savings_net":    float(savings["net"] or 0.0),
            "count":      int(totals["n"] or 0),
            "sum_in":     float(totals["sum_in"] or 0.0),
            "sum_out":    float(totals["sum_out"] or 0.0),
            "saved":      float(totals["saved"] or 0.0),
            "sum_net":    float(totals["sum_net"] or 0.0),
            "first_date": totals["first_date"] or "",
            "last_date":  totals["last_date"] or "",
            "top_expense": [dict(r) for r in top_expense],
            "top_income":  [dict(r) for r in top_income],
            "monthly":     [dict(r) for r in reversed(monthly)],  # ascending
            "by_category": [dict(r) for r in by_category],
        }

    def transactions_set_category(
        self, tx_ids: list[int], category: str,
    ) -> int:
        """Bulk-recategorise a set of transactions. Returns the row count
        that was actually updated (skips ids that don't exist or already
        carried the same category).

        Also pins the chosen category in the override table keyed by
        tx_hash, so a later re-extraction (e.g. an upgrade-time bulk
        reanalysis) restores the user's manual labels instead of
        silently reverting to whatever the LLM picked this time."""
        if not tx_ids:
            return 0
        placeholders = ",".join("?" * len(tx_ids))
        now = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            # Capture tx_hashes BEFORE the UPDATE so the override table
            # gets stamped even for rows whose category was already the
            # target (rowcount-skipped by the WHERE clause below).
            hashes = [
                row["tx_hash"]
                for row in self._conn.execute(
                    f"SELECT tx_hash FROM transactions "
                    f"WHERE id IN ({placeholders}) "
                    f"  AND tx_hash IS NOT NULL AND tx_hash != ''",
                    [int(i) for i in tx_ids],
                ).fetchall()
            ]
            cur = self._conn.execute(
                f"UPDATE transactions SET category = ? "
                f"WHERE id IN ({placeholders}) "
                f"  AND COALESCE(category, '') != ?",
                [category, *[int(i) for i in tx_ids], category],
            )
            n = cur.rowcount or 0
            for h in hashes:
                self._conn.execute(
                    """INSERT INTO transaction_category_overrides
                         (tx_hash, category, set_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(tx_hash) DO UPDATE SET
                         category = excluded.category,
                         set_at   = excluded.set_at""",
                    (h, category, now),
                )
            self._conn.commit()
            return n

    # ---------- Finance: charts & analytics ----------

    def finance_salary_periods_saving_cats(self) -> set[str]:
        return self.finance_saving_categories()

    def finance_salary_periods(
        self, *, salary_match: str = "", anchor_day: int = 23,
        monthly_budget: float = 0.0, today: str | None = None,
    ) -> list[dict[str, Any]]:
        """Salary-period ("Gehaltsmonat") cashflow, oldest first.

        Pulls every non-deleted booking together with its account's
        is_savings flag and hands them to `finance.periods.build_periods`,
        which slices the stream at each detected salary credit (or a
        fixed anchor day as fallback) and aggregates income / expense /
        savings per period. See that module for the boundary logic."""
        from .finance.periods import build_periods
        with self._lock:
            rows = self._conn.execute(
                """SELECT t.booking_date, t.amount, t.category, t.tx_type,
                          t.counterparty, t.purpose,
                          COALESCE(a.is_savings, 0) AS is_savings
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  d ON d.id = s.doc_id
                   LEFT JOIN accounts a ON a.id = t.account_id
                   WHERE d.deleted_at IS NULL
                     AND t.booking_date IS NOT NULL AND t.booking_date != ''"""
            ).fetchall()
        txs = [dict(r) for r in rows]
        return build_periods(
            txs, saving_cats=self.finance_saving_categories(), salary_match=salary_match, anchor_day=anchor_day,
            monthly_budget=monthly_budget, today=today,
        )

    def finance_available_periods(self) -> dict[str, list[str]]:
        """Distinct years and YYYY-MM months that have bookings — used
        by the /finance period selectors so the dropdown only offers
        valid choices instead of empty months padded around the
        dataset."""
        with self._lock:
            yrs = self._conn.execute(
                """SELECT DISTINCT substr(t.booking_date, 1, 4) AS y
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.booking_date IS NOT NULL AND t.booking_date != ''
                   ORDER BY y DESC"""
            ).fetchall()
            mns = self._conn.execute(
                """SELECT DISTINCT substr(t.booking_date, 1, 7) AS m
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.booking_date IS NOT NULL AND t.booking_date != ''
                   ORDER BY m DESC"""
            ).fetchall()
        return {
            "years":  [r["y"] for r in yrs if r["y"]],
            "months": [r["m"] for r in mns if r["m"]],
        }

    def finance_spend_by_category(self, *, month: str | None = None,
                                  periods: list[dict[str, Any]] | None = None,
                                  history: int = 6,
                                  account_ids: list[int] | None = None) -> dict[str, Any]:
        """Spending of one month by *transaction category* — the categories
        the user assigns, the classifier explains and the rules learn. One
        system for the whole app (v0.45 — Wunsch: „das tab ausgaben bekommt
        neukategorisierungen nicht mit").

        Two ways to cut a "month":
        - `periods` given → salary months (Gehalt bis Gehalt); `month` is the
          period's start date and the result carries `range_start`/`range_end`.
        - no `periods` → calendar months, `month` is 'YYYY-MM'.
        Only outgoing bookings on spending accounts count — savings accounts,
        internal transfers and saving categories (money moved, not spent) are
        excluded. Per category: total, count, share, top payees, the bookings
        themselves (for the drill-down with inline re-categorising), the
        previous month and the average over the last `history` months.
        """
        from .finance.buckets import merchant_key
        saving = sorted(self.finance_saving_categories()) or ["__none__"]
        fixed = set(self.finance_fixed_categories())
        # A booking is a fixed cost when it belongs to a counted item on
        # /fixkosten (same payee, same amount class) — not when its whole
        # category is „fixed" (Mobilität holds the car loan AND the fuel).
        fx = self.finance_fixed_costs(months_back=12)
        fixed_items = [(it["key"], float(it["amount"])) for it in fx["items"] if not it.get("is_avg")]
        avg_cats = {it["category"] for it in fx["items"] if it.get("is_avg")}

        def _is_fixed_row(r: dict[str, Any]) -> bool:
            k = merchant_key(r.get("counterparty"))
            a = -float(r["amount"])
            if any(k == fk and abs(a - fa) <= max(0.2 * fa, 1.0) for fk, fa in fixed_items):
                return True
            # a category counted as a monthly average on /fixkosten
            return (r.get("category") or "sonstiges") in avg_cats
        if periods:
            plist = [{"key": p["start"], "start": p["start"], "end": p["end"],
                      "is_current": p.get("is_current", False), "due_date": p.get("due_date", "")}
                     for p in reversed(periods)]      # newest first, like the calendar list
            keys = [p["key"] for p in plist]
            mode = "salary"
        else:
            keys = self.finance_available_periods()["months"]
            plist = []
            mode = "calendar"
        if not month or month not in keys:
            month = keys[0] if keys else ""
        base = {"month": month, "months": keys, "mode": mode, "fixed_categories": sorted(fixed),
                "saving_categories": [c for c in saving if c != "__none__"]}
        if not month:
            return {**base, "range_start": "", "range_end": "", "prev_month": "", "prev_range_start": "",
                    "prev_range_end": "", "total": 0.0, "fixed_total": 0.0, "variable_total": 0.0,
                    "prev_total": 0.0, "avg_total": 0.0, "avg_months": 0, "categories": [], "days": [],
                    "open_count": 0, "income_total": 0.0, "prev_income_total": 0.0, "net": 0.0, "prev_net": 0.0,
                    "income_categories": [], "income_open_count": 0}

        idx = keys.index(month)
        prev_month = keys[idx + 1] if idx + 1 < len(keys) else ""
        hist_keys = keys[idx + 1: idx + 1 + history]

        def _range(key: str) -> tuple[str, str]:
            if not key:
                return "", ""
            if mode == "salary":
                p = next((x for x in plist if x["key"] == key), None)
                if not p:
                    return "", ""
                end = p["end"]
                # 🔴 Der LAUFENDE Gehaltsmonat wird bis zu seinem erwarteten
                # Ende gezeichnet, nicht bis heute (Wunsch: „auch am
                # gehaltsmonatsanfang sollen alle tage schon zu sehen sein
                # und zeigen wieviel pro tag ausgegeben werden darf max").
                # `finance_salary_periods` schließt den offenen Zeitraum bei
                # heute ab — damit stünden am dritten Tag drei Balken statt
                # eines Monats. Die kommenden Tage markiert `_day_series`
                # als `future`; sie zählen nirgends mit.
                if p.get("is_current") and p.get("due_date"):
                    from datetime import date as _dd, timedelta as _td
                    try:
                        expected = (_dd.fromisoformat(p["due_date"]) - _td(days=1)).isoformat()
                        end = max(end, expected)
                    except ValueError:
                        pass
                return p["start"], end
            y, m = int(key[:4]), int(key[5:7])
            last = 31 if m in (1, 3, 5, 7, 8, 10, 12) else 30 if m != 2 else (29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28)
            return f"{key}-01", f"{key}-{last:02d}"

        def _rows(key: str, *, income: bool = False) -> list[dict[str, Any]]:
            """Bookings of the period on spending accounts, no transfers, no
            saving categories; `income=True` → the credits instead (v0.47.3:
            the owner wants income shown and netted against the spending)."""
            if not key:
                return []
            a, b = _range(key)
            # Kontenauswahl (Wunsch: „ein Konto raus, alle anderen drin —
            # oder eines drin, alle anderen raus"). Leer = alle Konten.
            acc_sql = ""
            acc_args: tuple[Any, ...] = ()
            if account_ids:
                acc_sql = " AND t.account_id IN (" + ",".join("?" * len(account_ids)) + ")"
                acc_args = tuple(account_ids)
            with self._lock:
                rows = self._conn.execute(
                    """SELECT t.id, t.booking_date, t.amount, COALESCE(t.category, '') AS category,
                              t.category_source, t.counterparty, t.purpose, t.tx_type,
                              a.bank_name, a.iban_last4
                       FROM transactions t
                       JOIN statements s ON s.id = t.statement_id
                       JOIN documents  d ON d.id = s.doc_id
                       LEFT JOIN accounts a ON a.id = t.account_id
                       WHERE d.deleted_at IS NULL
                         AND COALESCE(a.is_savings, 0) = 0
                         AND COALESCE(t.category, '') != 'uebertrag'
                         AND COALESCE(t.category, '') NOT IN (""" + ",".join("?" * len(saving)) + """)
                         AND """ + ("t.amount > 0" if income else "t.amount < 0") + acc_sql + """
                         AND t.booking_date >= ? AND t.booking_date <= ?
                       ORDER BY t.booking_date DESC, t.id DESC""",
                    (*saving, *acc_args, a, b),
                ).fetchall()
            return [dict(r) for r in rows]

        def _totals(rows: list[dict[str, Any]]) -> dict[str, float]:
            out: dict[str, float] = {}
            for r in rows:
                c = r["category"] or "sonstiges"
                out[c] = out.get(c, 0.0) + (-float(r["amount"]))
            return out

        cur_rows = _rows(month)
        cur = _totals(cur_rows)
        total = round(sum(cur.values()), 2)
        prev = _totals(_rows(prev_month)) if prev_month else {}
        hist = [_totals(_rows(k)) for k in hist_keys]
        avg: dict[str, float] = {}
        for c in set(cur) | {c for h in hist for c in h}:
            avg[c] = round(sum(h.get(c, 0.0) for h in hist) / len(hist), 2) if hist else 0.0
        per_day: dict[str, float] = {}
        cats: dict[str, dict[str, Any]] = {}
        for r in cur_rows:
            c = r["category"] or "sonstiges"
            e = cats.setdefault(c, {"category": c, "total": 0.0, "fixed": 0.0, "count": 0, "_merchants": {}, "rows": []})
            spend = -float(r["amount"])
            fixed_row = _is_fixed_row(r)
            e["total"] += spend
            e["fixed"] += spend if fixed_row else 0.0
            e["count"] += 1
            name = (r["counterparty"] or "").strip() or "—"
            m = e["_merchants"].setdefault(name, {"name": name, "total": 0.0, "count": 0})
            m["total"] += spend
            m["count"] += 1
            e["rows"].append({
                "id": r["id"], "booking_date": r["booking_date"], "amount": float(r["amount"]),
                "counterparty": name, "purpose": (r["purpose"] or "")[:140], "category": c,
                "category_source": r["category_source"] or "", "bank_name": r["bank_name"] or "",
                "iban_last4": r["iban_last4"] or "", "is_fixed": fixed_row,
            })
            day = str(r["booking_date"] or "")[:10]
            per_day[day] = per_day.get(day, 0.0) + spend
        categories = []
        for c, e in cats.items():
            tops = sorted(e.pop("_merchants").values(), key=lambda x: -x["total"])
            for t in tops:
                t["total"] = round(t["total"], 2)
            categories.append({
                **e, "total": round(e["total"], 2), "fixed": round(e["fixed"], 2),
                "share": round(e["total"] / total * 100, 1) if total else 0.0,
                "prev": round(prev.get(c, 0.0), 2), "avg": avg.get(c, 0.0),
                "is_fixed_category": c in fixed, "top_merchants": tops[:5],
            })
        categories.sort(key=lambda x: -x["total"])
        fixed_total = round(sum(x["fixed"] for x in categories), 2)

        # Income of the same period, by category, netted against the spending.
        inc_rows = _rows(month, income=True)
        inc_total = round(sum(float(r["amount"]) for r in inc_rows), 2)
        prev_inc_total = round(sum(float(r["amount"]) for r in _rows(prev_month, income=True)), 2) if prev_month else 0.0
        inc_cats: dict[str, dict[str, Any]] = {}
        for r in inc_rows:
            c = r["category"] or "sonstiges"
            e = inc_cats.setdefault(c, {"category": c, "total": 0.0, "count": 0, "_merchants": {}, "rows": []})
            amt = float(r["amount"])
            e["total"] += amt
            e["count"] += 1
            name = (r["counterparty"] or "").strip() or "—"
            m = e["_merchants"].setdefault(name, {"name": name, "total": 0.0, "count": 0})
            m["total"] += amt
            m["count"] += 1
            e["rows"].append({
                "id": r["id"], "booking_date": r["booking_date"], "amount": amt,
                "counterparty": name, "purpose": (r["purpose"] or "")[:140], "category": c,
                "category_source": r["category_source"] or "", "bank_name": r["bank_name"] or "",
                "iban_last4": r["iban_last4"] or "", "is_fixed": False,
            })
        income_categories = []
        for c, e in inc_cats.items():
            tops = sorted(e.pop("_merchants").values(), key=lambda x: -x["total"])
            for t in tops:
                t["total"] = round(t["total"], 2)
            income_categories.append({
                **e, "total": round(e["total"], 2),
                "share": round(e["total"] / inc_total * 100, 1) if inc_total else 0.0,
                "top_merchants": tops[:5],
            })
        income_categories.sort(key=lambda x: -x["total"])
        rs, re_ = _range(month)
        prs, pre = _range(prev_month)
        return {
            **base,
            "range_start": rs, "range_end": re_,
            "prev_month": prev_month, "prev_range_start": prs, "prev_range_end": pre,
            "total": total, "fixed_total": fixed_total, "variable_total": round(total - fixed_total, 2),
            "prev_total": round(sum(prev.values()), 2),
            "avg_total": round(sum(sum(h.values()) for h in hist) / len(hist), 2) if hist else 0.0,
            "avg_months": len(hist),
            "categories": categories,
            "income_total": inc_total, "prev_income_total": prev_inc_total,
            "net": round(inc_total - total, 2), "prev_net": round(prev_inc_total - round(sum(prev.values()), 2), 2),
            "income_categories": income_categories,
            "income_open_count": sum(1 for r in inc_rows if (r["category"] or "sonstiges") == "sonstiges"),
            "open_count": sum(1 for r in cur_rows if (r["category"] or "sonstiges") == "sonstiges"),
            # Tage OHNE Ausgabe gehören dazu (Wunsch: „es soll auch tage
            # anzeigen wo man nichts ausgegeben hat"). `per_day` kennt nur
            # Tage mit Buchung — die Reihe wird deshalb über den ganzen
            # Zeitraum aufgefüllt, aber höchstens bis HEUTE: ein Tag, der
            # noch nicht stattgefunden hat, ist kein Tag ohne Ausgaben.
            "days": _day_series(rs, re_, per_day, self._last_booking_date()),
            "data_through": self._last_booking_date(),
        }

    # ---- KI-Händlerzuordnung (merchant_buckets) ---------------------

    def merchant_bucket_overrides(self) -> dict[str, str]:
        """Alle gecachten Händler→Topf-Zuordnungen als {merchant_key → bucket}.
        Wird von finance_spend_buckets an buckets.summarise durchgereicht."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT merchant_key, bucket FROM merchant_buckets"
            ).fetchall()
        return {r["merchant_key"]: r["bucket"] for r in rows if r["merchant_key"]}

    def merchant_bucket_upsert(self, merchant_key: str, bucket: str, *,
                               sample_name: str = "", source: str = "ai") -> None:
        """Eine Händler→Topf-Zuordnung setzen/aktualisieren."""
        if not merchant_key or not bucket:
            return
        from datetime import datetime as _dt
        now = _dt.now().isoformat(timespec="seconds")
        with self._lock:
            self._conn.execute(
                """INSERT INTO merchant_buckets
                       (merchant_key, sample_name, bucket, source, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(merchant_key) DO UPDATE SET
                       sample_name = excluded.sample_name,
                       bucket      = excluded.bucket,
                       source      = excluded.source,
                       updated_at  = excluded.updated_at""",
                (merchant_key, sample_name[:200], bucket, source, now),
            )
            self._conn.commit()

    def finance_unclassified_merchants(self, *, limit: int = 400,
                                       include_cached: bool = False
                                       ) -> list[dict[str, Any]]:
        """Distinct Händler, die aktuell im Topf „Sonstiges" landen — also die
        Kandidaten, die die lokale KI noch zuordnen könnte.

        Läuft die reine Keyword-/Kategorie-Klassifizierung über ALLE
        Ausgaben-Buchungen (Spar-/Umbuchungen ausgenommen) und sammelt die,
        die als „sonstiges" enden, gruppiert pro normalisiertem Händler mit
        Gesamtausgabe + Beispiel-Verwendungszweck. Bereits gecachte Händler
        werden übersprungen (außer include_cached=True), damit ein erneuter
        KI-Lauf nur die echten Lücken abarbeitet. Nach Gesamtausgabe sortiert,
        damit die teuersten Unbekannten zuerst drankommen."""
        from .finance.buckets import classify, merchant_key

        cached = set() if include_cached else set(self.merchant_bucket_overrides())
        with self._lock:
            rows = self._conn.execute(
                """SELECT t.amount, t.category, t.counterparty, t.purpose
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  d ON d.id = s.doc_id
                   LEFT JOIN accounts a ON a.id = t.account_id
                   WHERE d.deleted_at IS NULL
                     AND COALESCE(a.is_savings, 0) = 0
                     AND (t.category IS NULL OR t.category != 'uebertrag')
                     AND t.amount < 0
                     AND t.counterparty IS NOT NULL
                     AND TRIM(t.counterparty) != ''"""
            ).fetchall()

        agg: dict[str, dict[str, Any]] = {}
        for r in rows:
            tx = dict(r)
            # Keyword-Klassifizierung OHNE Overrides — wir wollen ja gerade
            # die finden, die ohne KI im Sonstiges-Topf landen.
            if classify(tx) != "sonstiges":
                continue
            key = merchant_key(tx.get("counterparty"))
            if not key or key in cached:
                continue
            spend = -float(tx.get("amount") or 0.0)
            e = agg.get(key)
            if e is None:
                e = agg[key] = {
                    "key": key,
                    "name": (tx.get("counterparty") or "").strip(),
                    "sample": (tx.get("purpose") or "").strip(),
                    "total": 0.0,
                    "count": 0,
                }
            e["total"] += spend
            e["count"] += 1
        out = sorted(agg.values(), key=lambda x: x["total"], reverse=True)
        for e in out:
            e["total"] = round(e["total"], 2)
        return out[:limit]

    def finance_heatmap(self, *, year: str | None = None,
                        month: str | None = None) -> dict[str, Any]:
        """Daily expense totals for the calendar heatmap.

        - When `month` is given (YYYY-MM): returns daily totals for that
          month, plus the day-of-week of the 1st so the template can
          align the calendar grid.
        - When only `year` is given: returns daily totals for the full
          year (Jan 1 – Dec 31) for a GitHub-style annual heatmap.
        - When neither is given: defaults to the most-recent year that
          has any booking.

        Internal transfers are excluded so the heatmap reflects real
        spending intensity, not bookkeeping moves between own accounts.
        """
        if month and len(month) >= 7:
            ym = month[:7]
            start = ym + "-01"
            # SQLite has no direct "last day of month" — date(start, '+1 month', '-1 day').
            with self._lock:
                rows = self._conn.execute(
                    """SELECT t.booking_date AS date,
                              COALESCE(SUM(ABS(t.amount)), 0) AS spend,
                              COALESCE(SUM(CASE WHEN t.amount > 0 THEN  t.amount ELSE 0 END), 0) AS income,
                              COUNT(*) AS n
                       FROM transactions t
                       JOIN statements s ON s.id = t.statement_id
                       JOIN documents  d ON d.id = s.doc_id
                       WHERE d.deleted_at IS NULL
                         AND t.booking_date >= ?
                         AND t.booking_date <  date(?, '+1 month')
                       GROUP BY t.booking_date
                       ORDER BY t.booking_date""",
                    (start, start),
                ).fetchall()
            return {
                "mode": "month", "year": ym[:4], "month": ym,
                "days": [dict(r) for r in rows],
            }

        # Year mode (default to the year with the MOST bookings — the
        # chronologically-latest year is often a single end-of-year
        # statement and looks empty in the grid).
        if not year:
            with self._lock:
                row = self._conn.execute(
                    """SELECT substr(t.booking_date, 1, 4) AS y, COUNT(*) AS n
                       FROM transactions t
                       JOIN statements s ON s.id = t.statement_id
                       JOIN documents  d ON d.id = s.doc_id
                       WHERE d.deleted_at IS NULL
                         AND t.booking_date IS NOT NULL AND t.booking_date != ''
                       GROUP BY y
                       ORDER BY n DESC
                       LIMIT 1"""
                ).fetchone()
            year = (row["y"] if row else None) or ""
        if not year:
            return {"mode": "year", "year": "", "month": "", "days": []}
        start = f"{year}-01-01"
        end   = f"{int(year) + 1}-01-01"
        with self._lock:
            rows = self._conn.execute(
                """SELECT t.booking_date AS date,
                          COALESCE(SUM(ABS(t.amount)), 0) AS spend,
                          COALESCE(SUM(CASE WHEN t.amount > 0 THEN  t.amount ELSE 0 END), 0) AS income,
                          COUNT(*) AS n
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.booking_date >= ? AND t.booking_date < ?
                   GROUP BY t.booking_date
                   ORDER BY t.booking_date""",
                (start, end),
            ).fetchall()
        return {
            "mode": "year", "year": year, "month": "",
            "days": [dict(r) for r in rows],
        }

    def finance_category_monthly(self, *, start: str | None = None,
                                 end: str | None = None) -> dict[str, Any]:
        """Per-category spend per month for the stacked chart.

        `start` / `end` are inclusive YYYY-MM bounds. When neither is
        given the query covers the whole booking history — the
        previous behaviour clamped the window to "last 12 months" and
        the user couldn't see further back than that.
        """
        clauses = ["d.deleted_at IS NULL", "t.category != 'uebertrag'",
                   "t.amount < 0",
                   "t.booking_date IS NOT NULL AND t.booking_date != ''"]
        params: list[Any] = []
        if start:
            clauses.append("substr(t.booking_date, 1, 7) >= ?")
            params.append(start[:7])
        if end:
            clauses.append("substr(t.booking_date, 1, 7) <= ?")
            params.append(end[:7])
        where = " AND ".join(clauses)
        sql = (
            "SELECT substr(t.booking_date, 1, 7) AS month, "
            "       COALESCE(NULLIF(t.category, ''), 'sonstiges') AS category, "
            "       COALESCE(SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END), 0) AS spend, "
            "       COUNT(*) AS n "
            "FROM transactions t "
            "JOIN statements s ON s.id = t.statement_id "
            "JOIN documents  d ON d.id = s.doc_id "
            "WHERE " + where + " "
            "GROUP BY month, t.category "
            "ORDER BY month ASC"
        )
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        # Restructure into months × categories matrix for easy template
        # rendering. Categories ranked by total spend so the legend
        # reads largest-first.
        by_month: dict[str, dict[str, float]] = {}
        cat_totals: dict[str, float] = {}
        for r in rows:
            m, c, s = r["month"], r["category"], float(r["spend"])
            by_month.setdefault(m, {})[c] = s
            cat_totals[c] = cat_totals.get(c, 0.0) + s
        ranked_cats = [c for c, _ in sorted(cat_totals.items(), key=lambda kv: -kv[1])]
        months_sorted = sorted(by_month.keys())
        return {
            "months": months_sorted,
            "categories": ranked_cats,
            "matrix": [
                {"month": m, "values": [by_month[m].get(c, 0.0) for c in ranked_cats]}
                for m in months_sorted
            ],
        }

    def finance_by_weekday(self) -> list[dict[str, Any]]:
        """Spend totals per day of week (0=Mon … 6=Sun for display).
        SQLite's strftime('%w', …) returns 0=Sun … 6=Sat, we shift it
        in the SELECT so Monday is the leftmost column in charts.

        Filters NULL `dow` rows in Python instead of via HAVING — that
        catches transactions whose booking_date was stored in a non-ISO
        format (e.g. "31.07.2024") and would otherwise crash int(None)
        downstream."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT ((CAST(strftime('%w', t.booking_date) AS INTEGER) + 6) % 7) AS dow,
                          COALESCE(SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END), 0) AS spend,
                          COALESCE(AVG(CASE WHEN t.amount < 0 THEN -t.amount ELSE NULL END), 0) AS avg_spend,
                          COUNT(*) AS n
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'
                     AND t.booking_date IS NOT NULL AND t.booking_date != ''
                   GROUP BY dow
                   ORDER BY dow"""
            ).fetchall()
        present: dict[int, dict] = {}
        for r in rows:
            if r["dow"] is None:
                continue
            present[int(r["dow"])] = dict(r)
        return [
            present.get(d, {"dow": d, "spend": 0.0, "avg_spend": 0.0, "n": 0})
            for d in range(7)
        ]

    def finance_by_day_of_month(self) -> list[dict[str, Any]]:
        """Spend totals per day-of-month — surfaces "everything hits
        on the 1st" patterns (rent, insurance, subscriptions). Always
        returns rows 1–31 so the chart has a stable axis.

        Same NULL-tolerance as finance_by_weekday: a non-ISO
        booking_date makes strftime return NULL, which we drop here
        instead of crashing int(None)."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT CAST(strftime('%d', t.booking_date) AS INTEGER) AS dom,
                          COALESCE(SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END), 0) AS spend,
                          COUNT(*) AS n
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'
                     AND t.booking_date IS NOT NULL AND t.booking_date != ''
                   GROUP BY dom
                   ORDER BY dom"""
            ).fetchall()
        present: dict[int, dict] = {}
        for r in rows:
            if r["dom"] is None:
                continue
            present[int(r["dom"])] = dict(r)
        return [
            present.get(d, {"dom": d, "spend": 0.0, "n": 0})
            for d in range(1, 32)
        ]

    def finance_by_tx_type(self) -> list[dict[str, Any]]:
        """Total spend grouped by transaction type. Lets the user see
        whether their money goes via card, direct debit, transfer,
        cash withdrawals…"""
        with self._lock:
            rows = self._conn.execute(
                """SELECT COALESCE(NULLIF(t.tx_type, ''), 'sonstiges') AS tx_type,
                          COALESCE(SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END), 0) AS spend,
                          COUNT(*) AS n
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'
                     AND t.amount < 0
                   GROUP BY tx_type
                   ORDER BY spend DESC"""
            ).fetchall()
        return [dict(r) for r in rows]

    def finance_largest_tx(self, limit: int = 15) -> list[dict[str, Any]]:
        """Largest individual transactions by absolute amount. Useful
        to spot the one-off €1200 dentist that shows up in the
        category sums but isn't visible in monthly averages."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT t.booking_date, t.amount, t.counterparty, t.category, t.purpose,
                          s.doc_id, a.bank_name, a.iban_last4
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   LEFT JOIN accounts a ON a.id = t.account_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'
                   ORDER BY ABS(t.amount) DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def finance_balance_history(self, account_id: int | None = None) -> list[dict[str, Any]]:
        """Running balance per day, anchored to the earliest
        statement's opening balance for the account. Returns daily
        running totals for the line chart. We use the booking date
        for ordering — value_date can be NULL or out of order on
        Sparkasse statements."""
        with self._lock:
            # Pick the earliest statement (by period_start) per account
            # that carries an opening_balance. SQLite's window functions
            # let us do this without the aggregate-in-subquery trick
            # that earlier failed.
            anchor_sql = (
                "SELECT account_id, opening_balance, period_start FROM ("
                "  SELECT s.account_id, s.opening_balance, s.period_start, "
                "         ROW_NUMBER() OVER ("
                "           PARTITION BY s.account_id "
                "           ORDER BY s.period_start ASC) AS rn "
                "  FROM statements s "
                "  WHERE s.opening_balance IS NOT NULL "
                + ("AND s.account_id = ? " if account_id is not None else "")
                + ") WHERE rn = 1"
            )
            anchors = {}
            for r in self._conn.execute(
                anchor_sql, (account_id,) if account_id is not None else ()
            ).fetchall():
                anchors[r["account_id"]] = (
                    r["period_start"] or "",
                    float(r["opening_balance"] or 0.0),
                )

            params: list[Any] = []
            where = ["d.deleted_at IS NULL", "t.booking_date != ''"]
            if account_id is not None:
                where.append("t.account_id = ?")
                params.append(account_id)
            sql = (
                "SELECT t.account_id, t.booking_date, SUM(t.amount) AS net "
                "FROM transactions t "
                "JOIN statements s ON s.id = t.statement_id "
                "JOIN documents  d ON d.id = s.doc_id "
                "WHERE " + " AND ".join(where) + " "
                "GROUP BY t.account_id, t.booking_date "
                "ORDER BY t.account_id, t.booking_date"
            )
            rows = self._conn.execute(sql, params).fetchall()

        # Walk per-account, accumulating from the anchor opening balance.
        out: list[dict[str, Any]] = []
        running: dict[int | None, float] = {}
        for r in rows:
            acct = r["account_id"]
            date = r["booking_date"]
            if acct not in running:
                anchor = anchors.get(acct, ("", 0.0))
                running[acct] = anchor[1]
            running[acct] = running[acct] + float(r["net"] or 0.0)
            out.append({
                "account_id": acct,
                "date": date,
                "balance": round(running[acct], 2),
            })
        return out

    def finance_category_totals(self, *, start: str | None = None,
                                end: str | None = None,
                                direction: str = "spend") -> list[dict[str, Any]]:
        """Total spend (or income) per category over the selected
        period — feeds the donut / pie chart on /finance.

        `direction` is either 'spend' (negative amounts) or 'income'
        (positive). Internal transfers always excluded.
        """
        sign = "-1" if direction == "spend" else "+1"
        amount_clause = "t.amount < 0" if direction == "spend" else "t.amount > 0"
        clauses = ["d.deleted_at IS NULL", "t.category != 'uebertrag'", amount_clause,
                   "t.booking_date IS NOT NULL AND t.booking_date != ''"]
        params: list[Any] = []
        if start:
            clauses.append("substr(t.booking_date, 1, 7) >= ?")
            params.append(start[:7])
        if end:
            clauses.append("substr(t.booking_date, 1, 7) <= ?")
            params.append(end[:7])
        sql = (
            "SELECT COALESCE(NULLIF(t.category, ''), 'sonstiges') AS cat, "
            f"       COALESCE(SUM(t.amount * {sign}), 0) AS total, "
            "       COUNT(*) AS n "
            "FROM transactions t "
            "JOIN statements s ON s.id = t.statement_id "
            "JOIN documents  d ON d.id = s.doc_id "
            "WHERE " + " AND ".join(clauses) + " "
            "GROUP BY cat "
            "HAVING total > 0 "
            "ORDER BY total DESC"
        )
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [{"category": r["cat"], "total": r["total"], "n": r["n"]} for r in rows]

    def finance_counterparty_treemap(self, limit: int = 25) -> list[dict[str, Any]]:
        """Top counterparties by absolute spend — feeds the treemap.
        Counterparty names are normalised loosely (case-folded,
        leading whitespace stripped) so 'REWE' and 'rewe markt' merge
        into one entry."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT TRIM(LOWER(t.counterparty)) AS key,
                          MAX(t.counterparty) AS counterparty,
                          COUNT(*) AS times,
                          COALESCE(SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END), 0) AS spend,
                          MAX(COALESCE(NULLIF(t.category, ''), 'sonstiges')) AS dominant_category
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'
                     AND t.counterparty != ''
                     AND t.amount < 0
                   GROUP BY key
                   HAVING spend > 0
                   ORDER BY spend DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def finance_kpis(self) -> dict[str, Any]:
        """Single-number cards for the dashboard. Quick look-ups —
        average daily spend, biggest single tx amount, busiest month
        by tx count, busiest counterparty by tx count."""
        with self._lock:
            avg = self._conn.execute(
                """SELECT
                     COALESCE(AVG(daily.spend), 0) AS avg_daily_spend,
                     COALESCE(MAX(daily.spend), 0) AS peak_daily_spend
                   FROM (
                     SELECT t.booking_date AS d,
                            SUM(CASE WHEN t.amount < 0 THEN -t.amount ELSE 0 END) AS spend
                     FROM transactions t
                     JOIN statements s ON s.id = t.statement_id
                     JOIN documents  d ON d.id = s.doc_id
                     WHERE d.deleted_at IS NULL
                       AND t.category != 'uebertrag'
                       AND t.booking_date != ''
                     GROUP BY t.booking_date
                   ) daily"""
            ).fetchone()
            biggest = self._conn.execute(
                """SELECT t.amount, t.counterparty
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'
                   ORDER BY ABS(t.amount) DESC LIMIT 1"""
            ).fetchone()
            busiest_month = self._conn.execute(
                """SELECT substr(t.booking_date, 1, 7) AS month, COUNT(*) AS n
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'
                     AND t.booking_date != ''
                   GROUP BY month ORDER BY n DESC LIMIT 1"""
            ).fetchone()
            top_cp = self._conn.execute(
                """SELECT t.counterparty, COUNT(*) AS n
                   FROM transactions t
                   JOIN statements s ON s.id = t.statement_id
                   JOIN documents  d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.category != 'uebertrag'
                     AND t.counterparty != ''
                   GROUP BY LOWER(t.counterparty)
                   ORDER BY n DESC LIMIT 1"""
            ).fetchone()
        return {
            "avg_daily_spend":  float(avg["avg_daily_spend"]) if avg else 0.0,
            "peak_daily_spend": float(avg["peak_daily_spend"]) if avg else 0.0,
            "biggest_amount":   float(biggest["amount"]) if biggest else 0.0,
            "biggest_counterparty": (biggest["counterparty"] if biggest else "") or "",
            "busiest_month":    (busiest_month["month"] if busiest_month else "") or "",
            "busiest_month_n":  int(busiest_month["n"]) if busiest_month else 0,
            "top_counterparty": (top_cp["counterparty"] if top_cp else "") or "",
            "top_counterparty_n": int(top_cp["n"]) if top_cp else 0,
        }

    def top_items(self, limit: int = 10) -> list[dict[str, Any]]:
        """Most-bought item names with aggregate counts and spend."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT i.name,
                          COUNT(*)               AS times,
                          COALESCE(SUM(i.total_price), 0) AS spent,
                          COALESCE(AVG(i.unit_price),  0) AS avg_unit
                   FROM receipt_items i
                   JOIN receipts r ON r.id = i.receipt_id
                   JOIN documents d ON d.id = r.doc_id
                   WHERE d.deleted_at IS NULL AND i.name != ''
                   GROUP BY LOWER(i.name)
                   ORDER BY times DESC, spent DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]


_db_singleton: Database | None = None
_singleton_lock = threading.Lock()


def open_db(path: Path) -> Database:
    """Return a process-wide singleton Database for the given path."""
    global _db_singleton
    with _singleton_lock:
        if _db_singleton is None or _db_singleton.path != path:
            _db_singleton = Database(path)
        return _db_singleton
