"""SQLite storage for document metadata, token usage, and costs.

The database lives alongside the library (path comes from config) and is
created on first access. Callers should use `open_db()` to get a `Database`
instance — it owns a single connection in WAL mode so the watcher thread and
the web server can read concurrently while writes are serialized.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


logger = logging.getLogger("docusort.db")


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
        logger.info("Database ready at %s", path)

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
        ]
        for name, decl in migrations:
            if name not in cols:
                self._conn.execute(f"ALTER TABLE documents ADD COLUMN {name} {decl}")
                logger.info("DB migration: added column %s", name)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_hash    ON documents(content_hash)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_deleted ON documents(deleted_at)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_subcat  ON documents(subcategory)")

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

        # NOTE: 0.15.1 shipped a startup cleanup that deleted every
        # `statements` row whose document didn't have category =
        # 'Kontoauszug'. That was destructive — pre-0.13 installs
        # legitimately stored statements under the legacy `Bank`
        # category (`backfill_statements()` still handles that case),
        # so the cleanup wiped real transactions. The cleanup has
        # been removed; re-classification cascade still happens
        # inline in `update_metadata()` for the original use case
        # (user moves a non-statement away from Kontoauszug).

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
    ) -> None:
        """Apply a manual metadata edit. Token counts and confidence are
        preserved; status defaults to 'filed' since a human just verified it."""
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
            # If the user re-classified a doc away from Kontoauszug /
            # Kassenzettel, drop the corresponding extractor row so the
            # /finance "needs review" banner and the receipts dashboard
            # don't keep flagging an orphan that no longer belongs to
            # them. ON DELETE CASCADE on transactions / receipt_items
            # cleans up the children.
            if category != "Kontoauszug":
                self._conn.execute(
                    "DELETE FROM statements WHERE doc_id = ?", (doc_id,)
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
        order_by: str = "doc_date",  # 'doc_date' | 'created_at'
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        trash_clause = "d.deleted_at IS NOT NULL" if trash else "d.deleted_at IS NULL"
        trash_clause_plain = trash_clause.replace("d.", "")
        # Tag match: tags are JSON arrays of strings; LIKE on the JSON is fine
        # for the cardinalities we deal with (low thousands).
        tag_like = f'%"{tag}"%' if tag else None

        if query:
            sql = (
                "SELECT d.* FROM documents d "
                "JOIN documents_fts f ON f.rowid = d.id "
                f"WHERE documents_fts MATCH ? AND {trash_clause}"
            )
            params.append(query)
            if category:
                sql += " AND d.category = ?"
                params.append(category)
            if subcategory:
                sql += " AND d.subcategory = ?"
                params.append(subcategory)
            if tag_like:
                sql += " AND d.tags LIKE ?"
                params.append(tag_like)
            if status:
                sql += " AND d.status = ?"
                params.append(status)
            if year == "unknown":
                sql += " AND (d.doc_date IS NULL OR d.doc_date = '')"
            elif year:
                sql += " AND substr(d.doc_date, 1, 4) = ?"
                params.append(year)
            sql += " ORDER BY rank LIMIT ? OFFSET ?"
            params += [limit, offset]
        else:
            where.append(trash_clause_plain)
            if category:
                where.append("category = ?")
                params.append(category)
            if subcategory:
                where.append("subcategory = ?")
                params.append(subcategory)
            if tag_like:
                where.append("tags LIKE ?")
                params.append(tag_like)
            if status:
                where.append("status = ?")
                params.append(status)
            if year == "unknown":
                where.append("(doc_date IS NULL OR doc_date = '')")
            elif year:
                where.append("substr(doc_date, 1, 4) = ?")
                params.append(year)
            where_sql = " WHERE " + " AND ".join(where)
            order_sql = (
                "ORDER BY created_at DESC" if order_by == "created_at"
                else "ORDER BY COALESCE(doc_date, created_at) DESC"
            )
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
        trash_clause = "deleted_at IS NOT NULL" if trash else "deleted_at IS NULL"
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
        where = ["deleted_at IS NOT NULL" if trash else "deleted_at IS NULL"]
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
                FROM documents WHERE deleted_at IS NULL
            """).fetchone()
            by_cat = self._conn.execute("""
                SELECT category, COUNT(*) AS n, COALESCE(SUM(cost_usd),0) AS cost_usd
                FROM documents WHERE deleted_at IS NULL
                GROUP BY category ORDER BY n DESC
            """).fetchall()
            by_status = self._conn.execute("""
                SELECT status, COUNT(*) AS n FROM documents
                WHERE deleted_at IS NULL GROUP BY status
            """).fetchall()
            by_month = self._conn.execute("""
                SELECT substr(created_at,1,7) AS month,
                       COUNT(*) AS n,
                       COALESCE(SUM(cost_usd),0) AS cost_usd
                FROM documents WHERE deleted_at IS NULL
                GROUP BY month ORDER BY month DESC LIMIT 12
            """).fetchall()
            trash_count = self._conn.execute(
                "SELECT COUNT(*) FROM documents WHERE deleted_at IS NOT NULL"
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
                "AND deleted_at IS NULL ORDER BY y DESC"
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
            total_row = self._conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(cost_usd),0) AS cost_usd "
                "FROM documents WHERE deleted_at IS NULL"
            ).fetchone()
            rows = self._conn.execute("""
                SELECT COALESCE(NULLIF(substr(doc_date,1,4), ''), '—') AS year,
                       category,
                       COUNT(*) AS n,
                       COALESCE(SUM(cost_usd), 0) AS cost_usd
                FROM documents WHERE deleted_at IS NULL
                GROUP BY year, category
                ORDER BY year DESC, n DESC
            """).fetchall()
            status_rows = self._conn.execute("""
                SELECT status, COUNT(*) AS n FROM documents
                WHERE deleted_at IS NULL AND status IN ('review', 'failed')
                GROUP BY status
            """).fetchall()
            trash_row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM documents WHERE deleted_at IS NOT NULL"
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
            "SELECT r.*, d.subject AS doc_subject, d.library_path "
            "FROM receipts r JOIN documents d ON d.id = r.doc_id "
            "WHERE " + " AND ".join(where) +
            " ORDER BY r.receipt_date DESC, r.id DESC LIMIT ?"
        )
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

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
        with self._lock:
            rows = self._conn.execute(
                """SELECT a.*,
                          (SELECT COUNT(*) FROM statements   WHERE account_id = a.id) AS statement_count,
                          (SELECT COUNT(*) FROM transactions WHERE account_id = a.id) AS tx_count
                   FROM accounts a
                   ORDER BY a.bank_name, a.id"""
            ).fetchall()
        return [dict(r) for r in rows]

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

    def finance_monthly(self, months: int = 12) -> list[dict[str, Any]]:
        """Income + expense per month for the last N months, oldest first.

        Excludes internal transfers (category=uebertrag) for the same
        reason as `finance_summary`: a single big move between own
        accounts would dwarf every other month and make the chart
        useless."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT substr(t.booking_date, 1, 7) AS month,
                          COALESCE(SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END), 0) AS income,
                          COALESCE(SUM(CASE WHEN t.amount < 0 THEN t.amount ELSE 0 END), 0) AS expense,
                          COUNT(*) AS n
                   FROM transactions t
                   JOIN statements   s ON s.id = t.statement_id
                   JOIN documents    d ON d.id = s.doc_id
                   WHERE d.deleted_at IS NULL
                     AND t.booking_date IS NOT NULL AND t.booking_date != ''
                     AND t.category != 'uebertrag'
                   GROUP BY month
                   ORDER BY month DESC
                   LIMIT ?""",
                (months,),
            ).fetchall()
        return [dict(r) for r in reversed(rows)]

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
            "SELECT t.*, s.doc_id, a.bank_name, a.iban_last4 "
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
            exclude_uebertrag=True,
        )
        join = (
            "FROM transactions t "
            "JOIN statements   s ON s.id = t.statement_id "
            "JOIN documents    d ON d.id = s.doc_id "
            "LEFT JOIN accounts a ON a.id = t.account_id "
        )
        where_sql = "WHERE " + " AND ".join(where)
        with self._lock:
            totals = self._conn.execute(
                "SELECT "
                "  COUNT(*)                                            AS n, "
                "  COALESCE(SUM(CASE WHEN t.amount > 0 THEN t.amount END), 0) AS sum_in, "
                "  COALESCE(SUM(CASE WHEN t.amount < 0 THEN t.amount END), 0) AS sum_out, "
                "  COALESCE(SUM(t.amount), 0)                          AS sum_net, "
                "  COALESCE(MIN(t.booking_date), '')                   AS first_date, "
                "  COALESCE(MAX(t.booking_date), '')                   AS last_date "
                + join + where_sql,
                params,
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
        return {
            "count":      int(totals["n"] or 0),
            "sum_in":     float(totals["sum_in"] or 0.0),
            "sum_out":    float(totals["sum_out"] or 0.0),
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
