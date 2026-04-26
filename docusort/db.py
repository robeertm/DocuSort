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

    def close(self) -> None:
        with self._lock:
            self._conn.close()

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
