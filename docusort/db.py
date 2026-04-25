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


# Anthropic public list pricing (USD per 1M tokens, input / output).
# Keyed by model prefix — dated suffixes like "-20251001" match via startswith.
# Cache read/write multipliers are the documented ephemeral-cache factors.
MODEL_PRICING: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5":  (1.0,  5.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-opus-4-7":  (15.0, 75.0),
}
CACHE_WRITE_MULTIPLIER = 1.25  # 5-minute ephemeral cache write surcharge
CACHE_READ_MULTIPLIER  = 0.10  # cached token read factor


def calculate_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    *,
    cache_write: int = 0,
    cache_read: int = 0,
) -> float:
    for prefix, (in_price, out_price) in MODEL_PRICING.items():
        if model.startswith(prefix):
            return (
                input_tokens  * in_price
                + output_tokens * out_price
                + cache_write   * in_price * CACHE_WRITE_MULTIPLIER
                + cache_read    * in_price * CACHE_READ_MULTIPLIER
            ) / 1_000_000
    logger.warning("Unknown model %s – cost will be recorded as 0", model)
    return 0.0


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
        ]
        for name, decl in migrations:
            if name not in cols:
                self._conn.execute(f"ALTER TABLE documents ADD COLUMN {name} {decl}")
                logger.info("DB migration: added column %s", name)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_hash    ON documents(content_hash)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_deleted ON documents(deleted_at)")

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
        fields = {
            "category":      cls.category,
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
        doc_date: str,
        sender: str,
        subject: str,
        filename: str,
        library_path: str,
        status: str = "filed",
    ) -> None:
        """Apply a manual metadata edit. Token counts and confidence are
        preserved; status defaults to 'filed' since a human just verified it."""
        with self._lock:
            self._conn.execute(
                """UPDATE documents SET
                   category = ?, doc_date = ?, sender = ?, subject = ?,
                   filename = ?, library_path = ?, status = ?
                   WHERE id = ?""",
                (category, doc_date, sender, subject, filename,
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
            if status:
                sql += " AND d.status = ?"
                params.append(status)
            if year:
                sql += " AND substr(d.doc_date, 1, 4) = ?"
                params.append(year)
            sql += " ORDER BY rank LIMIT ? OFFSET ?"
            params += [limit, offset]
        else:
            where.append(trash_clause_plain)
            if category:
                where.append("category = ?")
                params.append(category)
            if status:
                where.append("status = ?")
                params.append(status)
            if year:
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
            bucket = by_year.setdefault(y, {"year": y, "count": 0, "cost_usd": 0.0, "categories": []})
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


_db_singleton: Database | None = None
_singleton_lock = threading.Lock()


def open_db(path: Path) -> Database:
    """Return a process-wide singleton Database for the given path."""
    global _db_singleton
    with _singleton_lock:
        if _db_singleton is None or _db_singleton.path != path:
            _db_singleton = Database(path)
        return _db_singleton
