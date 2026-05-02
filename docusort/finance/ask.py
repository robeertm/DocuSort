"""Natural-language Q&A over the finance database.

The user asks a question ("zeige mir alle Ausgaben bei MediaMarkt 2025"),
the LLM picks tool calls against the transactions table, we execute them,
feed the trimmed result back, and the LLM produces a final German answer.

The provider abstraction has no native tool-use API, so we run a JSON
loop: each turn the model returns ONE JSON object — either a tool call
or a final answer. Works across Anthropic / OpenAI / Gemini / Ollama /
the local bridge with no provider-specific changes.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date as _date
from typing import Any

from ..providers import ProviderError

logger = logging.getLogger("docusort.finance.ask")


_MAX_STEPS = 6
_MAX_ROWS_IN_PROMPT = 30
_MAX_RESULT_ROWS = 200

_SYSTEM_PROMPT = """You answer the user's question about their personal bank transactions by calling tools, then producing one short German answer.

Each turn you reply with ONE JSON object — either a tool call or the final answer. No markdown, no prose around the JSON.

Tool call (preferred form):
{"action": "tool", "tool": "<name>", "args": {...}}

Alternative shorter form (also accepted):
{"action": "<tool_name>", "args": {...}}

Final answer:
{"action": "answer", "text": "<concise German answer with totals and date range>"}

Tools:

1. search_transactions — returns individual rows (counterparty, purpose, booking_date, amount, category).
   args: query (substring of counterparty OR purpose; comma-separated tokens are OR'd, e.g. "mediamarkt, saturn"),
         category (one of the categories returned by list_categories),
         direction ("income" or "expense"),
         year (YYYY) OR month (YYYY-MM) OR explicit start/end (YYYY-MM-DD) — pick one,
         amount_min / amount_max (matched against the absolute amount),
         limit (default 50, max 200).

2. aggregate_transactions — returns totals + by_category + monthly + top counterparties.
   Same filter args (no limit). Use this for "wieviel / total / sum / durchschnitt" questions
   or to summarise without listing every row.

3. list_categories — no args. Returns category names that exist in the DB.

4. list_merchants — args: contains (substring), limit (default 20). Returns top counterparties matching.

5. get_date_range — no args. Returns first/last booking_date in the DB.

Rules:
- Use search_transactions when the user wants to see rows.
- Use aggregate_transactions when the user asks for totals or distribution.
- ALL amounts in the answer in EUR with 2 decimals, e.g. "−123,45 €".
- The final answer is German prose. Mention the count, the date range, and the total.
- If a tool returns 0 rows, say so explicitly. Never invent transactions.
- If the question is ambiguous about year, prefer aggregate_transactions across all years.
- Don't call the same tool with the same args twice — the result will not change.
"""


_TOOL_NAMES = {
    "search_transactions", "aggregate_transactions",
    "list_categories", "list_merchants", "get_date_range",
}


def _parse(raw: str) -> dict[str, Any]:
    """Pull the first valid JSON object out of the model's reply."""
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw[:4].lower() == "json":
            raw = raw[4:].lstrip()
    decoder = json.JSONDecoder()
    for i, ch in enumerate(raw):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(raw[i:])
            return obj
        except json.JSONDecodeError:
            continue
    raise ValueError(f"no JSON in reply: {raw[:200]!r}")


def _normalise_action(obj: dict[str, Any]) -> dict[str, Any]:
    """Coerce common LLM output shapes into the canonical
    {"action": "tool"|"answer", ...} form. Local 7B/14B models often
    drop the wrapper and emit `{"action": "aggregate_transactions", ...}`
    or `{"tool": "...", "args": {...}}` — accept those instead of
    crashing the loop."""
    if not isinstance(obj, dict):
        return {"action": "_invalid"}

    action = obj.get("action")

    # Canonical shape — pass through.
    if action in ("tool", "answer"):
        return obj

    # Shape: {"action": "<tool_name>", "args": {...}}
    if isinstance(action, str) and action in _TOOL_NAMES:
        return {"action": "tool", "tool": action, "args": obj.get("args") or {}}

    # Shape: {"tool": "<tool_name>", "args": {...}}  (no action key)
    tool = obj.get("tool")
    if isinstance(tool, str) and tool in _TOOL_NAMES:
        return {"action": "tool", "tool": tool, "args": obj.get("args") or {}}

    # Shape: {"answer": "..."} or {"text": "..."}  (final answer without wrapper)
    for key in ("answer", "text", "response", "reply"):
        v = obj.get(key)
        if isinstance(v, str) and v.strip():
            return {"action": "answer", "text": v}

    # Shape: tool name as a key with args as its value.
    for tn in _TOOL_NAMES:
        if tn in obj and isinstance(obj[tn], dict):
            return {"action": "tool", "tool": tn, "args": obj[tn]}

    return {"action": "_invalid", "raw": obj}


def _normalise_filter(args: dict[str, Any]) -> dict[str, Any]:
    """Translate year / month into start/end and only pass through the
    keys our DB methods accept. Skip anything blank or unparseable."""
    out: dict[str, Any] = {}
    for k in ("query", "category", "direction"):
        v = args.get(k)
        if v:
            out[k] = str(v).strip()

    start = args.get("start") or None
    end = args.get("end") or None
    year = args.get("year")
    month = args.get("month")

    if month:
        m = str(month).strip()
        if len(m) == 7 and m[4] == "-":
            start = start or f"{m}-01"
            end = end or f"{m}-31"
    if year and not (start and end):
        y = str(year).strip()
        start = start or f"{y}-01-01"
        end = end or f"{y}-12-31"

    if start:
        out["start"] = str(start)
    if end:
        out["end"] = str(end)

    for k in ("amount_min", "amount_max"):
        v = args.get(k)
        if v is None:
            continue
        try:
            out[k] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def _run_tool(db, name: str, args: dict[str, Any]) -> dict[str, Any]:
    if name == "search_transactions":
        f = _normalise_filter(args)
        try:
            limit = max(1, min(200, int(args.get("limit", 50))))
        except (TypeError, ValueError):
            limit = 50
        rows = db.transactions_list(**f, limit=limit)
        return {"count": len(rows), "rows": rows}

    if name == "aggregate_transactions":
        f = _normalise_filter(args)
        agg = db.transactions_aggregate(**f, top_n=10, monthly_limit=36)
        return agg

    if name == "list_categories":
        with db._lock:
            rows = db._conn.execute(
                "SELECT DISTINCT COALESCE(NULLIF(t.category, ''), 'sonstiges') AS cat "
                "FROM transactions t "
                "JOIN statements s ON s.id = t.statement_id "
                "JOIN documents  d ON d.id = s.doc_id "
                "WHERE d.deleted_at IS NULL "
                "ORDER BY cat"
            ).fetchall()
        return {"categories": [r["cat"] for r in rows]}

    if name == "list_merchants":
        contains = str(args.get("contains") or "").strip()
        try:
            limit = max(1, min(50, int(args.get("limit", 20))))
        except (TypeError, ValueError):
            limit = 20
        sql = (
            "SELECT t.counterparty AS counterparty, COUNT(*) AS times, "
            "       COALESCE(SUM(t.amount), 0) AS total "
            "FROM transactions t "
            "JOIN statements s ON s.id = t.statement_id "
            "JOIN documents  d ON d.id = s.doc_id "
            "WHERE d.deleted_at IS NULL "
            "      AND t.counterparty IS NOT NULL AND t.counterparty != '' "
        )
        params: list[Any] = []
        if contains:
            sql += "AND t.counterparty LIKE ? "
            params.append(f"%{contains}%")
        sql += (
            "GROUP BY LOWER(t.counterparty) "
            "ORDER BY times DESC LIMIT ?"
        )
        params.append(limit)
        with db._lock:
            rows = db._conn.execute(sql, params).fetchall()
        return {"merchants": [dict(r) for r in rows]}

    if name == "get_date_range":
        with db._lock:
            r = db._conn.execute(
                "SELECT MIN(t.booking_date) AS first, MAX(t.booking_date) AS last "
                "FROM transactions t "
                "JOIN statements s ON s.id = t.statement_id "
                "JOIN documents  d ON d.id = s.doc_id "
                "WHERE d.deleted_at IS NULL "
                "      AND t.booking_date IS NOT NULL AND t.booking_date != ''"
            ).fetchone()
        return {"first": r["first"] or "", "last": r["last"] or ""}

    raise ValueError(f"unknown tool: {name}")


def _trim_for_prompt(result: dict[str, Any]) -> dict[str, Any]:
    """Don't blow the LLM's context with thousands of rows — keep the
    first 30, slim them to the fields the model actually needs."""
    if isinstance(result.get("rows"), list):
        rows = result["rows"]
        kept = rows[:_MAX_ROWS_IN_PROMPT]
        slim = [
            {
                "id": r.get("id"),
                "date": r.get("booking_date"),
                "amount": r.get("amount"),
                "counterparty": r.get("counterparty") or "",
                "purpose": (r.get("purpose") or "")[:120],
                "category": r.get("category") or "",
            }
            for r in kept
        ]
        return {
            "count": result.get("count", len(rows)),
            "shown": len(slim),
            "rows": slim,
            "_truncated": len(rows) > _MAX_ROWS_IN_PROMPT,
        }
    return result


def _build_user_prompt(question: str, history: list[dict[str, Any]]) -> str:
    today = _date.today().isoformat()
    parts = [
        f"Today is {today}.",
        f"User question: {question}",
    ]
    if history:
        parts.append("\nPrevious tool calls and trimmed results:")
        for h in history:
            call = h["call"]
            args_json = json.dumps(call.get("args", {}), ensure_ascii=False)
            parts.append(f"\n→ {call['tool']}({args_json})")
            trimmed = _trim_for_prompt(h["result"])
            result_json = json.dumps(trimmed, ensure_ascii=False, default=str)
            if len(result_json) > 6000:
                result_json = result_json[:6000] + "...<truncated>"
            parts.append(f"← {result_json}")
    parts.append("\nReply with the next JSON action.")
    return "\n".join(parts)


@dataclass
class AskResult:
    question: str
    answer: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    tools_used: list[dict[str, Any]] = field(default_factory=list)
    model: str = ""
    cost_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    steps: int = 0


def answer_question(db, classifier, question: str) -> AskResult:
    question = (question or "").strip()
    if not question:
        raise ValueError("empty question")
    if len(question) > 500:
        raise ValueError("question too long (max 500 chars)")

    history: list[dict[str, Any]] = []
    rows_collected: list[dict[str, Any]] = []
    seen_row_ids: set[int] = set()

    total_in = 0
    total_out = 0
    total_cost = 0.0
    last_model = ""

    for step in range(_MAX_STEPS):
        user = _build_user_prompt(question, history)
        try:
            resp = classifier.provider.classify(
                system_prompt=_SYSTEM_PROMPT,
                user_prompt=user,
                model=classifier.settings.model,
                max_output_tokens=1500,
            )
        except ProviderError:
            raise

        last_model = resp.model
        total_in += resp.input_tokens
        total_out += resp.output_tokens
        total_cost += resp.cost_usd

        try:
            parsed = _parse(resp.raw_text)
        except ValueError as exc:
            raise RuntimeError(f"LLM did not return valid JSON: {exc}") from exc

        action = _normalise_action(parsed)
        kind = action.get("action")
        if kind == "_invalid":
            # Feed the malformed reply back as a tool error so the LLM
            # gets a chance to fix its format on the next turn instead
            # of crashing the whole request. Local models recover fast
            # once they see "your last reply wasn't a valid action".
            history.append({
                "call": {"tool": "_format_error", "args": {}},
                "result": {"error":
                    "Your last reply was not a valid action. Reply with one of: "
                    "{\"action\":\"tool\",\"tool\":\"<name>\",\"args\":{...}} "
                    "OR {\"action\":\"answer\",\"text\":\"...\"}. "
                    f"Your reply was: {json.dumps(parsed, ensure_ascii=False)[:200]}"},
            })
            continue

        if kind == "answer":
            text = str(action.get("text") or "").strip()
            if not text:
                raise RuntimeError("LLM returned empty answer")
            return AskResult(
                question=question,
                answer=text,
                rows=rows_collected[:_MAX_RESULT_ROWS],
                tools_used=[h["call"] for h in history],
                model=last_model,
                cost_usd=total_cost,
                input_tokens=total_in,
                output_tokens=total_out,
                steps=step + 1,
            )

        if kind != "tool":
            raise RuntimeError(f"unknown action: {action!r}")

        tool = str(action.get("tool") or "").strip()
        args = action.get("args") or {}
        if not isinstance(args, dict):
            args = {}

        try:
            result = _run_tool(db, tool, args)
        except ValueError as exc:
            result = {"error": str(exc)}
        except Exception as exc:  # noqa: BLE001
            logger.exception("tool %s failed", tool)
            result = {"error": f"{type(exc).__name__}: {exc}"}

        if isinstance(result.get("rows"), list):
            for r in result["rows"]:
                rid = r.get("id")
                if rid is None or rid in seen_row_ids:
                    continue
                seen_row_ids.add(rid)
                rows_collected.append(r)

        history.append({"call": {"tool": tool, "args": args}, "result": result})

    raise RuntimeError(f"LLM did not produce a final answer in {_MAX_STEPS} steps")
