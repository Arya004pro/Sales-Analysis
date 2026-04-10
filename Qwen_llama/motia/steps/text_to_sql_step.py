"""Step 4: Text-to-SQL - fully generalised for any flat-table dataset.

Key changes vs original:
  - Removed _SALES_SCHEMA_TABLES, _BUILDER_ENTITIES, _BUILDER_METRICS constants
    (hardcoded to the old e-commerce schema).
  - Replaced with _is_classic_ecommerce_schema() which detects from live DB
    whether the fast sql_builder path is safe to use.
  - force_llm now uses a single clear rule: only use the builder for the
    known e-commerce schema; use LLM for everything else.
  - _build_llm_prompt() metric rules now use the schema's own Metric column
    mappings section instead of hardcoded final_price/total_fare preferences.
  - All other logic (time_series hint, SQL safety, EXPLAIN, fallbacks) unchanged.
"""

import os
import sys
import re
import logging

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from datetime import date, datetime, timezone
from typing import Any
from motia import FlowContext, queue

from shared_config import (
    GROQ_API_TOKEN,
    LLAMA_MODEL,
    SQL_GENERATOR_MODEL,
    GROQ_URL,
    QWEN_ENABLE_REASONING,
    QWEN_REASONING_EFFORT,
)
from utils.token_logger import log_tokens, add_tokens_to_state, calc_max_tokens
from utils.llm_client import clean_model_text, post_chat_completion
from db.schema_context import get_schema_prompt
from db.sql_builder import build_sql
from db.duckdb_connection import explain_query, get_read_connection
from services.text_to_sql_prompting import (
    render_filter_clause as _svc_render_filter_clause,
    build_time_series_sql_hint as _svc_build_time_series_sql_hint,
    build_llm_prompt as _svc_build_llm_prompt,
    build_sql_fix_prompt as _svc_build_sql_fix_prompt,
)

logger = logging.getLogger(__name__)

config = {
    "name": "SQLPlanner",
    "description": (
        "Builds validated DuckDB SQL from parsed intent using the live schema. "
        "Uses a deterministic builder only for the classic e-commerce schema; "
        "all other datasets always go through the LLM path with the live schema "
        "injected into the prompt."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::text.to.sql")],
    "enqueues": ["query::execute"],
}

_SQL_LINE_RE = re.compile(r"(?im)^(WITH|SELECT)\b")
_FENCE_RE = re.compile(r"```(?:sql)?\s*\n?(.*?)```", re.DOTALL | re.IGNORECASE)
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE"
    r"|EXECUTE|COPY|VACUUM|ANALYZE|CALL|DO)\b",
    re.IGNORECASE,
)

# No schema-specific constants needed - all detection is live


# Schema helpers


def _get_existing_tables() -> set[str]:
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
        ).fetchall()
        conn.close()
        return {r[0].lower() for r in rows}
    except Exception:
        return set()


def _detect_id_name_pairs() -> dict[str, str]:
    """Return {name_col: discriminator_col} for deduplication in GROUP BY."""
    from collections import defaultdict

    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'main' ORDER BY table_name, ordinal_position"
        ).fetchall()
        conn.close()
    except Exception:
        return {}

    table_cols: dict = defaultdict(list)
    for table, col in rows:
        table_cols[table].append(col.lower())

    _CONTACT_DISCRIMINATORS = (
        "phone",
        "phone_number",
        "mobile",
        "mobile_number",
        "email",
        "email_address",
        "contact",
    )

    def _best_disc(name_col: str, col_set: set) -> str | None:
        base = name_col[:-5] if name_col.endswith("_name") else name_col
        for suffix in ("_id", "_code", "_key", "_uuid", "_no", "_number"):
            c = base + suffix
            if c in col_set:
                return c
        for c in _CONTACT_DISCRIMINATORS:
            if c in col_set:
                return c
        return None

    pairs: dict[str, str] = {}
    for table, cols in table_cols.items():
        col_set = set(cols)
        for col in cols:
            if col.endswith("_name"):
                disc = _best_disc(col, col_set)
                if disc and col not in pairs:
                    pairs[col] = disc
        if "name" in col_set and "name" not in pairs:
            for c in (f"{table}_id", "id", f"{table}_code"):
                if c in col_set:
                    pairs["name"] = c
                    break
    return pairs


# SQL extraction / safety


def _extract_sql(raw: str) -> str:
    text = clean_model_text(raw, strip_fences=False)
    fence = _FENCE_RE.search(text)
    if fence:
        text = fence.group(1).strip()
    m = _SQL_LINE_RE.search(text)
    if m:
        text = text[m.start() :]
    text = re.sub(r"(--[^\n]*|/\*.*?\*/)", "", text, flags=re.DOTALL)
    return text.strip().rstrip(";")


def _is_safe(sql: str) -> tuple[bool, str]:
    if not _SQL_LINE_RE.match(sql):
        return False, f"does not start WITH/SELECT: {sql[:80]!r}"
    m = _FORBIDDEN.search(sql)
    if m:
        return False, f"forbidden keyword: {m.group()!r}"
    if ";" in sql:
        return False, "contains semicolon"
    return True, ""


def _explain(sql: str, parsed: dict | None = None) -> tuple[bool, str]:
    n = sql.count("%s") + sql.count("?")
    d = date.today().replace(day=1)
    num = 5
    qt = (parsed or {}).get("query_type", "")
    thr = (parsed or {}).get("threshold") or {}
    typ = thr.get("type", "")
    if n == 0:
        params = []
    elif n == 1:
        params = [num]
    elif n == 2:
        params = [d, d]
    elif qt == "threshold" and typ == "percentage" and n == 5:
        params = [d, d, num, d, d]
    elif qt == "threshold" and typ == "absolute" and n == 3:
        params = [d, d, num]
    elif qt in ("comparison", "growth_ranking") and n == 5:
        params = [d, d, d, d, num]
    elif qt in ("comparison", "growth_ranking") and n == 4:
        params = [d, d, d, d]
    else:
        params = [d if i < n - 1 else num for i in range(n)]
    return explain_query(sql, params)


def _render_filter_clause(filters: dict) -> str:
    return _svc_render_filter_clause(filters)


def _score_revenue_column(col_name: str) -> int:
    c = (col_name or "").lower()
    score = 0

    if any(
        k in c
        for k in (
            "revenue",
            "sales",
            "earning",
            "amount",
            "total",
            "final",
            "net",
            "paid",
        )
    ):
        score += 10
    if "final" in c or "net" in c or "paid" in c:
        score += 8
    if "total" in c:
        score += 6
    if "price" in c or "fare" in c:
        score += 3
    if any(
        k in c
        for k in (
            "unit",
            "base",
            "list",
            "mrp",
            "msrp",
            "catalog",
            "original",
            "cost",
            "tax",
            "discount",
            "coupon",
            "shipping",
            "commission",
            "refund",
            "refunded",
            "before_",
        )
    ):
        score -= 7

    return score


def _pick_best_revenue_column(columns: list[str]) -> str | None:
    if not columns:
        return None

    ranked = sorted(
        columns,
        key=lambda c: (
            _score_revenue_column(c),
            1 if "final" in c.lower() else 0,
            1 if "total" in c.lower() else 0,
            1 if "amount" in c.lower() else 0,
            -len(c),
        ),
        reverse=True,
    )
    return ranked[0] if ranked else None


def _pick_best_count_key(columns: list[str]) -> str | None:
    id_cols = [c for c in columns if c.endswith("_id") or c == "id"]
    if not id_cols:
        return None

    def _score(c: str) -> tuple[int, int, int, int]:
        s = 0
        if any(
            k in c
            for k in (
                "order",
                "transaction",
                "invoice",
                "booking",
                "trip",
                "ride",
                "ticket",
                "request",
                "visit",
                "session",
                "sale",
                "payment",
            )
        ):
            s += 10
        if any(
            k in c for k in ("row", "line", "item", "detail", "record", "event", "log")
        ):
            s -= 10
        if c == "id":
            s -= 2
        return (
            s,
            1 if c.endswith("_id") else 0,
            1 if "order" in c else 0,
            -len(c),
        )

    ranked = sorted(id_cols, key=_score, reverse=True)
    return ranked[0]


def _is_repeat_entity_count_intent(parsed: dict, user_query: str) -> bool:
    if parsed.get("_repeat_entity_count"):
        return True
    q = f" {(user_query or '').lower()} "
    has_repeat = any(
        x in q for x in (" repeat ", " repeated ", " returning ", " return ")
    )
    has_actor = any(
        x in q
        for x in (
            "buyer",
            "customer",
            "user",
            "client",
            "account",
            "member",
            "driver",
            "vendor",
            "merchant",
            "seller",
            "partner",
            "employee",
            "agent",
            "store",
            "warehouse",
            "branch",
        )
    )
    return has_repeat and has_actor


def _top_percent_share_value(parsed: dict, user_query: str) -> float | None:
    raw = parsed.get("_top_percent_share")
    if raw is not None:
        try:
            pct = float(raw)
            if 0 < pct < 100:
                return pct
        except Exception:
            pass
    q = (user_query or "").lower()
    m = re.search(r"\btop\s+(\d+(?:\.\d+)?)\s*(?:%|percent\b)", q)
    if not m:
        return None
    if not any(
        x in q
        for x in ("contribution", "contribute", "share", "percent of", "percentage of")
    ):
        return None
    try:
        pct = float(m.group(1))
    except Exception:
        return None
    return pct if 0 < pct < 100 else None


def _check_filters_present(sql: str, filters: dict, ctx, query_id: str) -> None:
    for col in filters:
        if col.lower() not in sql.lower():
            ctx.logger.warn(
                f"Filter '{col}' may be missing from generated SQL",
                {"queryId": query_id, "col": col},
            )


# Time-series SQL hint


def _build_time_series_sql_hint(parsed: dict) -> str:
    return _svc_build_time_series_sql_hint(parsed)


# Prompt builder


def _build_llm_prompt(user_query: str, parsed: dict, schema: str) -> str:
    return _svc_build_llm_prompt(user_query, parsed, schema)


def _call_llm(model: str, prompt: str) -> tuple[str, dict]:
    messages = [{"role": "user", "content": prompt}]
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": calc_max_tokens(messages, task="text_to_sql", model=model),
        "temperature": 0.0,
    }

    if QWEN_ENABLE_REASONING and "qwen" in model.lower() and QWEN_REASONING_EFFORT:
        payload["reasoning_effort"] = QWEN_REASONING_EFFORT

    data = post_chat_completion(
        api_url=GROQ_URL,
        api_token=GROQ_API_TOKEN,
        payload=payload,
        timeout=45,
    )
    return data["choices"][0]["message"]["content"].strip(), data.get("usage", {})


def _build_sql_fix_prompt(
    user_query: str,
    parsed: dict,
    schema: str,
    bad_sql: str,
    error_msg: str,
) -> str:
    return _svc_build_sql_fix_prompt(user_query, parsed, schema, bad_sql, error_msg)


def _try_repair_sql(
    model: str,
    user_query: str,
    parsed: dict,
    schema: str,
    bad_sql: str,
    error_msg: str,
) -> tuple[str | None, dict]:
    try:
        fix_prompt = _build_sql_fix_prompt(
            user_query, parsed, schema, bad_sql, error_msg
        )
        messages = [{"role": "user", "content": fix_prompt}]
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": calc_max_tokens(messages, task="sql_repair", model=model),
            "temperature": 0.0,
        }
        if QWEN_ENABLE_REASONING and "qwen" in model.lower() and QWEN_REASONING_EFFORT:
            payload["reasoning_effort"] = QWEN_REASONING_EFFORT

        data = post_chat_completion(
            api_url=GROQ_URL,
            api_token=GROQ_API_TOKEN,
            payload=payload,
            timeout=45,
        )
        raw_fix = (data["choices"][0]["message"]["content"] or "").strip()
        usage_fix = data.get("usage", {})
        repaired_sql = _extract_sql(raw_fix)
        ok, reason = _is_safe(repaired_sql)
        if not ok:
            return None, usage_fix
        ok2, _ = _explain(repaired_sql, parsed)
        if not ok2:
            return None, usage_fix
        return repaired_sql, usage_fix
    except Exception:
        return None, {}


def _deterministic_time_series_fallback(parsed: dict) -> str | None:
    """
    Build a robust time-series SQL without LLM.
    Used when query_type=time_series and model generation fails.
    """
    metric = (parsed.get("metric") or "count").lower()
    bucket = (parsed.get("time_bucket") or "month").lower()
    filters = parsed.get("filters", {}) or {}
    count_key = parsed.get("_count_distinct_key")
    aov_revenue_col = (parsed.get("_aov_revenue_col") or "").lower().strip()

    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name"
        ).fetchall()
        tables = [r[0] for r in table_rows]
    except Exception:
        return None

    def _bucket_expr(date_col: str) -> str:
        if bucket == "year":
            return f"CAST(YEAR({date_col}) AS VARCHAR)"
        if bucket == "quarter":
            return f"CONCAT(CAST(YEAR({date_col}) AS VARCHAR), '-Q', CAST(QUARTER({date_col}) AS VARCHAR))"
        if bucket == "week":
            return f"STRFTIME({date_col}, '%Y-W%W')"
        if bucket == "day":
            return f"STRFTIME({date_col}, '%Y-%m-%d')"
        return f"STRFTIME({date_col}, '%Y-%m')"

    def _choose_plan() -> tuple[str, str, str] | None:
        best = None
        for t in tables:
            try:
                cols = [
                    (c[0], str(c[1]).upper())
                    for c in conn.execute(f'DESCRIBE "{t}"').fetchall()
                ]
            except Exception:
                continue
            col_names = [c[0].lower() for c in cols]
            date_cols = [
                c
                for c, typ in cols
                if (
                    "DATE" in typ
                    or "TIMESTAMP" in typ
                    or any(
                        k in c.lower()
                        for k in ("date", "time", "created", "updated", "at")
                    )
                )
            ]
            if not date_cols:
                continue
            date_col = next((c for c in date_cols if "date" in c.lower()), date_cols[0])

            agg_expr = None
            score = 0
            if metric == "aov":
                ck = (
                    count_key
                    if count_key and count_key in col_names
                    else _pick_best_count_key(col_names)
                )
                revenue_col = aov_revenue_col if aov_revenue_col in col_names else None
                if not revenue_col:
                    candidates = [
                        c
                        for c in col_names
                        if any(
                            k in c
                            for k in (
                                "amount",
                                "total",
                                "revenue",
                                "sales",
                                "earning",
                                "price",
                                "fare",
                                "cost",
                                "fee",
                                "payment",
                                "profit",
                                "final",
                                "net",
                                "paid",
                            )
                        )
                    ]
                    revenue_col = _pick_best_revenue_column(candidates)
                if ck and revenue_col:
                    agg_expr = (
                        f'SUM("{revenue_col}") / NULLIF(COUNT(DISTINCT "{ck}"), 0)'
                    )
                    score += 8
            elif metric == "count":
                ck = count_key
                if not ck or ck.lower() not in col_names:
                    ck = _pick_best_count_key(col_names)
                agg_expr = f'COUNT(DISTINCT "{ck}")' if ck else "COUNT(*)"
                score += 6 if ck else 2
            elif metric.startswith("avg_"):
                mcol = metric[4:]
                if mcol in col_names:
                    agg_expr = f'AVG("{mcol}")'
                    score += 6
            else:
                if metric in col_names:
                    agg_expr = f'SUM("{metric}")'
                    score += 7
                else:
                    candidates = [
                        c
                        for c in col_names
                        if any(
                            k in c
                            for k in (
                                "amount",
                                "total",
                                "revenue",
                                "sales",
                                "earning",
                                "price",
                                "fare",
                                "cost",
                                "fee",
                                "payment",
                                "profit",
                                "final",
                            )
                        )
                    ]
                    best_money_col = _pick_best_revenue_column(candidates)
                    if best_money_col:
                        agg_expr = f'SUM("{best_money_col}")'
                        score += 4
            if not agg_expr:
                continue

            if any(k in date_col.lower() for k in ("date", "created", "time")):
                score += 2
            if metric in col_names:
                score += 2
            if best is None or score > best[0]:
                best = (score, t, date_col, agg_expr)

        return (best[1], best[2], best[3]) if best else None

    try:
        plan = _choose_plan()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not plan:
        return None

    table, date_col, agg_expr = plan
    b_expr = _bucket_expr(f'CAST("{date_col}" AS DATE)')
    filter_clause = _render_filter_clause(filters)
    where_extra = f"\n{filter_clause}" if filter_clause else ""

    return (
        f"SELECT {b_expr} AS name, {agg_expr} AS value\n"
        f'FROM "{table}"\n'
        f'WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?'
        f"{where_extra}\n"
        f"GROUP BY {b_expr}\n"
        f"ORDER BY name ASC"
    )


def _deterministic_repeat_entity_count_fallback(
    parsed: dict, user_query: str
) -> str | None:
    """Count repeat entities (>=2 distinct events) in the selected period."""
    filters = parsed.get("filters", {}) or {}
    entity_hint = (parsed.get("entity") or "").lower().strip()
    count_key_hint = (parsed.get("_count_distinct_key") or "").lower().strip()

    def _is_entity_col(col: str, dtype: str, query_l: str) -> bool:
        c = col.lower()
        d = dtype.upper()
        if any(k in c for k in ("date", "time", "created", "updated", "timestamp")):
            return False
        if any(
            k in c
            for k in (
                "order",
                "booking",
                "transaction",
                "invoice",
                "payment",
                "trip",
                "ride",
                "ticket",
                "request",
                "session",
                "visit",
                "row",
                "line",
                "item",
                "detail",
                "record",
                "event",
                "log",
            )
        ):
            return False
        if "CHAR" in d or "TEXT" in d or "STRING" in d or "VARCHAR" in d:
            return (
                c.endswith("_name")
                or any(
                    k in c
                    for k in (
                        "email",
                        "phone",
                        "mobile",
                        "user",
                        "customer",
                        "buyer",
                        "client",
                        "driver",
                        "vendor",
                        "merchant",
                        "seller",
                        "store",
                        "warehouse",
                        "branch",
                    )
                )
                or (entity_hint and entity_hint in c)
            )
        if (
            c == "id"
            or c.endswith("_id")
            or c.endswith("_uuid")
            or c.endswith("_key")
            or c.endswith("_code")
        ):
            if any(
                k in c
                for k in (
                    "user",
                    "customer",
                    "buyer",
                    "client",
                    "driver",
                    "vendor",
                    "merchant",
                    "seller",
                    "store",
                    "warehouse",
                    "branch",
                    "account",
                    "member",
                    "employee",
                    "agent",
                    "partner",
                )
            ):
                return True
            if any(
                k in query_l
                for k in (
                    "buyer",
                    "customer",
                    "user",
                    "client",
                    "driver",
                    "vendor",
                    "store",
                    "warehouse",
                )
            ):
                return True
        return False

    def _pick_entity_key(cols: list[tuple[str, str]], query_l: str) -> str | None:
        col_names = [c[0].lower() for c in cols]
        if entity_hint and entity_hint in col_names:
            return entity_hint
        if entity_hint.endswith("_name") and entity_hint[:-5] in col_names:
            return entity_hint[:-5]

        best: tuple[int, str] | None = None
        for col, dtype in cols:
            if not _is_entity_col(col, dtype, query_l):
                continue
            c = col.lower()
            score = 0
            if c.endswith("_id") or c == "id":
                score += 4
            if c.endswith("_name"):
                score += 6
            if any(
                k in c
                for k in (
                    "user",
                    "customer",
                    "buyer",
                    "client",
                    "driver",
                    "vendor",
                    "merchant",
                    "seller",
                    "store",
                    "warehouse",
                    "branch",
                    "account",
                    "member",
                    "employee",
                    "agent",
                    "partner",
                )
            ):
                score += 8
            if any(k in query_l for k in c.replace("_", " ").split()):
                score += 3
            if best is None or score > best[0]:
                best = (score, c)
        return best[1] if best else None

    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name"
        ).fetchall()
        tables = [r[0] for r in table_rows]
    except Exception:
        return None

    query_l = (user_query or "").lower()
    try:
        best = None
        for t in tables:
            try:
                cols = [
                    (c[0], str(c[1]).upper())
                    for c in conn.execute(f'DESCRIBE "{t}"').fetchall()
                ]
            except Exception:
                continue
            col_names = [c[0].lower() for c in cols]
            date_cols = [
                c
                for c, typ in cols
                if (
                    "DATE" in typ
                    or "TIMESTAMP" in typ
                    or any(
                        k in c.lower()
                        for k in ("date", "time", "created", "updated", "at")
                    )
                )
            ]
            if not date_cols:
                continue
            date_col = next((c for c in date_cols if "date" in c.lower()), date_cols[0])

            entity_key = _pick_entity_key(cols, query_l)
            if not entity_key:
                continue

            event_key = (
                count_key_hint
                if count_key_hint in col_names
                else _pick_best_count_key(col_names)
            )
            if event_key and event_key.lower() == entity_key.lower():
                event_key = None

            score = 0
            if entity_hint and entity_hint in col_names:
                score += 5
            if event_key:
                score += 4
            if all((k in col_names) for k in filters.keys()):
                score += 2
            if "date" in date_col.lower() or "time" in date_col.lower():
                score += 2

            if best is None or score > best[0]:
                best = (score, t, date_col, entity_key, event_key, set(col_names))

        if not best:
            return None

        _, table, date_col, entity_key, event_key, table_cols = best
        filter_clause = _render_filter_clause(
            {k: v for k, v in filters.items() if k in table_cols}
        )
        where_extra = f"\n{filter_clause}" if filter_clause else ""
        having_expr = (
            f'COUNT(DISTINCT "{event_key}") >= 2' if event_key else "COUNT(*) >= 2"
        )

        return (
            "WITH repeats AS (\n"
            f'  SELECT "{entity_key}" AS entity_key\n'
            f'  FROM "{table}"\n'
            f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
            f'  GROUP BY "{entity_key}"\n'
            f"  HAVING {having_expr}\n"
            ")\n"
            "SELECT COUNT(*) AS value\n"
            "FROM repeats"
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _deterministic_top_percent_share_fallback(
    parsed: dict, user_query: str
) -> str | None:
    pct = _top_percent_share_value(parsed, user_query)
    if pct is None:
        return None

    metric = (parsed.get("metric") or "count").lower()
    entity = (parsed.get("entity") or "").lower().strip()
    filters = parsed.get("filters", {}) or {}
    count_key = parsed.get("_count_distinct_key")
    aov_revenue_col = (parsed.get("_aov_revenue_col") or "").lower().strip()
    entity_key_hint = (parsed.get("_entity_group_key") or "").lower().strip()

    if not entity:
        return None

    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name"
        ).fetchall()
        tables = [r[0] for r in table_rows]
    except Exception:
        return None

    def _pick_plan() -> tuple[str, str, str, str | None, str] | None:
        best = None
        for t in tables:
            try:
                cols = [
                    (c[0], str(c[1]).upper())
                    for c in conn.execute(f'DESCRIBE "{t}"').fetchall()
                ]
            except Exception:
                continue

            col_names = [c[0].lower() for c in cols]
            date_cols = [
                c
                for c, typ in cols
                if (
                    "DATE" in typ
                    or "TIMESTAMP" in typ
                    or any(
                        k in c.lower()
                        for k in ("date", "time", "created", "updated", "at")
                    )
                )
            ]
            if not date_cols:
                continue
            date_col = next((c for c in date_cols if "date" in c.lower()), date_cols[0])

            entity_col = None
            if entity in col_names:
                entity_col = entity
            elif entity.endswith("_name") and entity[:-5] in col_names:
                entity_col = entity[:-5]
            else:
                tokens = [tok for tok in entity.replace("_", " ").split() if tok]
                text_cols = [
                    c
                    for c, typ in cols
                    if any(tk in typ for tk in ("VARCHAR", "CHAR", "TEXT", "STRING"))
                ]
                for c in text_cols:
                    low = c.lower()
                    if any(tok in low for tok in tokens):
                        entity_col = c
                        break

            if not entity_col:
                continue

            group_key = entity_key_hint if entity_key_hint in col_names else None
            if not group_key:
                base = entity_col[:-5] if entity_col.endswith("_name") else entity_col
                for cand in (f"{base}_id", f"{base}_code", f"{base}_key", "id"):
                    if cand in col_names and cand != entity_col:
                        group_key = cand
                        break

            agg_expr = None
            if metric == "aov":
                ck = (
                    count_key
                    if count_key and count_key in col_names
                    else _pick_best_count_key(col_names)
                )
                revenue_col = aov_revenue_col if aov_revenue_col in col_names else None
                if not revenue_col:
                    revenue_candidates = [
                        c
                        for c in col_names
                        if any(
                            k in c
                            for k in (
                                "final",
                                "total",
                                "amount",
                                "price",
                                "revenue",
                                "sales",
                                "earning",
                                "fare",
                                "net",
                                "paid",
                            )
                        )
                    ]
                    revenue_col = _pick_best_revenue_column(revenue_candidates)
                if ck and revenue_col:
                    agg_expr = (
                        f'SUM("{revenue_col}") / NULLIF(COUNT(DISTINCT "{ck}"), 0)'
                    )
            elif metric == "count":
                ck = (
                    count_key
                    if count_key and count_key in col_names
                    else _pick_best_count_key(col_names)
                )
                agg_expr = f'COUNT(DISTINCT "{ck}")' if ck else "COUNT(*)"
            elif metric.startswith("avg_"):
                mcol = metric[4:]
                if mcol in col_names:
                    agg_expr = f'AVG("{mcol}")'
            elif metric in col_names:
                agg_expr = f'SUM("{metric}")'
            else:
                revenue_candidates = [
                    c
                    for c in col_names
                    if any(
                        k in c
                        for k in (
                            "final",
                            "total",
                            "amount",
                            "price",
                            "revenue",
                            "sales",
                            "earning",
                            "fare",
                            "net",
                            "paid",
                        )
                    )
                ]
                money_col = _pick_best_revenue_column(revenue_candidates)
                if money_col:
                    agg_expr = f'SUM("{money_col}")'

            if not agg_expr:
                continue

            score = 0
            if metric in col_names:
                score += 5
            if entity_col:
                score += 5
            if group_key:
                score += 2
            if all((k in col_names) for k in filters.keys()):
                score += 2
            if any(k in date_col.lower() for k in ("date", "time", "created")):
                score += 2

            if best is None or score > best[0]:
                best = (score, t, date_col, entity_col, group_key, agg_expr)

        return (best[1], best[2], best[3], best[4], best[5]) if best else None

    try:
        plan = _pick_plan()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not plan:
        return None

    table, date_col, entity_col, group_key, agg_expr = plan
    filter_clause = _render_filter_clause(filters)
    where_extra = f"\n{filter_clause}" if filter_clause else ""

    group_by_expr = f'"{entity_col}"'
    select_name_expr = f'"{entity_col}" AS name'
    if group_key and group_key != entity_col:
        group_by_expr = f'"{group_key}", "{entity_col}"'

    pct_str = f"{pct:.6f}"
    cutoff_expr = f"GREATEST(1, CEIL(total_entities * {pct_str} / 100.0))"

    return (
        "WITH entity_totals AS (\n"
        f"  SELECT {select_name_expr}, {agg_expr} AS value\n"
        f'  FROM "{table}"\n'
        f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
        f"  GROUP BY {group_by_expr}\n"
        "),\n"
        "ranked AS (\n"
        "  SELECT name,\n"
        "         value,\n"
        "         ROW_NUMBER() OVER (ORDER BY value DESC) AS rn,\n"
        "         COUNT(*) OVER () AS total_entities,\n"
        "         SUM(value) OVER () AS grand_total\n"
        "  FROM entity_totals\n"
        "),\n"
        "cut AS (\n"
        f"  SELECT *, {cutoff_expr} AS cutoff\n"
        "  FROM ranked\n"
        ")\n"
        "SELECT CASE\n"
        "         WHEN MAX(grand_total) IS NULL OR MAX(grand_total) = 0 THEN 0\n"
        "         ELSE (SUM(CASE WHEN rn <= cutoff THEN value ELSE 0 END) * 100.0) / MAX(grand_total)\n"
        "       END AS value\n"
        "FROM cut"
    )


def _deterministic_rank_within_time_fallback(parsed: dict) -> str | None:
    """Build SQL for top/bottom N entities within each time bucket."""
    metric = (parsed.get("metric") or "count").lower()
    entity = (parsed.get("entity") or "").lower().strip()
    bucket = (parsed.get("time_bucket") or "year").lower()
    filters = parsed.get("filters", {}) or {}
    top_n = int(parsed.get("top_n") or 5)
    qt = (parsed.get("query_type") or "top_n").lower()
    count_key = parsed.get("_count_distinct_key")
    aov_revenue_col = (parsed.get("_aov_revenue_col") or "").lower().strip()

    if not entity:
        return None

    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name"
        ).fetchall()
        tables = [r[0] for r in table_rows]
    except Exception:
        return None

    def _bucket_expr(date_col: str) -> str:
        d = f'CAST("{date_col}" AS DATE)'
        if bucket == "year":
            return f"CAST(YEAR({d}) AS VARCHAR)"
        if bucket == "quarter":
            return f"CONCAT(CAST(YEAR({d}) AS VARCHAR), '-Q', CAST(QUARTER({d}) AS VARCHAR))"
        if bucket == "week":
            return f"STRFTIME({d}, '%Y-W%W')"
        if bucket == "day":
            return f"STRFTIME({d}, '%Y-%m-%d')"
        return f"STRFTIME({d}, '%Y-%m')"

    def _pick_plan() -> tuple[str, str, str, str] | None:
        best = None
        for t in tables:
            try:
                cols = [
                    (c[0], str(c[1]).upper())
                    for c in conn.execute(f'DESCRIBE "{t}"').fetchall()
                ]
            except Exception:
                continue

            col_names = [c[0].lower() for c in cols]
            date_cols = [
                c
                for c, typ in cols
                if (
                    "DATE" in typ
                    or "TIMESTAMP" in typ
                    or any(
                        k in c.lower()
                        for k in ("date", "time", "created", "updated", "at")
                    )
                )
            ]
            if not date_cols:
                continue
            date_col = next((c for c in date_cols if "date" in c.lower()), date_cols[0])

            entity_col = None
            if entity in col_names:
                entity_col = entity
            elif entity.endswith("_name") and entity[:-5] in col_names:
                entity_col = entity[:-5]
            else:
                tokens = [tok for tok in entity.replace("_", " ").split() if tok]
                text_cols = [
                    c
                    for c, typ in cols
                    if any(tk in typ for tk in ("VARCHAR", "CHAR", "TEXT", "STRING"))
                ]
                for c in text_cols:
                    low = c.lower()
                    if any(tok in low for tok in tokens):
                        entity_col = c
                        break

            if not entity_col:
                continue

            agg_expr = None
            if metric == "aov":
                ck = (
                    count_key
                    if count_key and count_key in col_names
                    else _pick_best_count_key(col_names)
                )
                revenue_col = aov_revenue_col if aov_revenue_col in col_names else None
                if not revenue_col:
                    revenue_candidates = [
                        c
                        for c in col_names
                        if any(
                            k in c
                            for k in (
                                "final",
                                "total",
                                "amount",
                                "price",
                                "revenue",
                                "sales",
                                "earning",
                                "fare",
                                "net",
                                "paid",
                            )
                        )
                    ]
                    revenue_col = _pick_best_revenue_column(revenue_candidates)
                if ck and revenue_col:
                    agg_expr = (
                        f'SUM("{revenue_col}") / NULLIF(COUNT(DISTINCT "{ck}"), 0)'
                    )
            elif metric == "count":
                ck = (
                    count_key
                    if count_key and count_key in col_names
                    else _pick_best_count_key(col_names)
                )
                agg_expr = f'COUNT(DISTINCT "{ck}")' if ck else "COUNT(*)"
            elif metric.startswith("avg_"):
                mcol = metric[4:]
                if mcol in col_names:
                    agg_expr = f'AVG("{mcol}")'
            elif metric in col_names:
                agg_expr = f'SUM("{metric}")'
            else:
                revenue_candidates = [
                    c
                    for c in col_names
                    if any(
                        k in c
                        for k in (
                            "final",
                            "total",
                            "amount",
                            "price",
                            "revenue",
                            "sales",
                            "earning",
                            "fare",
                            "net",
                            "paid",
                        )
                    )
                ]
                money_col = _pick_best_revenue_column(revenue_candidates)
                if money_col:
                    agg_expr = f'SUM("{money_col}")'

            if not agg_expr:
                continue

            score = 0
            if metric in col_names:
                score += 5
            if entity_col:
                score += 5
            if any(k in date_col.lower() for k in ("date", "time", "created")):
                score += 2
            if all((k in col_names) for k in filters.keys()):
                score += 2

            if best is None or score > best[0]:
                best = (score, t, date_col, entity_col, agg_expr)

        return (best[1], best[2], best[3], best[4]) if best else None

    try:
        plan = _pick_plan()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not plan:
        return None

    table, date_col, entity_col, agg_expr = plan
    b_expr = _bucket_expr(date_col)
    direction = "ASC" if qt == "bottom_n" else "DESC"
    filter_clause = _render_filter_clause(filters)
    where_extra = f"\n{filter_clause}" if filter_clause else ""

    return (
        "WITH ranked AS (\n"
        f"  SELECT {b_expr} AS period,\n"
        f'         "{entity_col}" AS name,\n'
        f"         {agg_expr} AS value,\n"
        f"         ROW_NUMBER() OVER (PARTITION BY {b_expr} ORDER BY {agg_expr} {direction}) AS rn\n"
        f'  FROM "{table}"\n'
        f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
        f'  GROUP BY {b_expr}, "{entity_col}"\n'
        ")\n"
        "SELECT period, name, value\n"
        "FROM ranked\n"
        "WHERE rn <= ?\n"
        f"ORDER BY period ASC, value {direction}"
    )


def _deterministic_comparison_fallback(parsed: dict) -> str | None:
    """Build robust two-period comparison SQL without relying on LLM output."""
    metric = (parsed.get("metric") or "count").lower()
    entity = (parsed.get("entity") or "").lower().strip() or None
    filters = parsed.get("filters", {}) or {}
    top_n = int(parsed.get("top_n") or 5)
    count_key = parsed.get("_count_distinct_key")
    aov_revenue_col = (parsed.get("_aov_revenue_col") or "").lower().strip()

    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name"
        ).fetchall()
        tables = [r[0] for r in table_rows]
    except Exception:
        return None

    def _pick_plan() -> tuple[str, str, str | None, str] | None:
        best = None
        for t in tables:
            try:
                cols = [
                    (c[0], str(c[1]).upper())
                    for c in conn.execute(f'DESCRIBE "{t}"').fetchall()
                ]
            except Exception:
                continue
            col_names = [c[0].lower() for c in cols]
            date_cols = [
                c
                for c, typ in cols
                if (
                    "DATE" in typ
                    or "TIMESTAMP" in typ
                    or any(
                        k in c.lower()
                        for k in ("date", "time", "created", "updated", "at")
                    )
                )
            ]
            if not date_cols:
                continue
            date_col = next((c for c in date_cols if "date" in c.lower()), date_cols[0])

            entity_col = None
            if entity:
                if entity in col_names:
                    entity_col = entity
                elif entity.endswith("_name") and entity[:-5] in col_names:
                    entity_col = entity[:-5]
                else:
                    entity_col = next((c for c in col_names if entity in c), None)
                if not entity_col:
                    text_cols = [
                        c
                        for c, typ in cols
                        if any(
                            tk in typ for tk in ("VARCHAR", "CHAR", "TEXT", "STRING")
                        )
                    ]
                    entity_base = entity.replace("_name", "").replace("_id", "")
                    tokens = [tok for tok in entity_base.split("_") if tok]
                    entity_col = next(
                        (
                            c
                            for c in text_cols
                            if any(tok in c.lower() for tok in tokens)
                        ),
                        None,
                    )

            agg_expr = None
            if metric == "aov":
                ck = (
                    count_key
                    if count_key and count_key in col_names
                    else _pick_best_count_key(col_names)
                )
                revenue_col = aov_revenue_col if aov_revenue_col in col_names else None
                if not revenue_col:
                    revenue_candidates = [
                        c
                        for c in col_names
                        if any(
                            k in c
                            for k in (
                                "final",
                                "total",
                                "amount",
                                "price",
                                "revenue",
                                "sales",
                                "earning",
                                "fare",
                                "net",
                                "paid",
                            )
                        )
                    ]
                    revenue_col = _pick_best_revenue_column(revenue_candidates)
                if ck and revenue_col:
                    agg_expr = (
                        f'SUM("{revenue_col}") / NULLIF(COUNT(DISTINCT "{ck}"), 0)'
                    )
            elif metric == "count":
                ck = count_key if count_key and count_key in col_names else None
                if not ck:
                    ck = _pick_best_count_key(col_names)
                agg_expr = f'COUNT(DISTINCT "{ck}")' if ck else "COUNT(*)"
            elif metric.startswith("avg_"):
                mcol = metric[4:]
                if mcol in col_names:
                    agg_expr = f'AVG("{mcol}")'
            elif metric in col_names:
                agg_expr = f'SUM("{metric}")'
            else:
                revenue_candidates = [
                    c
                    for c in col_names
                    if any(
                        k in c
                        for k in (
                            "final",
                            "total",
                            "amount",
                            "price",
                            "revenue",
                            "sales",
                            "earning",
                            "fare",
                            "net",
                            "paid",
                        )
                    )
                ]
                money_col = _pick_best_revenue_column(revenue_candidates)
                if money_col:
                    agg_expr = f'SUM("{money_col}")'

            if not agg_expr:
                continue

            score = 0
            if metric in col_names:
                score += 5
            if entity_col:
                score += 4
            if all((k in col_names) for k in filters.keys()):
                score += 2
            if any(k in date_col.lower() for k in ("date", "time", "created")):
                score += 2

            if best is None or score > best[0]:
                best = (score, t, date_col, entity_col, agg_expr)

        return (best[1], best[2], best[3], best[4]) if best else None

    try:
        plan = _pick_plan()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not plan:
        return None

    table, date_col, entity_col, agg_expr = plan
    filter_clause = _render_filter_clause(filters)
    where_extra = f"\n{filter_clause}" if filter_clause else ""

    if entity_col:
        return (
            f"WITH p1 AS (\n"
            f'  SELECT "{entity_col}" AS name, {agg_expr} AS value1\n'
            f'  FROM "{table}"\n'
            f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
            f'  GROUP BY "{entity_col}"\n'
            f"),\n"
            f"p2 AS (\n"
            f'  SELECT "{entity_col}" AS name, {agg_expr} AS value2\n'
            f'  FROM "{table}"\n'
            f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
            f'  GROUP BY "{entity_col}"\n'
            f")\n"
            f"SELECT COALESCE(p1.name, p2.name) AS name,\n"
            f"       COALESCE(p1.value1, 0) AS value1,\n"
            f"       COALESCE(p2.value2, 0) AS value2,\n"
            f"       COALESCE(p2.value2, 0) - COALESCE(p1.value1, 0) AS delta\n"
            f"FROM p1\n"
            f"FULL OUTER JOIN p2 ON p1.name = p2.name\n"
            f"ORDER BY value1 DESC\n"
            f"LIMIT {top_n}"
        )

    return (
        f"WITH p1 AS (\n"
        f"  SELECT {agg_expr} AS value1\n"
        f'  FROM "{table}"\n'
        f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
        f"),\n"
        f"p2 AS (\n"
        f"  SELECT {agg_expr} AS value2\n"
        f'  FROM "{table}"\n'
        f'  WHERE CAST("{date_col}" AS DATE) >= ? AND CAST("{date_col}" AS DATE) < ?{where_extra}\n'
        f")\n"
        f"SELECT 'Total' AS name, p1.value1 AS value1, p2.value2 AS value2, (p2.value2 - p1.value1) AS delta\n"
        f"FROM p1 CROSS JOIN p2"
    )


# Handler


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    from services.text_to_sql_service import run_text_to_sql
    from types import SimpleNamespace

    step_module = sys.modules.get(__name__) or SimpleNamespace(**globals())
    await run_text_to_sql(step_module, input_data, ctx)
