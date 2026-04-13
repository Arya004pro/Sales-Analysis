"""Step 5: Execute Query — runs SQL against DuckDB.

Key fix: auto-detects BIGINT epoch timestamp columns (e.g. order_datetime stored as
Unix ms/s) and rewrites SQL patterns like CAST(col AS DATE) or EXTRACT(X FROM col)
to use the correct DuckDB epoch conversion (epoch_ms / to_timestamp).
This makes the system fully generalised — any dataset with BIGINT datetime columns
works without manual schema configuration.
"""

import os
import sys
import calendar
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_MODULE_DIR))
_MOTIA_DIR = os.path.join(_PROJECT_ROOT, "motia")
_STEPS_DIR = os.path.join(_MOTIA_DIR, "steps")
for _p in (_MODULE_DIR, _STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)


from motia import FlowContext, queue
from db.duckdb_connection import run_query as _run_sql_raw, get_read_connection
from db.query_validator import validate_query

config = {
    "name": "SQLExecutor",
    "description": (
        "Executes planned SQL against DuckDB with safety rewrites and validation. "
        "Auto-detects BIGINT epoch timestamp columns and rewrites SQL accordingly. "
        "Uses exclusive end-date (>= start AND < end+1day). "
        "Auto-repairs missing GROUP BY for time-series queries."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::execute")],
    "enqueues": ["query::forecast", "query::detect.anomalies"],
}

# ── Datetime column hints ──────────────────────────────────────────────────────
_DATETIME_COL_HINTS = {
    "date",
    "time",
    "timestamp",
    "created",
    "updated",
    "at",
    "on",
    "when",
}

# BIGINT-like types that might store epoch timestamps
_BIGINT_TYPES = ("BIGINT", "INT8", "LONG", "HUGEINT", "INT64")

# ── SQL rewrite patterns ──────────────────────────────────────────────────────
_EXTRACT_YEAR_PAT = re.compile(
    r"EXTRACT\s*\(\s*YEAR\s+FROM\s+(\w+)\s*\)\s*=\s*\?", re.IGNORECASE
)
_EXTRACT_YEAR_IN_PARAMS_PAT = re.compile(
    r"EXTRACT\s*\(\s*YEAR\s+FROM[^\)]*\)\s+IN\s*\(\s*\?\s*,\s*\?\s*\)",
    re.IGNORECASE,
)
_EXTRACT_MONTH_PAT = re.compile(
    r"\s*AND\s+EXTRACT\s*\(\s*MONTH\s+FROM\s+\w+\s*\)\s+BETWEEN\s+\d+\s+AND\s+\d+",
    re.IGNORECASE,
)
_EXTRACT_ANY_PAT = re.compile(
    r"EXTRACT\s*\(\s*(YEAR|MONTH|DAY|QUARTER)\s+FROM", re.IGNORECASE
)
_BETWEEN_PAT = re.compile(r"(\w+)\s+BETWEEN\s+\?\s+AND\s+\?", re.IGNORECASE)
_GROUP_BY_CLAUSE_RE = re.compile(
    r"(?is)\bGROUP\s+BY\b(?P<group_by>.*?)(?=(\bORDER\s+BY\b|\bLIMIT\b|\bHAVING\b|$))"
)
_NAME_EXPR_RE = re.compile(r"(?is)\bSELECT\s+(?P<expr>.*?)\s+AS\s+name\b")
_MISSING_GB_COL_RE = re.compile(
    r'column\s+"([^"]+)"\s+must appear in the GROUP BY', re.IGNORECASE
)
_HAS_GROUP_BY_RE = re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE)

_BIGINT_MAP_CACHE_TTL_SECONDS = 120.0
_bigint_map_cache_value: dict[str, str] | None = None
_bigint_map_cache_at: float = 0.0


# ── BIGINT epoch detection (NEW) ───────────────────────────────────────────────


def _build_bigint_datetime_map() -> dict[str, str]:
    """
    Scan live DuckDB schema for BIGINT columns that look like epoch timestamps.

    Returns {column_name_lower: epoch_cast_expr} for every such column found,
    e.g. {'order_datetime': 'epoch_ms("order_datetime")'}.

    This is fully schema-agnostic — it works for any dataset, not just Uber/Zomato.
    """
    global _bigint_map_cache_value, _bigint_map_cache_at
    now = time.monotonic()
    if (
        _bigint_map_cache_value is not None
        and (now - _bigint_map_cache_at) < _BIGINT_MAP_CACHE_TTL_SECONDS
    ):
        return dict(_bigint_map_cache_value)

    result: dict[str, str] = {}
    try:
        conn = get_read_connection()
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]

        for table in tables:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
            for col, dtype in cols:
                col_l = col.lower()
                # Only columns whose name contains a date-like keyword
                if not any(k in col_l for k in _DATETIME_COL_HINTS):
                    continue
                # Only integer-typed columns (DATE/TIMESTAMP handled natively by DuckDB)
                if not any(t in dtype for t in _BIGINT_TYPES):
                    continue
                # Skip if already in result (first table wins)
                if col_l in result:
                    continue

                # Determine epoch scale by sampling max value
                cast_expr = _detect_epoch_scale(conn, table, col)
                if cast_expr:
                    result[col_l] = cast_expr

        conn.close()
    except Exception:
        pass

    _bigint_map_cache_value = dict(result)
    _bigint_map_cache_at = now
    return result


def _detect_epoch_scale(conn, table: str, col: str) -> str | None:
    """
    Sample the column to determine if values are millisecond or second epoch.
    Returns the appropriate DuckDB conversion expression, or None if undetermined.

    Thresholds:
      > 1_500_000_000_000 → milliseconds (epoch_ms)   e.g. 1_700_000_000_000 = Nov 2023 ms
      > 1_000_000_000     → seconds      (to_timestamp) e.g. 1_700_000_000   = Nov 2023 s
    """
    try:
        r = conn.execute(
            f'SELECT MAX("{col}") FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT 1'
        ).fetchone()
        if r and r[0] is not None:
            mv = int(r[0])
            if mv > 1_500_000_000_000:
                return f'epoch_ms("{col}")'
            elif mv > 1_000_000_000:
                return f'to_timestamp("{col}")'
    except Exception:
        pass
    # Default: assume ms for unknown BIGINT date columns
    return f'epoch_ms("{col}")'


def _rewrite_bigint_epoch_cols(
    sql: str, bigint_map: dict[str, str]
) -> tuple[str, bool]:
    """
    Rewrite SQL so BIGINT epoch datetime columns use proper DuckDB epoch functions.

    Handles these SQL patterns (case-insensitive, with or without quotes on col):
      1. CAST(col AS DATE)          → CAST(epoch_ms(col) AS DATE)
      2. CAST("col" AS DATE)        → CAST(epoch_ms(col) AS DATE)
      3. EXTRACT(X FROM col)        → EXTRACT(X FROM epoch_ms(col))
      4. STRFTIME(col, '...')       → STRFTIME(epoch_ms(col), '...')
      5. DATE_TRUNC('x', col)       → DATE_TRUNC('x', epoch_ms(col))
      6. col >= ?  (bare comparison) → CAST(epoch_ms(col) AS DATE) >= ?

    This is a generalised fix — works for any BIGINT epoch column in any dataset.
    """
    if not bigint_map or not sql:
        return sql, False

    changed = False

    for col_l, epoch_expr in bigint_map.items():
        # Build a regex that matches col with or without double-quotes
        col_re = re.escape(col_l)
        col_pattern = rf'(?:"(?:{col_re})"|\b(?:{col_re})\b)'

        # 1. CAST(col AS DATE) — the most common failure case
        pat = re.compile(rf"CAST\s*\(\s*{col_pattern}\s+AS\s+DATE\s*\)", re.IGNORECASE)
        if pat.search(sql):
            sql = pat.sub(f"CAST({epoch_expr} AS DATE)", sql)
            changed = True

        # 2. EXTRACT(X FROM col) — used in year/month grouping
        pat = re.compile(
            rf"EXTRACT\s*\(\s*(\w+)\s+FROM\s+{col_pattern}\s*\)", re.IGNORECASE
        )
        if pat.search(sql):
            # epoch_ms() returns TIMESTAMP, EXTRACT works directly on TIMESTAMP
            sql = pat.sub(
                lambda m, ep=epoch_expr: f"EXTRACT({m.group(1)} FROM {ep})", sql
            )
            changed = True

        # 3. STRFTIME(col, '...') — used in time bucket expressions
        pat = re.compile(rf"STRFTIME\s*\(\s*{col_pattern}\s*,", re.IGNORECASE)
        if pat.search(sql):
            sql = pat.sub(f"STRFTIME({epoch_expr},", sql)
            changed = True

        # 4. DATE_TRUNC('bucket', col)
        pat = re.compile(
            rf"DATE_TRUNC\s*\(\s*('[^']+')\s*,\s*{col_pattern}\s*\)", re.IGNORECASE
        )
        if pat.search(sql):
            sql = pat.sub(
                lambda m, ep=epoch_expr: f"DATE_TRUNC({m.group(1)}, {ep})", sql
            )
            changed = True

        # 5. YEAR(col) / MONTH(col) / DAY(col) scalar functions
        for fn in ("YEAR", "MONTH", "DAY", "QUARTER", "WEEK"):
            pat = re.compile(rf"\b{fn}\s*\(\s*{col_pattern}\s*\)", re.IGNORECASE)
            if pat.search(sql):
                sql = pat.sub(f"{fn}({epoch_expr})", sql)
                changed = True

        # 6. Bare column comparison: col >= ? or col < ? (without any cast)
        # Only apply if no epoch conversion is already present for this column.
        if epoch_expr not in sql:
            pat = re.compile(rf"{col_pattern}\s*(>=|<=|>|<|=)\s*\?", re.IGNORECASE)
            if pat.search(sql):
                sql = pat.sub(
                    lambda m, ep=epoch_expr: (
                        f"CAST({ep} AS DATE) {m.group(1).strip()} ?"
                    ),
                    sql,
                )
                changed = True

    return sql, changed


# ── Schema helpers ─────────────────────────────────────────────────────────────


def _get_id_name_pairs() -> dict[str, str]:
    """Return {name_col: best_discriminator_col} for deduplication."""
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
            if (base + suffix) in col_set:
                return base + suffix
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


# ── Rewrite: add ID to GROUP BY ────────────────────────────────────────────────


def _repair_group_by_add_id(sql: str) -> tuple[str, bool]:
    """Prepend discriminator key to GROUP BY to prevent same-name entity merging."""
    if not _HAS_GROUP_BY_RE.search(sql):
        return sql, False

    id_name_pairs = _get_id_name_pairs()
    if not id_name_pairs:
        return sql, False

    gb_match = _GROUP_BY_CLAUSE_RE.search(sql)
    if not gb_match:
        return sql, False

    gb_raw = gb_match.group("group_by").strip()

    def _strip_alias(token: str) -> str:
        t = token.strip().strip('"').strip("`")
        if "." in t:
            t = t.split(".")[-1]
        return t.lower()

    gb_tokens_raw = [t.strip() for t in gb_raw.split(",") if t.strip()]
    gb_tokens_bare = [_strip_alias(t) for t in gb_tokens_raw]

    changed = False
    new_tokens_raw = list(gb_tokens_raw)

    for name_col, id_col in id_name_pairs.items():
        if name_col in gb_tokens_bare and id_col not in gb_tokens_bare:
            idx = gb_tokens_bare.index(name_col)
            new_tokens_raw.insert(idx, id_col)
            gb_tokens_bare.insert(idx, id_col)
            changed = True

    if not changed:
        return sql, False

    new_gb_str = ", ".join(new_tokens_raw)
    start, end = gb_match.span("group_by")
    return sql[:start] + " " + new_gb_str + " " + sql[end:], True


# ── Existing helpers ──────────────────────────────────────────────────────────


def _exclusive_end(d: date) -> date:
    return d + timedelta(days=1)


def _is_datetime_col(col_name: str) -> bool:
    low = col_name.lower()
    return any(hint in low for hint in _DATETIME_COL_HINTS)


def _rewrite_extract_to_range(sql: str) -> tuple[str, bool]:
    if not _EXTRACT_YEAR_PAT.search(sql):
        return sql, False
    new_sql = _EXTRACT_YEAR_PAT.sub(r"\1 >= ? AND \1 < ?", sql)
    new_sql = _EXTRACT_MONTH_PAT.sub("", new_sql)
    new_sql = re.sub(r"\s{2,}", " ", new_sql).strip()
    return new_sql, True


def _rewrite_between_to_range(sql: str) -> tuple[str, bool]:
    changed = False

    def _replacer(m: re.Match) -> str:
        nonlocal changed
        col = m.group(1)
        if _is_datetime_col(col):
            changed = True
            return f"{col} >= ? AND {col} < ?"
        return m.group(0)

    return _BETWEEN_PAT.sub(_replacer, sql), changed


def _has_extract(sql: str) -> bool:
    return bool(_EXTRACT_ANY_PAT.search(sql))


def _repair_group_by_missing_name(sql: str, error_text: str) -> str | None:
    err_m = _MISSING_GB_COL_RE.search(error_text or "")
    gb_m = _GROUP_BY_CLAUSE_RE.search(sql or "")
    name_m = _NAME_EXPR_RE.search(sql or "")
    if not (err_m and gb_m and name_m):
        return None
    missing_col = err_m.group(1).strip()
    name_expr = name_m.group("expr").strip()
    if not name_expr:
        return None
    group_by_raw = gb_m.group("group_by")
    if (
        missing_col.lower() in group_by_raw.lower()
        or name_expr.lower() in group_by_raw.lower()
    ):
        return None
    fixed = f"{group_by_raw.strip()}, {name_expr}"
    s, e = gb_m.span("group_by")
    return sql[:s] + " " + fixed + " " + sql[e:]


def _repair_add_missing_group_by(sql: str, error_text: str) -> str | None:
    if _HAS_GROUP_BY_RE.search(sql):
        return None
    name_m = _NAME_EXPR_RE.search(sql or "")
    if not name_m:
        return None
    name_expr = name_m.group("expr").strip()
    expr_upper = name_expr.upper()
    is_time_bucket = any(
        kw in expr_upper
        for kw in [
            "EXTRACT(",
            "STRFTIME(",
            "DATE_TRUNC(",
            "YEAR(",
            "MONTH(",
            "QUARTER(",
            "WEEK(",
            "DAY(",
            "EPOCH_MS(",
            "TO_TIMESTAMP(",
        ]
    )
    if not is_time_bucket:
        return None
    order_m = re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE)
    limit_m = re.search(r"\bLIMIT\b", sql, re.IGNORECASE)
    group_by_clause = f"\nGROUP BY {name_expr}"
    if order_m:
        pos = order_m.start()
        return sql[:pos].rstrip() + group_by_clause + "\n" + sql[pos:]
    elif limit_m:
        pos = limit_m.start()
        return sql[:pos].rstrip() + group_by_clause + "\n" + sql[pos:]
    else:
        return sql.rstrip() + group_by_clause


def _run_sql(sql: str, params: tuple) -> list:
    sql_duck = sql.replace("%s", "?")
    return _run_sql_raw(sql_duck, list(params))


def _rows_to_dicts(rows: list, parsed: dict | None = None) -> list[dict]:
    rank_within_time = bool((parsed or {}).get("_rank_within_time"))
    result = []
    for row in rows:
        if len(row) == 1:
            result.append({"value": float(row[0]) if row[0] is not None else None})
        elif len(row) == 2:
            result.append(
                {
                    "name": str(row[0]),
                    "value": float(row[1]) if row[1] is not None else 0.0,
                }
            )
        elif len(row) == 3:
            if rank_within_time:
                result.append(
                    {
                        "period": str(row[0]),
                        "name": str(row[1]),
                        "value": float(row[2]) if row[2] is not None else 0.0,
                    }
                )
            else:
                result.append(
                    {
                        "name": str(row[0]),
                        "value1": float(row[1]) if row[1] is not None else 0.0,
                        "value2": float(row[2]) if row[2] is not None else 0.0,
                    }
                )
        elif len(row) >= 4:
            result.append(
                {
                    "name": str(row[0]),
                    "value1": float(row[1]) if row[1] is not None else 0.0,
                    "value2": float(row[2]) if row[2] is not None else 0.0,
                    "delta": float(row[3]) if row[3] is not None else 0.0,
                }
            )
    return result


def _build_params(sql: str, parsed: dict) -> tuple:
    n = sql.count("%s") + sql.count("?")
    trs = parsed.get("time_ranges", [])
    qt = parsed.get("query_type", "top_n")
    top_n = parsed.get("top_n", 5) or 5
    thr = parsed.get("threshold") or {}

    if n == 0:
        return ()

    def _d(s: str) -> date:
        return date.fromisoformat(s)

    uses_exclusive = ">=" in sql and "< ?" in sql

    def _end(end_str: str) -> date:
        d = _d(end_str)
        return _exclusive_end(d) if uses_exclusive else d

    if (
        qt in ("comparison", "growth_ranking", "intersection", "retention")
        and len(trs) >= 2
    ):
        s1 = _d(trs[0]["start"])
        e1 = _end(trs[0]["end"])
        s2 = _d(trs[1]["start"])
        e2 = _end(trs[1]["end"])
        if n == 4 and _EXTRACT_YEAR_IN_PARAMS_PAT.search(sql):
            y1 = _d(trs[0]["start"]).year
            y2 = _d(trs[1]["start"]).year
            s_min = min(s1, s2)
            e_max = max(e1, e2)
            return (s_min, e_max, y1, y2)
        if n == 5 and _EXTRACT_YEAR_IN_PARAMS_PAT.search(sql):
            y1 = _d(trs[0]["start"]).year
            y2 = _d(trs[1]["start"]).year
            s_min = min(s1, s2)
            e_max = max(e1, e2)
            return (s_min, e_max, y1, y2, top_n)
        if n == 4:
            return (s1, e1, s2, e2)
        if n == 5:
            return (s1, e1, s2, e2, top_n)
        base = [s1, e1, s2, e2]
        while len(base) < n - 1:
            base += [s1, e1]
        if n > len(base):
            base.append(top_n)
        return tuple(base[:n])

    if trs:
        s = _d(trs[0]["start"])
        e = _end(trs[0]["end"])
    else:
        today = date.today()
        s = today.replace(month=1, day=1)
        e = _exclusive_end(date(today.year, 12, 31))

    if n == 2:
        return (s, e)
    if n == 3:
        if qt == "threshold" and thr.get("type") == "absolute":
            return (s, e, thr.get("value"))
        return (s, e, top_n)
    if n == 4 and qt == "threshold":
        return (s, e, s, e)
    if n == 5 and qt == "threshold" and thr.get("type") == "percentage":
        return (s, e, thr.get("value"), s, e)

    slots = []
    for i in range(n):
        if i == n - 1:
            if qt == "threshold" and thr.get("type") == "absolute":
                slots.append(thr.get("value"))
            else:
                slots.append(top_n)
        elif i % 2 == 0:
            slots.append(s)
        else:
            slots.append(e)
    return tuple(slots)


def _period_label(start_str: str, end_str: str) -> str:
    s = date.fromisoformat(start_str)
    e = date.fromisoformat(end_str)
    if s.month == e.month and s.year == e.year:
        return f"{calendar.month_name[s.month]} {s.year}"
    if s.day == 1 and e.day >= 28:
        return f"{calendar.month_abbr[s.month]}-{calendar.month_abbr[e.month]} {s.year}"
    return f"{s} to {e}"


async def _error(ctx, qs, query_id, msg):
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "status": "error",
                "error": msg,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "error": now_iso},
            },
        )


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    from services.execute_query_service import run_execute_query
    from types import SimpleNamespace

    step_module = sys.modules.get(__name__) or SimpleNamespace(**globals())
    await run_execute_query(step_module, input_data, ctx)
