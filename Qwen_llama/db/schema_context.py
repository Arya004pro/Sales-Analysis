"""Schema prompt builder for SQL generation.

Key additions:
  - _get_epoch_cast_expr(): detects millisecond vs second BIGINT epoch columns
  - _detect_date_columns(): now distinguishes native DATE/TIMESTAMP from BIGINT
    epoch columns and provides the correct SQL cast expression to the LLM
  - _detect_business_rules(): scans for well-known filter columns
  - All detection is live from DuckDB — fully schema-agnostic
"""

import hashlib
import re
import time

from db.duckdb_connection import get_read_connection
from db.semantic_layer import render_semantic_layer_lines
from utils.redis_cache import redis_cache_get, redis_cache_set

_STATIC_FALLBACK = """
No tables are currently loaded in the database.
Please upload a dataset first (CSV, JSON, or Parquet) to begin querying.

Once data is loaded, the schema will be auto-detected from the live database.
Use ? placeholders for all params in generated SQL.
"""

# ── Column patterns that signal "exclude this row from metrics" ───────────────
_FILTER_COLUMN_RULES: dict[str, str] = {
    "is_cancelled": "{col} = 0",
    "is_deleted": "{col} = 0",
    "cancelled": "{col} = 0",
    "is_active": "{col} = 1",
    "active": "{col} = 1",
    "is_refunded": "{col} = 0",
    "refunded": "{col} = 0",
    "is_void": "{col} = 0",
    "is_fraud": "{col} = 0",
    "is_test": "{col} = 0",
}

_STATUS_BAD_VALUES: set[str] = {
    "cancelled",
    "canceled",
    "refunded",
    "void",
    "failed",
    "rejected",
    "returned",
    "closed",
    "inactive",
    "deleted",
}

_ENTITY_NAME_HINTS: tuple[str, ...] = (
    "name",
    "title",
    "label",
    "code",
    "id",
    "location",
    "warehouse",
    "store",
    "branch",
    "driver",
    "brand",
    "vendor",
    "customer",
    "employee",
    "agent",
    "partner",
    "city",
    "state",
    "region",
)

_NON_ENTITY_NAME_HINTS: tuple[str, ...] = (
    "date",
    "time",
    "timestamp",
    "created",
    "updated",
    "deleted",
    "status",
    "type",
    "description",
    "comment",
    "note",
    "month",
    "year",
    "week",
    "day",
)

# Integer types that might hold epoch timestamps
_BIGINT_TYPES = ("BIGINT", "INT8", "LONG", "HUGEINT", "INT64")

_SCHEMA_PROMPT_CACHE: dict[str, tuple[float, str]] = {}
_SCHEMA_PROMPT_CACHE_TTL_SECS = 300.0
_SCHEMA_MINIFIED_QUERY_RE = re.compile(r"[^a-z0-9_ ]+")
_SCHEMA_PROMPT_CACHE_PREFIX = "schema_prompt_cache:v1:"


def _normalize_query_signature(user_query: str | None) -> str:
    text = (user_query or "").strip().lower()
    if not text:
        return "all"
    text = _SCHEMA_MINIFIED_QUERY_RE.sub(" ", text)
    parts = [p for p in text.split() if len(p) >= 3]
    if not parts:
        return "all"
    return " ".join(parts[:10])


def _cache_get(cache_key: str) -> str | None:
    redis_key = f"{_SCHEMA_PROMPT_CACHE_PREFIX}{cache_key}"
    cached = redis_cache_get(redis_key)
    if cached is not None:
        _SCHEMA_PROMPT_CACHE[cache_key] = (time.time(), cached)
        return cached

    hit = _SCHEMA_PROMPT_CACHE.get(cache_key)
    if not hit:
        return None
    ts, value = hit
    if (time.time() - ts) > _SCHEMA_PROMPT_CACHE_TTL_SECS:
        _SCHEMA_PROMPT_CACHE.pop(cache_key, None)
        return None
    return value


def _cache_put(cache_key: str, value: str) -> None:
    _SCHEMA_PROMPT_CACHE[cache_key] = (time.time(), value)
    redis_key = f"{_SCHEMA_PROMPT_CACHE_PREFIX}{cache_key}"
    redis_cache_set(redis_key, value, ttl_seconds=int(_SCHEMA_PROMPT_CACHE_TTL_SECS))


def _schema_fingerprint(conn) -> str:
    rows = conn.execute(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name NOT LIKE '_raw_%'
        ORDER BY table_name, ordinal_position
        """
    ).fetchall()
    if not rows:
        return "empty"
    serial = "\n".join(f"{t}|{c}|{d}" for t, c, d in rows)
    return hashlib.sha1(serial.encode("utf-8")).hexdigest()[:16]


def _column_priority(
    col_name: str, dtype: str, query_sig: str
) -> tuple[int, int, int, int]:
    n = (col_name or "").lower()
    d = (dtype or "").upper()
    qparts = set(query_sig.split()) if query_sig and query_sig != "all" else set()
    score = 0
    if qparts and any(q in n for q in qparts):
        score += 8
    if any(k in n for k in ("date", "time", "created", "updated", "timestamp")):
        score += 7
    if any(
        k in n
        for k in ("name", "title", "label", "category", "brand", "store", "region")
    ):
        score += 6
    if any(
        k in n
        for k in (
            "revenue",
            "sales",
            "amount",
            "total",
            "final",
            "earning",
            "qty",
            "quantity",
        )
    ):
        score += 6
    if any(k in n for k in ("status", "active", "deleted", "cancelled", "refunded")):
        score += 4
    if n == "id" or n.endswith("_id"):
        score += 3
    if any(k in d for k in ("DATE", "TIMESTAMP")):
        score += 4
    return (
        score,
        1 if _looks_like_entity_column(n) else 0,
        1 if _is_numeric_type(d) else 0,
        1 if _is_text_type(d) else 0,
    )


def _pick_table_columns(
    cols: list[tuple[str, str]], query_sig: str, max_cols: int
) -> list[tuple[str, str]]:
    if len(cols) <= max_cols:
        return cols
    ranked = sorted(
        cols,
        key=lambda x: (_column_priority(x[0], x[1], query_sig), -len(x[0])),
        reverse=True,
    )
    kept = ranked[:max_cols]
    kept_names = {c[0] for c in kept}
    ordered = [c for c in cols if c[0] in kept_names]
    return ordered[:max_cols]


def _rank_tables_for_query(
    table_to_cols: dict[str, list[tuple[str, str]]],
    query_sig: str,
) -> list[str]:
    qparts = set(query_sig.split()) if query_sig and query_sig != "all" else set()
    if not qparts:
        return sorted(table_to_cols.keys())
    scored: list[tuple[int, str]] = []
    for table, cols in table_to_cols.items():
        score = 0
        t = table.lower()
        if any(q in t for q in qparts):
            score += 10
        for col, _ in cols:
            c = col.lower()
            if any(q in c for q in qparts):
                score += 2
        if score <= 0:
            score = 1
        scored.append((score, table))
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [t for _, t in scored]


def _is_text_type(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in ("VARCHAR", "CHAR", "TEXT", "STRING"))


def _is_numeric_type(dtype: str) -> bool:
    d = dtype.upper()
    return any(
        t in d
        for t in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
    )


def _looks_like_date_column(col_name: str) -> bool:
    n = col_name.lower()
    return any(
        k in n for k in ("date", "time", "timestamp", "month", "year", "week", "day")
    )


def _looks_like_entity_column(col_name: str) -> bool:
    n = col_name.lower()
    if _looks_like_date_column(n):
        return False
    if any(k in n for k in _NON_ENTITY_NAME_HINTS):
        return False
    if n == "id" or n.endswith("_id"):
        return True
    return any(k in n for k in _ENTITY_NAME_HINTS)


# ── Epoch timestamp helpers (NEW) ─────────────────────────────────────────────


def _get_epoch_cast_expr(conn, table: str, col: str) -> str:
    """
    Determine the correct DuckDB epoch→timestamp conversion for a BIGINT column.

    Thresholds:
      > 1_500_000_000_000 → epoch_ms()       (ms since Unix epoch, e.g. ~Nov 2017)
      > 1_000_000_000     → to_timestamp()   (s since Unix epoch, e.g. ~Sep 2001)
      otherwise           → epoch_ms()       (safe default)

    Returns a DuckDB expression string, e.g. 'epoch_ms("order_datetime")'
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
    return f'epoch_ms("{col}")'  # safe default for unknown scale


def _is_bigint_epoch_col(col_name: str, dtype: str) -> bool:
    """Return True when this BIGINT column name looks like a datetime column."""
    _DATE_KEYWORDS = (
        "date",
        "time",
        "created",
        "updated",
        "at",
        "on",
        "when",
        "timestamp",
    )
    d = dtype.upper()
    if not any(t in d for t in _BIGINT_TYPES):
        return False
    return any(k in col_name.lower() for k in _DATE_KEYWORDS)


# ── Entity detection ──────────────────────────────────────────────────────────


def _detect_entities(conn, tables: list[str]) -> list[str]:
    hints: list[str] = []
    for table in tables:
        try:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
        except Exception:
            continue

        candidates: list[tuple[float, str, int, int, float]] = []
        for col_name, dtype in cols:
            if not _is_text_type(dtype):
                continue
            if _looks_like_date_column(col_name):
                continue
            if not _looks_like_entity_column(col_name):
                continue

            try:
                non_null, distinct_cnt = conn.execute(
                    f'SELECT COUNT("{col_name}"), COUNT(DISTINCT "{col_name}") FROM "{table}"'
                ).fetchone()
            except Exception:
                continue

            non_null = int(non_null or 0)
            distinct_cnt = int(distinct_cnt or 0)
            if non_null == 0:
                continue

            ratio = distinct_cnt / non_null
            id_like = col_name.lower() == "id" or col_name.lower().endswith("_id")
            high_card = (
                (distinct_cnt >= 10 and ratio >= 0.30)
                or (distinct_cnt >= 50 and ratio >= 0.15)
                or (id_like and distinct_cnt >= 10 and ratio >= 0.60)
            )
            if not high_card:
                continue

            score = ratio
            n = col_name.lower()
            if "name" in n:
                score += 0.40
            elif id_like:
                score += 0.30
            elif any(
                k in n for k in ("location", "store", "warehouse", "branch", "code")
            ):
                score += 0.25
            if distinct_cnt >= 100:
                score += 0.10

            candidates.append((score, col_name, distinct_cnt, non_null, ratio))

        if not candidates:
            continue

        candidates.sort(reverse=True)
        top = candidates[:2]
        col_bits = ", ".join(
            f"{col} (distinct={distinct_cnt}/{non_null}, ratio={ratio:.2f})"
            for _, col, distinct_cnt, non_null, ratio in top
        )
        hints.append(f'  "{table}" -> {col_bits}')

    return hints


def _detect_uniqueness_profile(conn, tables: list[str]) -> tuple[list[str], list[str]]:
    profile_lines: list[str] = []
    grouping_lines: list[str] = []

    for table in tables:
        try:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
        except Exception:
            continue

        if not cols:
            continue

        col_stats: dict[str, tuple[int, int, float]] = {}
        unique_like: list[str] = []
        label_non_unique: list[str] = []

        for col_name, dtype in cols:
            if not (_is_text_type(dtype) or _is_numeric_type(dtype)):
                continue
            if _looks_like_date_column(col_name):
                continue

            try:
                non_null, distinct_cnt = conn.execute(
                    f'SELECT COUNT("{col_name}"), COUNT(DISTINCT "{col_name}") FROM "{table}"'
                ).fetchone()
            except Exception:
                continue

            non_null = int(non_null or 0)
            distinct_cnt = int(distinct_cnt or 0)
            if non_null == 0:
                continue

            ratio = distinct_cnt / non_null
            col_stats[col_name.lower()] = (distinct_cnt, non_null, ratio)

            n = col_name.lower()
            id_like = (
                n == "id"
                or n.endswith("_id")
                or n.endswith("_uuid")
                or n.endswith("_key")
                or "email" in n
                or "phone" in n
                or "mobile" in n
                or n.endswith("_code")
                or ratio > 0.99
            )
            if distinct_cnt >= 2 and ratio >= 0.98 and id_like:
                unique_like.append(col_name)

            if _is_text_type(dtype) and distinct_cnt >= 2 and ratio < 0.98:
                label_non_unique.append(
                    f"{col_name} ({distinct_cnt}/{non_null}, ratio={ratio:.2f})"
                )

        if unique_like:
            profile_lines.append(
                f'  "{table}" UNIQUE columns (use for COUNT DISTINCT): {", ".join(unique_like[:10])}'
            )
        if label_non_unique:
            profile_lines.append(
                f'  "{table}" non-unique labels: {", ".join(label_non_unique[:6])}'
            )

        for raw_col, dtype in cols:
            c = raw_col.lower()
            if _is_text_type(dtype):
                base = c[:-5] if c.endswith("_name") else c
                key_candidates = [
                    f"{base}_id",
                    f"{base}_code",
                    f"{base}_key",
                    f"{base}_uuid",
                    "id",
                ]
                chosen = None
                for k in key_candidates:
                    if k == c:
                        continue
                    stats = col_stats.get(k)
                    if not stats:
                        continue
                    _, non_null, ratio = stats
                    if non_null >= 2 and ratio >= 0.98:
                        chosen = k
                        break
                if chosen:
                    lbl_stats = col_stats.get(c)
                    if lbl_stats and lbl_stats[2] < 0.98:
                        grouping_lines.append(
                            f'  "{table}": to group by "{c}", use GROUP BY "{chosen}", "{c}"'
                        )

    return profile_lines, grouping_lines


def _detect_business_rules(conn, tables: list[str]) -> list[str]:
    rules: list[str] = []

    for table in tables:
        try:
            cols = {
                c[0].lower(): c[1].upper()
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            }
        except Exception:
            continue

        for col_name, dtype in cols.items():
            rule_tmpl = _FILTER_COLUMN_RULES.get(col_name)
            if rule_tmpl:
                condition = rule_tmpl.format(col=col_name)
                rules.append(
                    f'  "{table}": always add WHERE {condition}'
                    f"  -- exclude {'inactive' if 'active' in col_name else 'cancelled/invalid'} rows"
                )
                continue

            if col_name in (
                "status",
                "order_status",
                "ride_status",
                "payment_status",
                "state",
            ):
                try:
                    rows = conn.execute(
                        f'SELECT DISTINCT "{col_name}" FROM "{table}" LIMIT 30'
                    ).fetchall()
                    values = {str(r[0]).lower() for r in rows if r[0] is not None}
                    bad = values & _STATUS_BAD_VALUES
                    good = values - _STATUS_BAD_VALUES - {""}
                    if bad and good:
                        good_list = ", ".join(f"'{v}'" for v in sorted(good)[:6])
                        rules.append(
                            f'  "{table}".{col_name}: only include rows where '
                            f"{col_name} IN ({good_list})"
                            f"  -- exclude {sorted(bad)}"
                        )
                except Exception:
                    pass

    return rules


def _detect_metric_columns(conn, tables: list[str]) -> list[str]:
    hints: list[str] = []

    MONETARY_KEYWORDS = (
        "price",
        "fare",
        "amount",
        "earning",
        "revenue",
        "commission",
        "fee",
        "cost",
        "sale",
        "payment",
        "total",
    )
    QUANTITY_KEYWORDS = ("quantity", "qty", "count", "units", "volume")
    DISTANCE_KEYWORDS = ("distance", "km", "mile")
    DURATION_KEYWORDS = ("duration", "minute", "min", "second", "hour")

    for table in tables:
        try:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
        except Exception:
            continue

        monetary_cols = [
            c
            for c, t in cols
            if any(k in c.lower() for k in MONETARY_KEYWORDS)
            and any(x in t for x in ("FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "INT"))
        ]
        quantity_cols = [
            c
            for c, t in cols
            if any(k in c.lower() for k in QUANTITY_KEYWORDS)
            and any(x in t for x in ("INT", "FLOAT", "DOUBLE"))
        ]
        distance_cols = [
            c for c, t in cols if any(k in c.lower() for k in DISTANCE_KEYWORDS)
        ]
        duration_cols = [
            c for c, t in cols if any(k in c.lower() for k in DURATION_KEYWORDS)
        ]

        if monetary_cols:
            preferred = next(
                (c for c in monetary_cols if "final" in c.lower()),
                next(
                    (c for c in monetary_cols if "total" in c.lower()), monetary_cols[0]
                ),
            )
            hints.append(
                f'  revenue/sales/earnings in "{table}" → SUM({preferred})'
                f"  (all monetary columns: {', '.join(monetary_cols)})"
            )

        if quantity_cols:
            hints.append(f'  quantity/units in "{table}" → SUM({quantity_cols[0]})')

        if distance_cols:
            hints.append(f'  distance in "{table}" → SUM({distance_cols[0]})')

        if duration_cols:
            hints.append(f'  duration in "{table}" → SUM({duration_cols[0]})')

    return hints


def _detect_date_columns(conn, tables: list[str]) -> list[str]:
    """
    Return lines telling the LLM which column to use for date filtering
    and — critically — what SQL cast expression to use based on the actual type.

    Distinguishes three cases:
      1. Native DATE / TIMESTAMP  → CAST(col AS DATE) works directly
      2. BIGINT millisecond epoch → epoch_ms(col) must be used first
      3. BIGINT second epoch      → to_timestamp(col) must be used first
    """
    hints: list[str] = []
    DATE_KEYWORDS = ("date", "time", "created", "updated", "at", "on", "when")

    for table in tables:
        try:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
        except Exception:
            continue

        for col, dtype in cols:
            col_l = col.lower()
            if not any(k in col_l for k in DATE_KEYWORDS):
                continue

            if "DATE" in dtype or "TIMESTAMP" in dtype:
                # Native date type — standard CAST works
                hints.append(
                    f'  date filter for "{table}"."{col}" (type: {dtype}): '
                    f'use CAST("{col}" AS DATE) >= ? AND CAST("{col}" AS DATE) < ?  '
                    f"(exclusive end — pass end+1day as second ?)"
                )

            elif any(t in dtype for t in _BIGINT_TYPES):
                # Integer epoch — CAST(col AS DATE) will FAIL.  Must convert first.
                epoch_expr = _get_epoch_cast_expr(conn, table, col)
                cast_for_date = f"CAST({epoch_expr} AS DATE)"
                hints.append(
                    f'  date filter for "{table}"."{col}" (type: {dtype} — INTEGER EPOCH TIMESTAMP):\n'
                    f'    *** CRITICAL: "{col}" is stored as an INTEGER, not a native date. ***\n'
                    f"    CORRECT:   {cast_for_date} >= ? AND {cast_for_date} < ?\n"
                    f'    WRONG:     CAST("{col}" AS DATE) >= ?   ← causes BIGINT->DATE error\n'
                    f'    WRONG:     "{col}" >= ?                  ← causes type mismatch\n'
                    f"    For EXTRACT:   EXTRACT(YEAR FROM {epoch_expr})\n"
                    f"    For STRFTIME:  STRFTIME({epoch_expr}, '%Y-%m')\n"
                    f"    For DATE_TRUNC: DATE_TRUNC('month', {epoch_expr})\n"
                    f"    Always use {epoch_expr} before any date operation on this column."
                )

    return hints


def _build_schema_examples(conn, tables: list[str]) -> list[str]:
    """
    Build dynamic few-shot SQL examples using actual table/column names.
    Now epoch-aware: if the date column is BIGINT, uses the right cast.
    """
    metric_keywords = (
        "final",
        "total",
        "amount",
        "revenue",
        "sales",
        "price",
        "fare",
        "earning",
        "commission",
        "quantity",
        "count",
    )
    date_keywords = ("date", "time", "created", "updated", "at")

    best: tuple[int, str, str, str, str, str] | None = None
    # score, table, entity_col, metric_col, date_col, date_cast_expr

    for table in tables:
        try:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
        except Exception:
            continue

        entity_cols = [
            c for c, t in cols if _is_text_type(t) and _looks_like_entity_column(c)
        ]
        if not entity_cols:
            continue

        metric_cols = [
            c
            for c, t in cols
            if any(
                x in t
                for x in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC")
            )
            and not c.lower().endswith("_id")
        ]
        if not metric_cols:
            continue

        date_col_info: list[tuple[str, str]] = []  # (col_name, dtype)
        for c, t in cols:
            if any(k in c.lower() for k in date_keywords) or any(
                x in t for x in ("DATE", "TIMESTAMP")
            ):
                date_col_info.append((c, t))
        if not date_col_info:
            continue

        entity_col = next(
            (c for c in entity_cols if "name" in c.lower()), entity_cols[0]
        )
        metric_col = next(
            (c for c in metric_cols if any(k in c.lower() for k in metric_keywords)),
            metric_cols[0],
        )
        date_col, date_dtype = date_col_info[0]

        # Determine the right date cast expression for this column
        if any(t in date_dtype.upper() for t in _BIGINT_TYPES):
            epoch_expr = _get_epoch_cast_expr(conn, table, date_col)
            date_cast_expr = f"CAST({epoch_expr} AS DATE)"
        else:
            date_cast_expr = f'CAST("{date_col}" AS DATE)'

        score = 0
        if "name" in entity_col.lower():
            score += 3
        if any(
            k in metric_col.lower()
            for k in ("revenue", "sales", "amount", "final", "total", "price")
        ):
            score += 3
        if "date" in date_col.lower():
            score += 2
        score += min(len(metric_cols), 3)

        if best is None or score > best[0]:
            best = (score, table, entity_col, metric_col, date_col, date_cast_expr)

    if best is None:
        return []

    _, table, entity_col, metric_col, date_col, date_cast_expr = best

    # For time series, build the right bucket expression
    if any(t in date_cast_expr for t in ("epoch_ms", "to_timestamp")):
        # Extract the epoch expression for use in STRFTIME etc.
        epoch_inner = date_cast_expr.replace("CAST(", "").replace(" AS DATE)", "")
        month_expr = f"STRFTIME({epoch_inner}, '%Y-%m')"
    else:
        month_expr = f"STRFTIME(CAST(\"{date_col}\" AS DATE), '%Y-%m')"

    return [
        f"  Example 1 (Top-N by metric):",
        f'    User: "Top 10 {entity_col} by {metric_col} in a time range"',
        f'    SQL:  SELECT "{entity_col}" AS name, SUM("{metric_col}") AS value',
        f'          FROM "{table}"',
        f"          WHERE {date_cast_expr} >= ? AND {date_cast_expr} < ?",
        f"          GROUP BY 1 ORDER BY value DESC LIMIT ?",
        "",
        f"  Example 2 (Aggregate total):",
        f'    User: "Total {metric_col} in a time range"',
        f'    SQL:  SELECT SUM("{metric_col}") AS value',
        f'          FROM "{table}"',
        f"          WHERE {date_cast_expr} >= ? AND {date_cast_expr} < ?",
        "",
        f"  Example 3 (Monthly trend):",
        f'    User: "Monthly trend of {metric_col}"',
        f'    SQL:  SELECT {month_expr} AS name, SUM("{metric_col}") AS value',
        f'          FROM "{table}"',
        f"          WHERE {date_cast_expr} >= ? AND {date_cast_expr} < ?",
        "          GROUP BY 1 ORDER BY 1",
    ]


def _detect_date_columns_compact(conn, tables: list[str]) -> list[str]:
    hints: list[str] = []
    DATE_KEYWORDS = ("date", "time", "created", "updated", "at", "on", "when")
    for table in tables:
        try:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
        except Exception:
            continue
        for col, dtype in cols:
            cl = col.lower()
            if not any(k in cl for k in DATE_KEYWORDS) and not any(
                x in dtype for x in ("DATE", "TIMESTAMP")
            ):
                continue
            if any(x in dtype for x in ("DATE", "TIMESTAMP")):
                hints.append(
                    f'  "{table}"."{col}" ({dtype}): use CAST("{col}" AS DATE) >= ? AND CAST("{col}" AS DATE) < ?'
                )
            elif any(t in dtype for t in _BIGINT_TYPES):
                epoch_expr = _get_epoch_cast_expr(conn, table, col)
                hints.append(
                    f'  "{table}"."{col}" ({dtype} epoch): use CAST({epoch_expr} AS DATE) >= ? AND CAST({epoch_expr} AS DATE) < ?'
                )
    return hints


def _live_schema_prompt() -> str:
    conn = get_read_connection()
    try:
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name NOT LIKE '_raw_%'
            ORDER BY table_name
            """
        ).fetchall()
        tables = [r[0] for r in rows]
        if not tables:
            return ""

        lines = ["Tables", "------"]
        for t in tables:
            cols = conn.execute(f'DESCRIBE "{t}"').fetchall()
            col_s = ", ".join(f"{c[0]} {c[1]}" for c in cols)
            lines.append(f"{t:<14}: {col_s}")

        entity_hints = _detect_entities(conn, tables)
        lines += ["", "Detected Entities", "-----------------"]
        lines += [
            "Use these as preferred grouping dimensions for entity questions.",
            "Map user wording to the closest detected dimension below.",
        ]
        if entity_hints:
            lines += entity_hints
        else:
            lines += ["  (No high-cardinality entity-like text columns detected.)"]

        uniqueness_lines, grouping_lines = _detect_uniqueness_profile(conn, tables)
        lines += ["", "Uniqueness Profile", "------------------"]
        if uniqueness_lines:
            lines += uniqueness_lines
        else:
            lines += ["  (No uniqueness signals detected.)"]
        lines += ["", "Safe Grouping Keys", "------------------"]
        if grouping_lines:
            lines += [
                "When display labels are not unique, group by stable key + label.",
            ]
            lines += grouping_lines
        else:
            lines += ["  (No explicit display->key pairing detected.)"]

        metric_hints = _detect_metric_columns(conn, tables)
        if metric_hints:
            lines += [
                "",
                "Metric column mappings (USE THESE EXACT COLUMN NAMES)",
                "-----------------------------------------------------------",
            ]
            lines += metric_hints

        # Date column hints — now epoch-aware
        date_hints = _detect_date_columns(conn, tables)
        if date_hints:
            lines += [
                "",
                "Date filter columns (READ CAREFULLY — some are BIGINT epochs)",
                "-------------------------------------------------------------------",
            ]
            lines += date_hints

        examples = _build_schema_examples(conn, tables)
        lines += ["", "Schema-Grounded Examples", "------------------------"]
        if examples:
            lines += [
                "Use these examples as patterns. Replace table/column names only when needed.",
                "Keep aliases exactly as: name, value.",
            ]
            lines += examples
        else:
            lines += [
                "  (No complete table profile found with entity + metric + date columns.)",
            ]

        lines += [""]
        lines += render_semantic_layer_lines(conn, tables)

        biz_rules = _detect_business_rules(conn, tables)
        if biz_rules:
            lines += [
                "",
                "MANDATORY business-validity filters (ALWAYS apply — never omit)",
                "-------------------------------------------------------------------",
                "These filters MUST appear in every query that touches these tables.",
            ]
            lines += biz_rules

        lines += [
            "",
            "SQL rules",
            "---------",
            "- SELECT only; no DML",
            "- Alias group key as name and metric as value",
            "- Use ? placeholders for ALL params (dates, limits, thresholds)",
            "- For ranked queries: ORDER BY value DESC/ASC LIMIT ?",
            "- For aggregate queries: single scalar column aliased 'value', no GROUP BY",
            "- APPLY all mandatory business-validity filters listed above",
            "- NEVER use CAST(bigint_epoch_col AS DATE) — see 'Date filter columns' above",
        ]

        return "\n".join(lines)

    finally:
        conn.close()


def _live_schema_prompt_compact(user_query: str | None = None) -> str:
    conn = get_read_connection()
    try:
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name NOT LIKE '_raw_%'
            ORDER BY table_name
            """
        ).fetchall()
        tables = [r[0] for r in rows]
        if not tables:
            return ""

        query_sig = _normalize_query_signature(user_query)

        table_to_cols: dict[str, list[tuple[str, str]]] = {}
        for t in tables:
            try:
                table_to_cols[t] = [
                    (c[0], c[1]) for c in conn.execute(f'DESCRIBE "{t}"').fetchall()
                ]
            except Exception:
                continue
        if not table_to_cols:
            return ""

        ranked_tables = _rank_tables_for_query(table_to_cols, query_sig)
        selected_tables = ranked_tables[: min(5, len(ranked_tables))]

        lines = ["Tables (focused)", "----------------"]
        for t in selected_tables:
            cols = table_to_cols.get(t, [])
            compact_cols = _pick_table_columns(cols, query_sig, max_cols=20)
            col_s = ", ".join(f"{c[0]} {c[1]}" for c in compact_cols)
            truncated = " ..." if len(cols) > len(compact_cols) else ""
            lines.append(f"{t:<14}: {col_s}{truncated}")

        # Keep section titles stable so upstream prompt rules still align.
        metric_hints = _detect_metric_columns(conn, selected_tables)
        lines += [
            "",
            "Metric column mappings (USE THESE EXACT COLUMN NAMES)",
            "-----------------------------------------------------------",
        ]
        lines += metric_hints or [
            "  (No obvious metric columns detected from names/types.)"
        ]

        date_hints = _detect_date_columns_compact(conn, selected_tables)
        lines += ["", "Date filter columns", "-------------------"]
        lines += date_hints or ["  (No date-like columns detected.)"]

        biz_rules = _detect_business_rules(conn, selected_tables)
        lines += [
            "",
            "MANDATORY business-validity filters (ALWAYS apply)",
            "----------------------------------------------------",
        ]
        lines += biz_rules or ["  (No mandatory business filters detected.)"]

        # Semantic layer is already capped (dims/metrics), useful for generality.
        lines += [""]
        lines += render_semantic_layer_lines(conn, selected_tables)

        lines += [
            "",
            "SQL rules",
            "---------",
            "- SELECT only; no DML",
            "- Alias group key as name and metric as value",
            "- Use ? placeholders for ALL params",
            "- Ranked queries: ORDER BY value DESC/ASC LIMIT ?",
            "- Aggregate queries: one scalar column aliased value",
            "- Use exclusive date ranges: col >= ? AND col < ?",
        ]
        return "\n".join(lines)
    finally:
        conn.close()


def get_schema_prompt(mode: str = "full", user_query: str | None = None) -> str:
    try:
        conn = get_read_connection()
        try:
            fp = _schema_fingerprint(conn)
        finally:
            conn.close()

        normalized_mode = (mode or "full").strip().lower()
        query_sig = _normalize_query_signature(
            user_query if normalized_mode == "compact" else ""
        )
        cache_key = f"{normalized_mode}|{fp}|{query_sig}"
        cached = _cache_get(cache_key)
        if cached:
            return cached

        if normalized_mode == "compact":
            live = _live_schema_prompt_compact(user_query=user_query).strip()
        else:
            live = _live_schema_prompt().strip()

        if live:
            _cache_put(cache_key, live)
            return live
    except Exception:
        pass
    return _STATIC_FALLBACK.strip()
