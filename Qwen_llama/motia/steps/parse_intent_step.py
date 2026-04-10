"""Step 2: Parse Intent — Qwen extracts full structured intent as JSON.

Changes vs original:
  - System prompt METRIC RULES section no longer hardcodes specific column names
    (final_price, total_fare, driver_earnings). It now tells the LLM to read the
    live "Metric column mappings" section from the schema instead.
  - _fallback_parse() no longer hardcodes entity/metric keyword→column maps
    for Uber/Zomato. It now reads the live schema to build those maps dynamically.
  - All other logic (time_series detection, post_process, mandatory filters) unchanged.
"""

import os
import sys
import re
import json
import time
from datetime import datetime, timezone
from typing import Any

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import FlowContext, queue
from shared_config import (
    GROQ_API_TOKEN,
    QWEN_MODEL,
    LLAMA_MODEL,
    GROQ_URL,
    QWEN_ENABLE_REASONING,
    QWEN_REASONING_EFFORT,
    PARSE_INTENT_MAX_RETRIES,
    PARSE_INTENT_USE_INSTRUCTOR,
)
from utils.token_logger import log_tokens, add_tokens_to_state, calc_max_tokens
from utils.time_parser import parse_time_ranges_from_query
from utils.llm_client import clean_model_text, is_rate_limit_error, post_chat_completion
from db.schema_context import get_schema_prompt
from db.duckdb_connection import get_read_connection
from llm.structured_intent import parse_intent_with_instructor

try:
    # Prefer the new generic resolver name if present.
    from db.semantic_layer import resolve_semantic_context as _semantic_resolver
except Exception:
    try:
        # Backward compatible resolver name used by existing semantic layer.
        from db.semantic_layer import (
            resolve_intent_with_semantic_layer as _semantic_resolver,
        )
    except Exception:
        _semantic_resolver = None

config = {
    "name": "IntentParser",
    "description": (
        "Parses natural language into structured analytics intent JSON. "
        "Schema injected live — no hardcoded column/domain names. "
        "Fallback parser uses live schema for entity/metric detection. "
        "Supports trend and forecast intents with mandatory business filters."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::intent.parse")],
    "enqueues": ["query::text.to.sql"],
}

# ── System prompt ─────────────────────────────────────────────────────────────
# NOTE: Metric rules no longer hardcode specific column names.
# The LLM is told to read the "Metric column mappings" section from the
# injected schema, which is built dynamically from the live DB.
_SYSTEM_TEMPLATE = """You are an analytics intent parser. Return ONLY valid JSON.

Schema:
{schema}

Output schema (all fields required):
{{"entity":string|null,"metric":string,"query_type":one of [top_n,bottom_n,aggregate,threshold,comparison,growth_ranking,intersection,zero_filter,time_series,forecast],"time_bucket":one of [month,week,quarter,year,day]|null,"forecast_periods":integer(default 3),"forecast_method":one of [auto,holt,linear,sma]|null,"top_n":integer(default 5),"time_ranges":[{{"start":"YYYY-MM-DD","end":"YYYY-MM-DD","label":string}}],"threshold":{{"value":number,"type":absolute|percentage,"operator":gt|lt}}|null,"filters":object,"is_complete":boolean,"clarification_question":string|null}}

Rules:
1) Use semantic terms from Semantic Layer/Metric mappings; resolver maps later.
2) entity is display/name column only; never raw IDs.
3) aggregate/time_series/forecast => entity must be null.
4) time_series for trend/by-time asks; set time_bucket and mark complete when metric+time_ranges exist.
5) forecast for predict/project/future asks; set time_bucket, forecast_periods (requested else 3), forecast_method=auto unless explicit.
6) count asks => metric=count with COUNT(DISTINCT order-like id); AOV => metric=aov; avg asks => avg_<column>.
7) Inject mandatory validity filters from schema (is_cancelled/is_deleted/is_refunded/status completed etc).
8) Completeness: ranked queries need entity+time_ranges; aggregate/time_series/forecast need metric+time_ranges; ask one critical clarification only.
9) Time ranges: parse absolute/relative periods; use full boundaries (year/quarter/month) and two ranges for comparison/YoY.
"""

_TOPN_RE = re.compile(r"\b(top|bottom)\s+(\d+)\b", re.IGNORECASE)

_TREND_KEYWORDS = {
    "month-wise",
    "monthly",
    "week-wise",
    "weekly",
    "day-wise",
    "daily",
    "quarterly",
    "quarter-wise",
    "by month",
    "per month",
    "by week",
    "per week",
    "by day",
    "per day",
    "by quarter",
    "per quarter",
    "over time",
    "trend",
    "time series",
    "breakdown by month",
    "breakdown by week",
    "revenue trend",
    "fare trend",
    "earnings trend",
    "how did",
    "how has",
    "yearly",
    "year-wise",
    "per year",
    "annual",
    "annually",
    "by year",
    "year over year",
    "yoy",
    "each year",
    "every year",
    "year-on-year",
}
_FORECAST_KEYWORDS = {
    "forecast",
    "predict",
    "projection",
    "project",
    "next month",
    "next quarter",
    "next year",
    "next 3 months",
    "next 6 months",
    "future",
    "anticipated",
    "expected revenue",
    "expected sales",
    "will be",
    "going to be",
}

_ALL_TIME_YEARLY_HINTS = {
    "each year",
    "every year",
    "yearly",
    "annual",
    "annually",
    "by year",
    "per year",
}

_RANK_WITHIN_TIME_CUES: dict[str, tuple[str, ...]] = {
    "year": (
        "per year",
        "by year",
        "each year",
        "every year",
        "year-wise",
        "yearly",
        "annual",
        "annually",
    ),
    "quarter": (
        "per quarter",
        "by quarter",
        "each quarter",
        "quarter-wise",
        "quarterly",
    ),
    "month": (
        "per month",
        "by month",
        "each month",
        "month-wise",
        "monthly",
    ),
    "week": (
        "per week",
        "by week",
        "each week",
        "week-wise",
        "weekly",
    ),
    "day": (
        "per day",
        "by day",
        "each day",
        "day-wise",
        "daily",
    ),
}

_REVENUE_QUERY_HINTS = (
    "revenue",
    "sales",
    "earning",
    "earnings",
    "income",
    "turnover",
    "gmv",
    "gross merchandise",
    "net sales",
)

_REVENUE_STRONG_COL_HINTS = (
    "revenue",
    "sales",
    "earning",
    "amount",
    "total",
    "final",
    "net",
    "paid",
    "gmv",
)

_REVENUE_WEAK_COL_HINTS = (
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

_AOV_QUERY_HINTS = (
    "aov",
    "average order value",
    "avg order value",
    "average basket value",
    "average transaction value",
    "average ticket size",
)

_MANDATORY_FILTER_COLS: dict[str, dict] = {
    "is_cancelled": {"is_cancelled": 0},
    "is_deleted": {"is_deleted": 0},
    "cancelled": {"cancelled": 0},
    "is_active": {"is_active": 1},
    "is_refunded": {"is_refunded": 0},
    "is_void": {"is_void": 0},
    "is_fraud": {"is_fraud": 0},
    "is_test": {"is_test": 0},
}

_FOLLOWUP_CUES = (
    "same",
    "same as above",
    "same as before",
    "as above",
    "as before",
    "of that",
    "for that",
    "that one",
    "those",
    "this one",
    "again",
    "previous",
    "earlier",
    "continue",
)

_EXPLICIT_NEW_INTENT_CUES = (
    "compare",
    "versus",
    " vs ",
    "forecast",
    "predict",
    "projection",
    "trend",
    "over time",
    "time series",
)


def _resolve_semantic_context(parsed: dict, user_query: str) -> dict:
    """Resolve semantic aliases defensively without failing intent parsing."""
    if not isinstance(parsed, dict):
        return parsed
    if _semantic_resolver is None:
        return parsed
    try:
        resolved = _semantic_resolver(parsed, user_query)
        return resolved if isinstance(resolved, dict) else parsed
    except Exception:
        return parsed


# ── Schema-driven fallback builder ────────────────────────────────────────────


def _build_schema_keyword_maps() -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Read the live DuckDB schema and return two keyword→column maps:
      entity_map : [(keyword, column_name), ...]  for entity (text) columns
      metric_map : [(keyword, column_name), ...]  for metric (numeric) columns

    These replace the hardcoded lists that were previously in _fallback_parse().
    """
    entity_map: list[tuple[str, str]] = []
    metric_map: list[tuple[str, str]] = []

    _TEXT_TYPES = ("VARCHAR", "CHAR", "TEXT", "STRING")
    _NUM_TYPES = ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
    _DATE_HINTS = ("date", "time", "created", "updated", "timestamp")
    _ID_HINTS = ("_id", "_key", "_uuid")

    _MONETARY = (
        "price",
        "fare",
        "amount",
        "earning",
        "revenue",
        "commission",
        "fee",
        "cost",
        "sale",
        "total",
        "final",
        "profit",
    )
    _COUNT_TRIGGERS = ("order", "ride", "trip", "booking", "transaction", "visit")

    try:
        conn = get_read_connection()
        try:
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='main' ORDER BY table_name"
                ).fetchall()
            ]

            for table in tables:
                cols = conn.execute(f'DESCRIBE "{table}"').fetchall()

                # Find primary-key-like column for count metric
                id_col = next(
                    (c[0] for c in cols if c[0].lower().endswith("_id")),
                    None,
                )

                for col, dtype, *_ in cols:
                    col_l = dtype.upper()
                    cname = col.lower()

                    # Skip date-like columns
                    if any(k in cname for k in _DATE_HINTS):
                        continue

                    if any(t in col_l for t in _TEXT_TYPES):
                        # Entity candidate
                        if any(k in cname for k in _ID_HINTS):
                            continue  # Skip raw id columns
                        # Build keyword from column name by stripping suffixes
                        base = cname
                        for sfx in ("_name", "_type", "_mode", "_status"):
                            if base.endswith(sfx):
                                base = base[: -len(sfx)]
                                break
                        base_words = base.replace("_", " ").strip()
                        if base_words:
                            entity_map.append((base_words, col))
                        # Also add the raw column name as keyword
                        if cname != base_words:
                            entity_map.append((cname.replace("_", " "), col))

                    elif any(t in col_l for t in _NUM_TYPES):
                        # Metric candidate — skip id-like columns
                        if any(k in cname for k in _ID_HINTS):
                            continue
                        if cname in ("year", "month", "day", "week", "quarter"):
                            continue

                        # Map natural language keywords → this column
                        base_words = cname.replace("_", " ")

                        # Monetary column → revenue/earnings/sales all map here
                        if any(k in cname for k in _MONETARY):
                            metric_map.append((base_words, col))
                            # Add revenue aliases only for strong revenue columns.
                            if any(
                                k in cname
                                for k in (
                                    "revenue",
                                    "sales",
                                    "amount",
                                    "earning",
                                    "total",
                                    "final",
                                    "net",
                                    "paid",
                                )
                            ):
                                for alias in (
                                    "revenue",
                                    "sales",
                                    "income",
                                    "earnings",
                                    "money",
                                ):
                                    metric_map.append((alias, col))

                        # Count trigger columns (order_id, ride_id etc.)
                        if id_col and any(k in cname for k in _COUNT_TRIGGERS):
                            for alias in (
                                "count",
                                "how many",
                                "number of",
                                "rides",
                                "trips",
                                "orders",
                                "bookings",
                                "visits",
                            ):
                                metric_map.append((alias, "count"))

                        # Quantity
                        if "quantity" in cname or "qty" in cname:
                            for alias in (
                                "quantity",
                                "units",
                                "items",
                                "pieces",
                                "how many items",
                                "units sold",
                            ):
                                metric_map.append((alias, col))

                        # Distance / duration
                        if "distance" in cname or "km" in cname:
                            metric_map.append(("distance", col))
                            metric_map.append(("km", col))
                        if "duration" in cname or "minute" in cname:
                            metric_map.append(("duration", col))
                            metric_map.append(("time taken", col))

        finally:
            conn.close()

    except Exception:
        pass  # Return whatever we have (may be empty — caller handles it)

    # Deduplicate while preserving order
    seen_e: set = set()
    seen_m: set = set()
    entity_map = [
        (k, v) for k, v in entity_map if (k, v) not in seen_e and not seen_e.add((k, v))
    ]  # type: ignore[func-returns-value]
    metric_map = [
        (k, v) for k, v in metric_map if (k, v) not in seen_m and not seen_m.add((k, v))
    ]  # type: ignore[func-returns-value]

    return entity_map, metric_map


# ── Mandatory filters ─────────────────────────────────────────────────────────


def _get_mandatory_filters() -> dict:
    mandatory: dict = {}
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        for (table,) in rows:
            cols = {
                c[0].lower() for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            }
            for col_name, filter_dict in _MANDATORY_FILTER_COLS.items():
                if col_name in cols:
                    mandatory.update(filter_dict)
        conn.close()
    except Exception:
        pass
    return mandatory


def _should_skip_mandatory_filter(
    col_name: str, user_query: str, chosen_entity: str | None = None
) -> bool:
    ql = (user_query or "").lower()
    c = (col_name or "").lower().strip()
    if not c:
        return False
    if chosen_entity and c == str(chosen_entity).lower():
        return True

    normalized = c.replace("is_", "")
    tokens = [t for t in normalized.split("_") if len(t) >= 3]
    if any(tok in ql for tok in tokens):
        return True
    return False


def _find_split_dimension_for_query(
    user_query: str, require_split_cue: bool = True
) -> str | None:
    ql = (user_query or "").lower()
    split_cue = any(x in ql for x in (" vs ", " versus ", " compared to ", " against "))
    grouping_cue = bool(re.search(r"\b(per|by|for each|each)\b", ql))
    if require_split_cue and not split_cue:
        return None
    if not split_cue and not grouping_cue:
        return None

    # Keep this generic: identify low-cardinality columns likely used for categorical splits.
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()

        best: tuple[int, str] | None = None
        for (table,) in rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            for col, dtype, *_ in cols:
                c = col.lower()
                d = str(dtype).upper()

                if any(
                    k in c for k in ("date", "time", "created", "updated", "timestamp")
                ):
                    continue
                if c.endswith("_id"):
                    continue
                if any(
                    k in c
                    for k in (
                        "price",
                        "amount",
                        "revenue",
                        "sales",
                        "earning",
                        "total",
                        "cost",
                        "discount",
                        "qty",
                        "quantity",
                        "distance",
                        "duration",
                        "score",
                        "rate",
                        "percent",
                        "ratio",
                    )
                ):
                    continue
                is_text = any(t in d for t in ("VARCHAR", "CHAR", "TEXT", "STRING"))
                is_numeric = any(
                    t in d
                    for t in (
                        "INT",
                        "BIGINT",
                        "FLOAT",
                        "DOUBLE",
                        "DECIMAL",
                        "NUMERIC",
                        "REAL",
                    )
                )
                if not (is_text or is_numeric):
                    continue

                try:
                    distinct_cnt, non_null = conn.execute(
                        f'SELECT COUNT(DISTINCT "{col}"), COUNT("{col}") FROM "{table}"'
                    ).fetchone()
                    distinct_cnt = int(distinct_cnt or 0)
                    non_null = int(non_null or 0)
                except Exception:
                    continue

                if non_null == 0 or distinct_cnt < 2 or distinct_cnt > 12:
                    continue

                score = 0
                name_tokens = [
                    t for t in c.replace("is_", "").split("_") if len(t) >= 3
                ]
                token_hit = any(tok in ql for tok in name_tokens)
                if token_hit:
                    score += 10
                if c.startswith("is_"):
                    score += 4
                if distinct_cnt <= 6:
                    score += 3
                if token_hit and any(
                    w in ql for w in ("count", "number of", "how many", "total")
                ):
                    score += 2

                if not split_cue and not token_hit:
                    continue

                if score > 0 and (best is None or score > best[0]):
                    best = (score, col)

        conn.close()
        return best[1] if best else None
    except Exception:
        return None


def _infer_boolean_flag_filters(
    user_query: str,
    split_cue: bool,
    chosen_entity: str | None = None,
) -> dict[str, int]:
    if split_cue:
        return {}

    ql = (user_query or "").lower()

    def _word(term: str) -> bool:
        return bool(re.search(rf"\b{re.escape(term)}\b", ql))

    inferred: dict[str, int] = {}
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        for (table,) in rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            for col, dtype, *_ in cols:
                c = str(col).lower()
                d = str(dtype).upper()
                if chosen_entity and c == str(chosen_entity).lower().strip():
                    continue
                if not any(
                    t in d for t in ("INT", "BIGINT", "SMALLINT", "TINYINT", "BOOLEAN")
                ):
                    continue

                try:
                    vals = conn.execute(
                        f'SELECT DISTINCT "{col}" FROM "{table}" '
                        f'WHERE "{col}" IS NOT NULL LIMIT 3'
                    ).fetchall()
                except Exception:
                    continue
                norm_vals = {str(v[0]).strip() for v in vals}
                if not norm_vals.issubset(
                    {"0", "1", "0.0", "1.0", "False", "True", "false", "true"}
                ):
                    continue

                base = c[3:] if c.startswith("is_") else c
                base_words = base.replace("_", " ")

                positive_hit = _word(base) or _word(base_words) or _word(c)
                negative_hit = any(
                    phrase in ql
                    for phrase in (
                        f"not {base_words}",
                        f"non {base_words}",
                        f"non-{base_words}",
                        f"without {base_words}",
                        f"no {base_words}",
                    )
                )
                if base == "active" and _word("inactive"):
                    negative_hit = True
                if base.endswith("ed") and _word(f"un{base}"):
                    negative_hit = True

                if negative_hit:
                    inferred[c] = 0
                elif positive_hit:
                    inferred[c] = 1

        conn.close()
    except Exception:
        return inferred

    return inferred


def _sanitize_scalar_filters(filters: Any) -> dict[str, Any]:
    """Keep only scalar filter values; drop nested structures from model output."""
    if not isinstance(filters, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in filters.items():
        key = str(k or "").strip()
        if not key:
            continue
        if isinstance(v, (bool, int, float, str)):
            out[key] = v
    return out


# ── Time-range helpers ────────────────────────────────────────────────────────


def _looks_like_all_time_trend(query: str) -> bool:
    q = (query or "").lower()
    return any(k in q for k in _ALL_TIME_YEARLY_HINTS)


def _infer_dataset_time_range() -> list[dict]:
    try:
        conn = get_read_connection()
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        best = None
        for (table,) in rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            for c in cols:
                col = c[0]
                typ = str(c[1]).upper()
                low = col.lower()
                if not (
                    "DATE" in typ
                    or "TIMESTAMP" in typ
                    or any(
                        k in low for k in ("date", "time", "created", "updated", "at")
                    )
                ):
                    continue
                try:
                    r = conn.execute(
                        f'SELECT MIN(CAST("{col}" AS DATE)), MAX(CAST("{col}" AS DATE)) '
                        f'FROM "{table}" WHERE "{col}" IS NOT NULL'
                    ).fetchone()
                    if not r or not r[0] or not r[1]:
                        continue
                    s, e = r[0], r[1]
                    if best is None or (s < best[0] or e > best[1]):
                        best = (s, e)
                except Exception:
                    continue
        conn.close()
        if best:
            s, e = best
            label = (
                f"All data ({s.year})"
                if s.year == e.year
                else f"All data ({s.year}-{e.year})"
            )
            return [{"start": s.isoformat(), "end": e.isoformat(), "label": label}]
    except Exception:
        pass
    return []


# ── JSON helpers ──────────────────────────────────────────────────────────────


def _extract_json_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        return ""
    depth, in_str, esc = 0, False, False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return ""


def _is_revenue_intent(query: str) -> bool:
    q = (query or "").lower()
    return any(k in q for k in _REVENUE_QUERY_HINTS)


def _revenue_col_score(col_name: str) -> int:
    c = (col_name or "").lower()
    score = 0

    if any(k in c for k in _REVENUE_STRONG_COL_HINTS):
        score += 10
    if "final" in c or "net" in c or "paid" in c:
        score += 8
    if "total" in c:
        score += 6
    if "price" in c or "fare" in c:
        score += 3
    if any(k in c for k in _REVENUE_WEAK_COL_HINTS):
        score -= 7

    return score


def _select_primary_revenue_column(metric_map: list[tuple[str, str]]) -> str | None:
    candidates: dict[str, int] = {}
    for _, col in metric_map:
        c = (col or "").lower().strip()
        if not c or c == "count" or c.startswith("avg_"):
            continue
        score = _revenue_col_score(c)
        prev = candidates.get(c)
        if prev is None or score > prev:
            candidates[c] = score

    if not candidates:
        return None

    ranked = sorted(
        candidates.items(),
        key=lambda kv: (
            kv[1],
            1 if "final" in kv[0] else 0,
            1 if "total" in kv[0] else 0,
            1 if "amount" in kv[0] else 0,
            -len(kv[0]),
        ),
        reverse=True,
    )
    return ranked[0][0]


def _select_primary_count_key() -> str | None:
    try:
        conn = get_read_connection()
        table_rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        best: tuple[int, str] | None = None
        for (table,) in table_rows:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            id_cols = [
                c[0].lower()
                for c in cols
                if c[0].lower().endswith("_id") or c[0].lower() == "id"
            ]
            for col in id_cols:
                score = 0
                if any(
                    k in col
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
                    score += 10
                if any(
                    k in col
                    for k in ("row", "line", "item", "detail", "record", "event", "log")
                ):
                    score -= 10
                if col == "id":
                    score -= 2

                try:
                    non_null, distinct_cnt = conn.execute(
                        f'SELECT COUNT("{col}"), COUNT(DISTINCT "{col}") FROM "{table}"'
                    ).fetchone()
                    non_null = int(non_null or 0)
                    distinct_cnt = int(distinct_cnt or 0)
                    if non_null > 0:
                        ratio = distinct_cnt / non_null
                        if 0.4 <= ratio < 0.995:
                            score += 6
                        elif ratio >= 0.995:
                            score -= 6
                        elif ratio < 0.05:
                            score -= 4
                except Exception:
                    pass

                if best is None or score > best[0]:
                    best = (score, col)
        conn.close()
        return best[1] if best else None
    except Exception:
        return None


def _is_aov_intent(query: str) -> bool:
    q = (query or "").lower()
    return any(k in q for k in _AOV_QUERY_HINTS)


def _default_clarification(parsed: dict) -> str:
    qt = parsed.get("query_type", "top_n")
    if qt in ("aggregate", "time_series", "forecast"):
        return "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
    if not parsed.get("entity"):
        return "Which dimension should I group by? (e.g. customer, product, region, channel, category)"
    if not parsed.get("time_ranges"):
        return "What time period should I use? (e.g. 2024, Q1 2024, March 2024)"
    if not parsed.get("metric"):
        return "What metric should I measure? (e.g. revenue, quantity, record count)"
    return "Please clarify: which dimension, metric, and time period do you want?"


def _detect_time_bucket(query: str) -> str:
    q = query.lower()
    if any(x in q for x in ["month", "monthly", "month-wise", "by month", "per month"]):
        return "month"
    if any(x in q for x in ["quarter", "q1", "q2", "q3", "q4", "quarterly"]):
        return "quarter"
    if any(x in q for x in ["week", "weekly", "week-wise", "per week"]):
        return "week"
    if any(x in q for x in ["day", "daily", "day-wise", "per day"]):
        return "day"
    if any(
        x in q
        for x in [
            "per year",
            "by year",
            "each year",
            "yearly",
            "year-wise",
            "annual",
            "annually",
            "yoy",
        ]
    ):
        return "year"
    return "month"


def _is_trend_query(query: str) -> bool:
    q = query.lower()
    return any(kw in q for kw in _TREND_KEYWORDS)


def _is_forecast_query(query: str) -> bool:
    q = (query or "").lower()
    return any(kw in q for kw in _FORECAST_KEYWORDS)


def _detect_forecast_bucket(query: str) -> str:
    q = (query or "").lower()
    if any(x in q for x in ["month", "monthly", "month-wise", "by month", "per month"]):
        return "month"
    if any(x in q for x in ["quarter", "q1", "q2", "q3", "q4", "quarterly"]):
        return "quarter"
    if any(x in q for x in ["week", "weekly", "week-wise", "per week"]):
        return "week"
    if any(x in q for x in ["day", "daily", "day-wise", "per day"]):
        return "day"
    # For forecast phrasing, "next year" usually implies monthly projections.
    if "next year" in q:
        return "month"
    if any(
        x in q
        for x in [
            "per year",
            "by year",
            "each year",
            "yearly",
            "year-wise",
            "annual",
            "annually",
        ]
    ):
        return "year"
    return _detect_time_bucket(query)


def _extract_forecast_periods(query: str, bucket: str = "month") -> int:
    q = (query or "").lower()
    m = re.search(r"next\s+(\d+)\s*(month|quarter|year|week|day)", q)
    if m:
        try:
            n = int(m.group(1))
            unit = m.group(2)
            if bucket == "month" and unit == "year":
                return n * 12
            if bucket == "quarter" and unit == "year":
                return n * 4
            if bucket == "week" and unit == "year":
                return n * 52
            if bucket == "day" and unit == "year":
                return n * 365
            if bucket == "month" and unit == "quarter":
                return n * 3
            return n
        except Exception:
            return 3
    if "next month" in q:
        return 1
    if "next quarter" in q:
        return 3 if bucket == "month" else 1
    if "next year" in q:
        if bucket == "month":
            return 12
        if bucket == "quarter":
            return 4
        if bucket == "week":
            return 52
        if bucket == "day":
            return 365
        return 1
    return 3


def _parse_scaled_number(num_text: str, suffix: str = "") -> float | None:
    raw = (num_text or "").replace(",", "").strip()
    if not raw:
        return None
    try:
        val = float(raw)
    except Exception:
        return None

    s = (suffix or "").strip().lower()
    mult = 1.0
    if s in {"k", "thousand"}:
        mult = 1_000.0
    elif s in {"m", "mn", "million"}:
        mult = 1_000_000.0
    elif s in {"b", "bn", "billion"}:
        mult = 1_000_000_000.0
    elif s in {"l", "lac", "lakh", "lakhs"}:
        mult = 100_000.0
    elif s in {"cr", "crore", "crores"}:
        mult = 10_000_000.0

    return val * mult


def _infer_rank_within_time_bucket(query: str) -> str | None:
    q = (query or "").lower()
    if not _has_ranking_cue(query):
        return None
    for bucket, cues in _RANK_WITHIN_TIME_CUES.items():
        if any(c in q for c in cues):
            return bucket
    return None


def _has_ranking_cue(query: str) -> bool:
    q = f" {query.lower()} "
    if _TOPN_RE.search(q):
        return True
    return any(
        x in q
        for x in [
            " top ",
            " bottom ",
            "highest",
            "lowest",
            "least",
            "most",
            "best",
            "worst",
            "top-",
            "bottom-",
        ]
    )


def _has_metric_cue(query: str) -> bool:
    q = (query or "").lower()
    metric_cues = (
        "revenue",
        "sales",
        "earning",
        "income",
        "turnover",
        "gmv",
        "count",
        "how many",
        "number of",
        "quantity",
        "units",
        "average",
        "avg",
        "price",
        "amount",
        "fare",
        "cost",
        "discount",
        "profit",
        "margin",
        "rate",
        "score",
    )
    return any(c in q for c in metric_cues)


def _is_followup_referential_query(query: str) -> bool:
    q = f" {(query or '').lower()} "
    if any(cue in q for cue in _FOLLOWUP_CUES):
        return True
    return False


def _is_explicit_new_intent_query(query: str) -> bool:
    q = f" {(query or '').lower()} "
    if _has_ranking_cue(query):
        return True
    if _is_trend_query(query) or _is_forecast_query(query):
        return True
    if any(cue in q for cue in _EXPLICIT_NEW_INTENT_CUES):
        return True
    return False


def _merge_followup_intent(
    parsed: dict[str, Any],
    user_query: str,
    followup_context: dict[str, Any] | None,
) -> dict[str, Any]:
    prev_parsed = dict((followup_context or {}).get("previousParsed") or {})
    if not prev_parsed:
        return parsed

    q = (user_query or "").strip()
    token_count = len(re.findall(r"[a-z0-9]+", q.lower()))
    missing_core = (
        not parsed.get("entity")
        or not parsed.get("metric")
        or not parsed.get("time_ranges")
    )
    referential = _is_followup_referential_query(q)
    short_followup = token_count <= 7 and missing_core
    explicit_new_intent = _is_explicit_new_intent_query(q)

    # Merge only when the user likely refers to previous context.
    if not referential and not short_followup:
        return parsed
    if explicit_new_intent and not referential:
        return parsed

    merged = dict(parsed)

    if not merged.get("entity") and prev_parsed.get("entity"):
        merged["entity"] = prev_parsed.get("entity")

    if not merged.get("metric") and prev_parsed.get("metric"):
        merged["metric"] = prev_parsed.get("metric")

    if not merged.get("time_ranges") and prev_parsed.get("time_ranges"):
        merged["time_ranges"] = prev_parsed.get("time_ranges")

    current_filters = dict(merged.get("filters") or {})
    previous_filters = dict(prev_parsed.get("filters") or {})
    if previous_filters:
        previous_filters.update(current_filters)
        merged["filters"] = previous_filters

    for key in ("time_bucket", "forecast_periods", "forecast_method", "top_n"):
        if not merged.get(key) and prev_parsed.get(key):
            merged[key] = prev_parsed.get(key)

    # For referential phrasing like "same for last year", keep previous type
    # unless the new turn clearly asks for a different intent family.
    if (
        referential
        and not explicit_new_intent
        and (not merged.get("query_type") or merged.get("query_type") == "top_n")
        and prev_parsed.get("query_type")
    ):
        merged["query_type"] = prev_parsed.get("query_type")

    if (merged.get("metric") or "").lower() == "aov":
        for key in ("_aov_revenue_col", "_count_distinct_key"):
            if not merged.get(key) and prev_parsed.get(key):
                merged[key] = prev_parsed.get(key)

    merged["_followup_applied"] = True
    merged["_followup_from_query_id"] = (followup_context or {}).get("previousQueryId")
    return merged


def _infer_entity_from_grouping_cue(
    user_query: str,
    entity_map: list[tuple[str, str]],
) -> str | None:
    q = f" {(user_query or '').lower()} "
    targets = []
    for m in re.finditer(
        r"\b(?:per|by|for each|each)\s+([a-z][a-z0-9]*(?:\s+[a-z][a-z0-9]*){0,2})", q
    ):
        phrase = (m.group(1) or "").strip()
        if not phrase:
            continue
        first = phrase.split()[0]
        if first in {"year", "month", "week", "day", "quarter", "date", "time"}:
            continue
        targets.append(phrase)

    best: tuple[int, str] | None = None

    for kw, col in entity_map:
        k = (kw or "").lower().strip()
        c = (col or "").lower().replace("_", " ").strip()
        if not k:
            continue
        variants = {k, k.rstrip("s"), c, c.rstrip("s")}
        k_tokens = [t for t in re.split(r"\s+", k.replace("_", " ")) if t]
        c_tokens = [t for t in re.split(r"\s+", c) if t]
        for t in k_tokens + c_tokens:
            if len(t) >= 3 and t not in {"name", "type", "status", "code", "id"}:
                variants.add(t)

        score = 0
        for v in variants:
            if len(v) < 3:
                continue
            if f" per {v} " in q:
                score += 12
            if f" by {v} " in q:
                score += 10
            if f" each {v} " in q or f" for each {v} " in q:
                score += 10
            for tgt in targets:
                if tgt == v:
                    score += 10
                elif tgt in v or v in tgt:
                    score += 6
        if score > 0 and (best is None or score > best[0]):
            best = (score, col)

    return best[1] if best else None


def _infer_entity_from_query_terms(
    user_query: str,
    entity_map: list[tuple[str, str]],
) -> str | None:
    q = (user_query or "").lower()
    query_terms = {
        t
        for t in re.findall(r"[a-z][a-z0-9_]+", q)
        if len(t) >= 3
        and t
        not in {
            "top",
            "bottom",
            "highest",
            "lowest",
            "most",
            "least",
            "show",
            "list",
            "give",
            "with",
            "from",
            "into",
            "over",
            "trend",
            "time",
            "year",
            "month",
            "week",
            "day",
            "quarter",
            "total",
            "average",
            "count",
            "number",
            "records",
            "values",
        }
    }
    if not query_terms:
        return None

    best: tuple[int, str] | None = None
    for kw, col in entity_map:
        k = (kw or "").lower().replace("_", " ").strip()
        c = (col or "").lower().replace("_", " ").strip()
        tokens = {
            t
            for t in re.findall(r"[a-z][a-z0-9]+", f"{k} {c}")
            if len(t) >= 3 and t not in {"name", "type", "status", "code", "id"}
        }
        if not tokens:
            continue

        overlap = len(tokens & query_terms)
        if overlap <= 0:
            continue

        score = overlap * 5
        if k and k in q:
            score += 4
        if c and c in q:
            score += 3

        if best is None or score > best[0]:
            best = (score, col)

    return best[1] if best else None


# ── Schema-aware fallback parser ──────────────────────────────────────────────
def _post_process(
    parsed: dict,
    user_query: str = "",
    mandatory_filters: dict | None = None,
) -> dict:
    ql = (user_query or "").lower()
    qt = parsed.get("query_type", "top_n")
    tr = parsed.get("time_ranges", [])
    m = parsed.get("metric")

    def _period_key(period: dict | None) -> tuple[str, str]:
        period = period or {}
        return (str(period.get("start") or ""), str(period.get("end") or ""))

    count_cues = (
        "how many",
        "number of",
        "count",
        "orders",
        "records",
        "transactions",
        "number of records",
    )
    avg_cues = ("average", "avg ", "aov", "average order value")
    aggregate_cues = (
        "total",
        "overall",
        "sum",
        "average",
        "avg",
        "how many",
        "number of",
    )
    growth_cues = ("growth", "grew", "increase", "decrease", "delta", "change")
    split_cue = any(x in ql for x in (" vs ", " versus ", " compared to ", " against "))
    grouping_cue = bool(re.search(r"\b(per|by|for each|each)\b", ql))

    schema_maps: tuple[list[tuple[str, str]], list[tuple[str, str]] | None] = None  # type: ignore[assignment]

    def _get_schema_maps() -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        nonlocal schema_maps
        if schema_maps is None:
            schema_maps = _build_schema_keyword_maps()
        return schema_maps  # type: ignore[return-value]

    if _is_aov_intent(user_query):
        parsed["metric"] = "aov"
        parsed["semantic_metric"] = "aov"
        m = "aov"
    elif any(c in ql for c in count_cues):
        parsed["metric"] = "count"
        m = "count"
    elif (
        m
        and isinstance(m, str)
        and not m.startswith("avg_")
        and any(c in ql for c in avg_cues)
    ):
        if m != "count":
            parsed["metric"] = f"avg_{m}"
            m = parsed["metric"]

    ranking_cue = _has_ranking_cue(user_query)
    rank_within_time_bucket = _infer_rank_within_time_bucket(user_query)

    # For plain period-vs-period comparisons without explicit grouping language,
    # keep comparison aggregate (entity=None) even if an entity-like token appears.
    if (
        qt == "comparison"
        and not grouping_cue
        and not ranking_cue
        and not rank_within_time_bucket
    ):
        parsed["entity"] = None

    if not parsed.get("entity") and grouping_cue:
        entity_map_for_grouping, _ = _get_schema_maps()
        inferred_entity = _infer_entity_from_grouping_cue(
            user_query, entity_map_for_grouping
        )
        if inferred_entity:
            parsed["entity"] = inferred_entity
        if not parsed.get("entity"):
            inferred_low_card_entity = _find_split_dimension_for_query(
                user_query,
                require_split_cue=False,
            )
            if inferred_low_card_entity:
                parsed["entity"] = inferred_low_card_entity

    if (
        rank_within_time_bucket
        and parsed.get("entity")
        and parsed.get("query_type") in ("top_n", "bottom_n")
    ):
        parsed["_rank_within_time"] = True
        parsed["time_bucket"] = rank_within_time_bucket

    # Guardrail: keep only scalar filters to avoid invalid SQL predicates from
    # nested objects accidentally emitted by the model.
    parsed["filters"] = _sanitize_scalar_filters(parsed.get("filters") or {})

    if split_cue and any(c in ql for c in count_cues):
        split_entity = _find_split_dimension_for_query(user_query)
        if split_entity:
            parsed["query_type"] = "top_n"
            parsed["entity"] = split_entity
            parsed["metric"] = "count"
            parsed["semantic_metric"] = "count"
            parsed["top_n"] = max(int(parsed.get("top_n") or 5), 20)
            # Split comparisons should return all groups, not an arbitrary top-N slice.
            parsed["_disable_limit"] = True
            current_filters = dict(parsed.get("filters") or {})
            current_filters.pop(split_entity, None)
            parsed["filters"] = current_filters
            qt = "top_n"
            m = "count"

            if _is_trend_query(user_query):
                split_label = split_entity.replace("is_", "").replace("_", " ")
                parsed["is_complete"] = False
                parsed["_force_clarification"] = True
                parsed["clarification_question"] = (
                    f"Do you want a time-bucket trend for one {split_label} value, "
                    f"or an overall split by {split_label}?"
                )

    inferred_flag_filters = _infer_boolean_flag_filters(
        user_query,
        split_cue,
        parsed.get("entity"),
    )
    if inferred_flag_filters:
        current_filters = dict(parsed.get("filters") or {})
        for fk, fv in inferred_flag_filters.items():
            current_filters[fk] = fv
        parsed["filters"] = current_filters

    if (
        parsed.get("metric") == "aov"
        and qt in ("top_n", "bottom_n")
        and not ranking_cue
        and not parsed.get("entity")
    ):
        parsed["query_type"] = "aggregate"
        parsed["entity"] = None
        qt = "aggregate"

    if any(c in ql for c in growth_cues) and not _is_trend_query(user_query):
        entity_growth_phrase = bool(parsed.get("entity")) or " by " in f" {ql} "
        if entity_growth_phrase and qt != "intersection":
            parsed["query_type"] = "growth_ranking"
            qt = "growth_ranking"

    if (
        qt in ("top_n", "bottom_n")
        and not ranking_cue
        and not split_cue
        and any(c in ql for c in aggregate_cues)
        and not (grouping_cue and parsed.get("entity"))
    ):
        parsed["query_type"] = "aggregate"
        parsed["entity"] = None
        qt = "aggregate"

    if (
        qt == "aggregate"
        and grouping_cue
        and parsed.get("entity")
        and any(c in ql for c in aggregate_cues)
    ):
        parsed["query_type"] = "top_n"
        parsed["_disable_limit"] = True
        qt = "top_n"

    if (
        qt in ("top_n", "bottom_n")
        and grouping_cue
        and parsed.get("entity")
        and not ranking_cue
    ):
        parsed["query_type"] = "top_n"
        parsed["_disable_limit"] = True
        qt = "top_n"

    if qt in ("top_n", "bottom_n") and not parsed.get("entity"):
        entity_map, _ = _get_schema_maps()
        inferred_entity = _infer_entity_from_grouping_cue(user_query, entity_map)
        if inferred_entity:
            parsed["entity"] = inferred_entity
        if not parsed.get("entity"):
            inferred_entity = _infer_entity_from_query_terms(user_query, entity_map)
            if inferred_entity:
                parsed["entity"] = inferred_entity

    if _is_forecast_query(user_query) and not ranking_cue:
        parsed["query_type"] = "forecast"
        parsed["entity"] = None
        parsed["time_bucket"] = _detect_forecast_bucket(user_query)
        parsed["forecast_periods"] = _extract_forecast_periods(
            user_query, parsed["time_bucket"]
        )
        if not parsed.get("forecast_method"):
            parsed["forecast_method"] = "auto"
        qt = "forecast"

    if (
        _is_trend_query(user_query)
        and not ranking_cue
        and qt not in ("time_series", "forecast")
        and not (split_cue and parsed.get("entity"))
    ):
        parsed["query_type"] = "time_series"
        parsed["entity"] = None
        qt = "time_series"
        if not parsed.get("time_bucket"):
            parsed["time_bucket"] = _detect_time_bucket(user_query)

    if qt == "time_series" and ranking_cue:
        mtop = _TOPN_RE.search(user_query)
        parsed["query_type"] = "top_n"
        if mtop:
            parsed["query_type"] = (
                "bottom_n" if mtop.group(1).lower() == "bottom" else "top_n"
            )
            parsed["top_n"] = int(mtop.group(2))
        elif any(
            x in user_query.lower() for x in ["lowest", "least", "bottom", "worst"]
        ):
            parsed["query_type"] = "bottom_n"
        parsed["time_bucket"] = None
        qt = parsed["query_type"]

    if qt == "time_series" and not parsed.get("time_bucket"):
        parsed["time_bucket"] = _detect_time_bucket(user_query)

    if qt == "forecast":
        parsed["entity"] = None
        if not parsed.get("time_bucket"):
            parsed["time_bucket"] = _detect_forecast_bucket(user_query)
        if not parsed.get("forecast_periods"):
            parsed["forecast_periods"] = _extract_forecast_periods(
                user_query, parsed.get("time_bucket", "month")
            )
        if not parsed.get("forecast_method"):
            parsed["forecast_method"] = "auto"

    if not tr and user_query:
        extracted, suggested_qt = parse_time_ranges_from_query(user_query)
        if extracted:
            parsed["time_ranges"] = extracted
            tr = extracted
            if (
                suggested_qt == "comparison"
                and parsed.get("query_type")
                not in ("comparison", "growth_ranking", "intersection")
                and not (
                    split_cue and parsed.get("entity") and parsed.get("_disable_limit")
                )
            ):
                parsed["query_type"] = "comparison"
                qt = "comparison"
        elif qt == "time_series" and _looks_like_all_time_trend(user_query):
            inferred = _infer_dataset_time_range()
            if inferred:
                parsed["time_ranges"] = inferred
                tr = inferred

    if not tr and qt not in ("comparison", "intersection"):
        inferred = _infer_dataset_time_range()
        if inferred:
            parsed["time_ranges"] = inferred
            tr = inferred

    # Guard: swap ID columns → name columns
    entity = parsed.get("entity", "") or ""
    if entity.endswith("_id"):
        parsed["entity"] = entity[:-3] + "_name"

    # Resolve semantic aliases to concrete schema columns.
    parsed = _resolve_semantic_context(parsed, user_query)

    if (parsed.get("metric") or "").lower() == "aov":
        _, metric_map = _get_schema_maps()
        parsed.setdefault(
            "_aov_revenue_col", _select_primary_revenue_column(metric_map)
        )
        parsed.setdefault("_count_distinct_key", _select_primary_count_key())
        parsed["semantic_metric"] = "aov"

    # Revenue safety: if user asked for revenue-like metrics, prefer the
    # strongest net/final monetary column from live schema over list/base price.
    if _is_revenue_intent(user_query):
        _, metric_map = _get_schema_maps()
        preferred_revenue = _select_primary_revenue_column(metric_map)
        if preferred_revenue:
            current_metric = (parsed.get("metric") or "").lower().strip()
            avg_mode = bool(current_metric.startswith("avg_"))
            current_base = current_metric[4:] if avg_mode else current_metric
            preferred_metric = (
                f"avg_{preferred_revenue}" if avg_mode else preferred_revenue
            )
            if (
                not current_metric
                or current_metric == "count"
                or _revenue_col_score(current_base)
                < _revenue_col_score(preferred_revenue)
            ):
                parsed["metric"] = preferred_metric
            parsed["semantic_metric"] = "avg_revenue" if avg_mode else "revenue"

    # Inject mandatory business filters
    if mandatory_filters:
        existing = dict(parsed.get("filters") or {})
        for k, v in mandatory_filters.items():
            if _should_skip_mandatory_filter(k, user_query, parsed.get("entity")):
                continue
            existing.setdefault(k, v)
        parsed["filters"] = existing

    # Late guard: if ranking-style query still has no entity, infer one from
    # user phrasing + live schema column names.
    if parsed.get("query_type") in (
        "top_n",
        "bottom_n",
        "threshold",
        "growth_ranking",
        "zero_filter",
    ) and not parsed.get("entity"):
        entity_map, _ = _get_schema_maps()
        inferred_entity = _infer_entity_from_grouping_cue(user_query, entity_map)
        if not inferred_entity:
            inferred_entity = _infer_entity_from_query_terms(user_query, entity_map)
        if inferred_entity:
            parsed["entity"] = inferred_entity

    if (
        rank_within_time_bucket
        and parsed.get("entity")
        and parsed.get("query_type") in ("top_n", "bottom_n")
    ):
        parsed["_rank_within_time"] = True
        parsed["time_bucket"] = rank_within_time_bucket

    # Refresh derived fields in case semantic resolution or previous guards updated intent.
    qt = parsed.get("query_type", qt)
    tr = parsed.get("time_ranges", tr)
    m = parsed.get("metric", m)

    if (
        qt == "comparison"
        and not grouping_cue
        and not ranking_cue
        and not parsed.get("_rank_within_time")
    ):
        parsed["entity"] = None

    if parsed.get("_force_clarification"):
        parsed["is_complete"] = False
        parsed.setdefault(
            "clarification_question",
            "Please clarify your request so I can avoid an incorrect comparison.",
        )
        return parsed

    # Completeness guards
    if (parsed.get("metric") or "").lower() == "aov":
        if not parsed.get("_aov_revenue_col") or not parsed.get("_count_distinct_key"):
            parsed["is_complete"] = False
            parsed["clarification_question"] = (
                "I need one revenue column and one order identifier column to compute AOV. "
                "Which should I use?"
            )
            return parsed

    if qt in ("aggregate", "time_series", "forecast") and m and tr:
        parsed["is_complete"] = True
        parsed["clarification_question"] = None
    if qt in ("comparison", "intersection"):
        if m and len(tr) >= 2 and _period_key(tr[0]) != _period_key(tr[1]):
            parsed["is_complete"] = True
            parsed["clarification_question"] = None
        else:
            parsed["is_complete"] = False
            parsed["clarification_question"] = (
                "Please provide two different time periods to compare "
                "(for example: 2022 vs 2023)."
            )
    if qt in ("top_n", "bottom_n", "threshold", "zero_filter"):
        if parsed.get("entity") and tr:
            parsed["is_complete"] = True
            parsed["clarification_question"] = None
    if qt == "growth_ranking":
        if not _has_metric_cue(user_query):
            parsed["is_complete"] = False
            parsed["clarification_question"] = (
                "Which metric should I use for growth ranking "
                "(e.g. revenue, quantity, record count)?"
            )
        elif (
            parsed.get("entity")
            and len(tr) >= 2
            and _period_key(tr[0]) != _period_key(tr[1])
        ):
            parsed["is_complete"] = True
            parsed["clarification_question"] = None
        else:
            parsed["is_complete"] = False
            parsed["clarification_question"] = (
                "Please provide an entity and two different time periods for growth ranking "
                "(for example: product growth in 2022 vs 2023)."
            )

    return parsed


# ── Qwen API call ─────────────────────────────────────────────────────────────


def _call_parse_model(user_query: str, schema: str, model: str) -> tuple[dict, dict]:
    system = _SYSTEM_TEMPLATE.format(schema=schema)
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_query},
    ]
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": calc_max_tokens(messages, task="parse_intent", model=model),
        "temperature": 0.0,
    }

    if QWEN_ENABLE_REASONING and "qwen" in model.lower() and QWEN_REASONING_EFFORT:
        payload["reasoning_effort"] = QWEN_REASONING_EFFORT

    if PARSE_INTENT_USE_INSTRUCTOR:
        try:
            return parse_intent_with_instructor(
                api_url=GROQ_URL,
                api_token=GROQ_API_TOKEN,
                model=model,
                system_prompt=system,
                user_query=user_query,
                max_tokens=payload["max_tokens"],
                enable_reasoning=QWEN_ENABLE_REASONING,
                reasoning_effort=QWEN_REASONING_EFFORT,
            )
        except Exception:
            # Fall back to legacy JSON parsing path for compatibility.
            pass

    data = post_chat_completion(
        api_url=GROQ_URL,
        api_token=GROQ_API_TOKEN,
        payload=payload,
        timeout=30,
    )
    raw = clean_model_text(data["choices"][0]["message"]["content"], strip_fences=True)
    try:
        return json.loads(raw), data.get("usage", {})
    except Exception:
        obj = _extract_json_object(raw)
        if not obj:
            raise
        return json.loads(obj), data.get("usage", {})


def _call_parse_with_retry_and_fallback(
    user_query: str,
    schema: str,
) -> tuple[dict, dict, str]:
    attempts = max(1, int(PARSE_INTENT_MAX_RETRIES or 1))
    last_exc: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            parsed, usage = _call_parse_model(user_query, schema, QWEN_MODEL)
            return parsed, usage, QWEN_MODEL
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            wait_sec = min((1.0 if is_rate_limit_error(exc) else 0.5) * attempt, 3.0)
            time.sleep(wait_sec)

    fallback_model = (LLAMA_MODEL or "").strip()
    if fallback_model and fallback_model != QWEN_MODEL:
        for attempt in range(1, 3):
            try:
                parsed, usage = _call_parse_model(user_query, schema, fallback_model)
                return parsed, usage, fallback_model
            except Exception as exc:
                last_exc = exc
                if attempt < 2:
                    wait_sec = 1.5 if is_rate_limit_error(exc) else 0.8
                    time.sleep(wait_sec)

    if last_exc:
        raise last_exc
    raise RuntimeError("Parse intent failed with unknown error")


def _period_key(period: dict | None) -> tuple[str, str]:
    period = period or {}
    return (str(period.get("start") or ""), str(period.get("end") or ""))


def _check_clarity(parsed: dict) -> tuple[bool, str | None]:
    """Inline completeness/clarity gate merged into parse step."""
    qt = parsed.get("query_type", "top_n")
    tr = parsed.get("time_ranges", [])
    m = parsed.get("metric")
    ent = parsed.get("entity")
    cq = parsed.get("clarification_question")

    if parsed.get("_force_clarification") and cq:
        return False, str(cq)

    if (m or "").lower() == "aov":
        if not parsed.get("_aov_revenue_col") or not parsed.get("_count_distinct_key"):
            return False, (
                cq
                or "I need one revenue column and one order identifier column to compute AOV. Which should I use?"
            )

    if qt in ("aggregate", "time_series", "forecast"):
        if m and tr:
            return True, None
        return False, cq or _default_clarification(parsed)

    if qt in ("top_n", "bottom_n", "threshold", "zero_filter"):
        if ent and tr:
            return True, None
        return False, cq or _default_clarification(parsed)

    if qt in ("comparison", "intersection", "growth_ranking"):
        if not tr or len(tr) < 2:
            return False, (
                cq or "Please specify two time periods to compare (e.g. 2023 vs 2024)."
            )
        if _period_key(tr[0]) == _period_key(tr[1]):
            return False, (
                cq
                or "Please specify two different time periods to compare (e.g. 2023 vs 2024)."
            )
        if qt == "growth_ranking" and not ent:
            return False, cq or _default_clarification(parsed)
        return True, None

    if not parsed.get("is_complete", True):
        return False, cq or _default_clarification(parsed)
    return True, None


# ── Handler ───────────────────────────────────────────────────────────────────


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    from services.parse_intent_service import run_parse_intent
    from types import SimpleNamespace

    step_module = sys.modules.get(__name__) or SimpleNamespace(**globals())
    await run_parse_intent(step_module, input_data, ctx)
