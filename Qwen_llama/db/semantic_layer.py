"""Semantic layer foundation for business analytics.

Purpose:
- Abstract raw schema columns into business-friendly dimensions and metrics.
- Provide prompt-time semantic hints for LLMs.
- Resolve parsed intent terms (entity/metric) to concrete physical columns.
"""

from __future__ import annotations

import json
from typing import Any

from db.duckdb_connection import get_read_connection


_META_SEMANTIC_HINTS = "_raw_meta_semantic_hints"


_DIMENSION_HINTS = (
    "name",
    "title",
    "label",
    "category",
    "city",
    "state",
    "region",
    "segment",
    "brand",
    "warehouse",
    "store",
    "location",
    "driver",
    "customer",
    "product",
    "item",
    "vendor",
    "channel",
    "department",
    "team",
)

_METRIC_HINTS = (
    "revenue",
    "sales",
    "amount",
    "total",
    "fare",
    "earning",
    "price",
    "cost",
    "quantity",
    "qty",
    "units",
    "volume",
    "discount",
    "commission",
    "distance",
    "duration",
    "score",
    "rate",
    "profit",
    "margin",
)

_REVENUE_COL_HINTS = (
    "revenue",
    "sales",
    "amount",
    "earning",
    "total",
    "final",
    "net",
    "paid",
)

_REVENUE_COL_PENALTIES = (
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

_COUNT_ID_STRONG_HINTS = (
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

_COUNT_ID_WEAK_HINTS = (
    "row",
    "line",
    "item",
    "detail",
    "record",
    "event",
    "log",
)


def _is_text(dtype: str) -> bool:
    d = dtype.upper()
    return any(t in d for t in ("VARCHAR", "CHAR", "TEXT", "STRING"))


def _is_numeric(dtype: str) -> bool:
    d = dtype.upper()
    return any(
        t in d
        for t in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
    )


def _is_date_like(name: str, dtype: str) -> bool:
    n = name.lower()
    d = dtype.upper()
    return any(
        k in n for k in ("date", "time", "timestamp", "created", "updated")
    ) or any(t in d for t in ("DATE", "TIMESTAMP"))


def _normalize_token(text: str) -> str:
    t = (text or "").lower().strip().replace("_", " ")
    t = " ".join(t.split())
    if t.endswith("s") and len(t) > 3:
        t = t[:-1]
    return t


def _semantic_name_from_col(col: str) -> str:
    c = col.lower()
    for suffix in ("_name", "_title", "_label", "_id", "_code", "_key", "_uuid"):
        if c.endswith(suffix):
            return c[: -len(suffix)]
    return c


def _collect_columns(conn, tables: list[str]) -> dict[str, list[tuple[str, str]]]:
    out: dict[str, list[tuple[str, str]]] = {}
    for table in tables:
        try:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
            out[table] = cols
        except Exception:
            continue
    return out


def _merge_alias_lists(existing: list[str], extra: list[str]) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for a in existing + extra:
        s = str(a or "").strip().lower()
        if not s or s in seen:
            continue
        seen.add(s)
        merged.append(s)
    return merged


def _load_llm_hints(conn, tables: list[str]) -> dict[str, dict[str, Any]]:
    if not tables:
        return {}
    try:
        placeholders = ", ".join(["?"] * len(tables))
        rows = conn.execute(
            f'SELECT table_name, hint_json FROM "{_META_SEMANTIC_HINTS}" WHERE table_name IN ({placeholders})',
            tables,
        ).fetchall()
    except Exception:
        return {}

    out: dict[str, dict[str, Any]] = {}
    for table_name, hint_json in rows:
        try:
            parsed = json.loads(str(hint_json or ""))
            if isinstance(parsed, dict):
                out[str(table_name)] = parsed
        except Exception:
            continue
    return out


def _score_revenue_column_name(col_name: str) -> int:
    c = (col_name or "").lower()
    score = 0
    if any(k in c for k in _REVENUE_COL_HINTS):
        score += 10
    if "final" in c or "net" in c or "paid" in c:
        score += 8
    if "total" in c:
        score += 6
    if "price" in c or "fare" in c:
        score += 3
    if any(k in c for k in _REVENUE_COL_PENALTIES):
        score -= 7
    return score


def _pick_primary_revenue_column(cols: list[tuple[str, str]]) -> str | None:
    numeric_cols = [
        c.lower()
        for c, dtype in cols
        if _is_numeric(dtype)
        and not c.lower().endswith("_id")
        and not _is_date_like(c, dtype)
    ]
    if not numeric_cols:
        return None
    ranked = sorted(
        numeric_cols,
        key=lambda c: (
            _score_revenue_column_name(c),
            1 if "final" in c else 0,
            1 if "total" in c else 0,
            1 if "amount" in c else 0,
            -len(c),
        ),
        reverse=True,
    )
    return ranked[0]


def _pick_count_identifier(conn, table: str, cols: list[tuple[str, str]]) -> str | None:
    id_cols = [
        c.lower() for c, _ in cols if c.lower().endswith("_id") or c.lower() == "id"
    ]
    if not id_cols:
        return None

    best: tuple[int, str] | None = None
    for col in id_cols:
        score = 0
        if any(k in col for k in _COUNT_ID_STRONG_HINTS):
            score += 10
        if any(k in col for k in _COUNT_ID_WEAK_HINTS):
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

    return best[1] if best else None


def build_semantic_catalog(conn, tables: list[str]) -> dict[str, Any]:
    tables = [t for t in tables if t and not str(t).startswith("_raw_meta_")]
    columns_by_table = _collect_columns(conn, tables)
    llm_hints = _load_llm_hints(conn, tables)
    dimensions: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    # Dimensions
    for table, cols in columns_by_table.items():
        col_set = {c.lower() for c, _ in cols}
        for col, dtype in cols:
            n = col.lower()
            if not _is_text(dtype):
                continue
            if _is_date_like(col, dtype):
                continue
            if not (n == "name" or any(h in n for h in _DIMENSION_HINTS)):
                continue

            base = _semantic_name_from_col(n)
            key_col = None
            for cand in (
                f"{base}_id",
                f"{base}_code",
                f"{base}_key",
                f"{base}_uuid",
                "id",
            ):
                if cand in col_set:
                    key_col = cand
                    break

            aliases = {
                base,
                n.replace("_", " "),
                base.replace("_", " "),
                f"{base}s".replace("_", " "),
            }
            dimensions.append(
                {
                    "name": base,
                    "table": table,
                    "label_column": n,
                    "key_column": key_col,
                    "aliases": sorted(a for a in aliases if a.strip()),
                }
            )

    for d in dimensions:
        hint = llm_hints.get(d["table"], {})
        dim_aliases = hint.get("dimension_aliases") if isinstance(hint, dict) else {}
        if not isinstance(dim_aliases, dict):
            continue
        wanted = {_normalize_token(d["name"]), _normalize_token(d["label_column"])}
        extra: list[str] = []
        for k, vals in dim_aliases.items():
            if _normalize_token(str(k)) not in wanted:
                continue
            if isinstance(vals, list):
                extra.extend([str(v) for v in vals])
        if extra:
            d["aliases"] = _merge_alias_lists(d["aliases"], extra)

    # Metrics
    seen_metric_keys: set[tuple[str, str, str]] = set()
    for table, cols in columns_by_table.items():
        col_set = {c.lower() for c, _ in cols}

        for col, dtype in cols:
            n = col.lower()
            if not _is_numeric(dtype):
                continue
            if _is_date_like(col, dtype):
                continue
            if n.endswith("_id"):
                continue

            semantic = _semantic_name_from_col(n)
            aliases = {semantic, n.replace("_", " ")}

            if any(
                k in n
                for k in (
                    "revenue",
                    "sales",
                    "amount",
                    "fare",
                    "earning",
                    "total",
                    "final",
                )
            ):
                aliases.update({"revenue", "sales", "earnings"})
            if any(k in n for k in ("quantity", "qty", "units", "volume")):
                aliases.update({"quantity", "units", "volume"})
            if "discount" in n:
                aliases.add("discount")
            if "commission" in n:
                aliases.add("commission")

            key = (table, n, "sum")
            if key not in seen_metric_keys:
                metrics.append(
                    {
                        "name": semantic,
                        "table": table,
                        "column": n,
                        "agg": "sum",
                        "aliases": sorted(a for a in aliases if a.strip()),
                    }
                )
                seen_metric_keys.add(key)

            # Average semantic metric for same physical column
            avg_key = (table, n, "avg")
            if avg_key not in seen_metric_keys:
                metrics.append(
                    {
                        "name": f"avg_{semantic}",
                        "table": table,
                        "column": n,
                        "agg": "avg",
                        "aliases": sorted(
                            {
                                f"average {semantic.replace('_', ' ')}",
                                f"avg {semantic.replace('_', ' ')}",
                            }
                        ),
                    }
                )
                seen_metric_keys.add(avg_key)

        count_id = _pick_count_identifier(conn, table, cols)
        if count_id:
            metrics.append(
                {
                    "name": f"{table}_count",
                    "table": table,
                    "column": count_id,
                    "agg": "count_distinct",
                    "aliases": [
                        "count",
                        "total count",
                        "number of",
                        "how many",
                        "records",
                        "entries",
                        "transactions",
                    ],
                }
            )

        # Derived metric: AOV (Average Order Value)
        revenue_col = _pick_primary_revenue_column(cols)
        if revenue_col and count_id:
            metrics.append(
                {
                    "name": "aov",
                    "table": table,
                    "numerator_column": revenue_col,
                    "denominator_column": count_id,
                    "agg": "ratio_sum_count_distinct",
                    "aliases": [
                        "aov",
                        "average order value",
                        "avg order value",
                        "average basket value",
                        "average transaction value",
                        "average ticket size",
                    ],
                }
            )

    for m in metrics:
        hint = llm_hints.get(m["table"], {})
        metric_aliases = hint.get("metric_aliases") if isinstance(hint, dict) else {}
        if not isinstance(metric_aliases, dict):
            continue
        metric_col = str(m.get("column") or "")
        wanted = {_normalize_token(m["name"]), _normalize_token(metric_col)}
        extra: list[str] = []
        for k, vals in metric_aliases.items():
            if _normalize_token(str(k)) not in wanted:
                continue
            if isinstance(vals, list):
                extra.extend([str(v) for v in vals])
        if extra:
            m["aliases"] = _merge_alias_lists(m["aliases"], extra)

    return {"dimensions": dimensions, "metrics": metrics}


def render_semantic_layer_lines(conn, tables: list[str]) -> list[str]:
    catalog = build_semantic_catalog(conn, tables)
    dims = catalog["dimensions"][:12]
    mets = catalog["metrics"][:16]

    lines = ["Semantic Layer", "--------------"]
    lines += [
        "Use business terms first, then map to physical columns listed below.",
        "Dimension = what to group by; Metric = what to aggregate.",
    ]

    lines += ["", "Dimensions (semantic -> physical)"]
    if dims:
        for d in dims:
            key_part = f', key="{d["key_column"]}"' if d.get("key_column") else ""
            alias_preview = ", ".join(d["aliases"][:4])
            lines.append(
                f'  {d["name"]} -> table "{d["table"]}", label="{d["label_column"]}"{key_part}; aliases: {alias_preview}'
            )
    else:
        lines += ["  (No dimensions detected)"]

    lines += ["", "Metrics (semantic -> aggregation)"]
    if mets:
        for m in mets:
            if m["agg"] == "sum":
                expr = f'SUM("{m["column"]}")'
            elif m["agg"] == "avg":
                expr = f'AVG("{m["column"]}")'
            elif m["agg"] == "ratio_sum_count_distinct":
                expr = (
                    f'SUM("{m["numerator_column"]}") / '
                    f'NULLIF(COUNT(DISTINCT "{m["denominator_column"]}"), 0)'
                )
            else:
                expr = f'COUNT(DISTINCT "{m["column"]}")'
            alias_preview = ", ".join(m["aliases"][:4])
            lines.append(
                f'  {m["name"]} -> {expr} on "{m["table"]}"; aliases: {alias_preview}'
            )
    else:
        lines += ["  (No metrics detected)"]

    lines += [
        "",
        "Semantic Resolution Rules",
        "-------------------------",
        "- Prefer semantic aliases from this section over hardcoded column assumptions.",
        "- If a label is non-unique, group by key+label when key is available.",
        "- For counts, prefer COUNT(DISTINCT order-like id) over COUNT(*).",
    ]
    return lines


def resolve_intent_with_semantic_layer(
    parsed: dict[str, Any], user_query: str = ""
) -> dict[str, Any]:
    """
    Resolve parsed entity/metric tokens using live semantic catalog.
    Keeps compatibility with existing downstream SQL generation contracts.
    """
    try:
        conn = get_read_connection()
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
        catalog = build_semantic_catalog(conn, tables)
        all_cols = {
            c[0].lower()
            for t in tables
            for c in conn.execute(f'DESCRIBE "{t}"').fetchall()
        }
    except Exception:
        return parsed
    finally:
        try:
            conn.close()
        except Exception:
            pass

    out = dict(parsed)
    entity = (out.get("entity") or "").strip().lower()
    metric = (out.get("metric") or "").strip().lower()

    # Entity resolution
    if entity and entity not in all_cols:
        target = _normalize_token(entity)
        for d in catalog["dimensions"]:
            alias_tokens = {_normalize_token(d["name"])} | {
                _normalize_token(a) for a in d["aliases"]
            }
            if target in alias_tokens:
                out["entity"] = d["label_column"]
                out["semantic_entity"] = d["name"]
                if d.get("key_column"):
                    out["_entity_group_key"] = d["key_column"]
                break

    # Metric resolution
    metric_known = (
        metric in all_cols
        or metric == "count"
        or metric == "aov"
        or (metric.startswith("avg_") and metric[4:] in all_cols)
    )
    if metric and not metric_known:
        target = _normalize_token(metric)
        candidates: list[tuple[int, dict[str, Any]]] = []
        for m in catalog["metrics"]:
            alias_tokens = {_normalize_token(m["name"])} | {
                _normalize_token(a) for a in m["aliases"]
            }
            if target not in alias_tokens:
                continue

            score = 1
            col = (m.get("column") or "").lower()
            name_norm = _normalize_token(m["name"])
            if target == name_norm:
                score += 4
            if m["agg"] == "sum":
                score += 1
            if m["agg"] == "ratio_sum_count_distinct" and (
                "aov" in target
                or "average order value" in target
                or "average basket" in target
            ):
                score += 8

            if target in {"revenue", "sales", "earning", "earnings", "income"}:
                if any(
                    k in col
                    for k in (
                        "revenue",
                        "sales",
                        "amount",
                        "fare",
                        "earning",
                        "total",
                        "final",
                    )
                ):
                    score += 5
                if "unit_price" in col or col.startswith("unit_"):
                    score -= 4

            if target in {"quantity", "units", "volume"} and any(
                k in col for k in ("quantity", "qty", "units", "volume")
            ):
                score += 4

            if target.startswith("avg") and m["agg"] == "avg":
                score += 5

            candidates.append((score, m))

        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            best = candidates[0][1]
            out["semantic_metric"] = best["name"]
            if best["agg"] == "sum":
                out["metric"] = best["column"]
            elif best["agg"] == "avg":
                out["metric"] = f"avg_{best['column']}"
            elif best["agg"] == "ratio_sum_count_distinct":
                out["metric"] = "aov"
                out["_aov_revenue_col"] = best["numerator_column"]
                out["_count_distinct_key"] = best["denominator_column"]
            else:
                out["metric"] = "count"
                out["_count_distinct_key"] = best["column"]

    return out
