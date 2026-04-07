"""Auto-discovery service for surprising findings."""

from __future__ import annotations

import hashlib
import statistics
import time
from datetime import datetime, timezone
from typing import Any

from db.duckdb_connection import get_read_connection


_CACHE_SCOPE = "auto_discovery"
_CACHE_KEY = "latest"
_CACHE_TTL_SECS = 30 * 60

_MONTH_NAME = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


def _is_numeric(dtype: str) -> bool:
    d = (dtype or "").upper()
    return any(
        t in d
        for t in ("INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL")
    )


def _is_text(dtype: str) -> bool:
    d = (dtype or "").upper()
    return any(t in d for t in ("VARCHAR", "CHAR", "TEXT", "STRING"))


def _is_date_type(dtype: str) -> bool:
    d = (dtype or "").upper()
    return "DATE" in d or "TIMESTAMP" in d


def _looks_date_col(col: str) -> bool:
    n = (col or "").lower()
    return any(
        k in n for k in ("date", "time", "timestamp", "created", "updated", "at", "on")
    )


def _metric_score(col: str) -> int:
    c = (col or "").lower()
    s = 0
    if any(
        k in c
        for k in (
            "revenue",
            "sales",
            "earning",
            "amount",
            "fare",
            "price",
            "total",
            "profit",
        )
    ):
        s += 12
    if any(
        k in c
        for k in ("qty", "quantity", "units", "count", "volume", "distance", "duration")
    ):
        s += 8
    if c.endswith("_id") or c == "id":
        s -= 10
    return s


def _dim_score(col: str) -> int:
    c = (col or "").lower()
    s = 0
    if c == "name" or c.endswith("_name"):
        s += 8
    if any(
        k in c
        for k in (
            "city",
            "state",
            "region",
            "store",
            "branch",
            "category",
            "platform",
            "type",
        )
    ):
        s += 7
    if any(k in c for k in ("status", "comment", "description", "note")):
        s -= 4
    return s


def _schema_fingerprint(conn) -> str:
    rows = conn.execute(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='main'
          AND table_name NOT LIKE '_raw_%'
        ORDER BY table_name, ordinal_position
        """
    ).fetchall()
    if not rows:
        return "empty"
    text = "\n".join(f"{t}|{c}|{d}" for t, c, d in rows)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _detect_epoch_date_expr(conn, table: str, col: str) -> str:
    try:
        r = conn.execute(
            f'SELECT MAX("{col}") FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT 1'
        ).fetchone()
        if r and r[0] is not None:
            mv = int(r[0])
            if mv > 1_500_000_000_000:
                return f'CAST(epoch_ms("{col}") AS DATE)'
            if mv > 1_000_000_000:
                return f'CAST(to_timestamp("{col}") AS DATE)'
    except Exception:
        pass
    return f'CAST(epoch_ms("{col}") AS DATE)'


def _pick_date_expr(conn, table: str, cols: list[tuple[str, str]]) -> str | None:
    for c, t in cols:
        if _is_date_type(t):
            return f'CAST("{c}" AS DATE)'
    for c, t in cols:
        if _looks_date_col(c) and any(
            x in t for x in ("BIGINT", "INT8", "LONG", "HUGEINT", "INT64")
        ):
            return _detect_epoch_date_expr(conn, table, c)
    for c, _t in cols:
        if _looks_date_col(c):
            return f'CAST("{c}" AS DATE)'
    return None


def _pick_flag_column(cols: list[tuple[str, str]]) -> tuple[str, str] | None:
    col_set = {c.lower(): t for c, t in cols}
    for cand in ("is_cancelled", "cancelled", "is_refunded", "refunded", "is_void"):
        if cand in col_set:
            return cand, "binary"
    for cand in ("status", "order_status", "ride_status", "payment_status", "state"):
        if cand in col_set:
            return cand, "status"
    return None


def _safe_ratio(a: float, b: float) -> float | None:
    if b <= 0:
        return None
    return a / b


def _fmt(v: float) -> str:
    if abs(v) >= 1000:
        if abs(v - round(v)) < 0.01:
            return f"{int(round(v)):,}"
        return f"{v:,.2f}"
    if abs(v - round(v)) < 0.01:
        return str(int(round(v)))
    return f"{v:.2f}"


def _query_param(req: Any, key: str, default: str = "") -> str:
    for attr in ("query", "query_params", "params"):
        obj = getattr(req, attr, None)
        if isinstance(obj, dict) and key in obj:
            return str(obj.get(key) or default)
    return default


def _scan_table_findings(
    conn, table: str, cols: list[tuple[str, str]]
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    metric_cols = [
        c
        for c, t in cols
        if _is_numeric(t) and not c.lower().endswith("_id") and not _looks_date_col(c)
    ]
    metric_cols = sorted(
        metric_cols, key=lambda c: (_metric_score(c), -len(c)), reverse=True
    )[:5]
    if not metric_cols:
        return findings

    cat_candidates = [
        c for c, t in cols if _is_text(t) and not c.lower().endswith("_id")
    ]
    cat_candidates = sorted(
        cat_candidates, key=lambda c: (_dim_score(c), -len(c)), reverse=True
    )[:8]
    cat_cols: list[str] = []
    for col in cat_candidates:
        try:
            distinct_cnt = int(
                conn.execute(
                    f'SELECT COUNT(DISTINCT "{col}") FROM "{table}"'
                ).fetchone()[0]
                or 0
            )
        except Exception:
            continue
        if 2 <= distinct_cnt <= 25:
            cat_cols.append(col)

    combo_budget = 0
    for dim in cat_cols:
        for metric in metric_cols:
            if combo_budget >= 40:
                break
            combo_budget += 1
            try:
                rows = conn.execute(
                    (
                        f'SELECT "{dim}" AS dim, COUNT(*) AS n, AVG("{metric}") AS avg_v '
                        f'FROM "{table}" '
                        f'WHERE "{dim}" IS NOT NULL AND "{metric}" IS NOT NULL '
                        f"GROUP BY 1 HAVING COUNT(*) >= 12 "
                        f"ORDER BY avg_v DESC"
                    )
                ).fetchall()
            except Exception:
                continue
            if len(rows) < 2:
                continue

            top = rows[0]
            bottom = rows[-1]
            avg_vals = [float(r[2] or 0.0) for r in rows]
            median_avg = statistics.median(avg_vals) if avg_vals else 0.0
            ratio = _safe_ratio(
                float(top[2] or 0.0), max(0.0001, float(bottom[2] or 0.0))
            )
            if (
                ratio
                and ratio >= 1.8
                and float(top[2] or 0.0) > 0
                and int(top[1] or 0) >= 12
                and int(bottom[1] or 0) >= 12
            ):
                findings.append(
                    {
                        "score": min(
                            100.0, (ratio - 1.0) * 24.0 + min(15.0, len(rows))
                        ),
                        "type": "dimension_gap",
                        "table": table,
                        "dimension": dim,
                        "metric": metric,
                        "text": (
                            f"{top[0]} has {_fmt(float(top[2]))} average {metric.replace('_', ' ')} "
                            f"vs {_fmt(float(bottom[2]))} for {bottom[0]} "
                            f"({ratio:.1f}x gap) in {table}."
                        ),
                    }
                )
            elif median_avg > 0 and float(top[2] or 0.0) / median_avg >= 1.8:
                spike_ratio = float(top[2] or 0.0) / median_avg
                findings.append(
                    {
                        "score": min(95.0, (spike_ratio - 1.0) * 20.0),
                        "type": "outlier_group",
                        "table": table,
                        "dimension": dim,
                        "metric": metric,
                        "text": (
                            f"{top[0]} is a strong outlier for {metric.replace('_', ' ')} "
                            f"({_fmt(float(top[2]))}, {spike_ratio:.1f}x above group median) in {table}."
                        ),
                    }
                )

    flag = _pick_flag_column(cols)
    if flag and cat_cols:
        flag_col, flag_kind = flag
        for dim in cat_cols[:4]:
            try:
                if flag_kind == "binary":
                    q = (
                        f'SELECT "{dim}" AS dim, COUNT(*) AS n, AVG(CASE WHEN "{flag_col}" = 1 THEN 1.0 ELSE 0.0 END) AS rate '
                        f'FROM "{table}" WHERE "{dim}" IS NOT NULL GROUP BY 1 HAVING COUNT(*) >= 20 ORDER BY rate DESC'
                    )
                else:
                    q = (
                        f'SELECT "{dim}" AS dim, COUNT(*) AS n, AVG(CASE WHEN LOWER(CAST("{flag_col}" AS VARCHAR)) IN '
                        "('cancelled','canceled','refunded','void','failed') THEN 1.0 ELSE 0.0 END) AS rate "
                        f'FROM "{table}" WHERE "{dim}" IS NOT NULL GROUP BY 1 HAVING COUNT(*) >= 20 ORDER BY rate DESC'
                    )
                rows = conn.execute(q).fetchall()
            except Exception:
                continue
            if len(rows) < 2:
                continue
            top = rows[0]
            low = rows[-1]
            r_top = float(top[2] or 0.0)
            r_low = float(low[2] or 0.0)
            rate_ratio = _safe_ratio(r_top, max(0.0001, r_low))
            if rate_ratio and r_top >= 0.05 and rate_ratio >= 1.5:
                label = "cancellation/refund"
                findings.append(
                    {
                        "score": min(98.0, (rate_ratio - 1.0) * 26.0 + r_top * 40.0),
                        "type": "rate_gap",
                        "table": table,
                        "dimension": dim,
                        "metric": flag_col,
                        "text": (
                            f"{top[0]} shows {rate_ratio:.1f}x higher {label} rate than {low[0]} "
                            f"in {table} ({r_top * 100:.1f}% vs {r_low * 100:.1f}%)."
                        ),
                    }
                )

    date_expr = _pick_date_expr(conn, table, cols)
    if date_expr:
        for metric in metric_cols[:3]:
            try:
                month_rows = conn.execute(
                    (
                        f'SELECT EXTRACT(MONTH FROM {date_expr}) AS m, AVG("{metric}") AS avg_v '
                        f'FROM "{table}" '
                        f'WHERE "{metric}" IS NOT NULL AND {date_expr} IS NOT NULL '
                        f"GROUP BY 1 ORDER BY 1"
                    )
                ).fetchall()
            except Exception:
                continue
            if len(month_rows) < 4:
                continue
            month_vals = {
                int(r[0]): float(r[1] or 0.0)
                for r in month_rows
                if r and r[0] is not None
            }
            if not month_vals:
                continue
            peak_month, peak_val = max(month_vals.items(), key=lambda x: x[1])
            base = statistics.median(list(month_vals.values()))
            if base > 0 and peak_val / base >= 1.7:
                findings.append(
                    {
                        "score": min(92.0, (peak_val / base - 1.0) * 24.0),
                        "type": "seasonality_spike",
                        "table": table,
                        "dimension": "month",
                        "metric": metric,
                        "text": (
                            f"{metric.replace('_', ' ').title()} spikes in {_MONTH_NAME.get(peak_month, str(peak_month))} "
                            f"({_fmt(peak_val)}, {peak_val / base:.1f}x above median month) in {table}."
                        ),
                    }
                )

    dist_cols = [
        c
        for c in metric_cols
        if any(k in c.lower() for k in ("distance", "km", "mile"))
    ]
    value_cols = [
        c
        for c in metric_cols
        if any(
            k in c.lower()
            for k in ("revenue", "sales", "earning", "fare", "amount", "total")
        )
    ]
    if dist_cols and value_cols and cat_cols:
        dcol = dist_cols[0]
        vcol = value_cols[0]
        for dim in cat_cols[:3]:
            try:
                rows = conn.execute(
                    (
                        f'SELECT "{dim}" AS dim, COUNT(*) AS n, AVG("{vcol}" / NULLIF("{dcol}", 0)) AS unit_value '
                        f'FROM "{table}" '
                        f'WHERE "{dim}" IS NOT NULL AND "{vcol}" IS NOT NULL AND "{dcol}" > 0 '
                        f"GROUP BY 1 HAVING COUNT(*) >= 15 "
                        f"ORDER BY unit_value DESC"
                    )
                ).fetchall()
            except Exception:
                continue
            if len(rows) < 2:
                continue
            top = rows[0]
            low = rows[-1]
            if top[2] is None or low[2] is None:
                continue
            ratio = _safe_ratio(float(top[2]), max(0.0001, float(low[2])))
            if ratio and ratio >= 1.8:
                findings.append(
                    {
                        "score": min(96.0, (ratio - 1.0) * 23.0),
                        "type": "per_unit_gap",
                        "table": table,
                        "dimension": dim,
                        "metric": f"{vcol}/{dcol}",
                        "text": (
                            f"{top[0]} earns {ratio:.1f}x more {vcol.replace('_', ' ')} per {dcol.replace('_', ' ')} "
                            f"than {low[0]} in {table}."
                        ),
                    }
                )

    return findings


def _discover_findings() -> list[dict[str, Any]]:
    conn = get_read_connection()
    try:
        tables = [
            r[0]
            for r in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='main'
                  AND table_name NOT LIKE '_raw_%'
                ORDER BY table_name
                """
            ).fetchall()
        ]
        findings: list[dict[str, Any]] = []
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
            findings.extend(_scan_table_findings(conn, table, cols))

        findings.sort(key=lambda f: float(f.get("score") or 0.0), reverse=True)
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for f in findings:
            txt = str(f.get("text") or "").strip()
            if not txt or txt in seen:
                continue
            seen.add(txt)
            deduped.append(f)
            if len(deduped) >= 5:
                break
        return deduped
    finally:
        conn.close()


async def get_discovery_response(request: Any, ctx: Any) -> tuple[int, dict[str, Any]]:
    refresh = _query_param(request, "refresh", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }

    conn = get_read_connection()
    try:
        schema_fp = _schema_fingerprint(conn)
    finally:
        conn.close()

    if not refresh:
        cached = await ctx.state.get(_CACHE_SCOPE, _CACHE_KEY)
        if cached:
            age = time.time() - float(cached.get("generated_epoch") or 0.0)
            if (
                age <= _CACHE_TTL_SECS
                and str(cached.get("schema_fp") or "") == schema_fp
            ):
                return 200, {
                    **cached,
                    "cached": True,
                    "age_seconds": int(age),
                }

    findings = _discover_findings()
    now = datetime.now(timezone.utc)
    payload = {
        "generated_at": now.isoformat(),
        "generated_epoch": time.time(),
        "schema_fp": schema_fp,
        "cached": False,
        "findings": findings,
        "highlights": [f.get("text", "") for f in findings],
        "count": len(findings),
    }
    await ctx.state.set(_CACHE_SCOPE, _CACHE_KEY, payload)
    return 200, payload
