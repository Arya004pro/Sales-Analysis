"""Generate weekly/monthly business digest reports from live data."""

from __future__ import annotations

import os
import sys
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

from db.duckdb_connection import get_read_connection
from utils.anomaly_utils import detect_anomalies
from utils.forecaster import forecast_auto

config = {
    "name": "BusinessReportGenerate",
    "description": (
        "Builds a weekly/monthly business summary in markdown using period deltas, "
        "top performers, anomaly scan, and one-step forecast."
    ),
    "flows": ["sales-analytics-digest"],
    "triggers": [queue("report::generate")],
    "enqueues": [],
}

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


def _looks_like_date_col(name: str) -> bool:
    n = (name or "").lower()
    return any(
        k in n for k in ("date", "time", "timestamp", "created", "updated", "at", "on")
    )


def _looks_entity_col(name: str) -> bool:
    n = (name or "").lower()
    if _looks_like_date_col(n):
        return False
    if any(k in n for k in ("status", "type", "description", "comment", "note")):
        return False
    if (
        n == "name"
        or n.endswith("_name")
        or n.endswith("_title")
        or n.endswith("_label")
    ):
        return True
    return any(
        k in n
        for k in (
            "city",
            "region",
            "state",
            "store",
            "branch",
            "driver",
            "customer",
            "category",
        )
    )


def _metric_score(name: str) -> int:
    n = (name or "").lower()
    score = 0
    if any(
        k in n
        for k in (
            "revenue",
            "sales",
            "amount",
            "total",
            "final",
            "earning",
            "fare",
            "profit",
        )
    ):
        score += 12
    if any(k in n for k in ("quantity", "qty", "units", "volume", "count")):
        score += 8
    if any(k in n for k in ("distance", "duration", "time")):
        score += 4
    if any(k in n for k in ("discount", "tax", "shipping", "refund", "commission")):
        score -= 4
    if n.endswith("_id") or n == "id":
        score -= 10
    return score


def _fmt_num(v: float | int | None) -> str:
    if v is None:
        return "n/a"
    try:
        f = float(v)
    except Exception:
        return str(v)
    if abs(f) >= 1000:
        if abs(f - round(f)) < 0.01:
            return f"{int(round(f)):,}"
        return f"{f:,.2f}"
    if abs(f - round(f)) < 0.01:
        return str(int(round(f)))
    return f"{f:.2f}"


def _pct_change(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return ((current - previous) / previous) * 100.0


def _detect_epoch_expr(conn, table: str, col: str) -> str:
    try:
        r = conn.execute(
            f'SELECT MAX("{col}") FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT 1'
        ).fetchone()
        if r and r[0] is not None:
            mv = int(r[0])
            if mv > 1_500_000_000_000:
                return f'epoch_ms("{col}")'
            if mv > 1_000_000_000:
                return f'to_timestamp("{col}")'
    except Exception:
        pass
    return f'epoch_ms("{col}")'


def _period_window(period: str, now_utc: datetime) -> dict[str, Any]:
    p = (period or "weekly").lower()
    today = now_utc.date()
    if p == "monthly":
        current_end = today.replace(day=1)
        current_start = (current_end - timedelta(days=1)).replace(day=1)
        prev_end = current_start
        prev_start = (prev_end - timedelta(days=1)).replace(day=1)
        train_start = (current_end - timedelta(days=550)).replace(day=1)
        return {
            "period": "monthly",
            "bucket": "month",
            "current_start": current_start,
            "current_end": current_end,
            "previous_start": prev_start,
            "previous_end": prev_end,
            "training_start": train_start,
            "label": current_start.strftime("%B %Y"),
            "next_label": current_end.strftime("%B %Y"),
        }

    current_end = today - timedelta(days=today.weekday())
    current_start = current_end - timedelta(days=7)
    prev_end = current_start
    prev_start = prev_end - timedelta(days=7)
    train_start = current_end - timedelta(days=7 * 18)
    wk = current_start.isocalendar().week
    yr = current_start.isocalendar().year
    nxt_wk = current_end.isocalendar().week
    nxt_yr = current_end.isocalendar().year
    return {
        "period": "weekly",
        "bucket": "week",
        "current_start": current_start,
        "current_end": current_end,
        "previous_start": prev_start,
        "previous_end": prev_end,
        "training_start": train_start,
        "label": f"Week {wk}, {yr}",
        "next_label": f"Week {nxt_wk}, {nxt_yr}",
    }


def _select_primary_table(conn) -> tuple[str | None, list[tuple[str, str]], int]:
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
    if not tables:
        return None, [], 0

    best: tuple[float, str, list[tuple[str, str]], int] | None = None
    for table in tables:
        try:
            cols = [
                (c[0], c[1].upper())
                for c in conn.execute(f'DESCRIBE "{table}"').fetchall()
            ]
            row_count = int(
                conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] or 0
            )
        except Exception:
            continue
        if not cols:
            continue

        has_date = any(_is_date_type(t) or _looks_like_date_col(c) for c, t in cols)
        has_numeric = any(
            _is_numeric(t) and not c.lower().endswith("_id") for c, t in cols
        )
        score = 0.0
        if has_date:
            score += 100.0
        if has_numeric:
            score += 60.0
        score += min(25.0, row_count / 50000.0)
        score += min(10.0, len(cols) / 8.0)
        if best is None or score > best[0]:
            best = (score, table, cols, row_count)

    if not best:
        return None, [], 0
    return best[1], best[2], best[3]


def _select_date_expr(
    conn, table: str, cols: list[tuple[str, str]]
) -> tuple[str | None, str | None]:
    # Prefer native DATE/TIMESTAMP columns.
    for col, dtype in cols:
        if _is_date_type(dtype):
            return col, f'CAST("{col}" AS DATE)'

    # Then infer from integer epoch-style date columns.
    for col, dtype in cols:
        if _looks_like_date_col(col) and any(
            t in dtype for t in ("BIGINT", "INT8", "LONG", "HUGEINT", "INT64")
        ):
            epoch_expr = _detect_epoch_expr(conn, table, col)
            return col, f"CAST({epoch_expr} AS DATE)"

    # Finally, date-like names with native conversion.
    for col, _dtype in cols:
        if _looks_like_date_col(col):
            return col, f'CAST("{col}" AS DATE)'
    return None, None


def _select_metric(cols: list[tuple[str, str]]) -> str | None:
    numeric = [
        c
        for c, t in cols
        if _is_numeric(t)
        and not c.lower().endswith("_id")
        and not _looks_like_date_col(c)
    ]
    if not numeric:
        return None
    ranked = sorted(
        numeric,
        key=lambda c: (_metric_score(c), 1 if "total" in c.lower() else 0, -len(c)),
        reverse=True,
    )
    return ranked[0]


def _select_count_key(cols: list[tuple[str, str]]) -> str | None:
    id_cols = [c for c, _ in cols if c.lower() == "id" or c.lower().endswith("_id")]
    if not id_cols:
        return None

    def _score(col: str) -> tuple[int, int]:
        c = col.lower()
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
                "sale",
                "payment",
            )
        ):
            s += 8
        if any(k in c for k in ("line", "item", "detail", "event", "log")):
            s -= 8
        if c == "id":
            s -= 2
        return s, -len(c)

    return sorted(id_cols, key=_score, reverse=True)[0]


def _select_entity_col(conn, table: str, cols: list[tuple[str, str]]) -> str | None:
    candidates = [c for c, t in cols if _is_text(t) and _looks_entity_col(c)]
    if not candidates:
        return None

    best: tuple[float, str] | None = None
    for col in candidates[:12]:
        try:
            non_null, distinct_cnt = conn.execute(
                f'SELECT COUNT("{col}"), COUNT(DISTINCT "{col}") FROM "{table}"'
            ).fetchone()
            non_null = int(non_null or 0)
            distinct_cnt = int(distinct_cnt or 0)
            if non_null == 0 or distinct_cnt <= 1:
                continue
            ratio = distinct_cnt / non_null
            score = ratio
            cl = col.lower()
            if "name" in cl:
                score += 0.35
            if distinct_cnt >= 5:
                score += 0.15
            if best is None or score > best[0]:
                best = (score, col)
        except Exception:
            continue
    return best[1] if best else None


def _mandatory_filters_sql(conn, table: str, cols: list[tuple[str, str]]) -> str:
    col_set = {c.lower() for c, _ in cols}
    rules: list[str] = []
    for col in sorted(col_set):
        tmpl = _FILTER_COLUMN_RULES.get(col)
        if tmpl:
            rules.append(tmpl.format(col=f'"{col}"'))

    for status_col in (
        "status",
        "order_status",
        "ride_status",
        "payment_status",
        "state",
    ):
        if status_col not in col_set:
            continue
        try:
            vals = [
                str(r[0]).strip().lower()
                for r in conn.execute(
                    f'SELECT DISTINCT "{status_col}" FROM "{table}" WHERE "{status_col}" IS NOT NULL LIMIT 30'
                ).fetchall()
                if r[0] is not None and str(r[0]).strip()
            ]
            unique_vals = sorted(set(vals))
            bad = [v for v in unique_vals if v in _STATUS_BAD_VALUES]
            good = [v for v in unique_vals if v not in _STATUS_BAD_VALUES]
            if bad and good:
                safe_good = ", ".join(_sql_quote(g) for g in good[:8])
                rules.append(f'LOWER("{status_col}") IN ({safe_good})')
        except Exception:
            continue

    if not rules:
        return ""
    return " AND ".join(rules)


def _sql_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _scalar_value(conn, sql: str, params: list[Any]) -> float:
    try:
        row = conn.execute(sql, params).fetchone()
        if not row or row[0] is None:
            return 0.0
        return float(row[0])
    except Exception:
        return 0.0


def _build_markdown(
    *,
    window: dict[str, Any],
    metric_title: str,
    current_total: float,
    previous_total: float,
    top_name: str | None,
    top_value: float | None,
    anomaly_line: str,
    forecast_line: str,
) -> str:
    pct = _pct_change(current_total, previous_total)
    if pct is None:
        delta_str = "vs previous period unavailable (no baseline)"
    else:
        sign = "+" if pct >= 0 else ""
        delta_str = f"{sign}{pct:.1f}% vs previous period"

    lines = [
        f"# {window['period'].title()} Business Summary - {window['label']}",
        "",
        f"- Total {metric_title}: {_fmt_num(current_total)} ({delta_str})",
    ]
    if top_name and top_value is not None:
        lines.append(f"- Top performer: {top_name} ({_fmt_num(top_value)})")
    else:
        lines.append("- Top performer: not enough grouped data in this period")
    lines.append(f"- Anomaly: {anomaly_line}")
    lines.append(f"- Forecast: {forecast_line}")
    return "\n".join(lines)


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    period = str((input_data or {}).get("period") or "weekly").strip().lower()
    if period not in {"weekly", "monthly"}:
        period = "weekly"

    run_id = str((input_data or {}).get("runId") or "")
    trigger = str((input_data or {}).get("trigger") or "queue")
    requested_by = str((input_data or {}).get("requestedBy") or "system")
    now = datetime.now(timezone.utc)
    window = _period_window(period, now)

    conn = get_read_connection()
    try:
        table, cols, row_count = _select_primary_table(conn)
        if not table or not cols:
            report = {
                "id": f"BR-{period}-{window['label'].replace(' ', '_')}",
                "status": "empty",
                "period": period,
                "periodLabel": window["label"],
                "generatedAt": now.isoformat(),
                "summaryMarkdown": (
                    f"# {window['period'].title()} Business Summary - {window['label']}\n\n"
                    "- No queryable tables were found. Upload data first."
                ),
                "trigger": trigger,
                "runId": run_id,
                "requestedBy": requested_by,
            }
        else:
            date_col, date_expr = _select_date_expr(conn, table, cols)
            metric_col = _select_metric(cols)
            count_key = _select_count_key(cols)
            entity_col = _select_entity_col(conn, table, cols)
            filters_sql = _mandatory_filters_sql(conn, table, cols)

            if metric_col:
                agg_expr = f'SUM("{metric_col}")'
                metric_title = metric_col.replace("_", " ").title()
            elif count_key:
                agg_expr = f'COUNT(DISTINCT "{count_key}")'
                metric_title = "Distinct Records"
            else:
                agg_expr = "COUNT(*)"
                metric_title = "Records"

            if not date_expr:
                where_current = "1=1"
                where_previous = "1=1"
                params_current: list[Any] = []
                params_previous: list[Any] = []
            else:
                where_current = f"{date_expr} >= ? AND {date_expr} < ?"
                where_previous = f"{date_expr} >= ? AND {date_expr} < ?"
                params_current = [window["current_start"], window["current_end"]]
                params_previous = [window["previous_start"], window["previous_end"]]

            if filters_sql:
                where_current = f"{where_current} AND {filters_sql}"
                where_previous = f"{where_previous} AND {filters_sql}"

            current_total = _scalar_value(
                conn,
                f'SELECT COALESCE({agg_expr}, 0) AS value FROM "{table}" WHERE {where_current}',
                params_current,
            )
            previous_total = _scalar_value(
                conn,
                f'SELECT COALESCE({agg_expr}, 0) AS value FROM "{table}" WHERE {where_previous}',
                params_previous,
            )

            top_name: str | None = None
            top_value: float | None = None
            if entity_col:
                try:
                    top_row = conn.execute(
                        (
                            f'SELECT "{entity_col}" AS name, COALESCE({agg_expr}, 0) AS value '
                            f'FROM "{table}" WHERE {where_current} '
                            f"GROUP BY 1 ORDER BY value DESC NULLS LAST LIMIT 1"
                        ),
                        params_current,
                    ).fetchone()
                    if top_row:
                        top_name = str(top_row[0]) if top_row[0] is not None else None
                        top_value = (
                            float(top_row[1]) if top_row[1] is not None else None
                        )
                except Exception:
                    pass

            anomaly_line = "No statistically strong anomalies were detected."
            if date_expr:
                try:
                    daily_rows = conn.execute(
                        (
                            f"SELECT {date_expr} AS name, COALESCE({agg_expr}, 0) AS value "
                            f'FROM "{table}" WHERE {where_current} GROUP BY 1 ORDER BY 1'
                        ),
                        params_current,
                    ).fetchall()
                    points = [
                        {"name": str(r[0]), "value": float(r[1] or 0.0)}
                        for r in daily_rows
                        if r and r[0] is not None
                    ]
                    anom = detect_anomalies(points, "time_series")
                    if anom.get("items"):
                        itm = anom["items"][0]
                        z = float(itm.get("z_score") or 0.0)
                        direction = "spike" if z > 0 else "drop"
                        anomaly_line = (
                            f"{itm.get('label')}: unusual {direction} "
                            f"({_fmt_num(itm.get('value'))}, z={z:.2f})"
                        )
                except Exception:
                    pass

            # Entity drop scan (if entity available) for a more business-readable anomaly line.
            if entity_col:
                try:
                    drop_row = conn.execute(
                        (
                            "WITH cur AS ("
                            f'  SELECT "{entity_col}" AS name, {agg_expr} AS value'
                            f'  FROM "{table}" WHERE {where_current} GROUP BY 1'
                            "), prev AS ("
                            f'  SELECT "{entity_col}" AS name, {agg_expr} AS value'
                            f'  FROM "{table}" WHERE {where_previous} GROUP BY 1'
                            ") "
                            "SELECT COALESCE(cur.name, prev.name) AS name, "
                            "       COALESCE(cur.value, 0) AS current_value, "
                            "       COALESCE(prev.value, 0) AS previous_value, "
                            "       CASE WHEN COALESCE(prev.value, 0) = 0 THEN NULL "
                            "            ELSE ((COALESCE(cur.value, 0) - COALESCE(prev.value, 0)) * 100.0 / prev.value) END AS pct_change "
                            "FROM cur FULL OUTER JOIN prev USING(name) "
                            "WHERE COALESCE(prev.value, 0) > 0 "
                            "ORDER BY pct_change ASC NULLS LAST "
                            "LIMIT 1"
                        ),
                        params_current + params_previous,
                    ).fetchone()
                    if drop_row and drop_row[0] is not None and drop_row[3] is not None:
                        pct_drop = float(drop_row[3])
                        if pct_drop <= -10.0:
                            anomaly_line = (
                                f"{drop_row[0]} dropped {pct_drop:.1f}% "
                                f"({_fmt_num(drop_row[1])} vs {_fmt_num(drop_row[2])})"
                            )
                except Exception:
                    pass

            forecast_line = "Insufficient history for forecast."
            forecast_payload = None
            if date_expr:
                bucket_expr = (
                    f"CAST(DATE_TRUNC('month', {date_expr}) AS DATE)"
                    if window["bucket"] == "month"
                    else f"CAST(DATE_TRUNC('week', {date_expr}) AS DATE)"
                )
                train_where = f"{date_expr} >= ? AND {date_expr} < ?"
                if filters_sql:
                    train_where = f"{train_where} AND {filters_sql}"
                try:
                    train_rows = conn.execute(
                        (
                            f"SELECT {bucket_expr} AS name, COALESCE({agg_expr}, 0) AS value "
                            f'FROM "{table}" WHERE {train_where} GROUP BY 1 ORDER BY 1'
                        ),
                        [window["training_start"], window["current_end"]],
                    ).fetchall()
                    train_values = [
                        float(r[1] or 0.0) for r in train_rows if r and r[1] is not None
                    ]
                    if len(train_values) >= 4:
                        fc = forecast_auto(
                            train_values, periods=1, bucket=window["bucket"]
                        )
                        if fc.forecast:
                            fc_val = float(fc.forecast[0])
                            fc_low = (
                                float(fc.lower_bound[0]) if fc.lower_bound else None
                            )
                            fc_high = (
                                float(fc.upper_bound[0]) if fc.upper_bound else None
                            )
                            forecast_line = (
                                f"Projected {metric_title.lower()} for {window['next_label']}: "
                                f"{_fmt_num(fc_val)}"
                            )
                            if fc_low is not None and fc_high is not None:
                                forecast_line += (
                                    f" (band {_fmt_num(fc_low)} to {_fmt_num(fc_high)})"
                                )
                            forecast_payload = {
                                "method": fc.method,
                                "value": fc_val,
                                "lower": fc_low,
                                "upper": fc_high,
                                "trendPct": fc.trend_pct,
                                "rmse": fc.rmse,
                                "points": len(train_values),
                            }
                except Exception:
                    pass

            markdown = _build_markdown(
                window=window,
                metric_title=metric_title,
                current_total=current_total,
                previous_total=previous_total,
                top_name=top_name,
                top_value=top_value,
                anomaly_line=anomaly_line,
                forecast_line=forecast_line,
            )

            report = {
                "id": f"BR-{period}-{window['current_start'].isoformat()}",
                "status": "ready",
                "period": period,
                "periodLabel": window["label"],
                "generatedAt": now.isoformat(),
                "summaryMarkdown": markdown,
                "table": table,
                "rowCount": row_count,
                "dateColumn": date_col,
                "metricColumn": metric_col,
                "countKey": count_key,
                "entityColumn": entity_col,
                "current": {
                    "start": window["current_start"].isoformat(),
                    "end": window["current_end"].isoformat(),
                    "value": current_total,
                },
                "previous": {
                    "start": window["previous_start"].isoformat(),
                    "end": window["previous_end"].isoformat(),
                    "value": previous_total,
                },
                "topPerformer": {"name": top_name, "value": top_value},
                "anomalySummary": anomaly_line,
                "forecast": forecast_payload,
                "trigger": trigger,
                "runId": run_id,
                "requestedBy": requested_by,
            }

        await ctx.state.set("business_reports", report["id"], report)
        await ctx.state.set("business_reports", f"latest_{period}", report)
        await ctx.state.set("business_reports", "latest", report)

        index = await ctx.state.get("business_reports", "_index")
        if not isinstance(index, list):
            index = []
        index = [report["id"]] + [x for x in index if x != report["id"]]
        index = index[:80]
        await ctx.state.set("business_reports", "_index", index)

        ctx.logger.info(
            "Business digest generated",
            {
                "reportId": report["id"],
                "period": period,
                "status": report.get("status"),
            },
        )
    finally:
        conn.close()
