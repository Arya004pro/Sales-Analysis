"""Step: Forecast — applies time-series forecasting to executed query results.

Sits between execute_query_step and detect_anomalies_step.
Triggered only when query_type == "forecast".

Pipeline position:
  execute_query → query::forecast → query::detect.anomalies → ...

Reads historical time-bucket rows, runs forecasting, appends projected
rows to results, and enriches parsed/chart metadata so format_result_step
can render a combined historical + forecast line chart.
"""

from __future__ import annotations

import os
import sys
import re
from datetime import datetime, timezone
from typing import Any

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_MODULE_DIR))
_MOTIA_DIR = os.path.join(_PROJECT_ROOT, "motia")
_STEPS_DIR = os.path.join(_MOTIA_DIR, "steps")
for _p in (_MODULE_DIR, _STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import FlowContext, queue
from utils.forecaster import forecast_auto, forecast, ForecastResult

config = {
    "name": "ForecastProjection",
    "description": (
        "Projects future time-bucket values from historical results. "
        "Supports auto/linear/holt/sma with confidence intervals."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::forecast")],
    "enqueues": ["query::detect.anomalies"],
}

# ── Label generation for future buckets ──────────────────────────────────────

_MONTH_ABBR = [
    "",
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


def _next_bucket_labels_from_now(
    bucket: str, n: int, now: datetime | None = None
) -> list[str]:
    """Generate N future labels starting from the next bucket after current time."""
    if n <= 0:
        return []
    now = now or datetime.now(timezone.utc)
    labels: list[str] = []

    if bucket == "month":
        year = now.year
        month = now.month
        for _ in range(n):
            month += 1
            if month > 12:
                month = 1
                year += 1
            labels.append(f"{_MONTH_ABBR[month]} {year}")
        return labels

    if bucket == "quarter":
        year = now.year
        q = ((now.month - 1) // 3) + 1
        for _ in range(n):
            q += 1
            if q > 4:
                q = 1
                year += 1
            labels.append(f"{year}-Q{q}")
        return labels

    if bucket == "year":
        year = now.year
        for _ in range(n):
            year += 1
            labels.append(str(year))
        return labels

    if bucket == "week":
        year, week, _ = now.isocalendar()
        for _ in range(n):
            week += 1
            if week > 52:
                week = 1
                year += 1
            labels.append(f"{year}-W{week:02d}")
        return labels

    if bucket == "day":
        cur = now
        for _ in range(n):
            cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)
            cur = cur.fromtimestamp(cur.timestamp() + 86400, tz=timezone.utc)
            labels.append(cur.strftime("%Y-%m-%d"))
        return labels

    return [f"Forecast +{i}" for i in range(1, n + 1)]


def _next_year_bucket_labels(bucket: str, n: int, end_date: str = "") -> list[str]:
    """Generate labels for the year immediately after the training range end year."""
    if n <= 0:
        return []
    try:
        base_year = int((end_date or "")[:4])
        target_year = base_year + 1
    except Exception:
        # Avoid anchoring to wall-clock time when query data is historical.
        return []

    labels: list[str] = []
    if bucket == "month":
        year = target_year
        month = 1
        for _ in range(n):
            labels.append(f"{_MONTH_ABBR[month]} {year}")
            month += 1
            if month > 12:
                month = 1
                year += 1
        return labels

    if bucket == "quarter":
        year = target_year
        q = 1
        for _ in range(n):
            labels.append(f"{year}-Q{q}")
            q += 1
            if q > 4:
                q = 1
                year += 1
        return labels

    if bucket == "year":
        year = target_year
        for _ in range(n):
            labels.append(str(year))
            year += 1
        return labels

    if bucket == "week":
        year = target_year
        week = 1
        for _ in range(n):
            labels.append(f"{year}-W{week:02d}")
            week += 1
            if week > 52:
                week = 1
                year += 1
        return labels

    if bucket == "day":
        cur = datetime(target_year, 1, 1, tzinfo=timezone.utc)
        for _ in range(n):
            labels.append(cur.strftime("%Y-%m-%d"))
            cur = cur.fromtimestamp(cur.timestamp() + 86400, tz=timezone.utc)
        return labels

    return [f"Forecast +{i}" for i in range(1, n + 1)]


def _next_bucket_labels(last_label: str, bucket: str, n: int) -> list[str]:
    """Generate N future bucket labels after last_label."""
    labels = []
    last = (last_label or "").strip()

    # Month labels: YYYY-MM or Mon YYYY
    if bucket == "month":
        m_iso = re.match(r"^(\d{4})[-/](\d{2})$", last)
        m_hum = re.match(r"^([A-Za-z]{3,9})\s+(\d{4})$", last)
        if m_iso:
            year, month = int(m_iso.group(1)), int(m_iso.group(2))
        elif m_hum:
            month_name = m_hum.group(1).lower()[:3]
            month_map = {
                "jan": 1,
                "feb": 2,
                "mar": 3,
                "apr": 4,
                "may": 5,
                "jun": 6,
                "jul": 7,
                "aug": 8,
                "sep": 9,
                "oct": 10,
                "nov": 11,
                "dec": 12,
            }
            if month_name not in month_map:
                year, month = -1, -1
            else:
                year, month = int(m_hum.group(2)), month_map[month_name]
        else:
            year, month = -1, -1

        if year > 0 and month > 0:
            for _ in range(n):
                month += 1
                if month > 12:
                    month = 1
                    year += 1
                labels.append(f"{_MONTH_ABBR[month]} {year}")
            return labels

    # Quarter labels: YYYY-Qn or Qn YYYY
    if bucket == "quarter":
        q_iso = re.match(r"^(\d{4})-Q([1-4])$", last)
        q_hum = re.match(r"^Q([1-4])\s+(\d{4})$", last, re.IGNORECASE)
        if q_iso:
            year, q = int(q_iso.group(1)), int(q_iso.group(2))
        elif q_hum:
            year, q = int(q_hum.group(2)), int(q_hum.group(1))
        else:
            year, q = -1, -1
        if year > 0 and q > 0:
            for _ in range(n):
                q += 1
                if q > 4:
                    q = 1
                    year += 1
                labels.append(f"{year}-Q{q}")
            return labels

    # Year labels: YYYY
    if bucket == "year":
        y = re.match(r"^(\d{4})$", last)
        if y:
            year = int(y.group(1))
            for _ in range(n):
                year += 1
                labels.append(str(year))
            return labels

    # Week labels: YYYY-Www
    if bucket == "week":
        w = re.match(r"^(\d{4})-W(\d{2})$", last)
        if w:
            year, week = int(w.group(1)), int(w.group(2))
            for _ in range(n):
                week += 1
                if week > 52:
                    week = 1
                    year += 1
                labels.append(f"{year}-W{week:02d}")
            return labels

    # Day labels: YYYY-MM-DD
    if bucket == "day" and re.match(r"^\d{4}-\d{2}-\d{2}$", last):
        cur = datetime.fromisoformat(last).replace(tzinfo=timezone.utc)
        for _ in range(n):
            cur = cur.fromtimestamp(cur.timestamp() + 86400, tz=timezone.utc)
            labels.append(cur.strftime("%Y-%m-%d"))
        return labels

    # Fallback: just number future periods
    for i in range(1, n + 1):
        labels.append(f"Forecast +{i}")
    return labels


def _human_labels(raw_labels: list[str], bucket: str) -> list[str]:
    """Convert raw YYYY-MM labels to human-readable (Jan 2024)."""
    out = []
    for lbl in raw_labels:
        if bucket == "month" and re.match(r"\d{4}-\d{2}", lbl):
            year, month = int(lbl[:4]), int(lbl[5:7])
            out.append(f"{_MONTH_ABBR[month]} {year}")
        else:
            out.append(lbl)
    return out


# ── Chart config builder for forecast ────────────────────────────────────────


def _build_forecast_chart(
    hist_labels: list[str],
    hist_values: list[float],
    fc_labels: list[str],
    fc_values: list[float],
    fc_lower: list[float],
    fc_upper: list[float],
    metric: str,
    currency: str,
    title: str,
    period_str: str,
    method: str,
    confidence_pct: float,
) -> dict:
    all_labels = hist_labels + fc_labels
    # Pad historical with nulls so it stops at the boundary
    hist_data = hist_values + [None] * len(fc_labels)
    # Pad forecast with null so it starts from last historical point
    fc_start = [hist_values[-1]] + fc_values  # connect the lines
    fc_data = [None] * (len(hist_labels) - 1) + fc_start

    # Confidence band data (only over forecast horizon)
    n_hist = len(hist_labels)
    lower_data = [None] * (n_hist - 1) + [hist_values[-1]] + fc_lower
    upper_data = [None] * (n_hist - 1) + [hist_values[-1]] + fc_upper

    def tick_fn():
        if currency:
            return (
                f"function(v){{"
                f"if(typeof v!=='number')return null;"
                f"if(Math.abs(v)>=10000000)return '{currency}'+(v/10000000).toFixed(1)+'Cr';"
                f"if(Math.abs(v)>=100000)return '{currency}'+(v/100000).toFixed(1)+'L';"
                f"if(Math.abs(v)>=1000)return '{currency}'+(v/1000).toFixed(1)+'K';"
                f"return '{currency}'+v.toLocaleString('en-IN');}}"
            )
        return (
            "function(v){if(typeof v!=='number')return null;"
            "if(Math.abs(v)>=1000000)return (v/1000000).toFixed(1)+'M';"
            "if(Math.abs(v)>=1000)return (v/1000).toFixed(1)+'K';"
            "return v.toLocaleString('en-IN');}"
        )

    tip_fn = (
        f"function(c){{"
        f"if(c.raw===null||c.raw===undefined)return null;"
        f"var v=c.raw,s=typeof v==='number'?v.toLocaleString('en-IN',{{minimumFractionDigits:2}}):String(v);"
        f"return ' '+c.dataset.label+': {currency}'+s;}}"
    )

    max_rotation = 45 if len(all_labels) > 8 else 0

    cfg = {
        "type": "line",
        "data": {
            "labels": all_labels,
            "datasets": [
                # Historical
                {
                    "label": "Historical",
                    "data": hist_data,
                    "borderColor": "rgba(99,179,237,1)",
                    "backgroundColor": "rgba(99,179,237,0.10)",
                    "pointBackgroundColor": "rgba(99,179,237,1)",
                    "pointBorderColor": "#1a1d27",
                    "pointRadius": 4,
                    "pointHoverRadius": 6,
                    "borderWidth": 2,
                    "fill": False,
                    "tension": 0.35,
                    "spanGaps": False,
                },
                # Forecast line
                {
                    "label": f"Forecast ({method.capitalize()}, {int(confidence_pct)}% CI)",
                    "data": fc_data,
                    "borderColor": "rgba(246,173,85,1)",
                    "backgroundColor": "rgba(246,173,85,0.0)",
                    "pointBackgroundColor": "rgba(246,173,85,1)",
                    "pointBorderColor": "#1a1d27",
                    "pointRadius": 4,
                    "pointHoverRadius": 6,
                    "borderWidth": 2,
                    "borderDash": [6, 4],
                    "fill": False,
                    "tension": 0.3,
                    "spanGaps": True,
                },
                # Upper CI band
                {
                    "label": f"Upper {int(confidence_pct)}% CI",
                    "data": upper_data,
                    "borderColor": "rgba(246,173,85,0.25)",
                    "backgroundColor": "rgba(246,173,85,0.12)",
                    "pointRadius": 0,
                    "borderWidth": 1,
                    "borderDash": [3, 3],
                    "fill": "+1",
                    "tension": 0.3,
                    "spanGaps": True,
                },
                # Lower CI band
                {
                    "label": f"Lower {int(confidence_pct)}% CI",
                    "data": lower_data,
                    "borderColor": "rgba(246,173,85,0.25)",
                    "backgroundColor": "rgba(246,173,85,0.12)",
                    "pointRadius": 0,
                    "borderWidth": 1,
                    "borderDash": [3, 3],
                    "fill": False,
                    "tension": 0.3,
                    "spanGaps": True,
                },
            ],
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": True,
            "interaction": {"mode": "index", "intersect": False},
            "plugins": {
                "legend": {
                    "display": True,
                    "labels": {
                        "color": "#94a3b8",
                        "font": {"size": 11},
                        "filter": "function(item){return !item.text.includes('CI')||item.text.includes('Forecast');}",
                    },
                },
                "tooltip": {
                    "enabled": True,
                    "backgroundColor": "#1e2130",
                    "titleColor": "#e2e8f0",
                    "bodyColor": "#94a3b8",
                    "borderColor": "#2d3148",
                    "borderWidth": 1,
                    "callbacks": {"label": tip_fn},
                    "filter": "function(item){return item.raw!==null&&item.raw!==undefined;}",
                },
                "annotation": {
                    "annotations": {
                        "forecastStart": {
                            "type": "line",
                            "xMin": len(hist_labels) - 1,
                            "xMax": len(hist_labels) - 1,
                            "borderColor": "rgba(255,255,255,0.25)",
                            "borderWidth": 1,
                            "borderDash": [4, 4],
                            "label": {
                                "display": True,
                                "content": "Forecast →",
                                "color": "rgba(246,173,85,0.8)",
                                "font": {"size": 10},
                                "position": "start",
                            },
                        }
                    }
                },
            },
            "scales": {
                "x": {
                    "title": {
                        "display": True,
                        "text": "Period",
                        "color": "#94a3b8",
                        "font": {"size": 11},
                    },
                    "ticks": {
                        "color": "#94a3b8",
                        "font": {"size": 10},
                        "maxRotation": max_rotation,
                        "minRotation": max_rotation,
                    },
                    "grid": {"color": "rgba(255,255,255,0.05)"},
                },
                "y": {
                    "title": {
                        "display": True,
                        "text": metric.replace("_", " ").title()
                        + (f" ({currency})" if currency else ""),
                        "color": "#94a3b8",
                        "font": {"size": 11},
                    },
                    "ticks": {
                        "color": "#94a3b8",
                        "font": {"size": 11},
                        "callback": tick_fn(),
                    },
                    "grid": {"color": "rgba(255,255,255,0.05)"},
                    "beginAtZero": False,
                },
            },
        },
    }
    return {
        "title": title,
        "subtitle": f"{period_str} — {method.capitalize()} forecast, {int(confidence_pct)}% confidence",
        "prefix": currency,
        "config": cfg,
        "is_forecast": True,
    }


# ── Currency inference (mirrors format_result_step) ───────────────────────────


def _infer_currency(metric: str) -> str:
    m = (metric or "").lower()
    if any(
        x in m
        for x in [
            "fare",
            "earnings",
            "commission",
            "revenue",
            "amount",
            "price",
            "total",
            "salary",
            "sales",
            "profit",
            "final",
            "net",
        ]
    ):
        return ""
    if any(x in m for x in ["count", "quantity", "units", "distance", "duration"]):
        return ""
    return ""


# ── Main handler ──────────────────────────────────────────────────────────────


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})
    results = input_data.get("results", []) or []
    period_labels = input_data.get("period_labels", [])
    start_date = input_data.get("startDate", "")
    end_date = input_data.get("endDate", "")

    metric = parsed.get("metric", "value")
    metric_disp = parsed.get("semantic_metric") or metric
    bucket = parsed.get("time_bucket", "month")
    method = parsed.get("forecast_method", "auto")
    periods = int(parsed.get("forecast_periods") or 3)
    conf_pct = float(parsed.get("forecast_confidence", 80.0))
    goal_cfg = (
        parsed.get("_goal_tracking")
        if isinstance(parsed.get("_goal_tracking"), dict)
        else {}
    )
    try:
        goal_target = float(goal_cfg.get("target_value")) if goal_cfg else None
    except Exception:
        goal_target = None

    ctx.logger.info(
        "🔮 Forecasting",
        {
            "queryId": query_id,
            "method": method,
            "periods": periods,
            "rows": len(results),
        },
    )

    # ── Extract historical data ───────────────────────────────────────────────
    raw_labels = [str(r.get("name", "?")) for r in results]
    hist_values = [float(r.get("value", 0) or 0) for r in results]

    if len(hist_values) < 2:
        ctx.logger.warn(
            "⚠️ Insufficient data for forecast (<2 points)", {"queryId": query_id}
        )
        goal_result = None
        if goal_cfg and goal_target and hist_values:
            actual_to_date = float(sum(hist_values))
            projected_total = actual_to_date
            gap = projected_total - float(goal_target)
            goal_result = {
                "mode": str(goal_cfg.get("mode") or "end_of_month"),
                "horizon_label": str(goal_cfg.get("horizon_label") or "target horizon"),
                "target_value": float(goal_target),
                "actual_to_date": actual_to_date,
                "projected_remaining": 0.0,
                "projected_total": projected_total,
                "gap": gap,
                "on_track": bool(gap >= 0),
                "remaining_periods": int(goal_cfg.get("remaining_periods") or 0),
                "required_per_period": 0.0,
                "forecast_avg_per_period": 0.0,
                "pace_ratio": 1.0 if gap >= 0 else 0.0,
                "status": "insufficient_history",
            }

        pass_through_parsed = parsed
        if goal_result:
            pass_through_parsed = {**parsed, "_goal_tracking_result": goal_result}

        # Pass through to anomaly detection unchanged
        await ctx.enqueue(
            {
                "topic": "query::detect.anomalies",
                "data": {
                    **input_data,
                    "parsed": pass_through_parsed,
                    "forecast_skipped": True,
                },
            }
        )
        return

    # Human-readable historical labels
    hist_labels = _human_labels(raw_labels, bucket)
    ql = (user_query or "").lower()
    fc_labels: list[str] = []
    if "next year" in ql:
        fc_labels = _next_year_bucket_labels(bucket, periods, end_date)
    if not fc_labels and raw_labels:
        fc_labels = _next_bucket_labels(raw_labels[-1], bucket, periods)
    if not fc_labels and hist_labels:
        fc_labels = _next_bucket_labels(hist_labels[-1], bucket, periods)
    if not fc_labels:
        # Last fallback only (avoids clock-time drift for historical queries).
        fc_labels = _next_bucket_labels_from_now(bucket, periods)

    # ── Run forecast ──────────────────────────────────────────────────────────
    try:
        if method == "auto":
            result: ForecastResult = forecast_auto(
                hist_values,
                periods,
                conf_pct,
                bucket=bucket,
            )
        else:
            result = forecast(
                hist_values,
                periods,
                method,
                conf_pct,
                bucket=bucket,
            )
    except Exception as exc:
        ctx.logger.error(
            "Forecast algorithm failed", {"queryId": query_id, "error": str(exc)}
        )
        await ctx.enqueue(
            {
                "topic": "query::detect.anomalies",
                "data": {**input_data, "forecast_skipped": True},
            }
        )
        return

    ctx.logger.info(
        "✅ Forecast complete",
        {
            "queryId": query_id,
            "method": result.method,
            "rmse": result.rmse,
            "trend_pct": result.trend_pct,
        },
    )

    goal_result = None
    if goal_cfg and goal_target and goal_target > 0:
        actual_to_date = float(sum(hist_values))
        projected_remaining = float(sum(result.forecast or []))
        projected_total = actual_to_date + projected_remaining
        gap = projected_total - float(goal_target)
        remaining_periods = int(goal_cfg.get("remaining_periods") or periods or 0)
        required_per_period = (
            max(float(goal_target) - actual_to_date, 0.0) / remaining_periods
            if remaining_periods > 0
            else 0.0
        )
        forecast_avg_per_period = (
            projected_remaining / max(len(result.forecast or []), 1)
            if result.forecast
            else 0.0
        )
        pace_ratio = (
            (forecast_avg_per_period / required_per_period)
            if required_per_period > 0
            else (1.0 if gap >= 0 else 0.0)
        )
        goal_result = {
            "mode": str(goal_cfg.get("mode") or "end_of_month"),
            "horizon_label": str(goal_cfg.get("horizon_label") or "target horizon"),
            "target_value": float(goal_target),
            "actual_to_date": actual_to_date,
            "projected_remaining": projected_remaining,
            "projected_total": projected_total,
            "gap": gap,
            "on_track": bool(gap >= 0),
            "remaining_periods": remaining_periods,
            "required_per_period": required_per_period,
            "forecast_avg_per_period": forecast_avg_per_period,
            "pace_ratio": pace_ratio,
            "status": "ok",
        }

    # ── Build combined results (historical + forecast rows) ───────────────────
    forecast_rows = []
    for i, (lbl, val, lo, hi) in enumerate(
        zip(fc_labels, result.forecast, result.lower_bound, result.upper_bound)
    ):
        forecast_rows.append(
            {
                "name": lbl,
                "value": round(val, 2),
                "lower": round(lo, 2),
                "upper": round(hi, 2),
                "is_forecast": True,
            }
        )

    # ── Build chart ───────────────────────────────────────────────────────────
    currency = _infer_currency(metric_disp)
    period_str = (
        f"{start_date} to {end_date}" if start_date and end_date else "selected period"
    )
    chart_title = f"Forecast: {user_query or metric_disp.replace('_', ' ').title()}"

    chart_config = _build_forecast_chart(
        hist_labels=hist_labels,
        hist_values=hist_values,
        fc_labels=fc_labels,
        fc_values=[round(v, 2) for v in result.forecast],
        fc_lower=[round(v, 2) for v in result.lower_bound],
        fc_upper=[round(v, 2) for v in result.upper_bound],
        metric=metric_disp,
        currency=currency,
        title=chart_title,
        period_str=period_str,
        method=result.method,
        confidence_pct=conf_pct,
    )

    # ── Enrich parsed with forecast metadata for format_result_step ──────────
    enriched_parsed = {
        **parsed,
        "query_type": "forecast",
        "_forecast_result": {
            "method": result.method,
            "periods": periods,
            "rmse": round(result.rmse, 2),
            "trend_pct": result.trend_pct,
            "confidence_pct": conf_pct,
            "hist_labels": hist_labels,
            "hist_values": hist_values,
            "fc_labels": fc_labels,
            "fc_values": [round(v, 2) for v in result.forecast],
            "fc_lower": [round(v, 2) for v in result.lower_bound],
            "fc_upper": [round(v, 2) for v in result.upper_bound],
        },
    }
    if goal_result:
        enriched_parsed["_goal_tracking_result"] = goal_result

    # Update state with chart config
    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "status": "forecast_computed",
                "chart_config": chart_config,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "forecast_computed": now_iso},
            },
        )

    await ctx.enqueue(
        {
            "topic": "query::detect.anomalies",
            "data": {
                **input_data,
                "parsed": enriched_parsed,
                "results": results + forecast_rows,
                "forecast_rows": forecast_rows,
                "_chart_config": chart_config,
            },
        }
    )
