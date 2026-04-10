"""Step 6: Format Result  formats any result shape into text + chart.

Changes vs original:
  - Added time_series result handling:
      Renders a LINE CHART (Chart.js type: 'line') for trend data.
      Month labels like "2024-01" are converted to "Jan 2024" for readability.
      Text output shows the trend table with all time buckets.
  - All other formatting (comparison, ranked, aggregate, etc.) unchanged.
  - FIX: Header no longer says "Top N" when result is a full distribution/breakdown.
      Only says "Top N" when results == top_n (truncated). Otherwise says "breakdown".
    - FIX: is_* boolean entity columns get human-readable labels (e.g. "Status breakdown").
  - FIX: _post_process top_n inflated to 20 for vs-queries now handled gracefully.
"""

import os
import sys
import re
import json
from typing import Any
from motia import FlowContext, queue

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in (_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared_config import GROQ_API_TOKEN, GROQ_URL, INSIGHTS_MODEL
from utils.llm_client import clean_model_text, post_chat_completion
from utils.token_logger import log_tokens, add_tokens_to_state, calc_max_tokens

config = {
    "name": "ResponseFormatter",
    "description": (
        "Builds final user response text, summary tables, and chart configuration. "
        "Supports scalar, ranking, comparison, trend, and forecast outputs."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::format.result")],
    "enqueues": [],
}

_PALETTE = [
    "rgba(99,179,237,0.85)",
    "rgba(104,211,145,0.85)",
    "rgba(246,173,85,0.85)",
    "rgba(252,129,129,0.85)",
    "rgba(154,117,221,0.85)",
    "rgba(79,209,197,0.85)",
    "rgba(246,135,179,0.85)",
    "rgba(183,148,255,0.85)",
]
_BORDERS = [c.replace("0.85", "1") for c in _PALETTE]

# Month abbreviations for label formatting
_MONTH_ABBR = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec",
}


def _format_bucket_label(raw_label: str, bucket: str) -> str:
    """Convert raw bucket label (e.g. '2024-01') to a human-readable string."""
    if bucket == "month" and len(raw_label) == 7 and "-" in raw_label:
        year, month = raw_label.split("-", 1)
        abbr = _MONTH_ABBR.get(month, month)
        return f"{abbr} {year}"
    if bucket == "quarter" and "Q" in raw_label:
        return raw_label
    if bucket == "week":
        return raw_label.replace("-W", " W")
    return raw_label


def _infer_currency(metric: str, hint: str = "") -> str:
    m = (metric or "").lower()
    h = (hint or "").lower()
    if re.search(r"\b(inr|rupee|rupees|rs)\b", f"{m} {h}"):
        return "Rs "
    if re.search(r"\b(usd|dollar|dollars)\b", f"{m} {h}") or "$" in m or "$" in h:
        return "$"
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
        ]
    ):
        return ""
    if any(
        x in m for x in ["count", "quantity", "units", "distance", "duration", "rides"]
    ):
        return ""
    return ""


def _metric_label(metric: str, currency: str) -> str:
    if not metric:
        return "Value"
    base = metric.strip().lower()
    if base in ("aov", "average_order_value"):
        label = "Average Order Value"
    elif base.startswith("avg_"):
        core = base[4:].replace("_", " ").strip().title()
        label = f"Average {core}" if core else "Average"
    else:
        label = metric.replace("_", " ").strip().title()
    if currency:
        return f"{label} ({currency})"
    return label


def _entity_label(entity: str) -> str:
    """Return a clean plural label for an entity column."""
    if not entity:
        return ""
    base = entity

    # Handle boolean flag columns: is_refunded -> "Refund Status"
    if base.startswith("is_"):
        core = base[3:].replace("_", " ").strip()
        return f"{core} status"

    for suffix in ("_name", "_id", "_type", "_code", "_key"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    words = base.replace("_", " ").strip()
    if words:
        if words.endswith("y") and not words.endswith(("ay", "ey", "iy", "oy", "uy")):
            words = words[:-1] + "ies"
        elif not words.endswith("s"):
            words += "s"
    return words


def _display_entity_name(raw_name: Any, entity: str) -> str:
    name = str(raw_name)
    if not entity:
        return name

    ent = entity.lower().strip()
    if not ent.startswith("is_"):
        return name

    base = ent[3:].replace("_", " ").strip()
    zero_vals = {"0", "0.0", "false", "False"}
    one_vals = {"1", "1.0", "true", "True"}

    if name in one_vals:
        if base == "active":
            return "Active"
        return base.title()
    if name in zero_vals:
        if base == "active":
            return "Inactive"
        return f"Not {base}".title()
    return name


def _ranked_header(
    qt: str,
    top_n: int,
    result_count: int,
    elabel: str,
    mlabel: str,
    period_str: str,
    disable_limit: bool = False,
) -> str:
    """
    Build a header that only says 'Top N' when results are genuinely truncated.
    If result_count < top_n it's a full distribution - label it as a breakdown.
    """
    elabel_title = elabel.title() if elabel else "Items"

    # Full distribution (all rows returned, no truncation)
    if disable_limit or result_count < top_n or result_count <= 5:
        return f"{elabel_title} breakdown by {mlabel} {period_str}:"

    if qt == "bottom_n":
        return f"Bottom {top_n} {elabel_title} by {mlabel} {period_str}:"

    return f"Top {top_n} {elabel_title} by {mlabel} {period_str}:"


def _tick_fn(metric: str, currency: str) -> str:
    pfx = "Rs " if currency == "Rs " else currency.replace("$", "\\u0024")
    if currency in ("Rs ", "$"):
        return (
            f"function(v){{"
            f"if(typeof v!=='number')return v;"
            f"var a=Math.abs(v);"
            f"if(a>=10000000)return '{pfx}'+(v/10000000).toLocaleString('en-IN',{{maximumFractionDigits:2}})+'Cr';"
            f"if(a>=100000)return '{pfx}'+(v/100000).toLocaleString('en-IN',{{maximumFractionDigits:2}})+'L';"
            f"if(a>=1000)return '{pfx}'+(v/1000).toLocaleString('en-IN',{{maximumFractionDigits:1}})+'K';"
            f"return '{pfx}'+v.toLocaleString('en-IN');"
            f"}}"
        )
    else:
        return (
            "function(v){"
            "if(typeof v!=='number')return v;"
            "var a=Math.abs(v);"
            "if(a>=1000000)return (v/1000000).toLocaleString('en-IN',{maximumFractionDigits:2})+'M';"
            "if(a>=1000)return (v/1000).toLocaleString('en-IN',{maximumFractionDigits:1})+'K';"
            "return v.toLocaleString('en-IN');"
            "}"
        )


def _tooltip_fn(metric: str, currency: str) -> str:
    pfx = "Rs " if currency == "Rs " else currency.replace("$", "\\u0024")
    if currency:
        return (
            f"function(c){{"
            f"var v=c.raw;"
            f"var s=typeof v==='number'"
            f"?v.toLocaleString('en-IN',{{minimumFractionDigits:2,maximumFractionDigits:2}})"
            f":String(v);"
            f"return ' {pfx}'+s;"
            f"}}"
        )
    else:
        return (
            "function(c){"
            "var v=c.raw;"
            "var s=typeof v==='number'"
            "?v.toLocaleString('en-IN',{maximumFractionDigits:0})"
            ":String(v);"
            "return ' '+s;"
            "}"
        )


def _cmp_tooltip_fn(metric: str, currency: str) -> str:
    dec = (
        "{minimumFractionDigits:2,maximumFractionDigits:2}"
        if currency
        else "{maximumFractionDigits:0}"
    )
    pfx = "Rs " if currency == "Rs " else currency.replace("$", "\\u0024")
    return (
        f"function(c){{"
        f"var v=c.raw;"
        f"var s=typeof v==='number'?v.toLocaleString('en-IN',{dec}):String(v);"
        f"return ' '+c.dataset.label+': {pfx}'+s;"
        f"}}"
    )


def _make_base(
    metric: str, currency: str, entity: str, legend: bool = False, index_axis: str = "y"
) -> dict:
    metric_lbl = _metric_label(metric, currency)
    entity_lbl = _entity_label(entity)

    if index_axis == "y":
        x_title = metric_lbl
        y_title = entity_lbl
        x_tick = _tick_fn(metric, currency)
        y_tick = None
    else:
        x_title = entity_lbl
        y_title = metric_lbl
        x_tick = None
        y_tick = _tick_fn(metric, currency)

    x_ticks: dict = {"color": "#94a3b8", "font": {"size": 11}}
    if x_tick:
        x_ticks["callback"] = x_tick
    y_ticks: dict = {"color": "#94a3b8", "font": {"size": 11}}
    if y_tick:
        y_ticks["callback"] = y_tick

    return {
        "responsive": True,
        "maintainAspectRatio": True,
        "indexAxis": index_axis,
        "interaction": {"mode": "nearest", "intersect": True},
        "plugins": {
            "legend": {
                "display": legend,
                "labels": {"color": "#94a3b8", "font": {"size": 12}},
            },
            "tooltip": {
                "enabled": True,
                "backgroundColor": "#1e2130",
                "titleColor": "#e2e8f0",
                "bodyColor": "#94a3b8",
                "borderColor": "#2d3148",
                "borderWidth": 1,
            },
        },
        "scales": {
            "x": {
                "title": {
                    "display": bool(x_title),
                    "text": x_title,
                    "color": "#94a3b8",
                    "font": {"size": 11, "weight": "normal"},
                },
                "ticks": x_ticks,
                "grid": {"color": "rgba(255,255,255,0.05)"},
            },
            "y": {
                "title": {
                    "display": bool(y_title),
                    "text": y_title,
                    "color": "#94a3b8",
                    "font": {"size": 11, "weight": "normal"},
                },
                "ticks": y_ticks,
                "grid": {"color": "rgba(255,255,255,0.05)"},
                "beginAtZero": True,
            },
        },
    }


def _make_line_chart(
    labels: list,
    values: list,
    metric: str,
    currency: str,
    title: str,
    subtitle: str,
    bucket: str,
) -> dict:
    """Build a Chart.js line chart config for time_series results."""
    mlabel = _metric_label(metric, currency)
    tick_fn = _tick_fn(metric, currency)
    tip_fn = _tooltip_fn(metric, currency)

    max_rotation = 45 if len(labels) > 6 else 0

    cfg = {
        "type": "line",
        "data": {
            "labels": labels,
            "datasets": [
                {
                    "label": mlabel,
                    "data": values,
                    "borderColor": "rgba(99,179,237,1)",
                    "backgroundColor": "rgba(99,179,237,0.12)",
                    "pointBackgroundColor": "rgba(99,179,237,1)",
                    "pointBorderColor": "#1a1d27",
                    "pointRadius": 4,
                    "pointHoverRadius": 6,
                    "borderWidth": 2,
                    "fill": True,
                    "tension": 0.35,
                }
            ],
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": True,
            "interaction": {"mode": "index", "intersect": False},
            "plugins": {
                "legend": {"display": False},
                "tooltip": {
                    "enabled": True,
                    "backgroundColor": "#1e2130",
                    "titleColor": "#e2e8f0",
                    "bodyColor": "#94a3b8",
                    "borderColor": "#2d3148",
                    "borderWidth": 1,
                    "callbacks": {"label": tip_fn},
                },
            },
            "scales": {
                "x": {
                    "title": {
                        "display": True,
                        "text": bucket.capitalize(),
                        "color": "#94a3b8",
                        "font": {"size": 11},
                    },
                    "ticks": {
                        "color": "#94a3b8",
                        "font": {"size": 11},
                        "maxRotation": max_rotation,
                        "minRotation": max_rotation,
                    },
                    "grid": {"color": "rgba(255,255,255,0.05)"},
                },
                "y": {
                    "title": {
                        "display": True,
                        "text": mlabel,
                        "color": "#94a3b8",
                        "font": {"size": 11},
                    },
                    "ticks": {
                        "color": "#94a3b8",
                        "font": {"size": 11},
                        "callback": tick_fn,
                    },
                    "grid": {"color": "rgba(255,255,255,0.05)"},
                    "beginAtZero": False,
                },
            },
        },
    }
    return {"title": title, "subtitle": subtitle, "prefix": currency, "config": cfg}


def _bar(labels, values, metric, currency, entity, title, subtitle):
    base = _make_base(metric, currency, entity, index_axis="y")
    tooltip = _tooltip_fn(metric, currency)
    base["plugins"]["tooltip"]["callbacks"] = {"label": tooltip}
    cfg = {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [
                {
                    "label": _metric_label(metric, currency),
                    "data": values,
                    "backgroundColor": [
                        _PALETTE[i % len(_PALETTE)] for i in range(len(labels))
                    ],
                    "borderColor": [
                        _BORDERS[i % len(_PALETTE)] for i in range(len(labels))
                    ],
                    "borderWidth": 1,
                    "borderRadius": 4,
                    "minBarLength": 6,
                }
            ],
        },
        "options": base,
    }
    return {"title": title, "subtitle": subtitle, "prefix": currency, "config": cfg}


def _token_summary(usage, totals):
    if not usage and not totals:
        return ""

    def _is_llm_entry(e: dict) -> bool:
        if "is_llm" in e:
            return bool(e.get("is_llm"))
        model = (e.get("model") or "").strip()
        return model not in ("rule_based", "adaptive_rule")

    lines = ["\n\n Token Usage "]
    llm_rows = 0
    for e in usage:
        short = (e.get("model", "") or "").split("/")[-1]
        is_llm = _is_llm_entry(e)
        if is_llm:
            llm_rows += 1
            prompt = f"{e.get('prompt_tokens', 0):>5}"
            completion = f"{e.get('completion_tokens', 0):>4}"
            total = f"{e.get('total_tokens', 0):>5}"
        else:
            prompt = f"{0:>5}"
            completion = f"{0:>4}"
            total = f"{0:>5}"
        lines.append(
            f"  {e.get('step', '?'):<20} {short:<28} "
            f"prompt={prompt}  completion={completion}  total={total}"
        )

    if llm_rows > 0 and totals:
        lines.append("  " + "" * 70)
        lines.append(
            f"  {'TOTAL':<20} {'':28} "
            f"prompt={totals.get('prompt_tokens', 0):>5}  "
            f"completion={totals.get('completion_tokens', 0):>4}  "
            f"total={totals.get('total_tokens', 0):>5}"
        )
    elif llm_rows == 0:
        lines.append("  LLM tokens: 0 (no LLM used)")

    return "\n".join(lines)


def _fmt_indian(v: Any, currency: str, decimals: int = 2) -> str:
    if v is None:
        return ""
    try:
        val = float(v)
    except Exception:
        return str(v)

    sign = "-" if val < 0 else ""
    abs_val = abs(val)

    s = f"{abs_val:.{decimals}f}"
    parts = s.split(".")
    integer_part = parts[0]
    decimal_part = parts[1] if len(parts) > 1 else ""

    res = ""
    if len(integer_part) > 3:
        res = "," + integer_part[-3:]
        remaining = integer_part[:-3]
        while len(remaining) > 2:
            res = "," + remaining[-2:] + res
            remaining = remaining[:-2]
        if remaining:
            res = remaining + res
    else:
        res = integer_part

    formatted = f"{sign}{currency}{res}"
    if decimals > 0:
        formatted += f".{decimal_part}"
    return formatted


def _fmt(v, currency):
    return _fmt_indian(v, currency)


def _delta_str(v1, v2, currency):
    if v1 is None or v2 is None:
        return "N/A"
    delta = v2 - v1
    sign = "+" if delta >= 0 else ""
    pct = (delta / v1 * 100) if v1 != 0 else float("inf")
    pct_s = f"{sign}{pct:.1f}%" if pct != float("inf") else "new"
    return f"{sign}{_fmt_indian(abs(delta), currency)} ({pct_s})"


def _insights_time_series(
    labels: list[str], values: list[float], currency: str
) -> list[str]:
    if not labels or not values:
        return []
    insights: list[str] = []
    first, last = values[0], values[-1]
    if first:
        pct = ((last - first) / first) * 100
        dir_word = "up" if pct >= 0 else "down"
        insights.append(
            f"Overall trend: {dir_word} {abs(pct):.1f}% from {labels[0]} to {labels[-1]}."
        )
    peak_idx = max(range(len(values)), key=lambda i: values[i])
    low_idx = min(range(len(values)), key=lambda i: values[i])
    insights.append(
        f"Peak period: {labels[peak_idx]} ({_fmt_indian(values[peak_idx], currency)}), "
        f"lowest: {labels[low_idx]} ({_fmt_indian(values[low_idx], currency)})."
    )
    total = sum(values)
    if total > 0:
        top3 = sorted(values, reverse=True)[:3]
        share = (sum(top3) / total) * 100
        insights.append(f"Top 3 periods contribute {share:.1f}% of total.")
    return insights


def _insights_ranked(items: list[dict], currency: str) -> list[str]:
    if not items:
        return []
    raw_vals = [float(r.get("raw_value", 0) or 0) for r in items]
    total = sum(raw_vals)
    if total <= 0:
        return []
    top = raw_vals[0]
    top_share = (top / total) * 100
    top3_share = (sum(raw_vals[:3]) / total) * 100 if len(raw_vals) >= 3 else 100.0
    insights = [
        f"Top contributor share: {top_share:.1f}% of listed total.",
        f"Top 3 concentration: {top3_share:.1f}% of listed total.",
    ]
    return insights


def _insights_ranked_by_period(items: list[dict], currency: str) -> list[str]:
    if not items:
        return []

    by_period: dict[str, list[dict]] = {}
    for row in items:
        period = str(row.get("period", "")).strip() or "Unknown"
        by_period.setdefault(period, []).append(row)

    if not by_period:
        return []

    periods = list(by_period.keys())
    all_values = [float(r.get("raw_value", 0) or 0) for r in items]
    overall_avg = (sum(all_values) / len(all_values)) if all_values else 0.0

    leaders = []
    for p in periods:
        rows = sorted(
            by_period[p], key=lambda r: float(r.get("raw_value", 0) or 0), reverse=True
        )
        if rows:
            leaders.append(
                (
                    p,
                    str(rows[0].get("name", "?")),
                    float(rows[0].get("raw_value", 0) or 0),
                )
            )

    unique_leaders = {n for _, n, _ in leaders}
    insights = [
        f"Covered {len(periods)} time buckets with top {max(len(v) for v in by_period.values())} entities per bucket.",
        f"Distinct period leaders: {len(unique_leaders)}.",
    ]
    if leaders:
        best = max(leaders, key=lambda x: x[2])
        insights.append(
            f"Strongest bucket leader: {best[1]} in {best[0]} at {_fmt_indian(best[2], currency)} (overall avg {_fmt_indian(overall_avg, currency)})."
        )
    return insights


def _is_binary_status_split(entity: str, items: list[dict]) -> bool:
    return bool((entity or "").lower().startswith("is_") and len(items) == 2)


def _insights_binary_status_split(entity: str, items: list[dict]) -> list[str]:
    raw = {
        str(i.get("name", "")).strip().lower(): float(i.get("raw_value", 0) or 0)
        for i in items
    }
    total = sum(raw.values())
    if total <= 0:
        return []

    entries = sorted(raw.items(), key=lambda kv: kv[1], reverse=True)
    top_label, top_value = entries[0]
    other_label, other_value = entries[1]
    ratio_txt = "N/A"
    if other_value > 0:
        ratio_txt = f"{(top_value / other_value):.1f}:1"
    return [
        f"{top_label.title()} share: {(top_value / total) * 100:.1f}%.",
        f"{other_label.title()} share: {(other_value / total) * 100:.1f}%.",
        f"{top_label.title()} to {other_label.title()} ratio: {ratio_txt}.",
    ]


def _anomaly_insights(anomalies: dict[str, Any], currency: str) -> list[str]:
    if not isinstance(anomalies, dict):
        return []
    flagged = anomalies.get("items") or []
    if not flagged:
        return []
    out = []
    for a in flagged[:3]:
        label = a.get("label", "unknown")
        value = _fmt_indian(a.get("value", 0), currency)
        z = a.get("z_score")
        ratio = a.get("ratio_to_mean")
        ratio_txt = f", {ratio:.1f}x mean" if isinstance(ratio, (int, float)) else ""
        if isinstance(z, (int, float)):
            out.append(f"Potential anomaly: {label} at {value} (z={z:.2f}{ratio_txt}).")
        else:
            out.append(f"Potential anomaly: {label} at {value}{ratio_txt}.")
    return out


def _inject_insight_block(formatted_text: str, lines: list[str]) -> str:
    if not lines:
        return formatted_text
    block = "\n\nAnomalies:\n" + "\n".join(f"- {x}" for x in lines)
    marker = "\n\n Token Usage "
    if marker in formatted_text:
        head, tail = formatted_text.split(marker, 1)
        return head + block + marker + tail
    return formatted_text + block


def _inject_ai_insights(formatted_text: str, insights: list[str]) -> str:
    lines = [str(x).strip() for x in (insights or []) if str(x).strip()]
    if not lines:
        return formatted_text
    block = "\n\nBusiness Insights:\n" + "\n".join(f"- {x}" for x in lines[:3])
    marker = "\n\n Token Usage "
    if marker in formatted_text:
        head, tail = formatted_text.split(marker, 1)
        return head + block + marker + tail
    return formatted_text + block


def _revenue_bridge_lines(decomposition: dict[str, Any], currency: str) -> list[str]:
    if not isinstance(decomposition, dict):
        return []
    if not decomposition.get("applies"):
        return []

    summary = decomposition.get("summary") or {}
    bridge = decomposition.get("bridge") or {}
    mix_shift = decomposition.get("mix_shift") or {}

    p1 = str(summary.get("period_1") or "Period 1")
    p2 = str(summary.get("period_2") or "Period 2")
    total_1 = float(summary.get("total_1") or 0.0)
    total_2 = float(summary.get("total_2") or 0.0)
    net_change = float(summary.get("net_change") or 0.0)
    change_pct = summary.get("change_pct")

    new_gain = float(bridge.get("new_entity_gain") or 0.0)
    expansion = float(bridge.get("existing_entity_expansion") or 0.0)
    contraction = float(bridge.get("existing_entity_contraction") or 0.0)
    lost_drag = float(bridge.get("lost_entity_drag") or 0.0)

    pct_txt = "N/A"
    if isinstance(change_pct, (int, float)):
        pct_txt = f"{change_pct:+.1f}%"

    lines = [
        f"Revenue Bridge ({p1} -> {p2})",
        f"- Start ({p1}): {_fmt_indian(total_1, currency)}",
        f"- End   ({p2}): {_fmt_indian(total_2, currency)}",
        f"- Net change: {_fmt_indian(net_change, currency)} ({pct_txt})",
        "- Decomposition:",
        f"  + New entity gain: {_fmt_indian(new_gain, currency)}",
        f"  + Existing entity expansion: {_fmt_indian(expansion, currency)}",
        f"  - Existing entity contraction: {_fmt_indian(contraction, currency)}",
        f"  - Lost entity drag: {_fmt_indian(lost_drag, currency)}",
    ]

    positives = mix_shift.get("top_positive") or []
    negatives = mix_shift.get("top_negative") or []

    if positives:
        lines.append("- Top positive mix shifts:")
        for row in positives[:3]:
            name = str(row.get("name") or "?")
            share_delta = float(row.get("share_delta_pct") or 0.0)
            lines.append(f"  + {name}: {share_delta:+.2f}pp")

    if negatives:
        lines.append("- Top negative mix shifts:")
        for row in negatives[:3]:
            name = str(row.get("name") or "?")
            share_delta = float(row.get("share_delta_pct") or 0.0)
            lines.append(f"  - {name}: {share_delta:+.2f}pp")

    return lines


def _inject_revenue_bridge(formatted_text: str, lines: list[str]) -> str:
    if not lines:
        return formatted_text
    block = "\n\n" + "\n".join(lines)
    marker = "\n\n Token Usage "
    if marker in formatted_text:
        head, tail = formatted_text.split(marker, 1)
        return head + block + marker + tail
    return formatted_text + block


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _find_numeric_by_tokens(
    row: dict[str, Any], tokens: tuple[str, ...]
) -> tuple[str, float] | None:
    for key, value in row.items():
        k = str(key).lower()
        if any(t in k for t in tokens):
            f = _safe_float(value)
            return str(key), f
    return None


def _extract_discount_leak(rows: list[dict[str, Any]]) -> tuple[float, int]:
    leak = 0.0
    hits = 0
    for row in rows:
        if not isinstance(row, dict):
            continue

        direct = _find_numeric_by_tokens(
            row,
            ("discount_amount", "discount_value", "discount", "coupon", "markdown"),
        )
        if direct and direct[1] > 0:
            leak += direct[1]
            hits += 1
            continue

        rate = _find_numeric_by_tokens(
            row,
            ("discount_rate", "discount_pct", "discount_percent"),
        )
        base = _find_numeric_by_tokens(
            row,
            ("revenue", "sales", "amount", "gross", "total", "value"),
        )
        if rate and base and base[1] > 0:
            r = rate[1]
            if 0 < r <= 1:
                r *= 100.0
            if r > 0:
                leak += base[1] * (r / 100.0)
                hits += 1

    return max(0.0, leak), hits


def _extract_return_leak(rows: list[dict[str, Any]]) -> tuple[float, int]:
    leak = 0.0
    hits = 0
    for row in rows:
        if not isinstance(row, dict):
            continue

        direct = _find_numeric_by_tokens(
            row,
            ("return_amount", "refund_amount", "returns", "refund", "chargeback"),
        )
        if direct and direct[1] > 0:
            leak += direct[1]
            hits += 1
            continue

        rate = _find_numeric_by_tokens(
            row,
            ("return_rate", "refund_rate", "cancel_rate", "return_pct", "refund_pct"),
        )
        base = _find_numeric_by_tokens(
            row,
            ("revenue", "sales", "amount", "gross", "total", "value"),
        )
        if rate and base and base[1] > 0:
            r = rate[1]
            if 0 < r <= 1:
                r *= 100.0
            if r > 0:
                leak += base[1] * (r / 100.0)
                hits += 1

    return max(0.0, leak), hits


def _extract_low_margin_mix_leak(
    rows: list[dict[str, Any]], decomposition: dict[str, Any]
) -> tuple[float, int, str]:
    revenue_margin_pairs: list[tuple[float, float]] = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        revenue = _find_numeric_by_tokens(
            row,
            ("revenue", "sales", "amount", "gross", "total", "value"),
        )
        if not revenue or revenue[1] <= 0:
            continue

        margin_pct = _find_numeric_by_tokens(
            row,
            ("margin_pct", "margin_percent", "margin_rate", "gm_pct", "profit_pct"),
        )
        if margin_pct:
            m = margin_pct[1]
            if 0 < m <= 1:
                m *= 100.0
            revenue_margin_pairs.append((revenue[1], m))
            continue

        margin_amt = _find_numeric_by_tokens(
            row,
            ("margin", "profit", "gross_profit", "contribution"),
        )
        if margin_amt:
            m = (margin_amt[1] / revenue[1]) * 100.0 if revenue[1] > 0 else 0.0
            revenue_margin_pairs.append((revenue[1], m))

    if revenue_margin_pairs:
        sorted_margins = sorted(m for _, m in revenue_margin_pairs)
        idx = int(0.75 * (len(sorted_margins) - 1))
        target_margin = max(0.0, sorted_margins[idx])
        leak = 0.0
        for rev, m in revenue_margin_pairs:
            gap = max(0.0, target_margin - m)
            leak += rev * (gap / 100.0)
        return max(0.0, leak), len(revenue_margin_pairs), "margin_profile"

    if isinstance(decomposition, dict) and decomposition.get("applies"):
        bridge = decomposition.get("bridge") or {}
        contraction = _safe_float(bridge.get("existing_entity_contraction"))
        lost_drag = _safe_float(bridge.get("lost_entity_drag"))
        proxy = max(0.0, contraction + lost_drag)
        if proxy > 0:
            return proxy, 1, "decomposition_proxy"

    return 0.0, 0, "insufficient_fields"


def _build_margin_leak_finder(
    rows: list[dict[str, Any]],
    decomposition: dict[str, Any],
) -> dict[str, Any]:
    discount_leak, discount_hits = _extract_discount_leak(rows)
    return_leak, return_hits = _extract_return_leak(rows)
    mix_leak, mix_hits, mix_source = _extract_low_margin_mix_leak(rows, decomposition)

    total_leak = discount_leak + return_leak + mix_leak
    signal_count = sum(
        1
        for v in (
            discount_leak,
            return_leak,
            mix_leak,
        )
        if v > 0
    )

    confidence = "low"
    if signal_count >= 2 and (discount_hits + return_hits + mix_hits) >= 3:
        confidence = "high"
    elif signal_count >= 1:
        confidence = "medium"

    return {
        "applies": bool(signal_count > 0),
        "total_leak": total_leak,
        "components": {
            "discount_leak": discount_leak,
            "return_leak": return_leak,
            "low_margin_mix_leak": mix_leak,
        },
        "evidence": {
            "discount_hits": discount_hits,
            "return_hits": return_hits,
            "mix_hits": mix_hits,
            "mix_source": mix_source,
        },
        "confidence": confidence,
    }


def _margin_leak_lines(finder: dict[str, Any], currency: str) -> list[str]:
    if not isinstance(finder, dict):
        return []

    components = finder.get("components") or {}
    discount_leak = _safe_float(components.get("discount_leak"))
    return_leak = _safe_float(components.get("return_leak"))
    mix_leak = _safe_float(components.get("low_margin_mix_leak"))
    total_leak = _safe_float(finder.get("total_leak"))
    confidence = str(finder.get("confidence") or "low").upper()

    lines = ["Margin Leak Finder"]
    if total_leak <= 0:
        lines.append(
            "- Leak estimate: no measurable leak signal from current result fields."
        )
        lines.append(
            "- Tip: include discount/refund/profit columns to improve leak attribution precision."
        )
        return lines

    lines.extend(
        [
            f"- Estimated leak value: {_fmt_indian(total_leak, currency)}",
            f"- High discount leak: {_fmt_indian(discount_leak, currency)}",
            f"- High return/refund leak: {_fmt_indian(return_leak, currency)}",
            f"- Low-margin mix-shift leak: {_fmt_indian(mix_leak, currency)}",
            f"- Confidence: {confidence}",
        ]
    )
    return lines


def _inject_margin_leak(formatted_text: str, lines: list[str]) -> str:
    if not lines:
        return formatted_text
    block = "\n\n" + "\n".join(lines)
    marker = "\n\n Token Usage "
    if marker in formatted_text:
        head, tail = formatted_text.split(marker, 1)
        return head + block + marker + tail
    return formatted_text + block


def _compress_rows(rows: list[dict[str, Any]], limit: int = 12) -> list[dict[str, Any]]:
    if not rows:
        return []
    if len(rows) <= limit:
        return rows
    head = max(3, limit // 2)
    tail = max(3, limit - head)
    return rows[:head] + rows[-tail:]


def _should_skip_ai_insights(
    parsed: dict[str, Any], rows: list[dict[str, Any]], anomalies: dict[str, Any]
) -> bool:
    if not rows:
        return True
    if len(rows) < 3:
        return True
    qt = (parsed or {}).get("query_type", "")
    has_anomalies = bool((anomalies or {}).get("items"))
    if has_anomalies:
        return False
    if len(rows) == 1 and isinstance(rows[0], dict) and "name" not in rows[0]:
        return True
    if qt in ("aggregate", "time_series", "forecast") and len(rows) <= 3:
        return True
    return False


def _call_ai_insights(
    user_query: str,
    parsed: dict[str, Any],
    rows: list[dict[str, Any]],
    anomalies: dict[str, Any],
) -> tuple[list[str], dict]:
    if not GROQ_API_TOKEN:
        return [], {}

    compact_rows = _compress_rows(rows, limit=12)
    anomaly_items = (anomalies or {}).get("items", [])[:5]
    prompt = (
        'Return ONLY JSON: {"insights":["...","..."]}. '
        "Write 2-3 concise business insights; use anomaly signals when present; avoid invented causality.\n"
        f"User query: {user_query}\n"
        f"Parsed intent: {json.dumps(parsed, ensure_ascii=False)}\n"
        f"Sample rows: {json.dumps(compact_rows, ensure_ascii=False)}\n"
        f"Anomalies: {json.dumps(anomaly_items, ensure_ascii=False)}\n"
    )
    messages = [{"role": "user", "content": prompt}]

    data = post_chat_completion(
        api_url=GROQ_URL,
        api_token=GROQ_API_TOKEN,
        payload={
            "model": INSIGHTS_MODEL,
            "messages": messages,
            "temperature": 0.0,
            "max_tokens": calc_max_tokens(
                messages, task="insights", model=INSIGHTS_MODEL
            ),
        },
        timeout=30,
        retry_without_reasoning_effort=False,
    )
    usage = data.get("usage", {})
    raw = clean_model_text(data["choices"][0]["message"]["content"], strip_fences=True)
    try:
        obj = json.loads(raw)
    except Exception:
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if not m:
            return [], usage
        obj = json.loads(m.group(0))
    lines = [str(x).strip() for x in (obj.get("insights") or []) if str(x).strip()]
    return lines[:3], usage


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    from services.format_result_service import run_format_result
    from types import SimpleNamespace

    step_module = sys.modules.get(__name__) or SimpleNamespace(**globals())
    await run_format_result(step_module, input_data, ctx)
