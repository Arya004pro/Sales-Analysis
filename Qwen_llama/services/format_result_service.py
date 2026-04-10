"""Service orchestration for format_result_step."""

from __future__ import annotations

from types import ModuleType
from typing import Any

from motia import FlowContext


async def run_format_result(
    step: ModuleType, input_data: Any, ctx: FlowContext[Any]
) -> None:
    import datetime as _dt

    _infer_currency = step._infer_currency
    _entity_label = step._entity_label
    _metric_label = step._metric_label
    _should_skip_ai_insights = step._should_skip_ai_insights
    _call_ai_insights = step._call_ai_insights
    _token_summary = step._token_summary
    _format_bucket_label = step._format_bucket_label
    _fmt_indian = step._fmt_indian
    _insights_time_series = step._insights_time_series
    _make_line_chart = step._make_line_chart
    _insights_ranked_by_period = step._insights_ranked_by_period
    _bar = step._bar
    _display_entity_name = step._display_entity_name
    _fmt = step._fmt
    _delta_str = step._delta_str
    _make_base = step._make_base
    _cmp_tooltip_fn = step._cmp_tooltip_fn
    _ranked_header = step._ranked_header
    _is_binary_status_split = step._is_binary_status_split
    _insights_binary_status_split = step._insights_binary_status_split
    _insights_ranked = step._insights_ranked
    _anomaly_insights = step._anomaly_insights
    _inject_insight_block = step._inject_insight_block
    _inject_ai_insights = step._inject_ai_insights

    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})
    results = input_data.get("results", [])
    anomalies = input_data.get("anomalies", {})
    auto_insights = input_data.get("auto_insights", [])
    period_labels = input_data.get("period_labels", [])
    start_date = input_data.get("startDate", "")
    end_date = input_data.get("endDate", "")

    qt = parsed.get("query_type", "top_n")
    entity = parsed.get("entity", "")
    metric = parsed.get("metric", "value")
    metric_display = parsed.get("semantic_metric") or metric
    top_n = parsed.get("top_n", 5)
    disable_limit = bool(parsed.get("_disable_limit"))
    thr = parsed.get("threshold")
    bucket = parsed.get("time_bucket", "month")

    currency = _infer_currency(metric_display, user_query)
    p = currency
    elabel = _entity_label(entity) or entity or "items"
    mlabel = _metric_label(metric_display, "")

    _trs = parsed.get("time_ranges", [])
    if _trs and _trs[0].get("label"):
        _period_phrase = f"in {_trs[0]['label']}"
    elif start_date and end_date:
        _period_phrase = f"between {start_date} and {end_date}"
    else:
        _period_phrase = ""

    qs = await ctx.state.get("queries", query_id)
    if not user_query and qs:
        user_query = qs.get("query", "")
    if not auto_insights and qs:
        auto_insights = qs.get("auto_insights", []) or []
    token_usage = (qs or {}).get("token_usage", [])
    token_totals = (qs or {}).get("token_totals", {})
    chart_config = None

    if not auto_insights:
        if _should_skip_ai_insights(parsed, results, anomalies):
            ctx.logger.info(
                "AI insights skipped in formatter",
                {
                    "queryId": query_id,
                    "rows": len(results),
                    "query_type": parsed.get("query_type"),
                },
            )
        else:
            try:
                ai_insights, usage = _call_ai_insights(
                    user_query, parsed, results, anomalies
                )
                auto_insights = ai_insights
                if usage:
                    step.log_tokens(
                        ctx,
                        query_id,
                        "GenerateInsights",
                        step.INSIGHTS_MODEL,
                        usage,
                    )
                    await step.add_tokens_to_state(
                        ctx,
                        query_id,
                        "GenerateInsights",
                        step.INSIGHTS_MODEL,
                        usage,
                    )
                    qs = await ctx.state.get("queries", query_id)
                    token_usage = (qs or {}).get("token_usage", token_usage)
                    token_totals = (qs or {}).get("token_totals", token_totals)
            except Exception as exc:
                ctx.logger.warn(
                    "Formatter AI insights failed; continuing",
                    {
                        "queryId": query_id,
                        "error": str(exc),
                    },
                )

    ctx.logger.info(
        " Formatting", {"queryId": query_id, "query_type": qt, "rows": len(results)}
    )

    has_delta = results and "delta" in results[0]
    has_value2 = results and "value2" in results[0] and not has_delta
    is_rank_within_time = (
        bool(parsed.get("_rank_within_time"))
        and bool(results)
        and "period" in results[0]
    )
    is_scalar = results and len(results) == 1 and "name" not in results[0]
    is_empty = not results or (is_scalar and results[0].get("value") is None)

    if is_empty:
        if qt == "time_series":
            text = (
                f"No {mlabel} data found {_period_phrase}. "
                "The dataset may not cover this time range."
            )
        elif qt == "zero_filter":
            text = (
                f"No {elabel} with zero {mlabel} found "
                f"between {start_date} and {end_date}."
            )
        elif qt == "threshold" and thr:
            unit = "%" if thr.get("type") == "percentage" else f" {mlabel}"
            text = (
                f"No {elabel} matched the filter "
                f"({mlabel} > {thr['value']}{unit}) "
                f"between {start_date} and {end_date}."
            )
        elif period_labels and len(period_labels) >= 2:
            text = f"No data found for {period_labels[1]}."
        else:
            text = "No data available for the selected period."
        formatted_text = text + _token_summary(token_usage, token_totals)
        items = []

    elif qt == "time_series":
        period_str = (
            _period_phrase if _period_phrase else f"between {start_date} and {end_date}"
        )
        _bucket_map = {
            "year": "Yearly",
            "month": "Monthly",
            "quarter": "Quarterly",
            "week": "Weekly",
            "day": "Daily",
        }
        bucket_label = _bucket_map.get(bucket, bucket.capitalize())
        header = f"{bucket_label} {mlabel} trend {period_str}:"

        raw_labels = [r.get("name", "?") for r in results]
        values = [r.get("value", 0) or 0 for r in results]
        labels = [_format_bucket_label(lbl, bucket) for lbl in raw_labels]

        items = []
        for lbl, val in zip(labels, values):
            items.append(
                {"period": lbl, "value": _fmt_indian(val, p), "raw_value": val}
            )

        lines = [header]
        if values:
            total = sum(values)
            average = total / len(values)
            lines.append(f"Points: {len(values)}")
            lines.append(f"Total: {_fmt_indian(total, p)}")
            lines.append(f"Average: {_fmt_indian(average, p)}")

            if len(values) <= 24:
                lines.append("")
                lines.append(f"{'Period':<14}  {'Value':>16}")
                lines.append(f"{'-' * 14}  {'-' * 16}")
                for lbl, val in zip(labels, values):
                    lines.append(f"{lbl:<14}  {_fmt_indian(val, p):>16}")
            elif len(values) <= 48:
                preview = min(3, len(values))
                lines.append("")
                lines.append("Preview (first and last points):")
                for lbl, val in zip(labels[:preview], values[:preview]):
                    lines.append(f"- {lbl}: {_fmt_indian(val, p)}")
                lines.append("- ...")
                for lbl, val in zip(labels[-preview:], values[-preview:]):
                    lines.append(f"- {lbl}: {_fmt_indian(val, p)}")
            else:
                lines.append("")
                lines.append(
                    "Detailed monthly values are shown in the chart/export table."
                )

        insights = _insights_time_series(labels, values, p)
        if insights:
            lines.append("")
            lines.append("Insights:")
            lines.extend([f"- {x}" for x in insights])
        formatted_text = "\n".join(lines) + _token_summary(token_usage, token_totals)

        if labels and values:
            chart_config = _make_line_chart(
                labels,
                values,
                metric_display,
                currency,
                user_query or f"{bucket_label} {mlabel} trend",
                period_str,
                bucket,
            )

    elif qt == "forecast":
        fr = parsed.get("_forecast_result", {})
        goal_result = (
            parsed.get("_goal_tracking_result")
            if isinstance(parsed.get("_goal_tracking_result"), dict)
            else None
        )
        hist_labels = fr.get("hist_labels", [])
        hist_values = fr.get("hist_values", [])
        fc_labels = fr.get("fc_labels", [])
        fc_values = fr.get("fc_values", [])
        fc_lower = fr.get("fc_lower", [])
        fc_upper = fr.get("fc_upper", [])
        method = fr.get("method", "auto")
        trend_pct = fr.get("trend_pct", 0.0)
        rmse = fr.get("rmse", 0.0)
        conf = fr.get("confidence_pct", 80.0)
        periods = fr.get("periods", 3)

        if goal_result:
            target = float(goal_result.get("target_value") or 0.0)
            actual_to_date = float(goal_result.get("actual_to_date") or 0.0)
            projected_total = float(goal_result.get("projected_total") or 0.0)
            projected_remaining = float(goal_result.get("projected_remaining") or 0.0)
            gap = float(goal_result.get("gap") or 0.0)
            remaining_periods = int(goal_result.get("remaining_periods") or 0)
            required_per_period = float(goal_result.get("required_per_period") or 0.0)
            forecast_avg_per_period = float(
                goal_result.get("forecast_avg_per_period") or 0.0
            )
            pace_ratio = float(goal_result.get("pace_ratio") or 0.0)
            on_track = bool(goal_result.get("on_track"))
            horizon_label = str(goal_result.get("horizon_label") or "target horizon")
            status = str(goal_result.get("status") or "ok")

            status_text = "ON TRACK" if on_track else "OFF TRACK"
            lines = [
                f"Goal tracking ({horizon_label}): {status_text}",
                "",
                f"Metric: {mlabel}",
                f"Target: {_fmt_indian(target, p)}",
                f"Actual to date: {_fmt_indian(actual_to_date, p)}",
                f"Projected remaining {bucket}(s): {_fmt_indian(projected_remaining, p)}",
                f"Projected by {horizon_label}: {_fmt_indian(projected_total, p)}",
            ]
            if gap >= 0:
                lines.append(f"Projected surplus: {_fmt_indian(gap, p)}")
            else:
                lines.append(f"Projected shortfall: {_fmt_indian(abs(gap), p)}")

            if remaining_periods > 0:
                lines.append(f"Remaining {bucket}(s): {remaining_periods}")
                lines.append(
                    f"Required average per remaining {bucket}: {_fmt_indian(required_per_period, p)}"
                )
                lines.append(
                    f"Forecast average per remaining {bucket}: {_fmt_indian(forecast_avg_per_period, p)}"
                )
                lines.append(f"Pace vs required: {pace_ratio * 100:.1f}%")

            if status == "insufficient_history":
                lines.append(
                    "Note: Forecast confidence is limited due to very few historical points."
                )

            if fc_labels:
                lines.append("")
                lines.append("Projected values:")
                for lbl, val, lo, hi in zip(fc_labels, fc_values, fc_lower, fc_upper):
                    lines.append(
                        f"  {lbl:<14}  {_fmt_indian(val, p):>16}  [{_fmt_indian(lo, p)} - {_fmt_indian(hi, p)}]"
                    )

            items = [
                {
                    "label": "Target",
                    "value": _fmt_indian(target, p),
                    "raw_value": target,
                },
                {
                    "label": "Actual to date",
                    "value": _fmt_indian(actual_to_date, p),
                    "raw_value": actual_to_date,
                },
                {
                    "label": "Projected total",
                    "value": _fmt_indian(projected_total, p),
                    "raw_value": projected_total,
                },
                {"label": "Gap", "value": _fmt_indian(gap, p), "raw_value": gap},
            ]
            formatted_text = "\n".join(lines) + _token_summary(
                token_usage, token_totals
            )
            chart_config = (qs or {}).get("chart_config") or input_data.get(
                "_chart_config"
            )
        else:
            if start_date and end_date:
                training_str = f"training: {start_date} to {end_date}"
            elif _period_phrase:
                training_str = f"training: {_period_phrase.replace('in ', '')}"
            else:
                training_str = "training: selected historical period"
            header = f"{mlabel} Forecast - next {periods} {bucket}(s) ({training_str})"

            lines = [header, ""]
            lines.append(f"Historical points: {len(hist_labels)}")
            lines.append(
                f"Method: {str(method).capitalize()}, Trend: {'UP' if float(trend_pct or 0) >= 0 else 'DOWN'} {abs(float(trend_pct or 0)):.1f}%/period"
            )
            lines.append(
                f"Confidence band: {int(float(conf or 80))}%  |  RMSE: {_fmt_indian(rmse, p)}"
            )
            lines.append("")
            lines.append("Projected values:")
            for lbl, val, lo, hi in zip(fc_labels, fc_values, fc_lower, fc_upper):
                lines.append(
                    f"  {lbl:<14}  {_fmt_indian(val, p):>16}  [{_fmt_indian(lo, p)} - {_fmt_indian(hi, p)}]"
                )

            items = [
                {
                    "period": lbl,
                    "value": _fmt_indian(v, p),
                    "raw_value": v,
                    "lower": _fmt_indian(lo, p),
                    "upper": _fmt_indian(hi, p),
                    "is_forecast": True,
                }
                for lbl, v, lo, hi in zip(fc_labels, fc_values, fc_lower, fc_upper)
            ]

            formatted_text = "\n".join(lines) + _token_summary(
                token_usage, token_totals
            )
            chart_config = (qs or {}).get("chart_config") or input_data.get(
                "_chart_config"
            )

    elif is_rank_within_time:
        period_str = (
            _period_phrase if _period_phrase else f"between {start_date} and {end_date}"
        )
        direction = "Top" if qt != "bottom_n" else "Bottom"
        header = f"{direction} {top_n} {elabel.title()} by {mlabel} for each {bucket} {period_str}:"

        grouped: dict[str, list[dict[str, Any]]] = {}
        ordered_periods: list[str] = []
        items = []
        for row in results:
            period = str(row.get("period", "?")).strip()
            if period not in grouped:
                grouped[period] = []
                ordered_periods.append(period)
            grouped[period].append(row)

        lines = [header]
        for period in ordered_periods:
            lines.append("")
            lines.append(f"{period}:")
            ranked_rows = grouped[period]
            for idx, row in enumerate(ranked_rows, 1):
                name = _display_entity_name(row.get("name", "?"), entity)
                value = row.get("value", 0) or 0
                lines.append(f"{idx}. {name}  {_fmt_indian(value, p)}")
                items.append(
                    {
                        "period": period,
                        "rank": idx,
                        "name": name,
                        "value": _fmt_indian(value, p),
                        "raw_value": value,
                    }
                )

        insights = _insights_ranked_by_period(items, p)
        if insights:
            lines.append("")
            lines.append("Insights:")
            lines.extend([f"- {x}" for x in insights])

        formatted_text = "\n".join(lines) + _token_summary(token_usage, token_totals)

        labels = [f"{it['period']} | {it['name']}" for it in items]
        values = [it.get("raw_value", 0) or 0 for it in items]
        if labels and values:
            chart_config = _bar(
                labels,
                values,
                metric_display,
                currency,
                entity,
                user_query or f"{direction} {elabel} by {mlabel} per {bucket}",
                period_str,
            )

    elif is_scalar:
        v = results[0]["value"]
        period_str = (
            _period_phrase if _period_phrase else f"between {start_date} and {end_date}"
        )
        val_s = _fmt_indian(v, p)
        formatted_text = f"Total {mlabel} {period_str} is {val_s}"
        items = [{"label": f"Total {mlabel}", "value": val_s}]

    elif has_delta:
        p1 = period_labels[0] if len(period_labels) > 0 else "Period 1"
        p2 = period_labels[1] if len(period_labels) > 1 else "Period 2"
        is_scalar_delta = (not entity) and len(results) == 1
        if is_scalar_delta:
            row = results[0]
            v1 = row.get("value1", 0)
            v2 = row.get("value2", 0)
            d = row.get("delta", 0)
            sign = "+" if d >= 0 else ""
            pct = (d / v1 * 100) if v1 != 0 else float("inf")
            pct_s = f"{sign}{pct:.1f}%" if pct != float("inf") else "new"

            lines = [
                f"{mlabel} comparison: {p1} vs {p2}",
                f"- {p1}: {_fmt_indian(v1, p)}",
                f"- {p2}: {_fmt_indian(v2, p)}",
                f"- Change: {sign}{_fmt_indian(abs(d), p)} ({pct_s})",
            ]
            formatted_text = "\n".join(lines) + _token_summary(
                token_usage, token_totals
            )
            items = [
                {
                    f"{p1}_value": _fmt(v1, p),
                    f"{p2}_value": _fmt(v2, p),
                    "delta": _delta_str(v1, v2, p),
                }
            ]
            chart_config = _bar(
                [p1, p2],
                [v1 or 0, v2 or 0],
                metric_display,
                currency,
                "period",
                user_query or f"{mlabel} comparison",
                f"{p1} vs {p2}",
            )
        else:
            direction = "highest" if qt != "bottom_n" else "lowest"
            header = f" {elabel.title()} with {direction} {mlabel} growth ({p1}  {p2}):"
            items = []
            for i, row in enumerate(results, 1):
                name = _display_entity_name(row.get("name", "?"), entity)
                v1, v2, d = (
                    row.get("value1", 0),
                    row.get("value2", 0),
                    row.get("delta", 0),
                )
                sign = "+" if d >= 0 else ""
                pct = (d / v1 * 100) if v1 != 0 else float("inf")
                pct_s = f"{sign}{pct:.1f}%" if pct != float("inf") else "new entry"
                header += (
                    f"\n{i}. {name}"
                    f"\n   {p1}: {_fmt_indian(v1, p)}"
                    f"\n   {p2}: {_fmt_indian(v2, p)}"
                    f"\n   Growth: {sign}{_fmt_indian(abs(d), p)} ({pct_s})"
                )
                items.append({"rank": i, "name": name, "delta": d})
            insights = _insights_ranked(items, p)
            insight_txt = ""
            if insights:
                insight_txt = "\n\nInsights:\n" + "\n".join(f"- {x}" for x in insights)
            formatted_text = (
                header + insight_txt + _token_summary(token_usage, token_totals)
            )
            names = [_display_entity_name(r.get("name", "?"), entity) for r in results]
            deltas = [r.get("delta", 0) for r in results]
            chart_config = _bar(
                names,
                deltas,
                metric_display,
                currency,
                entity,
                user_query or f"{elabel.title()} by {mlabel} growth: {p1}{p2}",
                f"Delta in {mlabel} ({p1}  {p2})",
            )

    elif has_value2:
        p1 = period_labels[0] if len(period_labels) > 0 else "Period 1"
        p2 = period_labels[1] if len(period_labels) > 1 else "Period 2"
        header = f" Top {top_n} {elabel} by {mlabel}: {p1} vs {p2}"
        col_w = max((len(r.get("name", "")) for r in results), default=20)
        col_w = max(col_w, 20)
        sep = "" * (col_w + 44)
        hdr = f"  {'#':>3}  {'Name':<{col_w}}  {p1:>16}  {p2:>16}  {' Change':>14}"
        lines = [header, sep, hdr, sep]
        items = []
        for i, row in enumerate(results, 1):
            name = _display_entity_name(row.get("name", "?"), entity)
            v1, v2 = row.get("value1"), row.get("value2")
            d = _delta_str(v1, v2, p)
            lines.append(
                f"  {i:>3}. {name:<{col_w}}  {_fmt(v1, p):>16}  {_fmt(v2, p):>16}  {d:>14}"
            )
            items.append(
                {
                    "rank": i,
                    "name": name,
                    f"{p1}_value": _fmt(v1, p),
                    f"{p2}_value": _fmt(v2, p),
                    "delta": d,
                }
            )
        formatted_text = "\n".join(lines) + _token_summary(token_usage, token_totals)
        names = [_display_entity_name(r.get("name", "?"), entity) for r in results]
        vals1 = [r.get("value1", 0) or 0 for r in results]
        vals2 = [r.get("value2", 0) or 0 for r in results]
        base_cmp = _make_base(
            metric_display, currency, entity, legend=True, index_axis="y"
        )
        base_cmp["plugins"]["tooltip"]["callbacks"] = {
            "label": _cmp_tooltip_fn(metric_display, currency)
        }
        cmp_cfg = {
            "type": "bar",
            "data": {
                "labels": names,
                "datasets": [
                    {
                        "label": p1,
                        "data": vals1,
                        "backgroundColor": step._PALETTE[0],
                        "borderColor": step._BORDERS[0],
                        "borderWidth": 1,
                        "borderRadius": 3,
                    },
                    {
                        "label": p2,
                        "data": vals2,
                        "backgroundColor": step._PALETTE[1],
                        "borderColor": step._BORDERS[1],
                        "borderWidth": 1,
                        "borderRadius": 3,
                    },
                ],
            },
            "options": base_cmp,
        }
        chart_config = {
            "title": user_query or f"{mlabel} comparison: {p1} vs {p2}",
            "subtitle": f"{p1} vs {p2}",
            "prefix": p,
            "config": cmp_cfg,
        }

    else:
        names, values = [], []
        if qt == "zero_filter":
            header = (
                f"{len(results)} {elabel} had zero {mlabel} "
                f"between {start_date} and {end_date}:"
            )
            items = []
            for i, row in enumerate(results, 1):
                name = _display_entity_name(row.get("name", "?"), entity)
                header += f"\n{i}. {name}"
                items.append({"rank": i, "name": name, "value": "0"})
        else:
            period_str = (
                _period_phrase
                if _period_phrase
                else f"between {start_date} and {end_date}"
            )

            if qt == "threshold" and thr:
                thr_op = thr.get("operator", "gt")
                thr_type = thr.get("type", "absolute")
                thr_val = thr.get("value", 0)
                direction = "less than" if thr_op == "lt" else "more than"
                thr_val_str = (
                    f"{thr_val:.0f}% of total"
                    if thr_type == "percentage"
                    else _fmt_indian(thr_val, p, decimals=0)
                )
                header = (
                    f"{len(results)} {elabel} where {mlabel} contributed "
                    f"{direction} {thr_val_str} "
                    f"between {start_date} and {end_date}:"
                )
            elif qt == "intersection":
                p1 = period_labels[0] if len(period_labels) > 0 else "Period 1"
                p2 = period_labels[1] if len(period_labels) > 1 else "Period 2"
                header = f" {elabel.title()} present in BOTH {p1} AND {p2} (combined {mlabel}):"
            else:
                header = _ranked_header(
                    qt, top_n, len(results), elabel, mlabel, period_str, disable_limit
                )

            items = []
            for i, row in enumerate(results, 1):
                name = _display_entity_name(row.get("name", "?"), entity)
                value = row.get("value", 0) or 0
                val_s = _fmt_indian(value, p)
                header += f"\n{i}. {name}  {val_s}"
                items.append(
                    {"rank": i, "name": name, "value": val_s, "raw_value": value}
                )
                names.append(name)
                values.append(value)

        if items and _is_binary_status_split(entity, items):
            insights = _insights_binary_status_split(entity, items)
        else:
            insights = _insights_ranked(items, p) if items else []
        insight_txt = ""
        if insights:
            insight_txt = "\n\nInsights:\n" + "\n".join(f"- {x}" for x in insights)
        formatted_text = (
            header + insight_txt + _token_summary(token_usage, token_totals)
        )
        if names:
            if disable_limit or len(names) < top_n or len(names) <= 5:
                chart_title = user_query or f"{elabel.title()} breakdown by {mlabel}"
            else:
                rl = "Top" if qt != "bottom_n" else "Bottom"
                chart_title = user_query or f"{rl} {elabel} by {mlabel}"
            chart_config = _bar(
                names,
                values,
                metric_display,
                currency,
                entity,
                chart_title,
                f"{start_date} to {end_date}",
            )

    anomaly_lines = _anomaly_insights(anomalies, p)
    if anomaly_lines:
        formatted_text = _inject_insight_block(formatted_text, anomaly_lines)
    formatted_text = _inject_ai_insights(formatted_text, auto_insights)

    ctx.logger.info(" Formatted", {"queryId": query_id})
    if qs:
        now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "status": "completed",
                "formattedText": formatted_text,
                "formattedItems": items,
                "chart_config": chart_config,
                "anomalies": anomalies,
                "auto_insights": auto_insights,
                "token_usage": token_usage,
                "token_totals": token_totals,
                "completedAt": now_iso,
                "updatedAt": now_iso,
                "status_timestamps": {
                    **prev_ts,
                    "insights_generated": now_iso,
                    "completed": now_iso,
                },
            },
        )
    ctx.logger.info(" Pipeline complete!", {"queryId": query_id})
