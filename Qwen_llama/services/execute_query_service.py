"""Service orchestration for execute_query_step."""

from __future__ import annotations

from types import ModuleType
from typing import Any

from motia import FlowContext
from utils.sql_memory import store_sql_example


async def run_execute_query(
    step: ModuleType, input_data: Any, ctx: FlowContext[Any]
) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})
    generated_sql = input_data.get("generated_sql")

    if not generated_sql:
        qs = await ctx.state.get("queries", query_id)
        await step._error(ctx, qs, query_id, "No SQL was generated.")
        return

    qt = parsed.get("query_type", "top_n")
    trs = parsed.get("time_ranges", [])

    generated_sql, was_rewritten = step._rewrite_extract_to_range(generated_sql)
    if was_rewritten:
        ctx.logger.info("Rewrote EXTRACT->range", {"queryId": query_id})
    if step._has_extract(generated_sql):
        ctx.logger.warn("SQL still contains EXTRACT", {"queryId": query_id})

    generated_sql, between_rewritten = step._rewrite_between_to_range(generated_sql)
    if between_rewritten:
        ctx.logger.info(
            "Rewrote BETWEEN->range for datetime col", {"queryId": query_id}
        )

    bigint_map = step._build_bigint_datetime_map()
    if bigint_map:
        generated_sql, epoch_rewritten = step._rewrite_bigint_epoch_cols(
            generated_sql, bigint_map
        )
        if epoch_rewritten:
            ctx.logger.info(
                "Rewrote BIGINT epoch datetime columns",
                {"queryId": query_id, "cols": list(bigint_map.keys())},
            )

    if qt == "time_series" and not step._HAS_GROUP_BY_RE.search(generated_sql):
        repaired = step._repair_add_missing_group_by(generated_sql, "")
        if repaired:
            ctx.logger.info(
                "Auto-added missing GROUP BY for time_series", {"queryId": query_id}
            )
            generated_sql = repaired

    ctx.logger.info("Executing SQL", {"queryId": query_id, "query_type": qt})

    params = step._build_params(generated_sql, parsed)
    ctx.logger.info("SQL params", {"queryId": query_id, "params": str(params)})

    ok, errors, warnings = step.validate_query(generated_sql, params)
    if warnings:
        ctx.logger.warn(
            "SQL validation warnings", {"queryId": query_id, "warnings": warnings}
        )
    if not ok:
        qs = await ctx.state.get("queries", query_id)
        msg = "SQL validation failed: " + " | ".join(errors)
        await step._error(ctx, qs, query_id, msg)
        return

    try:
        rows = step._run_sql(generated_sql, params)
        results = step._rows_to_dicts(rows, parsed)
    except Exception as exc:
        err_str = str(exc)
        repaired_sql = step._repair_add_missing_group_by(generated_sql, err_str)
        if not repaired_sql:
            repaired_sql = step._repair_group_by_missing_name(generated_sql, err_str)
        if not repaired_sql:
            qs = await ctx.state.get("queries", query_id)
            await step._error(ctx, qs, query_id, f"SQL execution failed: {exc}")
            return
        try:
            rows = step._run_sql(repaired_sql, params)
            results = step._rows_to_dicts(rows, parsed)
            generated_sql = repaired_sql
            ctx.logger.warn("Repaired GROUP BY and retried", {"queryId": query_id})
        except Exception as exc2:
            qs = await ctx.state.get("queries", query_id)
            await step._error(ctx, qs, query_id, f"SQL execution failed: {exc2}")
            return

    ranked_types = {
        "top_n",
        "bottom_n",
        "threshold",
        "intersection",
        "zero_filter",
        "growth_ranking",
        "comparison",
    }
    is_top_percent_share = bool(parsed.get("_top_percent_share"))
    if (
        qt in ranked_types
        and results
        and "name" not in results[0]
        and not is_top_percent_share
    ):
        qs = await ctx.state.get("queries", query_id)
        await step._error(
            ctx,
            qs,
            query_id,
            f"SQL returned scalar instead of rows for query_type={qt}.",
        )
        return

    stored = store_sql_example(
        user_query=user_query,
        generated_sql=generated_sql,
        parsed=parsed,
        result_rows=len(results),
    )
    if stored:
        ctx.logger.info("Stored SQL memory example", {"queryId": query_id})

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = step.datetime.now(step.timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "status": "executed",
                "results": results,
                "generated_sql": generated_sql,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "executed": now_iso},
            },
        )

    period_labels = [step._period_label(t["start"], t["end"]) for t in trs]
    start_date = trs[0]["start"] if trs else ""
    end_date = trs[-1]["end"] if trs else ""

    next_topic = "query::forecast" if qt == "forecast" else "query::detect.anomalies"

    await ctx.enqueue(
        {
            "topic": next_topic,
            "data": {
                "queryId": query_id,
                "query": user_query,
                "parsed": parsed,
                "results": results,
                "period_labels": period_labels,
                "startDate": start_date,
                "endDate": end_date,
            },
        }
    )
