"""Service orchestration for text_to_sql_step."""

from __future__ import annotations

from types import ModuleType
from typing import Any

from motia import FlowContext


async def run_text_to_sql(
    step: ModuleType, input_data: Any, ctx: FlowContext[Any]
) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    parsed = input_data.get("parsed", {})

    parsed["_user_query"] = user_query

    qt = parsed.get("query_type", "top_n")
    rank_within_time = bool(parsed.get("_rank_within_time"))
    filters = parsed.get("filters", {}) or {}

    existing_tables = step._get_existing_tables()

    ctx.logger.info(
        "📋 DB tables found",
        {
            "queryId": query_id,
            "tables": list(existing_tables),
        },
    )

    ctx.logger.info(
        "🧬 TextToSQL",
        {
            "queryId": query_id,
            "query_type": qt,
            "time_bucket": parsed.get("time_bucket"),
            "filters": filters,
        },
    )

    generated_sql = None
    usage = {}
    fallback_used = False
    sql_source = "builder"
    is_repeat_entity_count = step._is_repeat_entity_count_intent(parsed, user_query)
    is_top_percent_share = step._top_percent_share_value(parsed, user_query) is not None

    if generated_sql is None and is_repeat_entity_count:
        fb = step._deterministic_repeat_entity_count_fallback(parsed, user_query)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source = "deterministic_repeat_entity_count_fallback"
            ctx.logger.info(
                "✅ Deterministic repeat-entity SQL built", {"queryId": query_id}
            )

    if generated_sql is None and is_top_percent_share:
        fb = step._deterministic_top_percent_share_fallback(parsed, user_query)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source = "deterministic_top_percent_share_fallback"
            ctx.logger.info(
                "✅ Deterministic top-percent-share SQL built", {"queryId": query_id}
            )

    use_builder = (
        qt in ("top_n", "bottom_n", "aggregate", "zero_filter")
        and not rank_within_time
        and not is_repeat_entity_count
        and not is_top_percent_share
    )

    if use_builder:
        built = step.build_sql(parsed)
        if built:
            ok, reason = step._is_safe(built)
            if ok:
                ok2, err = step._explain(built, parsed)
                if ok2:
                    generated_sql = built
                    ctx.logger.info("✅ Builder SQL validated", {"queryId": query_id})
                else:
                    ctx.logger.warn(
                        "⚠️ Builder EXPLAIN failed — falling to LLM",
                        {"queryId": query_id, "error": err},
                    )
            else:
                ctx.logger.warn(
                    "⚠️ Builder safety failed — falling to LLM",
                    {"queryId": query_id, "reason": reason},
                )

    if generated_sql is None and rank_within_time:
        fb = step._deterministic_rank_within_time_fallback(parsed)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source = "deterministic_rank_within_time_fallback"
            ctx.logger.info(
                "✅ Deterministic rank-within-time SQL built", {"queryId": query_id}
            )

    if generated_sql is None and qt in ("comparison", "growth_ranking", "intersection"):
        fb = step._deterministic_comparison_fallback(parsed)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source = "deterministic_comparison_fallback"
            ctx.logger.info(
                "✅ Deterministic comparison SQL built", {"queryId": query_id}
            )

    if generated_sql is None:
        model = step.SQL_GENERATOR_MODEL or step.LLAMA_MODEL
        sql_source = f"llm_{model.split('/')[0]}"

        ctx.logger.info("🤖 LLM SQL generation", {"queryId": query_id, "model": model})

        try:
            schema = step.get_schema_prompt(mode="compact", user_query=user_query)
            prompt = step._build_llm_prompt(user_query, parsed, schema)
            raw, usage = step._call_llm(model, prompt)
            ctx.logger.info("🔬 LLM raw", {"queryId": query_id, "preview": raw[:400]})

            sql = step._extract_sql(raw)
            ok, reason = step._is_safe(sql)
            if ok:
                step._check_filters_present(sql, filters, ctx, query_id)
                ok2, err = step._explain(sql, parsed)
                if ok2:
                    generated_sql = sql
                    ctx.logger.info("✅ LLM SQL validated", {"queryId": query_id})
                else:
                    ctx.logger.warn(
                        "⚠️ LLM EXPLAIN failed — trying SQL self-repair",
                        {"queryId": query_id, "error": err},
                    )
                    repaired_sql, repair_usage = step._try_repair_sql(
                        model=model,
                        user_query=user_query,
                        parsed=parsed,
                        schema=schema,
                        bad_sql=sql,
                        error_msg=err,
                    )
                    if repair_usage:
                        step.log_tokens(
                            ctx, query_id, "TextToSQLRepair", model, repair_usage
                        )
                        await step.add_tokens_to_state(
                            ctx, query_id, "TextToSQLRepair", model, repair_usage
                        )

                    if repaired_sql:
                        generated_sql = repaired_sql
                        ctx.logger.info(
                            "✅ SQL repaired and validated", {"queryId": query_id}
                        )
                    else:
                        ctx.logger.warn(
                            "⚠️ SQL self-repair failed; using deterministic fallback if available",
                            {"queryId": query_id},
                        )
            else:
                ctx.logger.warn(
                    "⚠️ LLM safety failed", {"queryId": query_id, "reason": reason}
                )
        except Exception as exc:
            ctx.logger.error("❌ LLM error", {"queryId": query_id, "error": str(exc)})

        if usage:
            step.log_tokens(ctx, query_id, "TextToSQL", model, usage)
            await step.add_tokens_to_state(ctx, query_id, "TextToSQL", model, usage)

    if generated_sql is None and qt in ("time_series", "forecast"):
        fb = step._deterministic_time_series_fallback(parsed)
        if fb:
            generated_sql = fb
            fallback_used = True
            sql_source = "deterministic_time_series_fallback"
            ctx.logger.warn(
                "Deterministic time_series fallback used", {"queryId": query_id}
            )

    if generated_sql is None:
        msg = (
            f"Could not generate SQL for query_type={qt} "
            f"entity={parsed.get('entity')} metric={parsed.get('metric')}. "
            "Try rephrasing with explicit metric, dimension, and period."
        )
        qs = await ctx.state.get("queries", query_id)
        if qs:
            now_iso = step.datetime.now(step.timezone.utc).isoformat()
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
        return

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = step.datetime.now(step.timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "status": "sql_generated",
                "generated_sql": generated_sql,
                "sql_source": sql_source,
                "sql_fallback": fallback_used,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "sql_generated": now_iso},
            },
        )

    await ctx.enqueue(
        {
            "topic": "query::execute",
            "data": {
                "queryId": query_id,
                "query": user_query,
                "parsed": parsed,
                "generated_sql": generated_sql,
            },
        }
    )
