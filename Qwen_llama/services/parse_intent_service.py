"""Service orchestration for parse_intent_step."""

from __future__ import annotations

from types import ModuleType
from typing import Any

from motia import FlowContext


async def run_parse_intent(
    step: ModuleType, input_data: Any, ctx: FlowContext[Any]
) -> None:
    query_id = input_data.get("queryId")
    user_query = input_data.get("query", "")
    merged_parsed = input_data.get("mergedParsed")
    followup_ctx = input_data.get("followupContext") or {}

    mandatory_filters = step._get_mandatory_filters()
    if mandatory_filters:
        ctx.logger.info(
            "🔒 Mandatory filters detected",
            {"queryId": query_id, "filters": mandatory_filters},
        )

    if merged_parsed:
        ctx.logger.info("Clarification path", {"queryId": query_id})
        parsed = step._post_process(merged_parsed, user_query, mandatory_filters)
        parsed["_parser_source"] = "clarification_merge"
    else:
        ctx.logger.info(
            "Qwen intent extraction", {"queryId": query_id, "query": user_query}
        )
        try:
            schema = step.get_schema_prompt(mode="compact", user_query=user_query)
            parsed, usage, model_used = step._call_parse_with_retry_and_fallback(
                user_query, schema
            )
            if followup_ctx:
                parsed = step._merge_followup_intent(parsed, user_query, followup_ctx)
            parsed = step._post_process(parsed, user_query, mandatory_filters)
            source = f"llm_{model_used.split('/')[-1]}"
            if parsed.get("_followup_applied"):
                source += "_followup_merge"
                ctx.logger.info(
                    "Follow-up context merged",
                    {
                        "queryId": query_id,
                        "previousQueryId": parsed.get("_followup_from_query_id"),
                    },
                )
            parsed["_parser_source"] = source
            step.log_tokens(ctx, query_id, "ParseIntent", model_used, usage)
            await step.add_tokens_to_state(
                ctx, query_id, "ParseIntent", model_used, usage
            )
            ctx.logger.info("Intent parsed", {"queryId": query_id, "parsed": parsed})
        except Exception as exc:
            ctx.logger.error(
                "Intent parse failed", {"error": str(exc), "queryId": query_id}
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
                        "error": f"Parse intent model failed: {exc}",
                        "updatedAt": now_iso,
                        "status_timestamps": {**prev_ts, "error": now_iso},
                    },
                )
            return

    qs = await ctx.state.get("queries", query_id)

    is_complete, clarification = step._check_clarity(parsed)
    if qs:
        now_iso = step.datetime.now(step.timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        ts = {**prev_ts, "intent_parsed": now_iso}
        if not is_complete:
            ts["needs_clarification"] = now_iso
            await ctx.state.set(
                "queries",
                query_id,
                {
                    **qs,
                    "status": "needs_clarification",
                    "parsed": parsed,
                    "clarification": clarification,
                    "updatedAt": now_iso,
                    "status_timestamps": ts,
                },
            )
            return

        ts["ambiguity_checked"] = now_iso
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "status": "ambiguity_checked",
                "parsed": parsed,
                "updatedAt": now_iso,
                "status_timestamps": ts,
            },
        )

    await ctx.enqueue(
        {
            "topic": "query::text.to.sql",
            "data": {"queryId": query_id, "query": user_query, "parsed": parsed},
        }
    )
