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

    goal_cfg = (
        parsed.get("_goal_tracking")
        if isinstance(parsed.get("_goal_tracking"), dict)
        else None
    )
    if goal_cfg:
        target_val = None
        try:
            target_val = float(goal_cfg.get("target_value"))
        except Exception:
            target_val = None

        if not target_val or target_val <= 0:
            metric_for_goal = str(parsed.get("metric") or "").strip().lower()
            period_key = str(goal_cfg.get("period_key") or "")
            store = await step._load_goal_store(ctx)
            remembered_target, remembered_label = step._lookup_goal_target(
                store, metric_for_goal, period_key
            )
            if remembered_target and remembered_target > 0:
                goal_cfg["target_value"] = remembered_target
                if remembered_label:
                    goal_cfg["period_label"] = remembered_label
                parsed["_goal_tracking"] = goal_cfg
                parsed["time_ranges"] = step._goal_time_range_for_period_key(
                    str(goal_cfg.get("period_key") or "")
                )
                parsed["_force_clarification"] = False
                parsed["clarification_question"] = None
                parsed["is_complete"] = True

        try:
            final_target = float(goal_cfg.get("target_value"))
        except Exception:
            final_target = 0.0
        if final_target > 0:
            await step._save_goal_target(
                ctx,
                metric=str(parsed.get("metric") or "").strip().lower(),
                period_key=str(goal_cfg.get("period_key") or ""),
                period_label=str(goal_cfg.get("period_label") or ""),
                target_value=final_target,
                query_id=query_id,
            )

    goal_set_cfg = (
        parsed.get("_goal_set_only")
        if isinstance(parsed.get("_goal_set_only"), dict)
        else None
    )
    if goal_set_cfg:
        metric = str(goal_set_cfg.get("metric") or "").strip().lower()
        period_key = str(goal_set_cfg.get("period_key") or "")
        period_label = str(goal_set_cfg.get("period_label") or "")
        horizon_label = str(goal_set_cfg.get("horizon_label") or "end of month")
        try:
            target_value = float(goal_set_cfg.get("target_value"))
        except Exception:
            target_value = 0.0

        if metric and period_key and target_value > 0:
            await step._save_goal_target(
                ctx,
                metric=metric,
                period_key=period_key,
                period_label=period_label,
                target_value=target_value,
                query_id=query_id,
            )

            if qs:
                now_iso = step.datetime.now(step.timezone.utc).isoformat()
                prev_ts = qs.get("status_timestamps", {})
                metric_label = step._human_metric_label(metric)
                msg = (
                    f"Target saved: {step._human_metric_label(metric)} target for {period_label or period_key} "
                    f"is {target_value:,.2f}.\n"
                    f"Ask: 'Are we on track by {horizon_label}?'"
                )
                await ctx.state.set(
                    "queries",
                    query_id,
                    {
                        **qs,
                        "status": "completed",
                        "parsed": parsed,
                        "formattedText": msg,
                        "formattedItems": [
                            {"label": "Metric", "value": metric_label},
                            {"label": "Period", "value": period_label or period_key},
                            {"label": "Target", "value": f"{target_value:,.2f}"},
                        ],
                        "updatedAt": now_iso,
                        "completedAt": now_iso,
                        "status_timestamps": {
                            **prev_ts,
                            "intent_parsed": now_iso,
                            "ambiguity_checked": now_iso,
                            "completed": now_iso,
                        },
                    },
                )
            return

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
