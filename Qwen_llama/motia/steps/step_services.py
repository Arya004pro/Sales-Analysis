"""Shared service functions for Motia HTTP/utility steps.

This module keeps business/orchestration logic outside step files so
steps remain thin request/response adapters.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from db.data_ingester import ingest_files
from db.schema_view import build_schema_view_payload, normalize_view_mode


_SESSIONS_NS = "query_sessions"


def query_param(request: Any, key: str, default: str = "") -> str:
    for attr in ("query", "query_params", "params"):
        obj = getattr(request, attr, None)
        if isinstance(obj, dict) and key in obj:
            return str(obj.get(key) or default)
    return default


def _new_query_id(now: datetime) -> str:
    return f"Q-{int(now.timestamp() * 1000)}-{uuid.uuid4().hex[:6]}"


async def _resolve_session_context(
    ctx: Any,
    session_id: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not session_id:
        return None, None

    session_state = await ctx.state.get(_SESSIONS_NS, session_id)
    previous = None

    if session_state and session_state.get("last_query_id"):
        previous = await ctx.state.get("queries", session_state.get("last_query_id"))

    if not previous:
        previous = await ctx.state.get("queries", session_id)

    return session_state, previous


async def _upsert_session(
    ctx: Any,
    session_id: str,
    query_id: str,
    now_iso: str,
) -> None:
    existing = await ctx.state.get(_SESSIONS_NS, session_id) or {}
    query_ids = list(existing.get("query_ids") or [])
    if not query_ids or query_ids[-1] != query_id:
        query_ids.append(query_id)
    query_ids = query_ids[-30:]

    await ctx.state.set(
        _SESSIONS_NS,
        session_id,
        {
            **existing,
            "id": session_id,
            "createdAt": existing.get("createdAt", now_iso),
            "updatedAt": now_iso,
            "last_query_id": query_id,
            "query_ids": query_ids,
        },
    )


def _build_followup_context(
    session_id: str,
    previous: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not previous:
        return None

    prev_parsed = previous.get("parsed") or {}
    prev_query = str(previous.get("query") or "").strip()
    prev_query_id = str(previous.get("id") or "").strip()

    if not prev_parsed and not prev_query:
        return None

    return {
        "sessionId": session_id,
        "previousQueryId": prev_query_id,
        "previousQuery": prev_query,
        "previousParsed": prev_parsed,
    }


async def receive_query_response(
    body: dict[str, Any],
    ctx: Any,
) -> tuple[int, dict[str, Any]]:
    user_query = str(body.get("query", "")).strip()
    session_id = str(body.get("sessionId", "")).strip()

    if not user_query:
        return 400, {"error": "Missing 'query' field"}

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    if not session_id:
        query_id = _new_query_id(now)
        session_id = query_id
        ctx.logger.info(
            "New query (new session)",
            {
                "queryId": query_id,
                "sessionId": session_id,
                "query": user_query,
            },
        )

        await ctx.state.set(
            "queries",
            query_id,
            {
                "id": query_id,
                "sessionId": session_id,
                "query": user_query,
                "status": "received",
                "createdAt": now_iso,
                "updatedAt": now_iso,
                "status_timestamps": {"received": now_iso},
            },
        )
        await _upsert_session(ctx, session_id, query_id, now_iso)

        await ctx.enqueue(
            {
                "topic": "query::intent.parse",
                "data": {"queryId": query_id, "query": user_query},
            }
        )

        return 200, {
            "queryId": query_id,
            "sessionId": session_id,
            "status": "processing",
            "message": "Query accepted",
        }

    _session_state, previous = await _resolve_session_context(ctx, session_id)

    if previous and previous.get("status") == "needs_clarification":
        query_id = str(previous.get("id") or session_id)
        ctx.logger.info(
            "Clarification reply",
            {
                "sessionId": session_id,
                "queryId": query_id,
                "query": user_query,
            },
        )

        combined_query = f"{previous.get('query', '')}. To clarify: {user_query}"
        prev_ts = (previous or {}).get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **previous,
                "sessionId": session_id,
                "status": "received",
                "lastQuery": user_query,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "received": now_iso},
            },
        )
        await _upsert_session(ctx, session_id, query_id, now_iso)

        await ctx.enqueue(
            {
                "topic": "query::intent.parse",
                "data": {"queryId": query_id, "query": combined_query},
            }
        )

        return 200, {
            "queryId": query_id,
            "sessionId": session_id,
            "status": "processing",
            "message": "Clarification accepted",
        }

    query_id = _new_query_id(now)
    followup_context = _build_followup_context(session_id, previous)
    previous_query_id = (followup_context or {}).get("previousQueryId")

    ctx.logger.info(
        "Session query",
        {
            "queryId": query_id,
            "sessionId": session_id,
            "query": user_query,
            "has_followup_context": bool(followup_context),
        },
    )

    await ctx.state.set(
        "queries",
        query_id,
        {
            "id": query_id,
            "sessionId": session_id,
            "previousQueryId": previous_query_id,
            "query": user_query,
            "status": "received",
            "createdAt": now_iso,
            "updatedAt": now_iso,
            "status_timestamps": {"received": now_iso},
        },
    )
    await _upsert_session(ctx, session_id, query_id, now_iso)

    enqueue_data: dict[str, Any] = {"queryId": query_id, "query": user_query}
    if followup_context:
        enqueue_data["followupContext"] = followup_context

    await ctx.enqueue(
        {
            "topic": "query::intent.parse",
            "data": enqueue_data,
        }
    )

    return 200, {
        "queryId": query_id,
        "sessionId": session_id,
        "status": "processing",
        "message": "Query accepted",
    }


async def ingest_response(
    body: dict[str, Any],
    ctx: Any,
) -> tuple[int, dict[str, Any]]:
    files = body.get("files") or []
    reset_db = bool(body.get("reset_db", False))
    merge_confirm = bool(body.get("merge_confirm", False))
    discover_files = bool(body.get("discover_files", False))
    discover_dirs = body.get("discover_dirs") or []
    discover_latest_only = bool(body.get("discover_latest_only", True))
    include_seen_discovered = bool(body.get("include_seen_discovered", False))
    llm_schema_infer = bool(body.get("llm_schema_infer", True))
    use_llm_grouping = bool(body.get("use_llm_grouping", False))
    auto_structure = bool(body.get("auto_structure", True))

    if not isinstance(files, list):
        return 400, {"error": "files[] must be a list"}
    if not files and not discover_files:
        return 400, {"error": "files[] is required unless discover_files=true"}

    try:
        result = ingest_files(
            files,
            reset_db=reset_db,
            auto_structure=auto_structure,
            merge_confirm=merge_confirm,
            discover_files=discover_files,
            discover_dirs=discover_dirs,
            discover_latest_only=discover_latest_only,
            include_seen_discovered=include_seen_discovered,
            llm_schema_infer=llm_schema_infer,
        )
    except Exception as exc:
        ctx.logger.error("Ingest failed", {"error": str(exc)})
        return 500, {"error": str(exc)}

    now_iso = datetime.now(timezone.utc).isoformat()
    await ctx.state.set(
        "schema_registry",
        "current",
        {
            "updatedAt": now_iso,
            "source": "drop-scan" if (discover_files and not files) else "upload",
            "use_llm_grouping": use_llm_grouping,
            "auto_structure": auto_structure,
            **result,
        },
    )
    return 200, {"ok": True, **result}


async def schema_response(
    request: Any,
    ctx: Any,
) -> tuple[int, dict[str, Any]]:
    view_mode = normalize_view_mode(query_param(request, "view", "derived"))
    schema_state = await ctx.state.get("schema_registry", "current")
    payload = build_schema_view_payload(schema_state=schema_state, view_mode=view_mode)
    return 200, payload


async def get_query_result_response(
    request: Any,
    ctx: Any,
) -> tuple[int, dict[str, Any]]:
    path_params = getattr(request, "path_params", None)
    if not isinstance(path_params, dict):
        path_params = {}
    query_id = str(path_params.get("queryId", ""))

    if not query_id:
        return 400, {"error": "Missing queryId parameter"}

    ctx.logger.info("Fetching query result", {"queryId": query_id})

    query_state = await ctx.state.get("queries", query_id)
    if not query_state:
        return 404, {"error": f"Query {query_id} not found"}

    return 200, query_state


async def list_queries_response(ctx: Any) -> tuple[int, dict[str, Any]]:
    queries = await ctx.state.list("queries")
    ctx.logger.info("Listing all queries", {"count": len(queries)})
    return 200, {
        "queries": queries,
        "count": len(queries),
    }


def normalize_report_period(raw: str, allow_both: bool = False) -> str:
    p = (raw or "").strip().lower()
    if allow_both and p in {"weekly", "monthly", "both"}:
        return p
    if p in {"weekly", "monthly"}:
        return p
    return "weekly" if allow_both else ""


async def run_report_response(
    body: dict[str, Any],
    ctx: Any,
) -> tuple[int, dict[str, Any]]:
    period = normalize_report_period(
        str(body.get("period") or "weekly"), allow_both=True
    )
    requested_by = str(body.get("requestedBy") or "manual")
    run_id = f"RPT-{int(datetime.now(timezone.utc).timestamp() * 1000)}-{uuid.uuid4().hex[:6]}"

    periods = ["weekly", "monthly"] if period == "both" else [period]
    for p in periods:
        await ctx.enqueue(
            {
                "topic": "report::generate",
                "data": {
                    "period": p,
                    "trigger": "http",
                    "runId": run_id,
                    "requestedBy": requested_by,
                },
            }
        )

    return 202, {
        "status": "accepted",
        "runId": run_id,
        "periods": periods,
        "message": "Business digest generation queued",
    }


async def get_report_response(
    request: Any,
    ctx: Any,
) -> tuple[int, dict[str, Any]]:
    period = normalize_report_period(
        query_param(request, "period", ""), allow_both=False
    )
    key = f"latest_{period}" if period else "latest"

    report = await ctx.state.get("business_reports", key)
    if not report:
        return 404, {
            "error": "No business report found yet",
            "period": period or "latest",
        }

    return 200, report
