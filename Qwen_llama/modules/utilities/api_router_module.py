"""Consolidated HTTP API router module for Motia step surface reduction."""

from __future__ import annotations

import os
import re
import sys
from typing import Any

from motia import ApiRequest, ApiResponse, FlowContext, http

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_MODULE_DIR))
_MOTIA_DIR = os.path.join(_PROJECT_ROOT, "motia")
_STEPS_DIR = os.path.join(_MOTIA_DIR, "steps")
for _p in (_MODULE_DIR, _STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from modules.utilities.step_services import (
    get_query_result_response,
    get_report_response,
    ingest_response,
    list_queries_response,
    run_report_response,
    schema_response,
)
from services.chart_service import get_chart_response
from services.discovery_service import get_discovery_response
from services.suggestions_service import get_suggestions_response

config = {
    "name": "ApiRouter",
    "description": "Consolidated HTTP router for ingest, utility, and report endpoints.",
    "flows": [
        "sales-analytics-ingest",
        "sales-analytics-utilities",
    ],
    "triggers": [
        http("POST", "/ingest"),
        http("GET", "/schema"),
        http("GET", "/queries"),
        http("GET", "/query/:queryId"),
        http("GET", "/query/:queryId/chart"),
        http("GET", "/discover"),
        http("GET", "/suggestions"),
        http("GET", "/reports/latest"),
        http("POST", "/reports/run"),
    ],
    "enqueues": ["report::generate"],
}


def _request_path(request: Any) -> str:
    for attr in ("path", "url", "route", "raw_path", "endpoint"):
        value = getattr(request, attr, None)
        if isinstance(value, str) and value.strip():
            return value.lower()
    return ""


def _path_has(path: str, token: str) -> bool:
    return token in (path or "")


_TRIGGER_ROUTE_MAP: dict[int, tuple[str, str]] = {
    0: ("POST", "/ingest"),
    1: ("GET", "/schema"),
    2: ("GET", "/queries"),
    3: ("GET", "/query/"),
    4: ("GET", "/query/chart"),
    5: ("GET", "/discover"),
    6: ("GET", "/suggestions"),
    7: ("GET", "/reports/latest"),
    8: ("POST", "/reports/run"),
}


def _extract_trigger_index(request: Any, ctx: Any) -> int | None:
    trigger_obj = getattr(ctx, "trigger", None)
    trigger_idx = getattr(trigger_obj, "index", None)
    if isinstance(trigger_idx, int):
        return trigger_idx

    probe_values: list[str] = []

    def _collect(obj: Any) -> None:
        for attr in (
            "function_id",
            "fn_id",
            "trigger_id",
            "invocation_id",
            "handler_id",
            "route_id",
            "id",
            "name",
        ):
            val = getattr(obj, attr, None)
            if isinstance(val, str) and val:
                probe_values.append(val)

    _collect(request)
    _collect(ctx)

    headers = getattr(request, "headers", None)
    if isinstance(headers, dict):
        for k, v in headers.items():
            if isinstance(k, str):
                probe_values.append(k)
            if isinstance(v, str):
                probe_values.append(v)

    probe_values.extend([repr(request), repr(ctx)])

    for text in probe_values:
        m = re.search(r"trigger::(\d+)", text)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                continue
    return None


def _request_debug_snapshot(request: Any, ctx: Any) -> dict[str, Any]:
    snap: dict[str, Any] = {}
    for attr in (
        "method",
        "path",
        "url",
        "route",
        "raw_path",
        "endpoint",
        "trigger_id",
        "function_id",
        "handler_id",
        "path_params",
        "query",
        "query_params",
        "params",
        "headers",
    ):
        try:
            val = getattr(request, attr, None)
            if isinstance(val, (str, int, float, bool)) or val is None:
                snap[attr] = val
            elif isinstance(val, dict):
                snap[attr] = {str(k): str(v) for k, v in list(val.items())[:12]}
            else:
                snap[attr] = str(val)
        except Exception:
            snap[attr] = "<unavailable>"

    try:
        snap["ctx_repr"] = str(ctx)
    except Exception:
        snap["ctx_repr"] = "<unavailable>"
    return snap


async def _handle_post(
    request: ApiRequest[Any],
    ctx: FlowContext[Any],
    path: str,
) -> ApiResponse[Any]:
    body = request.body if isinstance(getattr(request, "body", None), dict) else {}

    if _path_has(path, "/ingest"):
        status, payload = await ingest_response(body, ctx)
        return ApiResponse(status=status, body=payload)

    if _path_has(path, "/reports/run"):
        status, payload = await run_report_response(body, ctx)
        return ApiResponse(status=status, body=payload)

    if any(k in body for k in ("files", "discover_files", "discover_dirs", "reset_db")):
        status, payload = await ingest_response(body, ctx)
        return ApiResponse(status=status, body=payload)

    if any(k in body for k in ("period", "requestedBy")):
        status, payload = await run_report_response(body, ctx)
        return ApiResponse(status=status, body=payload)

    return ApiResponse(status=400, body={"error": "Unsupported POST endpoint"})


async def _handle_get(
    request: ApiRequest[Any],
    ctx: FlowContext[Any],
    path: str,
) -> ApiResponse[Any]:
    path_params = getattr(request, "path_params", None)
    if not isinstance(path_params, dict):
        path_params = {}

    has_query_id = bool(str(path_params.get("queryId") or "").strip())

    if _path_has(path, "/schema"):
        status, payload = await schema_response(request, ctx)
        return ApiResponse(status=status, body=payload)

    if _path_has(path, "/discover"):
        status, payload = await get_discovery_response(request, ctx)
        return ApiResponse(status=status, body=payload)

    if _path_has(path, "/suggestions"):
        status, payload = await get_suggestions_response(request, ctx)
        return ApiResponse(status=status, body=payload)

    if _path_has(path, "/reports/latest"):
        status, payload = await get_report_response(request, ctx)
        return ApiResponse(status=status, body=payload)

    if _path_has(path, "/queries") and not _path_has(path, "/query/"):
        status, payload = await list_queries_response(request, ctx)
        return ApiResponse(status=status, body=payload)

    if (_path_has(path, "/query/") and _path_has(path, "/chart")) or (
        has_query_id and _path_has(path, "chart")
    ):
        status, body, headers = await get_chart_response(request, ctx)
        return ApiResponse(status=status, body=body, headers=headers)

    if _path_has(path, "/query/") or has_query_id:
        status, payload = await get_query_result_response(request, ctx)
        return ApiResponse(status=status, body=payload)

    return ApiResponse(status=400, body={"error": "Unsupported GET endpoint"})


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    method = str(getattr(request, "method", "")).upper()
    path = _request_path(request)
    trigger_idx = _extract_trigger_index(request, ctx)

    trigger_obj = getattr(ctx, "trigger", None)
    trigger_method = str(getattr(trigger_obj, "method", "") or "").upper()
    trigger_path = str(getattr(trigger_obj, "path", "") or "").lower()
    if not method and trigger_method:
        method = trigger_method
    if not path and trigger_path:
        path = trigger_path

    # Runtime fallback: some trigger invocations may omit method/path on request,
    # but still include trigger metadata that identifies the configured endpoint.
    if (not method or not path) and request is not None:
        if trigger_idx is not None and trigger_idx in _TRIGGER_ROUTE_MAP:
            inferred_method, inferred_path = _TRIGGER_ROUTE_MAP[trigger_idx]
            if not method:
                method = inferred_method
            if not path:
                path = inferred_path

    if method == "POST":
        return await _handle_post(request, ctx, path)
    if method == "GET":
        return await _handle_get(request, ctx, path)

    # Some runtimes may not populate request.method reliably for HTTP triggers.
    # Infer intent from endpoint path/payload so utility APIs still work.
    if _path_has(path, "/ingest") or _path_has(path, "/reports/run"):
        return await _handle_post(request, ctx, path)

    known_get_paths = (
        "/schema",
        "/queries",
        "/query/",
        "/discover",
        "/suggestions",
        "/reports/latest",
    )
    if any(_path_has(path, token) for token in known_get_paths):
        return await _handle_get(request, ctx, path)

    body = request.body if isinstance(getattr(request, "body", None), dict) else {}
    if any(k in body for k in ("files", "discover_files", "period", "requestedBy")):
        return await _handle_post(request, ctx, path)
    if path:
        return await _handle_get(request, ctx, path)

    ctx.logger.warn(
        "ApiRouter unresolved request",
        {
            "method": method,
            "path": path,
            "trigger_idx": trigger_idx,
            "request": _request_debug_snapshot(request, ctx),
        },
    )

    return ApiResponse(status=405, body={"error": "Method not allowed"})
