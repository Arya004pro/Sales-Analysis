"""Consolidated HTTP API router module for Motia step surface reduction."""

from __future__ import annotations

import os
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
        status, payload = await list_queries_response(ctx)
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

    if method == "POST":
        return await _handle_post(request, ctx, path)
    if method == "GET":
        return await _handle_get(request, ctx, path)

    return ApiResponse(status=405, body={"error": "Method not allowed"})
