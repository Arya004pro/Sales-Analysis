"""POST /ingest - ingest uploaded data files into DuckDB tables."""

import os
import sys
from datetime import datetime, timezone
from typing import Any

from motia import ApiRequest, ApiResponse, FlowContext, http

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db.data_ingester import ingest_files


config = {
    "name": "IngestData",
    "description": "Ingest one or more uploaded files into DuckDB",
    "flows": ["sales-analytics-ingest"],
    "triggers": [http("POST", "/ingest")],
    "enqueues": [],
}


async def handler(
    request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]
) -> ApiResponse[Any]:
    body = request.body or {}
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
        return ApiResponse(status=400, body={"error": "files[] must be a list"})
    if not files and not discover_files:
        return ApiResponse(
            status=400,
            body={"error": "files[] is required unless discover_files=true"},
        )

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
        return ApiResponse(status=500, body={"error": str(exc)})

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
    return ApiResponse(status=200, body={"ok": True, **result})
