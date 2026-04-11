"""POST /ingest - ingest uploaded or discovered data files into DuckDB tables."""

import os
import sys
from typing import Any

from motia import ApiRequest, ApiResponse, FlowContext, http

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

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
    from modules.utilities.step_services import ingest_response

    status, body = await ingest_response(request.body or {}, ctx)
    return ApiResponse(status=status, body=body)
