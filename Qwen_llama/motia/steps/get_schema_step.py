"""GET /schema - return current ingested schema information."""

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
    "name": "GetSchema",
    "description": "Utility endpoint: returns current DuckDB schema and relationships",
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("GET", "/schema")],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    from modules.utilities.step_services import schema_response

    status, body = await schema_response(request, ctx)
    return ApiResponse(status=status, body=body)
