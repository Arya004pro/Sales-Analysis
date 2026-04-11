"""List All Queries — HTTP endpoint to show all processed queries."""

import os
import sys
from typing import Any

from motia import ApiRequest, ApiResponse, FlowContext, http

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in (_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

config = {
    "name": "ListQueries",
    "description": "Utility endpoint: lists tracked queries and their current processing status",
    "flows": ["sales-analytics-utilities"],
    "triggers": [
        http("GET", "/queries"),
    ],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    _ = request
    from modules.utilities.step_services import list_queries_response

    status, body = await list_queries_response(ctx)
    return ApiResponse(status=status, body=body)
