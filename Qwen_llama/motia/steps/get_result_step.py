"""Get Query Result — HTTP endpoint to retrieve processed query results."""

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
    "name": "GetQueryResult",
    "description": "Utility endpoint: returns status and results for a previously submitted query id",
    "flows": ["sales-analytics-utilities"],
    "triggers": [
        http("GET", "/query/:queryId"),
    ],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    from step_services import get_query_result_response

    status, body = await get_query_result_response(request, ctx)
    return ApiResponse(status=status, body=body)
