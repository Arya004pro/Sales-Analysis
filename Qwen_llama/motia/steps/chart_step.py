"""Chart Step — GET /query/:queryId/chart."""

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
    "name": "ChartQuery",
    "description": "Returns a Chart.js HTML page for a completed query result",
    "flows": ["sales-analytics-utilities"],
    "triggers": [
        http("GET", "/query/:queryId/chart"),
    ],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    from services.chart_service import get_chart_response

    status, body, headers = await get_chart_response(request, ctx)
    return ApiResponse(status=status, body=body, headers=headers)
