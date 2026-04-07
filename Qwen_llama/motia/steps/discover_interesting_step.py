"""Auto-discovery endpoint: scan all tables and return surprising findings."""

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
    "name": "InterestingDiscovery",
    "description": (
        "Scans all loaded tables and surfaces the top statistically surprising findings."
    ),
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("GET", "/discover")],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    from services.discovery_service import get_discovery_response

    status, body = await get_discovery_response(request, ctx)
    return ApiResponse(status=status, body=body)
