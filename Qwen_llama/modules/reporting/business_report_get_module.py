"""Utility endpoint to fetch latest auto-generated business digest report."""

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

config = {
    "name": "BusinessReportGet",
    "description": "Returns the latest weekly/monthly business digest report.",
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("GET", "/reports/latest")],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    from modules.utilities.step_services import get_report_response

    status, body = await get_report_response(request, ctx)
    return ApiResponse(status=status, body=body)
