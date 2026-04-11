"""Manual trigger endpoint for business digest reports."""

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
    "name": "BusinessReportRun",
    "description": "HTTP endpoint to trigger weekly/monthly business digest generation.",
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("POST", "/reports/run")],
    "enqueues": ["report::generate"],
}


async def handler(
    request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]
) -> ApiResponse[Any]:
    from modules.utilities.step_services import run_report_response

    status, body = await run_report_response(request.body or {}, ctx)
    return ApiResponse(status=status, body=body)
