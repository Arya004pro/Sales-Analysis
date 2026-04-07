"""Step 1: Receive Query - HTTP entry point."""

import os
import sys
from typing import Any

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import ApiRequest, ApiResponse, FlowContext, http


config = {
    "name": "QueryIntake",
    "description": "Receives user query and starts or resumes the analytics workflow.",
    "flows": ["sales-analytics-flow"],
    "triggers": [http("POST", "/query")],
    "enqueues": ["query::intent.parse"],
}


async def handler(
    request: ApiRequest[dict[str, Any]], ctx: FlowContext[Any]
) -> ApiResponse[Any]:
    from step_services import receive_query_response

    status, body = await receive_query_response(request.body or {}, ctx)
    return ApiResponse(status=status, body=body)
