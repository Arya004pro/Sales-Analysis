"""Step: Query Suggestions — GET /suggestions."""

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
    "name": "QuerySuggestions",
    "description": (
        "Returns 8 schema-aware example analytics questions. "
        "Results are LLM-generated and cached for 30 minutes. "
        "Pass ?refresh=1 to force regeneration."
    ),
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("GET", "/suggestions")],
    "enqueues": [],
}


async def handler(
    request: ApiRequest[Any],
    ctx: FlowContext[Any],
) -> ApiResponse[Any]:
    from services.suggestions_service import get_suggestions_response

    status, body = await get_suggestions_response(request, ctx)
    return ApiResponse(status=status, body=body)
