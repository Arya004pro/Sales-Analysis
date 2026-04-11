"""Consolidated business report step routing module.

Keeps Motia step surface minimal while preserving existing reporting behavior.
"""

import os
import sys
from typing import Any

from motia import FlowContext, cron, queue

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_MODULE_DIR))
_MOTIA_DIR = os.path.join(_PROJECT_ROOT, "motia")
_STEPS_DIR = os.path.join(_MOTIA_DIR, "steps")
for _p in (_MODULE_DIR, _STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from modules.reporting.business_report_generate_module import (
    handler as generate_handler,
)
from modules.reporting.business_report_scheduler_module import (
    handler as scheduler_handler,
)

config = {
    "name": "BusinessReport",
    "description": "Consolidated business report worker step (cron + queue).",
    "flows": ["sales-analytics-digest"],
    "triggers": [
        queue("report::generate"),
        cron("0 0 8 * * *"),
    ],
    "enqueues": ["report::generate"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> Any:
    data = input_data if isinstance(input_data, dict) else {}
    period = str(data.get("period") or "").lower().strip()
    if period in {"weekly", "monthly"}:
        await generate_handler(data, ctx)
        return None

    await scheduler_handler(input_data, ctx)
    return None
