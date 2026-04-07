"""Revenue decomposition step.

Builds a period-over-period revenue bridge for comparison-style queries.
Pipeline position:
  ... -> query::detect.anomalies -> query::decompose.revenue -> query::format.result
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any

from motia import FlowContext, queue

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in (_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

config = {
    "name": "RevenueDecomposition",
    "description": "Computes a revenue bridge decomposition for period comparison queries.",
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::decompose.revenue")],
    "enqueues": ["query::format.result"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    from utils.revenue_decomposition import build_revenue_decomposition

    query_id = input_data.get("queryId")
    parsed = input_data.get("parsed", {}) or {}
    results = input_data.get("results", []) or []
    period_labels = input_data.get("period_labels", []) or []

    decomposition = build_revenue_decomposition(
        rows=results,
        parsed=parsed,
        period_labels=period_labels,
    )

    ctx.logger.info(
        "Revenue decomposition complete",
        {
            "queryId": query_id,
            "applies": bool(decomposition.get("applies")),
            "reason": decomposition.get("reason"),
        },
    )

    qs = await ctx.state.get("queries", query_id)
    if qs:
        now_iso = datetime.now(timezone.utc).isoformat()
        prev_ts = qs.get("status_timestamps", {})
        await ctx.state.set(
            "queries",
            query_id,
            {
                **qs,
                "revenue_decomposition": decomposition,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "revenue_decomposed": now_iso},
            },
        )

    await ctx.enqueue(
        {
            "topic": "query::format.result",
            "data": {
                **input_data,
                "revenue_decomposition": decomposition,
            },
        }
    )
