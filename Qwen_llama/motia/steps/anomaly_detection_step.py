"""Step: Anomaly Detection.

Detects statistical outliers from query result rows before formatting.
Uses shared helpers from utils.anomaly_utils.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in (_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import FlowContext, queue
from utils.anomaly_utils import detect_anomalies

config = {
    "name": "AnomalyScanner",
    "description": (
        "Scans result values for statistical outliers and forwards anomaly metadata."
    ),
    "flows": ["sales-analytics-flow"],
    "triggers": [queue("query::detect.anomalies")],
    "enqueues": ["query::format.result"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    query_id = input_data.get("queryId")
    parsed = input_data.get("parsed", {}) or {}
    results = input_data.get("results", []) or []

    qt = parsed.get("query_type", "")
    anomalies = detect_anomalies(results, qt)
    flagged_cnt = len(anomalies.get("items", []))
    ctx.logger.info(
        "Anomaly detection complete",
        {
            "queryId": query_id,
            "flagged": flagged_cnt,
            "value_key": anomalies.get("value_key"),
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
                "anomalies": anomalies,
                "updatedAt": now_iso,
                "status_timestamps": {**prev_ts, "anomaly_detected": now_iso},
            },
        )

    await ctx.enqueue(
        {
            "topic": "query::format.result",
            "data": {
                **input_data,
                "anomalies": anomalies,
            },
        }
    )
