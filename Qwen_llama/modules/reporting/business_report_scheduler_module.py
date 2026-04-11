"""Scheduled trigger for auto-generated business digest reports."""

import os
import sys
from datetime import datetime, timezone
from typing import Any

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_MODULE_DIR))
_MOTIA_DIR = os.path.join(_PROJECT_ROOT, "motia")
_STEPS_DIR = os.path.join(_MOTIA_DIR, "steps")
for _p in (_MODULE_DIR, _STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from motia import FlowContext, cron

config = {
    "name": "BusinessReportScheduler",
    "description": (
        "Runs daily on cron and enqueues weekly/monthly business digest generation."
    ),
    "flows": ["sales-analytics-digest"],
    "triggers": [cron("0 0 8 * * *")],  # sec min hour day month day-of-week
    "enqueues": ["report::generate"],
}


async def handler(input_data: Any, ctx: FlowContext[Any]) -> None:
    _ = input_data
    now = datetime.now(timezone.utc)
    today_iso = now.date().isoformat()

    sched = await ctx.state.get("report_scheduler", "digest") or {}
    last_weekly = str(sched.get("last_weekly_date") or "")
    last_monthly = str(sched.get("last_monthly_date") or "")

    should_weekly = now.weekday() == 0 and last_weekly != today_iso  # Monday
    should_monthly = now.day == 1 and last_monthly != today_iso

    if not should_weekly and not should_monthly:
        ctx.logger.info("Digest cron tick skipped", {"today": today_iso})
        return

    if should_weekly:
        await ctx.enqueue(
            {
                "topic": "report::generate",
                "data": {"period": "weekly", "trigger": "cron"},
            }
        )
        sched["last_weekly_date"] = today_iso

    if should_monthly:
        await ctx.enqueue(
            {
                "topic": "report::generate",
                "data": {"period": "monthly", "trigger": "cron"},
            }
        )
        sched["last_monthly_date"] = today_iso

    sched["updated_at"] = now.isoformat()
    await ctx.state.set("report_scheduler", "digest", sched)
    ctx.logger.info(
        "Digest cron enqueued",
        {
            "today": today_iso,
            "weekly": should_weekly,
            "monthly": should_monthly,
        },
    )
