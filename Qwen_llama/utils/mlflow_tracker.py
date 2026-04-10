"""MLflow helper for forecast experiment tracking.

This module is optional and should never break query execution.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _enabled() -> bool:
    return os.getenv("MLFLOW_ENABLED", "1") == "1"


def _default_tracking_uri() -> str:
    root = Path(__file__).resolve().parents[1]
    return f"file:{root / 'motia' / 'data' / 'mlruns'}"


def log_forecast_run(
    *,
    query_id: str,
    user_query: str,
    parsed: dict[str, Any],
    method: str,
    periods: int,
    confidence_pct: float,
    hist_values: list[float],
    forecast_values: list[float],
    rmse: float,
    trend_pct: float,
    goal_result: dict[str, Any] | None,
) -> dict[str, Any]:
    if not _enabled():
        return {"enabled": False, "logged": False, "reason": "disabled"}

    try:
        import mlflow
    except Exception as exc:
        return {
            "enabled": True,
            "logged": False,
            "reason": f"mlflow import failed: {exc}",
        }

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", _default_tracking_uri())
    experiment_name = os.getenv("MLFLOW_EXPERIMENT", "sales_forecasting")

    try:
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)

        run_name = f"forecast_{(query_id or 'query')[:12]}"
        with mlflow.start_run(run_name=run_name):
            mlflow.log_param("query_id", query_id or "")
            mlflow.log_param("query_type", str(parsed.get("query_type") or ""))
            mlflow.log_param("metric", str(parsed.get("metric") or ""))
            mlflow.log_param("entity", str(parsed.get("entity") or ""))
            mlflow.log_param("time_bucket", str(parsed.get("time_bucket") or ""))
            mlflow.log_param("method", method)
            mlflow.log_param("periods", int(periods))
            mlflow.log_param("confidence_pct", float(confidence_pct))
            mlflow.log_param("user_query", (user_query or "")[:500])

            mlflow.log_metric("history_points", float(len(hist_values or [])))
            mlflow.log_metric("forecast_points", float(len(forecast_values or [])))
            mlflow.log_metric("rmse", float(rmse or 0.0))
            mlflow.log_metric("trend_pct", float(trend_pct or 0.0))
            mlflow.log_metric(
                "hist_total", float(sum(hist_values or []) if hist_values else 0.0)
            )
            mlflow.log_metric(
                "forecast_total",
                float(sum(forecast_values or []) if forecast_values else 0.0),
            )

            if isinstance(goal_result, dict):
                for key in (
                    "target_value",
                    "actual_to_date",
                    "projected_total",
                    "gap",
                    "required_per_period",
                    "forecast_avg_per_period",
                    "pace_ratio",
                ):
                    val = goal_result.get(key)
                    if isinstance(val, (int, float)):
                        mlflow.log_metric(f"goal_{key}", float(val))
                on_track = goal_result.get("on_track")
                if isinstance(on_track, bool):
                    mlflow.log_metric("goal_on_track", 1.0 if on_track else 0.0)

        return {
            "enabled": True,
            "logged": True,
            "tracking_uri": tracking_uri,
            "experiment": experiment_name,
        }
    except Exception as exc:
        return {
            "enabled": True,
            "logged": False,
            "reason": f"mlflow logging failed: {exc}",
            "tracking_uri": tracking_uri,
            "experiment": experiment_name,
        }
