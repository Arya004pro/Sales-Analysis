"""Structured intent parsing via Instructor + OpenAI-compatible endpoint (Groq).

This module is intentionally optional at runtime:
- If Instructor/OpenAI is unavailable, callers can fall back to legacy JSON parsing.
- If the model response fails schema validation, callers can also fall back.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class IntentTimeRange(BaseModel):
    start: str
    end: str
    label: str


class IntentThreshold(BaseModel):
    value: float
    type: str
    operator: str


class IntentPayload(BaseModel):
    entity: str | None = None
    metric: str = "count"
    query_type: str = "top_n"
    time_bucket: str | None = None
    forecast_periods: int = 3
    forecast_method: str | None = None
    top_n: int = 5
    time_ranges: list[IntentTimeRange] = Field(default_factory=list)
    threshold: IntentThreshold | None = None
    filters: dict[str, Any] = Field(default_factory=dict)
    is_complete: bool = False
    clarification_question: str | None = None


def _groq_base_url(api_url: str) -> str:
    # Convert .../chat/completions to ... for OpenAI-compatible clients.
    marker = "/chat/completions"
    if api_url.endswith(marker):
        return api_url[: -len(marker)]
    return api_url


def parse_intent_with_instructor(
    *,
    api_url: str,
    api_token: str,
    model: str,
    system_prompt: str,
    user_query: str,
    max_tokens: int,
    enable_reasoning: bool,
    reasoning_effort: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (parsed_intent_dict, usage_dict) using structured output."""
    try:
        import instructor
        from openai import OpenAI
    except Exception as exc:
        raise RuntimeError(f"Instructor stack unavailable: {exc}") from exc

    if not api_token:
        raise RuntimeError("Missing API token for structured intent parsing")

    client = instructor.from_openai(
        OpenAI(base_url=_groq_base_url(api_url), api_key=api_token),
        mode=instructor.Mode.JSON,
    )

    kwargs: dict[str, Any] = {
        "model": model,
        "response_model": IntentPayload,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query},
        ],
        "temperature": 0.0,
        "max_tokens": max_tokens,
    }
    if enable_reasoning and "qwen" in model.lower() and reasoning_effort:
        kwargs["extra_body"] = {"reasoning_effort": reasoning_effort}

    usage: dict[str, Any] = {}
    create_with_completion = getattr(
        client.chat.completions, "create_with_completion", None
    )

    if callable(create_with_completion):
        payload, completion = create_with_completion(**kwargs)
        raw_usage = getattr(completion, "usage", None)
        if raw_usage is not None:
            usage = {
                "prompt_tokens": int(getattr(raw_usage, "prompt_tokens", 0) or 0),
                "completion_tokens": int(
                    getattr(raw_usage, "completion_tokens", 0) or 0
                ),
                "total_tokens": int(getattr(raw_usage, "total_tokens", 0) or 0),
            }
    else:
        payload = client.chat.completions.create(**kwargs)

    if hasattr(payload, "model_dump"):
        parsed = payload.model_dump()
    elif isinstance(payload, dict):
        parsed = payload
    else:
        raise RuntimeError("Structured parser returned unsupported payload type")

    return parsed, usage
