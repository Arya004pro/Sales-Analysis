"""utils/token_logger.py

Centralised token-usage tracking for all LLM calls.

Usage (in any Motia step):
    from utils.token_logger import log_tokens, add_tokens_to_state

    usage = response.json().get("usage", {})
    log_tokens(ctx, query_id, step_name, model_name, usage)
    await add_tokens_to_state(ctx, query_id, step_name, model_name, usage)

Usage (in terminal app):
    from utils.token_logger import print_token_usage
    print_token_usage(step_name, model_name, usage)
"""

from __future__ import annotations
from typing import Any


# ─── Dynamic token budget helpers ───────────────────────────────────────────

_CHARS_PER_TOKEN = 3.8

_MODEL_CONTEXT: dict[str, int] = {
    "qwen/qwen3-32b": 32768,
    "llama-3.3-70b-versatile": 32768,
    "llama-3.1-8b-instant": 8192,
    "default": 32768,
}

_TASK_CEILINGS: dict[str, int] = {
    "parse_intent": 320,
    "text_to_sql": 700,
    "sql_repair": 520,
    "schema_map": 256,
    "insights": 180,
    "ambiguity": 128,
}

_TASK_MINIMUMS: dict[str, int] = {
    "parse_intent": 90,
    "text_to_sql": 140,
    "sql_repair": 120,
    "schema_map": 100,
    "insights": 80,
    "ambiguity": 60,
}


def estimate_prompt_tokens(prompt: str | list[dict]) -> int:
    if isinstance(prompt, list):
        text = " ".join(
            str(m.get("content", "")) for m in prompt if isinstance(m, dict)
        )
    else:
        text = str(prompt)
    return max(1, int(len(text) / _CHARS_PER_TOKEN))


def calc_max_tokens(
    prompt: str | list[dict],
    task: str,
    model: str = "default",
    headroom_ratio: float = 0.25,
) -> int:
    model_key = model
    if model_key not in _MODEL_CONTEXT:
        short = model.split("/")[-1] if "/" in model else model
        model_key = next(
            (k for k in _MODEL_CONTEXT if short in k or k in short), "default"
        )

    context_window = _MODEL_CONTEXT[model_key]
    prompt_tokens = estimate_prompt_tokens(prompt)
    available = int(context_window * (1 - headroom_ratio)) - prompt_tokens
    ceiling = _TASK_CEILINGS.get(task, 512)
    minimum = _TASK_MINIMUMS.get(task, 100)
    return max(minimum, min(available, ceiling))


# ─── helpers ────────────────────────────────────────────────────────────────


def _safe_int(val) -> int:
    try:
        return int(val or 0)
    except (TypeError, ValueError):
        return 0


def _build_entry(step: str, model: str, usage: dict) -> dict:
    prompt = _safe_int(usage.get("prompt_tokens"))
    completion = _safe_int(usage.get("completion_tokens"))
    total = _safe_int(usage.get("total_tokens")) or prompt + completion
    is_llm = model not in ("rule_based", "adaptive_rule")
    return {
        "step": step,
        "model": model,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "is_llm": is_llm,
    }


# ─── Motia step helpers ──────────────────────────────────────────────────────


def log_tokens(ctx: Any, query_id: str, step: str, model: str, usage: dict) -> None:
    """Emit a structured token-usage log line through the Motia logger."""
    entry = _build_entry(step, model, usage)
    ctx.logger.info(
        "🪙 Token usage",
        {
            "queryId": query_id,
            "step": entry["step"],
            "model": entry["model"],
            "prompt_tokens": entry["prompt_tokens"],
            "completion_tokens": entry["completion_tokens"],
            "total_tokens": entry["total_tokens"],
        },
    )


async def add_tokens_to_state(
    ctx: Any,
    query_id: str,
    step: str,
    model: str,
    usage: dict,
) -> None:
    """
    Append a token-usage entry to the query's state and keep a running total.

    State shape added / updated:
        query_state["token_usage"]  = [
            {"step": "AmbiguityCheck", "model": "qwen/...", "prompt_tokens": 120,
             "completion_tokens": 8, "total_tokens": 128},
            ...
        ]
        query_state["token_totals"] = {
            "prompt_tokens": 240, "completion_tokens": 18, "total_tokens": 258
        }
    """
    query_state = await ctx.state.get("queries", query_id)
    if not query_state:
        return

    entry = _build_entry(step, model, usage)
    log_list = list(query_state.get("token_usage", []))
    log_list.append(entry)

    # Running totals
    totals = query_state.get(
        "token_totals", {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    )
    totals = {
        "prompt_tokens": totals["prompt_tokens"] + entry["prompt_tokens"],
        "completion_tokens": totals["completion_tokens"] + entry["completion_tokens"],
        "total_tokens": totals["total_tokens"] + entry["total_tokens"],
    }

    await ctx.state.set(
        "queries",
        query_id,
        {
            **query_state,
            "token_usage": log_list,
            "token_totals": totals,
        },
    )


# ─── Terminal (app.py) helper ────────────────────────────────────────────────

# Running totals for the current terminal session query
_session_totals: dict[str, int] = {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0,
}


def print_token_usage(step: str, model: str, usage: dict) -> None:
    """Print a formatted token-usage line and update session totals."""
    global _session_totals
    entry = _build_entry(step, model, usage)

    _session_totals["prompt_tokens"] += entry["prompt_tokens"]
    _session_totals["completion_tokens"] += entry["completion_tokens"]
    _session_totals["total_tokens"] += entry["total_tokens"]

    short_model = model.split("/")[-1]
    print(
        f"  [tokens] {step} | {short_model} | "
        f"prompt={entry['prompt_tokens']} "
        f"completion={entry['completion_tokens']} "
        f"total={entry['total_tokens']} "
        f"| session_total={_session_totals['total_tokens']}"
    )


def reset_session_totals() -> None:
    """Call after each completed query in the terminal loop."""
    global _session_totals
    _session_totals = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}


def get_session_totals() -> dict:
    return dict(_session_totals)
