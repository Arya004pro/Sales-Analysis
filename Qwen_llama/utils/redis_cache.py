"""Tiny Redis cache helper with safe fallbacks.

All functions are no-op when Redis is unavailable, so pipeline execution
never depends on Redis uptime.
"""

from __future__ import annotations

import os
from typing import Optional

_CLIENT = None
_CLIENT_INIT_DONE = False


def _is_enabled() -> bool:
    return os.getenv("REDIS_URL", "").strip() != ""


def _get_client():
    global _CLIENT, _CLIENT_INIT_DONE
    if _CLIENT_INIT_DONE:
        return _CLIENT
    _CLIENT_INIT_DONE = True

    if not _is_enabled():
        _CLIENT = None
        return None

    try:
        import redis

        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = redis.Redis.from_url(url, decode_responses=True, socket_timeout=0.5)
        client.ping()
        _CLIENT = client
    except Exception:
        _CLIENT = None
    return _CLIENT


def redis_cache_get(key: str) -> Optional[str]:
    client = _get_client()
    if client is None:
        return None
    try:
        value = client.get(key)
        if value is None:
            return None
        return str(value)
    except Exception:
        return None


def redis_cache_set(key: str, value: str, ttl_seconds: int | None = None) -> bool:
    client = _get_client()
    if client is None:
        return False
    try:
        if ttl_seconds and ttl_seconds > 0:
            client.setex(key, ttl_seconds, value)
        else:
            client.set(key, value)
        return True
    except Exception:
        return False
