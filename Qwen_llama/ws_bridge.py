"""FastAPI websocket bridge for live query state streaming.

This bridges existing polling-based Motia query state into websocket pushes.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI(title="Sales Analysis WS Bridge", version="0.1.0")

MOTIA_API_BASE = os.getenv("MOTIA_API_BASE_URL", "http://localhost:3121").rstrip("/")
POLL_SECONDS = float(os.getenv("WS_BRIDGE_POLL_SEC", "0.7"))


def _stable_hash(payload: dict[str, Any]) -> str:
    try:
        return json.dumps(payload, sort_keys=True, default=str)
    except Exception:
        return str(payload)


async def _fetch_query_state(
    client: httpx.AsyncClient, query_id: str
) -> dict[str, Any] | None:
    try:
        resp = await client.get(f"{MOTIA_API_BASE}/query/{query_id}")
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "motia_api_base": MOTIA_API_BASE}


@app.websocket("/ws/query/{query_id}")
async def ws_query_state(websocket: WebSocket, query_id: str) -> None:
    await websocket.accept()
    last_hash: str | None = None

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            while True:
                state = await _fetch_query_state(client, query_id)
                if state is not None:
                    cur_hash = _stable_hash(state)
                    if cur_hash != last_hash:
                        await websocket.send_json(state)
                        last_hash = cur_hash

                    status = str(state.get("status") or "")
                    if status in {"completed", "error", "needs_clarification"}:
                        # Keep socket open briefly for client to process terminal state.
                        await asyncio.sleep(0.2)
                        break

                await asyncio.sleep(POLL_SECONDS)
        except WebSocketDisconnect:
            return
        except Exception:
            try:
                await websocket.send_json(
                    {
                        "status": "bridge_error",
                        "error": "websocket bridge encountered an unexpected error",
                    }
                )
            except Exception:
                pass
        finally:
            try:
                await websocket.close()
            except Exception:
                pass
