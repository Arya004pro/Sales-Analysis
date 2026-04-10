"""Chroma-backed SQL memory for few-shot retrieval.

Stores successful query->SQL pairs and retrieves nearest examples for prompt
conditioning. Safe no-op when Chroma is disabled or unavailable.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_COLLECTION = None
_CLIENT_INIT_DONE = False


def _enabled() -> bool:
    return os.getenv("CHROMA_ENABLED", "1") == "1"


def _get_collection():
    global _COLLECTION, _CLIENT_INIT_DONE
    if _CLIENT_INIT_DONE:
        return _COLLECTION
    _CLIENT_INIT_DONE = True

    if not _enabled():
        _COLLECTION = None
        return None

    try:
        import chromadb

        root = Path(__file__).resolve().parents[1]
        db_path = os.getenv("CHROMA_PATH", str(root / "motia" / "data" / "chroma"))
        Path(db_path).mkdir(parents=True, exist_ok=True)
        client = chromadb.PersistentClient(path=db_path)
        _COLLECTION = client.get_or_create_collection(
            name=os.getenv("CHROMA_COLLECTION", "sql_memory")
        )
    except Exception:
        _COLLECTION = None
    return _COLLECTION


def _example_id(user_query: str, sql: str) -> str:
    key = f"{user_query.strip()}::{sql.strip()}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def store_sql_example(
    user_query: str,
    generated_sql: str,
    parsed: dict[str, Any] | None = None,
    result_rows: int | None = None,
) -> bool:
    collection = _get_collection()
    if collection is None:
        return False

    q = (user_query or "").strip()
    s = (generated_sql or "").strip()
    if not q or not s:
        return False

    try:
        parsed_obj = parsed if isinstance(parsed, dict) else {}
        meta = {
            "query_type": str(parsed_obj.get("query_type") or ""),
            "metric": str(parsed_obj.get("metric") or ""),
            "entity": str(parsed_obj.get("entity") or ""),
            "time_bucket": str(parsed_obj.get("time_bucket") or ""),
            "result_rows": int(result_rows or 0),
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        doc = f"User query: {q}\n\nSQL:\n{s}"
        collection.upsert(
            ids=[_example_id(q, s)],
            documents=[doc],
            metadatas=[meta],
        )
        return True
    except Exception:
        return False


def fetch_sql_examples(user_query: str, limit: int = 3) -> list[dict[str, Any]]:
    collection = _get_collection()
    q = (user_query or "").strip()
    if collection is None or not q or limit <= 0:
        return []

    try:
        res = collection.query(
            query_texts=[q],
            n_results=max(1, limit),
            include=["documents", "metadatas", "distances"],
        )
    except Exception:
        return []

    docs = (res.get("documents") or [[]])[0] or []
    metas = (res.get("metadatas") or [[]])[0] or []
    dists = (res.get("distances") or [[]])[0] or []

    out: list[dict[str, Any]] = []
    for i, doc in enumerate(docs):
        meta = metas[i] if i < len(metas) and isinstance(metas[i], dict) else {}
        distance = dists[i] if i < len(dists) else None

        sql_text = ""
        if isinstance(doc, str) and "SQL:" in doc:
            sql_text = doc.split("SQL:", 1)[1].strip()
        elif isinstance(doc, str):
            sql_text = doc.strip()

        out.append(
            {
                "sql": sql_text,
                "metadata": meta,
                "distance": distance,
            }
        )
    return out


def render_examples_block(examples: list[dict[str, Any]]) -> str:
    if not examples:
        return ""

    lines = [
        "Few-shot SQL examples from past successful queries",
        "-----------------------------------------------",
    ]
    for idx, ex in enumerate(examples, start=1):
        sql_text = str(ex.get("sql") or "").strip()
        if not sql_text:
            continue
        meta = ex.get("metadata") if isinstance(ex.get("metadata"), dict) else {}
        qtype = str(meta.get("query_type") or "unknown")
        metric = str(meta.get("metric") or "")
        entity = str(meta.get("entity") or "")
        descriptor = ", ".join(x for x in [qtype, metric, entity] if x)
        lines.append(f"Example {idx} ({descriptor or 'historical'}):")
        lines.append(sql_text)
        lines.append("")

    rendered = "\n".join(lines).strip()
    return rendered
