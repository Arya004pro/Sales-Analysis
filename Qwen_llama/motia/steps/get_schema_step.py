"""GET /schema - return current ingested schema information."""

import os
import sys
from typing import Any
from motia import ApiRequest, ApiResponse, FlowContext, http

_STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
_MOTIA_DIR = os.path.dirname(_STEPS_DIR)
_PROJECT_ROOT = os.path.dirname(_MOTIA_DIR)
for _p in [_STEPS_DIR, _MOTIA_DIR, _PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db.duckdb_connection import get_read_connection


_REL_PREFIX = "_raw_rel_"


def _base_display_name(name: str) -> str:
    if name.startswith(_REL_PREFIX):
        return name[len(_REL_PREFIX) :]
    return name


def _build_display_name_map(table_names: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    taken: set[str] = set()
    for t in table_names:
        base = _base_display_name(t)
        cand = base
        i = 2
        while cand in taken:
            cand = f"{base}_{i}"
            i += 1
        mapping[t] = cand
        taken.add(cand)
    return mapping


def _map_relationships_for_display(
    rels: list[dict[str, Any]], name_map: dict[str, str]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rels or []:
        out.append(
            {
                **r,
                "from_table": name_map.get(
                    str(r.get("from_table", "")), str(r.get("from_table", ""))
                ),
                "to_table": name_map.get(
                    str(r.get("to_table", "")), str(r.get("to_table", ""))
                ),
            }
        )
    return out


def _filter_to_primary_component(
    tables: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
    row_counts: dict[str, int] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Keep the most relevant connected component for ER display.

    This is schema-agnostic and avoids noisy disconnected subgraphs.
    Component score uses total rows first, then table count.
    """
    if not tables:
        return tables, relationships
    if not relationships:
        return tables, relationships

    names = [str(t.get("table", "")) for t in tables if str(t.get("table", "")).strip()]
    if not names:
        return tables, relationships
    name_set = set(names)

    adj: dict[str, set[str]] = {n: set() for n in names}
    for r in relationships:
        a = str(r.get("from_table", ""))
        b = str(r.get("to_table", ""))
        if a in name_set and b in name_set and a != b:
            adj[a].add(b)
            adj[b].add(a)

    visited: set[str] = set()
    comps: list[set[str]] = []
    for n in names:
        if n in visited:
            continue
        stack = [n]
        comp: set[str] = set()
        visited.add(n)
        while stack:
            cur = stack.pop()
            comp.add(cur)
            for nb in adj.get(cur, set()):
                if nb not in visited:
                    visited.add(nb)
                    stack.append(nb)
        comps.append(comp)

    if len(comps) <= 1:
        return tables, relationships

    rc = row_counts or {}
    best = max(
        comps,
        key=lambda comp: (
            sum(int(rc.get(t, 0)) for t in comp),
            len(comp),
        ),
    )

    kept_tables = [t for t in tables if str(t.get("table", "")) in best]
    kept_rels = [
        r
        for r in relationships
        if str(r.get("from_table", "")) in best and str(r.get("to_table", "")) in best
    ]
    return kept_tables, kept_rels


def _filter_relationships_by_visible_columns(
    tables: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Keep only relationships whose endpoint columns exist in visible table schemas.

    This prevents stale/invalid links from older ingests from polluting the ER view.
    """
    if not tables or not relationships:
        return relationships

    cols_by_table: dict[str, set[str]] = {}
    for t in tables:
        name = str(t.get("table", "")).strip()
        if not name:
            continue
        cols = {str(c.get("name", "")).strip() for c in (t.get("columns") or [])}
        cols_by_table[name] = {c for c in cols if c}

    out: list[dict[str, Any]] = []
    for r in relationships:
        ft = str(r.get("from_table", "")).strip()
        fc = str(r.get("from_column", "")).strip()
        tt = str(r.get("to_table", "")).strip()
        tc = str(r.get("to_column", "")).strip()
        if not ft or not fc or not tt or not tc:
            continue
        if fc not in cols_by_table.get(ft, set()):
            continue
        if tc not in cols_by_table.get(tt, set()):
            continue
        out.append(r)
    return out


config = {
    "name": "GetSchema",
    "description": "Utility endpoint: returns current DuckDB schema and relationships",
    "flows": ["sales-analytics-utilities"],
    "triggers": [http("GET", "/schema")],
    "enqueues": [],
}


async def handler(request: ApiRequest[Any], ctx: FlowContext[Any]) -> ApiResponse[Any]:
    def _query_param(req: ApiRequest[Any], key: str, default: str = "") -> str:
        # Be tolerant to runtime request shape.
        for attr in ("query", "query_params", "params"):
            obj = getattr(req, attr, None)
            if isinstance(obj, dict) and key in obj:
                return str(obj.get(key) or default)
        return default

    view_mode = _query_param(request, "view", "derived").strip().lower()
    if view_mode not in {"derived", "raw", "all"}:
        view_mode = "derived"

    schema_state = await ctx.state.get("schema_registry", "current")
    auto_structure = bool((schema_state or {}).get("auto_structure", False))
    latest_tables = [
        str(t)
        for t in ((schema_state or {}).get("tables_created") or [])
        if str(t).strip()
    ]

    conn = get_read_connection()
    try:
        all_tables_rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main'
              AND table_name NOT LIKE '_raw_meta_%'
            ORDER BY table_name
            """
        ).fetchall()
        all_table_names = [t for (t,) in all_tables_rows]

        # Prefer showing only tables from the most recent ingest snapshot.
        if latest_tables:
            tables = [(t,) for t in latest_tables if t in all_table_names]
        else:
            tables = all_tables_rows

        # Split schema views by intent:
        # - derived: only generated helper tables
        # - raw: only uploaded/base tables
        # - all: no split
        if view_mode == "derived":
            derived = [(t,) for (t,) in tables if t.startswith(_REL_PREFIX)]
            if derived:
                tables = derived
            elif auto_structure:
                # If user asked derived view but no derived tables exist, keep empty.
                tables = []
        elif view_mode == "raw":
            tables = [(t,) for (t,) in tables if not t.startswith(_REL_PREFIX)]

        table_names = [t for (t,) in tables]
        display_map = _build_display_name_map(table_names)
        table_schema = []
        for (t,) in tables:
            cols = conn.execute(f'DESCRIBE "{t}"').fetchall()
            table_schema.append(
                {
                    "table": display_map.get(t, t),
                    "columns": [{"name": c[0], "type": c[1]} for c in cols],
                }
            )
    finally:
        conn.close()

    mapped_relationships = _map_relationships_for_display(
        (schema_state or {}).get("relationships", []),
        display_map if "display_map" in locals() else {},
    )

    shown_tables = {t["table"] for t in table_schema}
    mapped_relationships = [
        r
        for r in mapped_relationships
        if r.get("from_table") in shown_tables and r.get("to_table") in shown_tables
    ]
    mapped_relationships = _filter_relationships_by_visible_columns(
        table_schema,
        mapped_relationships,
    )

    # In auto-structured mode, show the primary connected component only
    # for very large derived schemas.
    # This keeps the ER view focused and removes weak/disconnected fragments.
    if auto_structure and view_mode == "derived" and len(table_schema) > 12:
        display_row_counts: dict[str, int] = {}
        raw_row_counts = (schema_state or {}).get("row_counts") or {}
        for raw_name, count in raw_row_counts.items():
            disp = display_map.get(str(raw_name), str(raw_name))
            display_row_counts[disp] = int(count or 0)
        table_schema, mapped_relationships = _filter_to_primary_component(
            table_schema,
            mapped_relationships,
            row_counts=display_row_counts,
        )

    return ApiResponse(
        status=200,
        body={
            "tables": table_schema,
            "relationships": mapped_relationships,
            "view": view_mode,
            "registry": schema_state or {},
        },
    )
