"""User data ingestion helpers for DuckDB."""

from __future__ import annotations

import base64
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import DUCKDB_PATH
from db.duckdb_connection import get_write_connection

_PROJECT_ROOT = Path(__file__).resolve().parents[1]

_META_INGEST_REGISTRY = "_raw_meta_ingest_registry"
_META_SEMANTIC_HINTS = "_raw_meta_semantic_hints"
_SUPPORTED_DISCOVERY_EXTS = {".csv", ".tsv", ".json", ".jsonl", ".parquet"}
_DISCOVERY_SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv"}

_REL_PREFIX = "_raw_rel_"
_REL_KEY_SUFFIXES = ("_id", "_code", "_key", "_uuid")
_BUSINESS_TOKEN_SKIP = {
    "id",
    "key",
    "code",
    "uuid",
    "name",
    "date",
    "time",
    "datetime",
    "timestamp",
    "created",
    "updated",
    "is",
    "has",
    "row",
    "index",
    "no",
    "type",
    "status",
    "value",
    "amount",
    "total",
    "final",
    "unit",
}
_GENERIC_BUSINESS_ROOTS = {
    "id",
    "key",
    "code",
    "uuid",
    "name",
    "date",
    "time",
    "datetime",
    "timestamp",
    "created",
    "updated",
    "row",
    "index",
    "type",
    "status",
    "value",
    "amount",
    "total",
    "final",
    "unit",
}


def _safe_table_name(name: str, used: set[str]) -> str:
    stem = Path(name).stem.lower()
    stem = re.sub(r"[^a-z0-9_]+", "_", stem).strip("_") or "uploaded_data"
    table = stem
    i = 2
    while table in used:
        table = f"{stem}_{i}"
        i += 1
    used.add(table)
    return table


def _write_temp_file(name: str, b64_data: str, upload_dir: Path) -> Path:
    upload_dir.mkdir(parents=True, exist_ok=True)
    p = upload_dir / name
    p.write_bytes(base64.b64decode(b64_data))
    return p


def _sql_quote_path(file_path: Path) -> str:
    return str(file_path.resolve()).replace("\\", "/").replace("'", "''")


def _qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _path_key(p: Path) -> str:
    try:
        return str(p.resolve()).replace("\\", "/")
    except Exception:
        return str(p).replace("\\", "/")


def _ensure_meta_tables(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_qident(_META_INGEST_REGISTRY)} (
            file_path VARCHAR PRIMARY KEY,
            file_name VARCHAR,
            file_size BIGINT,
            file_mtime DOUBLE,
            table_name VARCHAR,
            ingested_at VARCHAR
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_qident(_META_SEMANTIC_HINTS)} (
            table_name VARCHAR PRIMARY KEY,
            hint_json VARCHAR,
            updated_at VARCHAR
        )
        """
    )


def _load_known_files(conn) -> dict[str, tuple[int, float]]:
    try:
        rows = conn.execute(
            f"SELECT file_path, file_size, file_mtime FROM {_qident(_META_INGEST_REGISTRY)}"
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, tuple[int, float]] = {}
    for fp, size, mtime in rows:
        out[str(fp)] = (int(size or 0), float(mtime or 0.0))
    return out


def _record_ingested_file(conn, fp: Path, table: str) -> None:
    try:
        st = fp.stat()
        file_size = int(st.st_size)
        file_mtime = float(st.st_mtime)
    except Exception:
        file_size = 0
        file_mtime = 0.0

    pkey = _path_key(fp)
    conn.execute(
        f"DELETE FROM {_qident(_META_INGEST_REGISTRY)} WHERE file_path = ?",
        [pkey],
    )
    conn.execute(
        f"""
        INSERT INTO {_qident(_META_INGEST_REGISTRY)}
            (file_path, file_name, file_size, file_mtime, table_name, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [pkey, fp.name, file_size, file_mtime, table, _now_iso()],
    )


def _record_semantic_hint(conn, table: str, hint: dict[str, Any]) -> None:
    payload = json.dumps(hint, ensure_ascii=True)
    conn.execute(
        f"DELETE FROM {_qident(_META_SEMANTIC_HINTS)} WHERE table_name = ?",
        [table],
    )
    conn.execute(
        f"""
        INSERT INTO {_qident(_META_SEMANTIC_HINTS)} (table_name, hint_json, updated_at)
        VALUES (?, ?, ?)
        """,
        [table, payload, _now_iso()],
    )


def _discover_drop_files(
    conn,
    scan_dirs: list[Path],
    include_seen: bool = False,
    latest_only: bool = True,
    max_files: int = 10,
) -> tuple[list[dict[str, Any]], list[str]]:
    known = _load_known_files(conn)
    candidates: list[tuple[float, Path]] = []

    for root in scan_dirs:
        if not root.exists() or not root.is_dir():
            continue
        try:
            walker = root.rglob("*")
        except Exception:
            continue

        for p in walker:
            if not p.is_file():
                continue
            if p.suffix.lower() not in _SUPPORTED_DISCOVERY_EXTS:
                continue
            if any(part in _DISCOVERY_SKIP_DIRS for part in p.parts):
                continue

            key = _path_key(p)
            try:
                st = p.stat()
                sig = (int(st.st_size), float(st.st_mtime))
            except Exception:
                sig = (0, 0.0)

            if (not include_seen) and key in known and known[key] == sig:
                continue
            candidates.append((sig[1], p))

    if not candidates:
        return [], []

    candidates.sort(key=lambda x: x[0], reverse=True)
    if latest_only:
        candidates = candidates[:1]
    else:
        candidates = candidates[:max_files]

    files: list[dict[str, Any]] = []
    discovered: list[str] = []
    seen_paths: set[str] = set()
    for _mtime, p in candidates:
        k = _path_key(p)
        if k in seen_paths:
            continue
        seen_paths.add(k)
        files.append({"name": p.name, "path": str(p)})
        discovered.append(k)
    return files, discovered


def _strip_markdown_fence(text: str) -> str:
    s = (text or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z0-9_\-]*", "", s).strip()
        if s.endswith("```"):
            s = s[:-3].strip()
    return s


def _extract_json_object(text: str) -> dict[str, Any] | None:
    raw = _strip_markdown_fence(text)
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _safe_aliases(v: Any, max_items: int = 8) -> list[str]:
    if not isinstance(v, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for x in v:
        s = str(x or "").strip().lower()
        if not s or len(s) > 60:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= max_items:
            break
    return out


def _infer_schema_hint_with_llm(
    conn, table: str, cols: list[tuple[Any, ...]]
) -> dict[str, Any] | None:
    try:
        from config import GROQ_API_TOKEN, QWEN_MODEL
        from llm.client import call_llm
    except Exception:
        return None

    if not GROQ_API_TOKEN:
        return None

    col_defs = [{"name": str(c[0]), "type": str(c[1])} for c in cols[:80]]
    col_names = [str(c[0]) for c in cols[:20]]
    sample_rows: list[dict[str, Any]] = []
    if col_names:
        try:
            select_sql = ", ".join(_qident(c) for c in col_names)
            rows = conn.execute(
                f"SELECT {select_sql} FROM {_qident(table)} WHERE TRUE LIMIT 5"
            ).fetchall()
            for r in rows:
                sample_rows.append(
                    {
                        col_names[i]: (None if r[i] is None else str(r[i])[:120])
                        for i in range(len(col_names))
                    }
                )
        except Exception:
            sample_rows = []

    system_prompt = (
        "You infer analytics semantics for a newly ingested table. "
        "Return ONLY valid JSON with this schema: "
        '{"table_summary": string, "dimensions": [{"name": string, "aliases": [string], "join_key": string}], '
        '"metrics": [{"name": string, "aliases": [string]}], "join_keys": [string]}. '
        "Keep aliases short business terms and avoid guessing columns that do not exist."
    )
    user_payload = {
        "table": table,
        "columns": col_defs,
        "sample_rows": sample_rows,
    }

    try:
        llm_result = call_llm(
            model_name=QWEN_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": json.dumps(user_payload, ensure_ascii=True),
                },
            ],
            token=GROQ_API_TOKEN,
            max_tokens=500,
            step_name="IngestSchemaHint",
        )
        content = str(llm_result["choices"][0]["message"]["content"])
    except Exception:
        return None

    parsed = _extract_json_object(content)
    if not parsed:
        return None

    dim_aliases: dict[str, list[str]] = {}
    for d in parsed.get("dimensions") or []:
        if not isinstance(d, dict):
            continue
        name = str(d.get("name") or "").strip().lower()
        if not name:
            continue
        als = _safe_aliases(d.get("aliases"))
        if als:
            dim_aliases[name] = als

    metric_aliases: dict[str, list[str]] = {}
    for m in parsed.get("metrics") or []:
        if not isinstance(m, dict):
            continue
        name = str(m.get("name") or "").strip().lower()
        if not name:
            continue
        als = _safe_aliases(m.get("aliases"))
        if als:
            metric_aliases[name] = als

    join_keys = _safe_aliases(parsed.get("join_keys"), max_items=10)
    summary = str(parsed.get("table_summary") or "").strip()

    hint: dict[str, Any] = {
        "table": table,
        "dimension_aliases": dim_aliases,
        "metric_aliases": metric_aliases,
        "join_keys": join_keys,
    }
    if summary:
        hint["table_summary"] = summary[:240]
    return hint


def _resolve_input_file(file_obj: dict[str, Any], upload_dir: Path) -> Path:
    raw_path = str(file_obj.get("path") or "").strip()
    if raw_path:
        p = Path(raw_path)
        if p.exists() and p.is_file():
            return p
    name = str(file_obj.get("name") or "").strip()
    data = str(file_obj.get("data") or "").strip()
    if name and data:
        return _write_temp_file(name, data, upload_dir)
    raise ValueError("Each file must include either a valid path or (name + data).")


def _load_file_to_table(conn, file_path: Path, table: str) -> int:
    ext = file_path.suffix.lower()
    p = _sql_quote_path(file_path)
    if ext in (".csv", ".tsv"):
        delim_clause = "delim='\\t'," if ext == ".tsv" else "delim=',',"
        try:
            conn.execute(
                f'CREATE OR REPLACE TABLE "{table}" AS '
                f"SELECT * FROM read_csv_auto('{p}', {delim_clause} header=true)"
            )
        except Exception:
            conn.execute(
                f'CREATE OR REPLACE TABLE "{table}" AS '
                f"SELECT * FROM read_csv_auto('{p}', {delim_clause} header=true, sample_size=-1, "
                "all_varchar=true, ignore_errors=true, null_padding=true)"
            )
    elif ext in (".json", ".jsonl"):
        conn.execute(
            f"CREATE OR REPLACE TABLE \"{table}\" AS SELECT * FROM read_json_auto('{p}')"
        )
    elif ext == ".parquet":
        conn.execute(
            f"CREATE OR REPLACE TABLE \"{table}\" AS SELECT * FROM read_parquet('{p}')"
        )
    else:
        raise ValueError(f"Unsupported file format: {ext}")
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def _pluralize(token: str) -> str:
    t = token.strip().lower()
    if not t:
        return "entities"
    if t.endswith("y") and len(t) > 1 and t[-2] not in "aeiou":
        return t[:-1] + "ies"
    if t.endswith(("s", "x", "z", "ch", "sh")):
        return t + "es"
    return t + "s"


def _derive_rel_table_name(base_table: str, token: str, used: set[str]) -> str:
    base = f"{_REL_PREFIX}{_pluralize(token)}"
    if base not in used:
        used.add(base)
        return base

    scoped = f"{_REL_PREFIX}{base_table}_{_pluralize(token)}"
    if scoped not in used:
        used.add(scoped)
        return scoped

    i = 2
    while True:
        cand = f"{scoped}_{i}"
        if cand not in used:
            used.add(cand)
            return cand
        i += 1


def _all_main_tables(conn) -> set[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()
    return {r[0] for r in rows}


def _table_business_signature(
    col_names: list[str],
) -> tuple[set[str], set[str], set[str]]:
    """
    Return (semantic_tokens, key_roots, topic_tokens) for compatibility checks.
    Fully schema-agnostic and name-pattern based.
    """
    tokens: set[str] = set()
    key_roots: set[str] = set()
    topic_tokens: set[str] = set()

    for c in col_names:
        cl = str(c).strip().lower()
        if not cl:
            continue

        for suf in _REL_KEY_SUFFIXES:
            if cl.endswith(suf) and len(cl) > len(suf):
                root = cl[: -len(suf)]
                key_roots.add(root)
                if root not in _GENERIC_BUSINESS_ROOTS:
                    topic_tokens.add(root)
                break

        if cl.endswith("_name") and len(cl) > 5:
            root = cl[:-5]
            if root and root not in _GENERIC_BUSINESS_ROOTS:
                topic_tokens.add(root)

        parts = [p for p in re.split(r"[^a-z0-9]+", cl) if p]
        for p in parts:
            if len(p) <= 2:
                continue
            if p in _BUSINESS_TOKEN_SKIP:
                continue
            tokens.add(p)
            if p not in _GENERIC_BUSINESS_ROOTS:
                topic_tokens.add(p)

    return tokens, key_roots, topic_tokens


def _is_business_compatible(
    anchor_tokens: set[str],
    anchor_keys: set[str],
    anchor_topics: set[str],
    cand_tokens: set[str],
    cand_keys: set[str],
    cand_topics: set[str],
) -> bool:
    """
    Determine if two files likely belong to the same business schema.
    """
    anchor_specific_keys = {k for k in anchor_keys if k not in _GENERIC_BUSINESS_ROOTS}
    cand_specific_keys = {k for k in cand_keys if k not in _GENERIC_BUSINESS_ROOTS}
    shared_specific_keys = anchor_specific_keys & cand_specific_keys
    if shared_specific_keys:
        return True

    # Strong signal: overlap in domain/topic vocabulary (non-generic).
    if anchor_topics and cand_topics:
        shared_topics = anchor_topics & cand_topics
        topic_jaccard = len(shared_topics) / max(len(anchor_topics | cand_topics), 1)
        if len(shared_topics) >= 2 or topic_jaccard >= 0.22:
            return True

    if not anchor_tokens or not cand_tokens:
        return False

    shared_tokens = anchor_tokens & cand_tokens
    jaccard = len(shared_tokens) / max(len(anchor_tokens | cand_tokens), 1)
    # Generic-token fallback must be stricter to avoid cross-business mixing.
    return (len(shared_tokens) >= 4) and (jaccard >= 0.18)


def _text_profile(conn, table: str, col: str) -> tuple[int, int, float, int]:
    """Return (distinct_count, total_non_null, avg_len, max_len) for a text column."""
    try:
        r = conn.execute(
            f"SELECT COUNT(DISTINCT {_qident(col)}), COUNT({_qident(col)}), "
            f"AVG(LENGTH(CAST({_qident(col)} AS VARCHAR))), "
            f"MAX(LENGTH(CAST({_qident(col)} AS VARCHAR))) "
            f"FROM {_qident(table)} WHERE {_qident(col)} IS NOT NULL"
        ).fetchone()
        return int(r[0] or 0), int(r[1] or 0), float(r[2] or 0.0), int(r[3] or 0)
    except Exception:
        return 0, 0, 0.0, 0


def _is_dimension_text_column(
    conn, table: str, col: str, dtype: str
) -> tuple[bool, float]:
    """
    Data-driven dimension detector for text columns.

    Keeps likely categorical/dimension fields and excludes:
      - key/date-like columns
      - near-constant fields
      - free-form long text columns
      - near-unique identifiers
    """
    dtu = str(dtype).upper()
    if not any(t in dtu for t in ("VARCHAR", "CHAR", "TEXT", "STRING")):
        return False, 0.0

    cl = col.lower()
    if cl in _SKIP_COLS:
        return False, 0.0
    if any(cl.endswith(s) for s in _REL_KEY_SUFFIXES):
        return False, 0.0
    if any(k in cl for k in _DATE_HINTS):
        return False, 0.0

    distinct_cnt, total_cnt, avg_len, max_len = _text_profile(conn, table, col)
    if total_cnt < 2 or distinct_cnt < 2:
        return False, 0.0

    ratio = distinct_cnt / max(total_cnt, 1)

    # Keep high-cardinality business labels (e.g. customer_name, driver_name).
    # They can still be useful dimensions even when close to unique.
    name_like = bool(re.search(r"(?:^|_)(name|title|label)$", cl))
    if ratio > 0.98 and not name_like:
        return False, 0.0
    if avg_len > 40 or max_len > 200:
        return False, 0.0

    return True, ratio


def _dependency_score(conn, table: str, key_col: str, attr_col: str) -> float:
    """Return how often attr_col has a single value per key_col (0..1)."""
    if key_col == attr_col:
        return 0.0
    try:
        r = conn.execute(
            f"SELECT AVG(CASE WHEN cnt <= 1 THEN 1.0 ELSE 0.0 END) "
            f"FROM ("
            f"  SELECT {_qident(key_col)} AS k, COUNT(DISTINCT {_qident(attr_col)}) AS cnt "
            f"  FROM {_qident(table)} "
            f"  WHERE {_qident(key_col)} IS NOT NULL "
            f"  GROUP BY 1"
            f") s"
        ).fetchone()
        return float(r[0] or 0.0)
    except Exception:
        return 0.0


def _select_dependent_attrs(
    conn,
    table: str,
    key_col: str,
    candidate_cols: list[str],
    max_attrs: int = 3,
    min_score: float = 0.97,
) -> list[str]:
    """Pick columns that are functionally dependent on key_col in most rows."""
    picked: list[tuple[float, str]] = []
    for c in candidate_cols:
        cl = c.lower()
        if c == key_col or cl in _SKIP_COLS:
            continue
        score = _dependency_score(conn, table, key_col, c)
        if score >= min_score:
            picked.append((score, c))
    picked.sort(key=lambda x: (-x[0], x[1]))
    return [c for _, c in picked[:max_attrs]]


def _auto_structure_flat_table(
    conn, base_table: str, used_tables: set[str]
) -> tuple[list[str], dict[str, int], list[dict[str, Any]]]:
    """
    Build relational helper tables from a wide uploaded table.

    Strategy (schema-agnostic):
      - Infer entity anchors from key-like columns (*_id, *_code, *_key, *_uuid).
      - Build one helper table per anchor with functionally dependent attributes.
      - Build lookup dimensions from categorical text columns when key anchors are absent.
      - Infer relationships only from high-confidence shared join columns.
    """
    cols = conn.execute(f"DESCRIBE {_qident(base_table)}").fetchall()
    if not cols:
        return [], {}, []

    col_names = [c[0] for c in cols]
    col_by_lower = {c.lower(): c for c in col_names}
    col_type_by_name = {c[0]: str(c[1]) for c in cols}

    def _tokenize_col(col_name: str) -> str:
        c = col_name.lower()
        for suf in ("_id", "_code", "_key", "_uuid", "_name"):
            if c.endswith(suf) and len(c) > len(suf):
                return c[: -len(suf)]
        return c

    card_cache: dict[str, tuple[int, int]] = {}

    def _card(col_name: str) -> tuple[int, int]:
        if col_name not in card_cache:
            card_cache[col_name] = _col_cardinality(conn, base_table, col_name)
        return card_cache[col_name]

    def _card_ratio(col_name: str) -> float:
        d, n = _card(col_name)
        return (d / max(n, 1)) if n else 0.0

    groups: dict[str, dict[str, Any]] = {}
    for col in col_names:
        c = col.lower()
        if c in _SKIP_COLS:
            continue
        m = re.match(r"^([a-z][a-z0-9]*)_(id|code|key|uuid)$", c)
        if not m:
            continue
        prefix = m.group(1)
        groups[prefix] = {"key_col": col, "attrs": []}

    # Rank anchors so stable/business-meaningful entities are created first.
    ranked_groups: list[tuple[float, str, str]] = []
    for prefix, g in groups.items():
        key_col = g["key_col"]
        d_key, n_key = _card(key_col)
        if d_key < 2 or n_key < 2:
            continue
        key_ratio = _card_ratio(key_col)
        pref_attrs = [
            c for c in col_names if c != key_col and c.lower().startswith(prefix + "_")
        ]
        dep_hint = min(len(pref_attrs), 5) * 0.15
        ratio_score = 1.0 if key_ratio >= 0.95 else (0.5 if key_ratio >= 0.70 else 0.0)
        size_score = min(d_key, 10000) / 10000.0
        ranked_groups.append((ratio_score + dep_hint + size_score, prefix, key_col))
    ranked_groups.sort(key=lambda x: (-x[0], x[1]))
    fact_key_col = ranked_groups[0][2] if ranked_groups else None

    created: list[str] = []
    row_counts: dict[str, int] = {}
    rels: list[dict[str, Any]] = []
    derived_tokens: set[str] = set()
    table_signatures: set[tuple[str, ...]] = set()
    table_cols_map: dict[str, list[str]] = {}
    lookup_surrogate_map: dict[str, tuple[str, str, str]] = {}
    fact_rel_table: str | None = None

    for _score, prefix, key_col in ranked_groups:
        # Prefer same-prefix descriptive columns (customer_name, order_datetime, etc.)
        attrs: list[str] = [
            c for c in col_names if c != key_col and c.lower().startswith(prefix + "_")
        ]

        # Only the primary fact-like anchor should carry broad FK references.
        # Applying this to every high-cardinality key pollutes dimensions.
        if fact_key_col and key_col == fact_key_col:
            for c in col_names:
                cl = c.lower()
                if c == key_col or cl in _SKIP_COLS:
                    continue
                if any(cl.endswith(s) for s in _REL_KEY_SUFFIXES):
                    attrs.append(c)

        # For the primary fact-like anchor (usually orders/transactions), carry
        # richer analytical columns so derived tables remain useful.
        if fact_key_col and key_col == fact_key_col:
            for c in col_names:
                cl = c.lower()
                if c == key_col or cl in _SKIP_COLS:
                    continue
                dtype = col_type_by_name.get(c, "").upper()
                if any(k in cl for k in _DATE_HINTS):
                    attrs.append(c)
                    continue
                if any(
                    t in dtype
                    for t in (
                        "INT",
                        "DOUBLE",
                        "FLOAT",
                        "DECIMAL",
                        "NUMERIC",
                        "REAL",
                        "BOOLEAN",
                    )
                ):
                    attrs.append(c)
                    continue
                ok_dim, _ratio = _is_dimension_text_column(conn, base_table, c, dtype)
                if ok_dim:
                    attrs.append(c)

        # Keep deterministic order + dedupe.
        dedup: list[str] = []
        seen_cols: set[str] = set()
        for c in attrs:
            if c not in seen_cols:
                seen_cols.add(c)
                dedup.append(c)

        # Add high-confidence dependent attributes (data-driven).
        extra_candidates = [
            c
            for c in col_names
            if (
                c != key_col
                and c not in dedup
                and c.lower() not in _SKIP_COLS
                and not any(k in c.lower() for k in _DATE_HINTS)
                and (
                    (fact_key_col and key_col == fact_key_col)
                    or not any(c.lower().endswith(s) for s in _REL_KEY_SUFFIXES)
                )
            )
        ]
        dep_max_attrs = 12 if (fact_key_col and key_col == fact_key_col) else 8
        dep_min_score = 0.88 if (fact_key_col and key_col == fact_key_col) else 0.93
        dedup.extend(
            _select_dependent_attrs(
                conn,
                base_table,
                key_col,
                extra_candidates,
                max_attrs=dep_max_attrs,
                min_score=dep_min_score,
            )
        )

        # Final dedupe after appending dependent attrs.
        final_attrs: list[str] = []
        seen_final: set[str] = set()
        for c in dedup:
            if c not in seen_final:
                seen_final.add(c)
                final_attrs.append(c)
        max_cols = 20 if (fact_key_col and key_col == fact_key_col) else 10
        attrs = final_attrs[:max_cols]

        select_cols = [key_col] + attrs
        # Never emit single-column helper tables.
        if len(select_cols) < 2:
            continue

        sig = tuple(sorted(c.lower() for c in select_cols))
        if sig in table_signatures:
            continue

        rel_table = _derive_rel_table_name(base_table, prefix, used_tables)
        cols_sql = ", ".join(_qident(c) for c in select_cols)

        conn.execute(
            f"CREATE OR REPLACE TABLE {_qident(rel_table)} AS "
            f"SELECT DISTINCT {cols_sql} FROM {_qident(base_table)} "
            f"WHERE {_qident(key_col)} IS NOT NULL"
        )
        n = conn.execute(f"SELECT COUNT(*) FROM {_qident(rel_table)}").fetchone()[0]
        if int(n or 0) <= 0:
            conn.execute(f"DROP TABLE IF EXISTS {_qident(rel_table)}")
            continue

        created.append(rel_table)
        row_counts[rel_table] = int(n)
        table_signatures.add(sig)
        table_cols_map[rel_table] = list(select_cols)
        derived_tokens.add(prefix)
        if fact_key_col and key_col == fact_key_col:
            fact_rel_table = rel_table

    # Build lookup-style dimensions from entity-like text columns
    # (works even when *_id keys are absent in the source file).
    lookup_candidates: list[tuple[str, float]] = []
    for col_name, dtype, *_ in cols:
        ok, ratio = _is_dimension_text_column(conn, base_table, col_name, str(dtype))
        if not ok:
            continue
        d_col, n_col = _card(col_name)
        if d_col < 3 or n_col < 3:
            continue
        lookup_candidates.append((col_name, ratio))

    lookup_candidates.sort(key=lambda x: (x[1], x[0]), reverse=True)

    for col_name, _ratio in lookup_candidates[:10]:
        cl = col_name.lower()
        token = _tokenize_col(cl)
        if token in derived_tokens:
            continue

        key_guess = f"{token}_id"
        key_col = col_by_lower.get(key_guess)
        rel_table = _derive_rel_table_name(base_table, token, used_tables)
        dep_candidates = [
            c
            for c in col_names
            if (
                c != col_name
                and c.lower() not in _SKIP_COLS
                and not any(k in c.lower() for k in _DATE_HINTS)
                and not any(c.lower().endswith(s) for s in _REL_KEY_SUFFIXES)
            )
        ]
        dep_key = key_col if key_col and key_col != col_name else col_name
        dep_attrs = _select_dependent_attrs(
            conn, base_table, dep_key, dep_candidates, max_attrs=4, min_score=0.97
        )
        dep_attrs = [c for c in dep_attrs if c != key_col and c != col_name]

        if key_col and key_col != col_name:
            select_cols = [key_col, col_name] + dep_attrs
            sig = tuple(sorted(c.lower() for c in select_cols))
            if sig in table_signatures:
                continue
            select_sql = ", ".join(_qident(c) for c in select_cols)
            conn.execute(
                f"CREATE OR REPLACE TABLE {_qident(rel_table)} AS "
                f"SELECT DISTINCT {select_sql} "
                f"FROM {_qident(base_table)} WHERE {_qident(col_name)} IS NOT NULL"
            )
            n = conn.execute(f"SELECT COUNT(*) FROM {_qident(rel_table)}").fetchone()[0]
            if int(n or 0) <= 0:
                conn.execute(f"DROP TABLE IF EXISTS {_qident(rel_table)}")
                continue
            created.append(rel_table)
            row_counts[rel_table] = int(n)
            table_signatures.add(sig)
            table_cols_map[rel_table] = list(select_cols)
            lookup_surrogate_map[col_name] = (rel_table, key_col, col_name)
        else:
            surrogate_col = f"{token}_id"
            if surrogate_col.lower() in col_by_lower or surrogate_col == col_name:
                surrogate_col = f"{token}_sk"

            dim_cols = [col_name] + dep_attrs
            sig = tuple(sorted(c.lower() for c in ([surrogate_col] + dim_cols)))
            if sig in table_signatures:
                continue
            dim_select = ", ".join(_qident(c) for c in dim_cols)
            conn.execute(
                f"CREATE OR REPLACE TABLE {_qident(rel_table)} AS "
                f"WITH dim_base AS ("
                f"  SELECT DISTINCT {dim_select} "
                f"  FROM {_qident(base_table)} "
                f"  WHERE {_qident(col_name)} IS NOT NULL"
                f") "
                f"SELECT ROW_NUMBER() OVER (ORDER BY {_qident(col_name)}) AS {_qident(surrogate_col)}, "
                f"{dim_select} "
                f"FROM dim_base"
            )
            n = conn.execute(f"SELECT COUNT(*) FROM {_qident(rel_table)}").fetchone()[0]
            if int(n or 0) <= 0:
                conn.execute(f"DROP TABLE IF EXISTS {_qident(rel_table)}")
                continue
            created.append(rel_table)
            row_counts[rel_table] = int(n)
            table_signatures.add(sig)
            table_cols_map[rel_table] = [surrogate_col] + dim_cols
            lookup_surrogate_map[col_name] = (rel_table, surrogate_col, col_name)
        derived_tokens.add(token)

    # Enrich derived tables with surrogate FK columns from lookup dimensions.
    # This turns text links (e.g., student_name) into stable ID links (student_id).
    for label_col, (dim_table, fk_col, dim_label_col) in lookup_surrogate_map.items():
        for t in list(created):
            if t == dim_table:
                continue
            if fact_rel_table and t != fact_rel_table:
                t_rows = int(row_counts.get(t, 0) or 0)
                dim_rows = int(row_counts.get(dim_table, 0) or 0)
                # Avoid adding lookup FKs across peer dimensions. Keep this for
                # the primary fact table and tables that are clearly larger.
                if t_rows <= max(dim_rows * 2, dim_rows + 5):
                    continue
            t_cols = table_cols_map.get(t)
            if not t_cols:
                t_cols = [
                    r[0] for r in conn.execute(f"DESCRIBE {_qident(t)}").fetchall()
                ]
                table_cols_map[t] = list(t_cols)
            t_cols_l = {c.lower() for c in t_cols}
            if label_col.lower() not in t_cols_l:
                continue
            if fk_col.lower() in t_cols_l:
                continue
            existing_sql = ", ".join(f"t.{_qident(c)}" for c in t_cols)
            conn.execute(
                f"CREATE OR REPLACE TABLE {_qident(t)} AS "
                f"SELECT {existing_sql}, d.{_qident(fk_col)} AS {_qident(fk_col)} "
                f"FROM {_qident(t)} t "
                f"LEFT JOIN {_qident(dim_table)} d "
                f"ON t.{_qident(label_col)} = d.{_qident(dim_label_col)}"
            )
            table_cols_map[t] = list(t_cols) + [fk_col]

    # Build cross-dimension FK columns directly from base-table dependencies.
    # Example: courses.category -> categories.category_id, students.city -> cities.city_id.
    dim_items = [
        (label_col, dim_table, fk_col, dim_label_col)
        for label_col, (
            dim_table,
            fk_col,
            dim_label_col,
        ) in lookup_surrogate_map.items()
    ]
    for src_label, src_table, _src_fk, src_dim_label in dim_items:
        src_cols = table_cols_map.get(src_table)
        if not src_cols:
            src_cols = [
                r[0] for r in conn.execute(f"DESCRIBE {_qident(src_table)}").fetchall()
            ]
            table_cols_map[src_table] = list(src_cols)
        src_cols_l = {c.lower() for c in src_cols}
        for dst_label, dst_table, dst_fk, dst_dim_label in dim_items:
            if src_table == dst_table:
                continue
            if dst_fk.lower() in src_cols_l:
                continue
            d_src_lbl, n_src_lbl = _col_cardinality(conn, base_table, src_label)
            d_dst_lbl, _n_dst_lbl = _col_cardinality(conn, base_table, dst_label)
            # Keep many-to-one mappings only: finer-grain source to coarser target.
            if d_src_lbl < max(d_dst_lbl + 2, int(d_dst_lbl * 1.05)):
                continue
            # Keep only strong, near-functional mappings: src_label -> dst_label.
            dep = _dependency_score(conn, base_table, src_label, dst_label)
            if dep < 0.97:
                continue
            # Require enough signal (not tiny buckets).
            if d_src_lbl < 5 or n_src_lbl < 20:
                continue

            existing_sql = ", ".join(f"s.{_qident(c)}" for c in src_cols)
            conn.execute(
                f"CREATE OR REPLACE TABLE {_qident(src_table)} AS "
                f"WITH map AS ("
                f"  SELECT {_qident(src_label)} AS src_v, MIN({_qident(dst_label)}) AS dst_v "
                f"  FROM {_qident(base_table)} "
                f"  WHERE {_qident(src_label)} IS NOT NULL AND {_qident(dst_label)} IS NOT NULL "
                f"  GROUP BY 1"
                f") "
                f"SELECT {existing_sql}, d.{_qident(dst_fk)} AS {_qident(dst_fk)} "
                f"FROM {_qident(src_table)} s "
                f"LEFT JOIN map m ON s.{_qident(src_dim_label)} = m.src_v "
                f"LEFT JOIN {_qident(dst_table)} d ON d.{_qident(dst_dim_label)} = m.dst_v"
            )
            src_cols = list(src_cols) + [dst_fk]
            src_cols_l.add(dst_fk.lower())
            table_cols_map[src_table] = src_cols

    # Build an interconnected relationship graph using only derived tables.
    derived_cols: dict[str, set[str]] = {}
    for t in created:
        t_cols = table_cols_map.get(t)
        if not t_cols:
            t_cols = [r[0] for r in conn.execute(f"DESCRIBE {_qident(t)}").fetchall()]
        derived_cols[t] = set(t_cols)

    def _col_link_score(col: str) -> int:
        c = col.lower()
        if any(c.endswith(s) for s in _REL_KEY_SUFFIXES):
            return 120
        if c.endswith("_name"):
            return 90
        if any(k in c for k in _DATE_HINTS):
            return -50
        return 60

    def _singularize_name(name: str) -> str:
        n = str(name or "").strip().lower()
        if n.endswith("ies") and len(n) > 3:
            return n[:-3] + "y"
        if n.endswith("es") and len(n) > 2:
            return n[:-2]
        if n.endswith("s") and len(n) > 1:
            return n[:-1]
        return n

    table_name_tokens: dict[str, set[str]] = {}
    for t in created:
        raw_name = str(t).lower()
        clean = (
            raw_name[len(_REL_PREFIX) :]
            if raw_name.startswith(_REL_PREFIX)
            else raw_name
        )
        tokens = {raw_name, clean, _singularize_name(clean)}
        parts = [p for p in re.split(r"[^a-z0-9]+", clean) if p]
        s_parts = [_singularize_name(p) for p in parts]
        for p in parts:
            tokens.add(p)
            tokens.add(_singularize_name(p))
        for i in range(len(s_parts) - 1):
            tokens.add(s_parts[i] + "_" + s_parts[i + 1])
        table_name_tokens[t] = {x for x in tokens if x}

    def _join_col_priority(col: str, t1: str, t2: str) -> int:
        c = str(col or "").lower()
        base = _col_link_score(c)
        root = c
        for suf in _REL_KEY_SUFFIXES:
            if root.endswith(suf) and len(root) > len(suf):
                root = root[: -len(suf)]
                break
        t1_tokens = table_name_tokens.get(t1, set())
        t2_tokens = table_name_tokens.get(t2, set())
        if root in t1_tokens:
            base += 45
        if root in t2_tokens:
            base += 45
        if root and (root in str(t1).lower() or root in str(t2).lower()):
            base += 15
        return base

    edge_candidates: list[tuple[int, str, str, str, str, str]] = []

    for i, t1 in enumerate(created):
        for t2 in created[i + 1 :]:
            shared = [
                c
                for c in derived_cols.get(t1, set())
                if (
                    c in derived_cols.get(t2, set())
                    and c.lower() not in _SKIP_COLS
                    and (
                        any(c.lower().endswith(s) for s in _REL_KEY_SUFFIXES)
                        or c.lower().endswith("_name")
                    )
                )
            ]
            if not shared:
                continue
            shared.sort(
                key=lambda c: (_join_col_priority(c, t1, t2), _col_link_score(c), c),
                reverse=True,
            )
            join_col = shared[0]
            rel_type = (
                "FK"
                if any(join_col.lower().endswith(s) for s in _REL_KEY_SUFFIXES)
                else "SHARED_DIM"
            )
            confidence = "HIGH" if _col_link_score(join_col) >= 90 else "MEDIUM"
            score = _col_link_score(join_col)
            edge_candidates.append((score, t1, t2, join_col, rel_type, confidence))

    edge_candidates.sort(key=lambda x: x[0], reverse=True)

    rels = []
    seen_fk: set[tuple[str, str, str, str, str]] = set()
    seen_sd: set[tuple[str, str, str, str, str]] = set()
    for _score, a, b, join_col, rel_type, confidence in edge_candidates:
        # Orient edge from wider/denser table toward narrower dimension table.
        cols_a = len(derived_cols.get(a, set()))
        cols_b = len(derived_cols.get(b, set()))
        rows_a = row_counts.get(a, 0)
        rows_b = row_counts.get(b, 0)
        # Prefer many-to-one direction by join cardinality when available.
        d_a, n_a = _col_cardinality(conn, a, join_col)
        d_b, n_b = _col_cardinality(conn, b, join_col)
        r_a = d_a / max(n_a, 1) if n_a else 0.0
        r_b = d_b / max(n_b, 1) if n_b else 0.0
        # FK direction should be many -> one (lower distinct-ratio to higher).
        if r_a < r_b:
            from_t, to_t = a, b
        elif r_b < r_a:
            from_t, to_t = b, a
        elif rows_a > rows_b:
            from_t, to_t = a, b
        elif rows_b > rows_a:
            from_t, to_t = b, a
        elif cols_a >= cols_b:
            from_t, to_t = a, b
        else:
            from_t, to_t = b, a
        rel_obj = {
            "from_table": from_t,
            "from_column": join_col,
            "to_table": to_t,
            "to_column": join_col,
            "type": rel_type,
            "confidence": confidence,
            "source": "AUTO_STRUCTURE",
        }
        k = (
            rel_obj["from_table"],
            rel_obj["from_column"],
            rel_obj["to_table"],
            rel_obj["to_column"],
            rel_obj["type"],
        )
        if rel_type == "FK":
            if k in seen_fk:
                continue
            seen_fk.add(k)
            rels.append(rel_obj)
        else:
            # Keep shared-dimension links only as backup when no FK exists for this pair.
            pair_has_fk = any(
                (x["type"] == "FK")
                and ({x["from_table"], x["to_table"]} == {from_t, to_t})
                for x in rels
            )
            if pair_has_fk or k in seen_sd:
                continue
            seen_sd.add(k)
            rels.append(rel_obj)

    # Deduplicate derived relationships.
    uniq: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for r in rels:
        k = (
            r["from_table"],
            r["from_column"],
            r["to_table"],
            r["to_column"],
            r["type"],
        )
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)

    # Generic transitive reduction for FK graph:
    # if A->C is already reachable via A->...->C, drop direct A->C.
    def _has_path(
        edges: list[dict[str, Any]], src: str, dst: str, skip_idx: int
    ) -> bool:
        adj: dict[str, set[str]] = {}
        for i, e in enumerate(edges):
            if i == skip_idx:
                continue
            if e.get("type") != "FK":
                continue
            a = str(e.get("from_table", ""))
            b = str(e.get("to_table", ""))
            if not a or not b or a == b:
                continue
            adj.setdefault(a, set()).add(b)

        stack = [src]
        visited: set[str] = set()
        while stack:
            cur = stack.pop()
            if cur == dst:
                return True
            if cur in visited:
                continue
            visited.add(cur)
            for nb in adj.get(cur, set()):
                if nb not in visited:
                    stack.append(nb)
        return False

    pruned: list[dict[str, Any]] = []
    for i, e in enumerate(uniq):
        if e.get("type") == "FK":
            s = str(e.get("from_table", ""))
            t = str(e.get("to_table", ""))
            if s and t and _has_path(uniq, s, t, i):
                continue
        pruned.append(e)
    uniq = pruned

    # Additional deterministic 2-hop reduction (robust against path-order quirks):
    # drop A->C if there exists A->B and B->C among FK edges.
    fk_edges = [e for e in uniq if e.get("type") == "FK"]
    two_hop_drop: set[int] = set()
    for i, e in enumerate(fk_edges):
        a = str(e.get("from_table", ""))
        c = str(e.get("to_table", ""))
        if not a or not c or a == c:
            continue
        has_two_hop = any(
            (x is not e)
            and (y is not e)
            and str(x.get("from_table", "")) == a
            and str(x.get("to_table", "")) == str(y.get("from_table", ""))
            and str(y.get("to_table", "")) == c
            and x.get("type") == "FK"
            and y.get("type") == "FK"
            for x in fk_edges
            for y in fk_edges
        )
        if has_two_hop:
            two_hop_drop.add(i)

    if two_hop_drop:
        keep_fk: list[dict[str, Any]] = [
            e for i, e in enumerate(fk_edges) if i not in two_hop_drop
        ]
        keep_non_fk = [e for e in uniq if e.get("type") != "FK"]
        uniq = keep_fk + keep_non_fk

    return created, row_counts, uniq


# ── Relationship detection ────────────────────────────────────────────────────

_ID_SUFFIXES = ("_id", "_code", "_key", "_uuid", "_no")
_SKIP_COLS = {"id", "row_id", "row_no", "index"}
_DATE_HINTS = ("date", "time", "created", "updated", "timestamp")


# Column name patterns that are likely dimension keys worth linking
def _is_linkable_col(col: str) -> bool:
    c = col.lower()
    if c in _SKIP_COLS:
        return False
    if any(k in c for k in _DATE_HINTS):
        return False
    return any(c.endswith(s) for s in _ID_SUFFIXES)


def _col_cardinality(conn, table: str, col: str) -> tuple[int, int]:
    """Return (distinct_count, total_count) for a column."""
    try:
        r = conn.execute(
            f'SELECT COUNT(DISTINCT "{col}"), COUNT("{col}") FROM "{table}"'
        ).fetchone()
        return int(r[0] or 0), int(r[1] or 0)
    except Exception:
        return 0, 0


def _detect_relationships(conn, tables: list[str]) -> list[dict[str, Any]]:
    """
    Detect FK-style relationships between tables by:
    1. Finding columns with matching normalized names across tables
    2. Verifying cardinality (one side should be higher-cardinality = FK side)
    3. Sampling overlapping values to confirm the relationship

    Returns list of dicts:
      {from_table, from_column, to_table, to_column, type, confidence}
    """
    if len(tables) < 2:
        return []

    # Build per-table column index: {table: {normalized_col: actual_col}}
    table_cols: dict[str, dict[str, str]] = {}
    for table in tables:
        try:
            cols = [c[0] for c in conn.execute(f'DESCRIBE "{table}"').fetchall()]
            table_cols[table] = {c.lower(): c for c in cols}
        except Exception:
            table_cols[table] = {}

    relationships: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()

    for i, t1 in enumerate(tables):
        for t2 in tables[i + 1 :]:
            cols1 = table_cols.get(t1, {})
            cols2 = table_cols.get(t2, {})

            # Find shared normalized column names that look like linkable keys
            shared = {c for c in cols1 if c in cols2 and _is_linkable_col(c)}

            for col_lower in shared:
                c1 = cols1[col_lower]
                c2 = cols2[col_lower]

                key = tuple(sorted([(t1, c1), (t2, c2)]))
                if key in seen:
                    continue
                seen.add(key)

                d1, n1 = _col_cardinality(conn, t1, c1)
                d2, n2 = _col_cardinality(conn, t2, c2)

                if d1 == 0 or d2 == 0:
                    continue

                # Sample overlap: check if values from one appear in the other
                try:
                    overlap = conn.execute(
                        f'SELECT COUNT(*) FROM (SELECT DISTINCT "{c1}" FROM "{t1}" LIMIT 100) s1 '
                        f'JOIN (SELECT DISTINCT "{c2}" FROM "{t2}") s2 ON s1."{c1}" = s2."{c2}"'
                    ).fetchone()[0]
                except Exception:
                    overlap = 0

                if overlap == 0:
                    continue

                # Determine direction: higher distinct = FK (many) side
                r1 = d1 / max(n1, 1)
                r2 = d2 / max(n2, 1)

                # At least one side should look key-like (near-unique).
                if max(r1, r2) < 0.95:
                    continue

                if r1 <= r2:
                    from_t, from_c = t1, c1
                    to_t, to_c = t2, c2
                else:
                    from_t, from_c = t2, c2
                    to_t, to_c = t1, c1

                # Confidence: HIGH if strong overlap, MEDIUM otherwise
                overlap_ratio = overlap / min(d1, d2, 100)
                confidence = "HIGH" if overlap_ratio >= 0.5 else "MEDIUM"

                relationships.append(
                    {
                        "from_table": from_t,
                        "from_column": from_c,
                        "to_table": to_t,
                        "to_column": to_c,
                        "type": "FK",
                        "confidence": confidence,
                        "shared_col": col_lower,
                    }
                )

    return relationships


# ── Also detect shared value columns (non-ID but matching dimensions) ────────


def _detect_shared_dimensions(conn, tables: list[str]) -> list[dict[str, Any]]:
    """
    Detect shared categorical/dimension columns (e.g. city, category)
    that appear in multiple tables with overlapping values.
    """
    if len(tables) < 2:
        return []

    table_text_cols: dict[str, list[str]] = {}
    for table in tables:
        try:
            cols = conn.execute(f'DESCRIBE "{table}"').fetchall()
            text_cols = []
            for c in cols:
                ok, _ratio = _is_dimension_text_column(conn, table, c[0], str(c[1]))
                if ok:
                    text_cols.append(c[0])
            table_text_cols[table] = text_cols
        except Exception:
            table_text_cols[table] = []

    relationships: list[dict[str, Any]] = []
    seen: set[tuple] = set()

    for i, t1 in enumerate(tables):
        for t2 in tables[i + 1 :]:
            cols1 = {c.lower(): c for c in table_text_cols.get(t1, [])}
            cols2 = {c.lower(): c for c in table_text_cols.get(t2, [])}
            shared = set(cols1) & set(cols2)

            for col_lower in shared:
                key = tuple(sorted([(t1, col_lower), (t2, col_lower)]))
                if key in seen:
                    continue
                seen.add(key)

                c1, c2 = cols1[col_lower], cols2[col_lower]
                try:
                    overlap = conn.execute(
                        f"SELECT COUNT(*) FROM "
                        f'(SELECT DISTINCT "{c1}" FROM "{t1}" LIMIT 50) s1 '
                        f'JOIN (SELECT DISTINCT "{c2}" FROM "{t2}") s2 '
                        f'ON s1."{c1}" = s2."{c2}"'
                    ).fetchone()[0]
                except Exception:
                    overlap = 0

                if overlap >= 2:
                    relationships.append(
                        {
                            "from_table": t1,
                            "from_column": c1,
                            "to_table": t2,
                            "to_column": c2,
                            "type": "SHARED_DIM",
                            "confidence": "MEDIUM",
                            "shared_col": col_lower,
                        }
                    )

    return relationships


def ingest_files(
    files: list[dict[str, Any]],
    reset_db: bool = False,
    auto_structure: bool = True,
    merge_confirm: bool = False,
    discover_files: bool = False,
    discover_dirs: list[str] | None = None,
    discover_latest_only: bool = True,
    include_seen_discovered: bool = False,
    llm_schema_infer: bool = True,
) -> dict[str, Any]:
    db_file = Path(DUCKDB_PATH)
    if not db_file.is_absolute():
        db_file = (_PROJECT_ROOT / db_file).resolve()
    upload_dir = db_file.parent / "uploads"
    conn = get_write_connection()

    work_files: list[dict[str, Any]] = []
    discovered_paths: list[str] = []
    used: set[str] = set()
    created: list[str] = []
    row_counts: dict[str, int] = {}
    generated_relationships: list[dict[str, Any]] = []
    skipped_files: list[dict[str, Any]] = []
    llm_hints: dict[str, Any] = {}
    anchor_tokens: set[str] | None = None
    anchor_keys: set[str] | None = None
    anchor_topics: set[str] | None = None
    anchor_table: str | None = None

    try:
        _ensure_meta_tables(conn)

        if reset_db:
            existing = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
            for (t,) in existing:
                conn.execute(f'DROP TABLE IF EXISTS "{t}"')
            _ensure_meta_tables(conn)

        # Normalize incoming file payloads first.
        seen_payload_paths: set[str] = set()
        for f in files or []:
            if not isinstance(f, dict):
                continue
            p = str(f.get("path") or "").strip()
            key = p.lower()
            if key and key in seen_payload_paths:
                continue
            if key:
                seen_payload_paths.add(key)
            work_files.append(f)

        if discover_files:
            raw_dirs = discover_dirs or [
                str(_PROJECT_ROOT.parent),
                str(_PROJECT_ROOT),
                str(upload_dir),
            ]
            scan_dirs: list[Path] = []
            for raw in raw_dirs:
                try:
                    p = Path(str(raw).strip())
                except Exception:
                    continue
                if not p.is_absolute():
                    p = (_PROJECT_ROOT / p).resolve()
                scan_dirs.append(p)

            discovered_files, discovered_paths = _discover_drop_files(
                conn,
                scan_dirs=scan_dirs,
                include_seen=include_seen_discovered,
                latest_only=discover_latest_only,
            )
            for f in discovered_files:
                p = str(f.get("path") or "").strip().lower()
                if p and p in seen_payload_paths:
                    continue
                if p:
                    seen_payload_paths.add(p)
                work_files.append(f)

        if not work_files:
            raise ValueError(
                "No files provided. Upload files[] or enable discover_files to auto-stitch dropped files."
            )
        if len(work_files) > 1 and not merge_confirm:
            raise ValueError(
                "Multiple files detected. Upload one file at a time, or confirm these files are from the same business to merge."
            )

        used |= _all_main_tables(conn)

        for f in work_files:
            fp = _resolve_input_file(f, upload_dir)
            table = _safe_table_name(fp.name, used)
            n = _load_file_to_table(conn, fp, table)

            cols = conn.execute(f"DESCRIBE {_qident(table)}").fetchall()
            col_names = [c[0] for c in cols]
            cand_tokens, cand_keys, cand_topics = _table_business_signature(col_names)

            if anchor_tokens is None or anchor_keys is None or anchor_topics is None:
                anchor_tokens = set(cand_tokens)
                anchor_keys = set(cand_keys)
                anchor_topics = set(cand_topics)
                anchor_table = table
            else:
                compatible = _is_business_compatible(
                    anchor_tokens,
                    anchor_keys,
                    anchor_topics,
                    cand_tokens,
                    cand_keys,
                    cand_topics,
                )
                if not compatible:
                    conn.execute(f"DROP TABLE IF EXISTS {_qident(table)}")
                    skipped_files.append(
                        {
                            "file": str(fp.name),
                            "table": table,
                            "reason": (
                                f"Incompatible with active business schema "
                                f"(anchor table: {anchor_table})."
                            ),
                        }
                    )
                    continue
                anchor_tokens |= cand_tokens
                anchor_keys |= cand_keys
                anchor_topics |= cand_topics

            created.append(table)
            row_counts[table] = n
            _record_ingested_file(conn, fp, table)

            if llm_schema_infer:
                hint = _infer_schema_hint_with_llm(conn, table, cols)
                if hint:
                    _record_semantic_hint(conn, table, hint)
                    llm_hints[table] = hint

            if auto_structure:
                rel_tables, rel_counts, rels = _auto_structure_flat_table(
                    conn, table, used
                )
                if rel_tables:
                    created.extend(rel_tables)
                    row_counts.update(rel_counts)
                    generated_relationships.extend(rels)

        schema = []
        for t in created:
            cols = conn.execute(f'DESCRIBE "{t}"').fetchall()
            schema.append(
                {
                    "table": t,
                    "columns": [{"name": c[0], "type": c[1]} for c in cols],
                }
            )

        # ── Detect relationships between loaded tables ────────────────────────
        all_tables = [
            r[0]
            for r in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='main'
                  AND table_name NOT LIKE '_raw_meta_%'
                """
            ).fetchall()
        ]
        if auto_structure:
            # In auto-structure mode, relationship graph is already curated in
            # _auto_structure_flat_table. Running global detectors here tends to
            # reintroduce redundant/transitive links.
            fk_rels = []
            dim_rels = []
        else:
            fk_rels = _detect_relationships(conn, all_tables)
            dim_rels = _detect_shared_dimensions(conn, all_tables)

        # Merge, deduplicate by (from_table, from_column, to_table, to_column)
        all_rels = generated_relationships + fk_rels + dim_rels
        seen_rels: set[tuple] = set()
        unique_rels: list[dict] = []
        for r in all_rels:
            left = (r["from_table"], r["from_column"])
            right = (r["to_table"], r["to_column"])
            pair = tuple(sorted([left, right]))
            key = (pair, r.get("type", ""))
            if key not in seen_rels:
                seen_rels.add(key)
                unique_rels.append(r)

        try:
            from db.semantic_layer import build_semantic_catalog

            semantic_catalog = build_semantic_catalog(conn, all_tables)
        except Exception:
            semantic_catalog = {"dimensions": [], "metrics": []}

        dbt_result = {"enabled": False, "status": "skipped"}
        try:
            from db.dbt_runner import run_dbt_models

            dbt_result = run_dbt_models()
        except Exception as exc:
            dbt_result = {
                "enabled": True,
                "status": "error",
                "reason": f"dbt hook failed: {exc}",
            }

        return {
            "tables_created": created,
            "row_counts": row_counts,
            "relationships": unique_rels,
            "auto_structure": auto_structure,
            "skipped_files": skipped_files,
            "discovered_files": discovered_paths,
            "llm_schema_hints": llm_hints,
            "semantic_catalog": semantic_catalog,
            "dbt_result": dbt_result,
            "schema": schema,
        }
    finally:
        conn.close()
