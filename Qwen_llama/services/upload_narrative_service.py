"""Generate a short business narrative after dataset ingestion."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from config import GROQ_API_TOKEN, QWEN_MODEL
from db.duckdb_connection import get_read_connection
from llm.client import call_llm


def _sql_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _extract_schema_map(result: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = {}
    for entry in result.get("schema") or []:
        table = str(entry.get("table") or "").strip()
        cols = entry.get("columns") or []
        if not table or not isinstance(cols, list):
            continue
        parsed_cols: list[dict[str, str]] = []
        for c in cols:
            name = str((c or {}).get("name") or "").strip()
            typ = str((c or {}).get("type") or "").strip()
            if name:
                parsed_cols.append({"name": name, "type": typ})
        out[table] = parsed_cols
    return out


def _is_date_col(col_name: str, col_type: str) -> bool:
    n = col_name.lower()
    t = col_type.upper()
    return (
        "DATE" in t
        or "TIME" in t
        or any(k in n for k in ("date", "time", "created", "updated", "timestamp"))
    )


def _is_money_col(col_name: str, col_type: str) -> bool:
    n = col_name.lower()
    t = col_type.upper()
    return any(
        k in n
        for k in ("fare", "price", "amount", "revenue", "sales", "total", "earning")
    ) and any(k in t for k in ("INT", "DECIMAL", "DOUBLE", "FLOAT", "NUMERIC", "REAL"))


def _choose_primary_table(result: dict[str, Any]) -> str:
    row_counts = result.get("row_counts") or {}
    best_table = ""
    best_count = -1
    for table_name, count in row_counts.items():
        try:
            c = int(count or 0)
        except Exception:
            c = 0
        if c > best_count:
            best_count = c
            best_table = str(table_name)

    if best_table:
        return best_table

    created = [str(t) for t in (result.get("tables_created") or []) if str(t).strip()]
    return created[0] if created else ""


def _profile_dataset(
    result: dict[str, Any], logger: Any | None = None
) -> dict[str, Any]:
    row_counts = result.get("row_counts") or {}
    schema_map = _extract_schema_map(result)
    tables = [str(t) for t in (result.get("tables_created") or []) if str(t).strip()]
    if not tables:
        tables = list(schema_map.keys())

    total_rows = 0
    for _, count in row_counts.items():
        try:
            total_rows += int(count or 0)
        except Exception:
            pass

    primary = _choose_primary_table(result)
    profile: dict[str, Any] = {
        "tables": len(tables),
        "total_rows": total_rows,
        "primary_table": primary,
        "time_start": None,
        "time_end": None,
        "years_covered": None,
        "city_count": None,
        "money_column": None,
        "money_total": None,
        "money_avg": None,
        "peak_quarter": None,
    }

    if not primary or primary not in schema_map:
        return profile

    cols = schema_map.get(primary, [])
    date_cols = [c["name"] for c in cols if _is_date_col(c["name"], c.get("type", ""))]
    money_cols = [
        c["name"] for c in cols if _is_money_col(c["name"], c.get("type", ""))
    ]

    city_col = None
    for c in cols:
        cname = c["name"].lower()
        if cname in {"city", "city_name"} or "city" in cname:
            city_col = c["name"]
            break

    conn = None
    try:
        conn = get_read_connection()

        for dcol in date_cols[:4]:
            try:
                q = (
                    f"SELECT MIN(CAST({_sql_ident(dcol)} AS DATE)), "
                    f"MAX(CAST({_sql_ident(dcol)} AS DATE)) "
                    f"FROM {_sql_ident(primary)} WHERE {_sql_ident(dcol)} IS NOT NULL"
                )
                mn, mx = conn.execute(q).fetchone()
                if not mn or not mx:
                    continue
                mn_s, mx_s = str(mn), str(mx)
                years = max(1, int(mx_s[:4]) - int(mn_s[:4]) + 1)
                profile["time_start"] = mn_s
                profile["time_end"] = mx_s
                profile["years_covered"] = years

                q_q = (
                    f"SELECT EXTRACT(QUARTER FROM CAST({_sql_ident(dcol)} AS DATE)) AS q, COUNT(*) AS c "
                    f"FROM {_sql_ident(primary)} WHERE {_sql_ident(dcol)} IS NOT NULL "
                    "GROUP BY 1 ORDER BY c DESC LIMIT 1"
                )
                row = conn.execute(q_q).fetchone()
                if row and row[0]:
                    profile["peak_quarter"] = f"Q{int(row[0])}"
                break
            except Exception:
                continue

        if city_col:
            try:
                q_city = (
                    f"SELECT COUNT(DISTINCT {_sql_ident(city_col)}) "
                    f"FROM {_sql_ident(primary)} WHERE {_sql_ident(city_col)} IS NOT NULL"
                )
                val = conn.execute(q_city).fetchone()
                if val and val[0] is not None:
                    profile["city_count"] = int(val[0])
            except Exception:
                pass

        money_col = money_cols[0] if money_cols else None
        if money_col:
            profile["money_column"] = money_col
            try:
                q_money = (
                    f"SELECT SUM(CAST({_sql_ident(money_col)} AS DOUBLE)), "
                    f"AVG(CAST({_sql_ident(money_col)} AS DOUBLE)) "
                    f"FROM {_sql_ident(primary)} WHERE {_sql_ident(money_col)} IS NOT NULL"
                )
                total_val, avg_val = conn.execute(q_money).fetchone()
                profile["money_total"] = _num(total_val)
                profile["money_avg"] = _num(avg_val)
            except Exception:
                pass

    except Exception as exc:
        if logger is not None:
            try:
                logger.warn("Dataset profiling failed", {"error": str(exc)})
            except Exception:
                pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return profile


def _fallback_three_sentences(profile: dict[str, Any]) -> str:
    tables = int(profile.get("tables") or 0)
    rows = int(profile.get("total_rows") or 0)
    years = profile.get("years_covered")
    city_count = profile.get("city_count")
    peak_q = profile.get("peak_quarter")
    money_total = _num(profile.get("money_total"))
    money_avg = _num(profile.get("money_avg"))
    money_col = str(profile.get("money_column") or "value")

    s1 = f"This upload contains {rows:,} records across {tables} table{'s' if tables != 1 else ''}."

    if years and city_count:
        s2 = f"It covers about {int(years)} year{'s' if int(years) != 1 else ''} across {int(city_count)} cities."
    elif years:
        s2 = f"It covers about {int(years)} year{'s' if int(years) != 1 else ''} of business activity."
    elif city_count:
        s2 = f"It includes operations across {int(city_count)} cities."
    else:
        s2 = "It captures a broad operational snapshot ready for trend and segment analysis."

    parts: list[str] = []
    if money_total is not None:
        parts.append(f"Total {money_col} is approximately INR {money_total:,.0f}")
    if money_avg is not None:
        parts.append(f"average {money_col} is INR {money_avg:,.0f}")
    if peak_q:
        parts.append(f"peak activity appears in {peak_q}")

    if parts:
        s3 = "; ".join(parts) + "."
    else:
        s3 = "Use this dataset to quickly identify peak periods, top segments, and growth opportunities."

    return f"{s1} {s2} {s3}"


def _normalize_to_three_sentences(text: str, fallback_text: str) -> str:
    src = str(text or "").strip()
    if not src:
        return fallback_text

    # Keep exactly three readable sentences.
    chunks = [c.strip() for c in re.split(r"(?<=[.!?])\s+", src) if c.strip()]
    if len(chunks) < 3:
        fb = [c.strip() for c in re.split(r"(?<=[.!?])\s+", fallback_text) if c.strip()]
        chunks.extend(fb)
    chunks = chunks[:3]

    cleaned: list[str] = []
    for c in chunks:
        c = c.strip().strip("-*")
        if not c:
            continue
        if c[-1] not in ".!?":
            c = c + "."
        cleaned.append(c)

    if len(cleaned) < 3:
        return fallback_text
    return " ".join(cleaned[:3])


def _llm_three_sentence_summary(profile: dict[str, Any], fallback_text: str) -> str:
    if not GROQ_API_TOKEN:
        return fallback_text

    prompt = (
        "Write exactly 3 short plain-English business sentences about this newly uploaded dataset. "
        "No bullets, no numbering, no markdown, and no extra text. "
        "Prefer INR formatting for money values.\n\n"
        f"Dataset profile JSON:\n{json.dumps(profile, ensure_ascii=True)}\n\n"
        "Focus on: coverage/scale, geographic or time spread, and one business KPI insight."
    )

    try:
        result = call_llm(
            model_name=QWEN_MODEL,
            messages=[{"role": "user", "content": prompt}],
            token=GROQ_API_TOKEN,
            max_tokens=170,
            step_name="UploadNarrative",
        )
        raw = (((result or {}).get("choices") or [{}])[0].get("message") or {}).get(
            "content"
        ) or ""
        return _normalize_to_three_sentences(str(raw), fallback_text)
    except Exception:
        return fallback_text


def generate_upload_narrative(
    ingest_result: dict[str, Any],
    logger: Any | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    profile = _profile_dataset(ingest_result or {}, logger=logger)
    fallback_text = _fallback_three_sentences(profile)
    text = _llm_three_sentence_summary(profile, fallback_text)
    source = "llm" if text != fallback_text else "fallback"

    return {
        "text": text,
        "source": source,
        "generatedAt": generated_at,
        "profile": profile,
    }
