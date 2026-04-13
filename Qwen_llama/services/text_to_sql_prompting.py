"""Prompt-building helpers for text-to-SQL generation.

This keeps the Motia step thinner by moving prompt composition and SQL-fix
prompt construction into the services layer.
"""

from __future__ import annotations


def render_filter_clause(filters: dict) -> str:
    if not filters:
        return ""

    lines: list[str] = []
    for col, val in filters.items():
        # Ignore structured filters; complex predicates are handled elsewhere.
        if isinstance(val, (dict, list, tuple, set)):
            continue
        if isinstance(val, bool):
            lines.append(f"  AND {col} = {1 if val else 0}")
        elif isinstance(val, int):
            lines.append(f"  AND {col} = {val}")
        elif isinstance(val, float):
            lines.append(f"  AND {col} = {val}")
        else:
            safe_val = str(val).replace("'", "''")
            lines.append(f"  AND {col} = '{safe_val}'")
    return "\n".join(lines)


def build_time_series_sql_hint(parsed: dict) -> str:
    bucket = parsed.get("time_bucket", "month")
    metric = parsed.get("metric", "value")
    aov_revenue_col = parsed.get("_aov_revenue_col") or "<revenue_column>"
    aov_count_key = parsed.get("_count_distinct_key") or "<order_identifier_column>"

    if bucket == "year":
        bucket_expr = "CAST(YEAR(date_col) AS VARCHAR)"
        bucket_label = "YYYY"
    elif bucket == "month":
        bucket_expr = "STRFTIME(date_col, '%Y-%m')"
        bucket_label = "YYYY-MM"
    elif bucket == "week":
        bucket_expr = "STRFTIME(date_col, '%Y-W%W')"
        bucket_label = "YYYY-W##"
    elif bucket == "quarter":
        bucket_expr = "CONCAT(CAST(YEAR(date_col) AS VARCHAR), '-Q', CAST(QUARTER(date_col) AS VARCHAR))"
        bucket_label = "YYYY-Q#"
    else:
        bucket_expr = "STRFTIME(date_col, '%Y-%m-%d')"
        bucket_label = "YYYY-MM-DD"

    if metric == "aov":
        agg_expr = (
            f'SUM("{aov_revenue_col}") / NULLIF(COUNT(DISTINCT "{aov_count_key}"), 0)'
        )
    elif metric == "count":
        agg_expr = "COUNT(DISTINCT <order_pk_column>)"
    elif metric.startswith("avg_"):
        actual_col = metric[4:]
        agg_expr = f"AVG({actual_col})"
    else:
        agg_expr = f"SUM({metric})"

    dc = "<actual_date_column>"
    bucket_expr_filled = bucket_expr.replace("date_col", dc)

    return f"""
REQUIRED SQL PATTERN for time_series ({bucket}):

  SELECT {bucket_expr_filled} AS name,
         {agg_expr} AS value
  FROM <table_name>
  WHERE {dc} >= ?
    AND {dc} < ?
    <BUSINESS_FILTERS>
  GROUP BY {bucket_expr_filled}
  ORDER BY name ASC

CRITICAL RULES for time_series (ALL MUST BE FOLLOWED):
1. GROUP BY IS MANDATORY.
2. Replace <actual_date_column> with the REAL date/datetime column from the schema.
3. Replace <table_name> with the REAL table name.
4. Replace <order_pk_column> with the primary order identifier column.
5. Replace <BUSINESS_FILTERS> with mandatory filter conditions.
6. ORDER BY name ASC for chronological output.
7. Do NOT add LIMIT for time_series.
8. Label format: {bucket_label}
9. For count metric: COUNT(DISTINCT <order_id_col>) NOT COUNT(*).
10. For avg_ metrics: use AVG(<column>) not SUM.
11. For AOV metric: SUM(<revenue_column>) / NULLIF(COUNT(DISTINCT <order_id_col>), 0).
"""


def build_llm_prompt(user_query: str, parsed: dict, schema: str) -> str:
    entity = parsed.get("entity")
    metric = parsed.get("metric", "count")
    entity_key = parsed.get("_entity_group_key")
    count_key = parsed.get("_count_distinct_key")
    aov_revenue_col = parsed.get("_aov_revenue_col")
    qt = parsed.get("query_type", "top_n")
    top_n = parsed.get("top_n", 5)
    disable_limit = bool(parsed.get("_disable_limit"))
    rank_within_time = bool(parsed.get("_rank_within_time"))
    thr = parsed.get("threshold") or {}
    filters = parsed.get("filters", {}) or {}
    trs = parsed.get("time_ranges", [])
    bucket = parsed.get("time_bucket", "month")

    filter_clause = render_filter_clause(filters)
    filter_desc = (
        f"\n  MANDATORY FILTERS (from Rule 11):\n{filter_clause}"
        if filter_clause
        else "  none"
    )

    if entity:
        groupby_rule = (
            f"10. GROUPING (CRITICAL):\n"
            f"    Start with display column: {entity}\n"
            f"    Check 'Uniqueness Profile' and 'Safe Grouping Keys' sections.\n"
            f"    If {entity} is non-unique, GROUP BY stable_key + {entity}.\n"
            f"    Semantic suggestion key: {entity_key or 'N/A'}\n"
            f"    Do NOT assume any *_name column is unique.\n"
        )
    else:
        groupby_rule = (
            "10. GROUPING: aggregate or time_series query — no entity GROUP BY needed."
        )

    if rank_within_time:
        rank_instr = (
            f"Return TOP/BOTTOM {top_n} entities WITHIN EACH {bucket.upper()} bucket using a window function.\n"
            f"Pattern: ROW_NUMBER() OVER (PARTITION BY <bucket_expr> ORDER BY value {'DESC' if qt != 'bottom_n' else 'ASC'}) AS rn\n"
            f"Then filter rn <= {top_n}.\n"
            f"Output columns must be: period, name, value.\n"
            f"ORDER BY period ASC, value {'DESC' if qt != 'bottom_n' else 'ASC'}."
        )
    elif qt in ("time_series", "forecast"):
        ts_hint = build_time_series_sql_hint(parsed)
        rank_instr = (
            f"This is a TIME SERIES / TREND query.\n"
            f"Group by time bucket ({bucket}), NOT by any business entity.\n"
            f"ORDER BY name ASC (chronological). NO LIMIT.\n"
            f"\n{ts_hint}"
        )
    elif qt == "top_n":
        rank_instr = (
            "ORDER BY value DESC\nNO LIMIT"
            if disable_limit
            else f"ORDER BY value DESC\nLIMIT {top_n}"
        )
    elif qt == "bottom_n":
        rank_instr = (
            "ORDER BY value ASC\nNO LIMIT"
            if disable_limit
            else f"ORDER BY value ASC\nLIMIT {top_n}"
        )
    elif qt == "aggregate":
        rank_instr = (
            "No GROUP BY, no ORDER BY, no LIMIT. Return single scalar aliased 'value'."
        )
    elif qt == "threshold":
        op = ">" if thr.get("operator", "gt") == "gt" else "<"
        ttype = thr.get("type", "absolute")
        tval = thr.get("value", 0)
        if ttype == "percentage":
            rank_instr = (
                f"HAVING {metric}_expr {op} ({tval} / 100.0) * (SELECT SUM(...) total)\n"
                "ORDER BY value DESC"
            )
        else:
            rank_instr = f"HAVING aggregation {op} {tval}\nORDER BY value DESC"
    elif qt == "comparison":
        rank_instr = (
            f"Two-period comparison. Use CTEs. ORDER BY value1 DESC LIMIT {top_n}"
        )
    elif qt == "growth_ranking":
        rank_instr = f"Rank by delta = period2_value - period1_value. ORDER BY delta DESC LIMIT {top_n}"
    elif qt == "intersection":
        rank_instr = (
            f"Only entities present in BOTH periods. ORDER BY value DESC LIMIT {top_n}"
        )
    elif qt == "retention":
        rank_instr = (
            "Two-period cohort retention. Build DISTINCT entity cohorts for period 1 and period 2, "
            "then compute retention percentage as (returned_count * 100.0) / cohort_count. "
            "Return a single scalar column aliased as value. No LIMIT."
        )
    elif qt == "zero_filter":
        rank_instr = "Entities where metric = 0 or no rows in period. ORDER BY name"
    else:
        rank_instr = f"ORDER BY value DESC LIMIT {top_n}"

    date_hints = ""
    if trs:
        for i, tr in enumerate(trs[:2]):
            date_hints += f"\n  Period {i + 1}: {tr.get('start')} to {tr.get('end')}"

    if filter_clause:
        filter_rule = (
            f"11. BUSINESS FILTERS — MANDATORY:\n"
            f"    The following WHERE conditions MUST be included:\n"
            f"{filter_clause}\n"
            f"    Add after date filter:\n"
            f"      WHERE date_col BETWEEN ? AND ?\n"
            f"{filter_clause}\n"
        )
    else:
        filter_rule = "11. No additional business filters required."

    return f"""You are a DuckDB SQL expert. Output ONLY raw SQL — no prose, no markdown fences.

User question: \"{user_query}\"

{schema}

Parsed intent:
  query_type : {qt}
  entity     : {entity}
  metric     : {metric}
    aov_numerator_col: {aov_revenue_col or "N/A"}
    count_distinct_key: {count_key or "N/A"}
    time_bucket: {bucket if (qt in ("time_series", "forecast") or rank_within_time) else "N/A"}
  top_n      : {top_n}
    disable_limit: {disable_limit}
  filters    : {filter_desc}{date_hints}

RULES:
1. Use exact schema columns only; no guessing.
2. Output one SELECT/WITH statement only; no comments or semicolons.
3. Alias display column as name and metric expression as value.
4. Date filters must use exclusive range: col >= ? AND col < ? (never BETWEEN/EXTRACT equality).
5. Use ? for all runtime values.
6. Metric logic: count => COUNT(DISTINCT {count_key or "<primary_order_id_column>"}); avg_* => AVG(base_col); aov => SUM({aov_revenue_col or "<revenue_column>"}) / NULLIF(COUNT(DISTINCT {count_key or "<order_identifier_column>"}),0); otherwise SUM(metric).
7. Ranking/query behavior: {rank_instr}
8. Respect uniqueness + business filters:
{groupby_rule}
{filter_rule}

SQL:"""


def build_sql_fix_prompt(
    user_query: str,
    parsed: dict,
    schema: str,
    bad_sql: str,
    error_msg: str,
) -> str:
    return f"""You are a DuckDB SQL repair assistant. Return ONLY corrected SQL.

User question: \"{user_query}\"

{schema}

Parsed intent:
  query_type : {parsed.get("query_type")}
  entity     : {parsed.get("entity")}
  metric     : {parsed.get("metric")}
  time_bucket: {parsed.get("time_bucket")}

Broken SQL:
{bad_sql}

Error:
{error_msg}

Repair rules:
1. Keep original intent and metric semantics unchanged.
2. Use EXACT table/column names from schema.
3. Keep placeholders (?) for dynamic values; do not inline dates.
4. Return one SELECT/WITH statement only; no semicolon, no comments.
5. Alias grouped label as name and aggregated value as value.

SQL:"""
