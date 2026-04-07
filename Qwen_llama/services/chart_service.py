"""Chart rendering service for query results."""

from __future__ import annotations

import json
from typing import Any

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: #0f1117;
    color: #e2e8f0;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 32px 16px;
  }}
  .card {{
    background: #1a1d27;
    border: 1px solid #2d3148;
    border-radius: 12px;
    padding: 28px 32px;
    width: 100%;
    max-width: 860px;
  }}
  h1 {{
    font-size: 18px;
    font-weight: 600;
    color: #f1f5f9;
    margin-bottom: 4px;
    line-height: 1.4;
  }}
  .subtitle {{
    font-size: 13px;
    color: #64748b;
    margin-bottom: 28px;
  }}
  .chart-wrap {{
    position: relative;
    width: 100%;
  }}
  .token-box {{
    margin-top: 24px;
    background: #12141e;
    border: 1px solid #2d3148;
    border-radius: 8px;
    padding: 12px 16px;
    font-size: 12px;
    color: #64748b;
    display: flex;
    gap: 24px;
    flex-wrap: wrap;
  }}
  .token-box span {{ color: #94a3b8; }}
  .no-chart {{
    text-align: center;
    color: #64748b;
    padding: 48px 0;
    font-size: 14px;
  }}
</style>
</head>
<body>
<div class="card">
  <h1>{title}</h1>
  <p class="subtitle">{subtitle}</p>
  {body}
</div>
<script>
{script}
</script>
</body>
</html>"""


def _infer_currency(metric: str) -> str:
    m = (metric or "").lower()
    if any(x in m for x in ["inr", "rupee", "rs "]):
        return "Rs "
    if any(x in m for x in ["usd", "dollar", "$"]):
        return "$"
    if any(x in m for x in ["eur", "euro"]):
        return "\u20ac"
    if any(x in m for x in ["gbp", "\u00a3", "pound"]):
        return "\u00a3"
    if any(
        x in m
        for x in [
            "count",
            "quantity",
            "units",
            "distance",
            "duration",
            "rides",
            "trips",
            "orders",
            "km",
            "miles",
            "minutes",
        ]
    ):
        return ""
    return ""


def _build_html(query_state: dict[str, Any]) -> str:
    chart_config = query_state.get("chart_config")
    token_totals = query_state.get("token_totals", {})
    parsed = query_state.get("parsed", {})
    metric = parsed.get("metric", "")

    currency_prefix = _infer_currency(metric)

    token_html = ""
    if token_totals.get("total_tokens"):
        token_html = (
            f'<div class="token-box">'
            f"Token usage - "
            f"<span>prompt: {token_totals.get('prompt_tokens', 0)}</span>"
            f"<span>completion: {token_totals.get('completion_tokens', 0)}</span>"
            f"<span>total: {token_totals.get('total_tokens', 0)}</span>"
            f"</div>"
        )

    if not chart_config:
        body = (
            '<div class="no-chart">No chart data available for this query type.</div>'
        )
        script = ""
        title = query_state.get("query", "Query result")
        subtitle = f"Query ID: {query_state.get('id', '')}"
        return _HTML_TEMPLATE.format(
            title=title,
            subtitle=subtitle,
            body=body + token_html,
            script=script,
        )

    title = chart_config.get("title", "Analytics chart")
    subtitle = chart_config.get("subtitle", f"Query ID: {query_state.get('id', '')}")
    body = f'<div class="chart-wrap"><canvas id="myChart"></canvas></div>{token_html}'

    cfg = chart_config.get("config", {})
    json_str = json.dumps(cfg, indent=2)
    safe_prefix = currency_prefix.replace("'", "\\'")

    script = f"""
const ctx = document.getElementById('myChart');
const rawConfig = {json_str};

function reviveFunctions(node) {{
  if (Array.isArray(node)) {{
    return node.map(reviveFunctions);
  }}
  if (node && typeof node === 'object') {{
    const out = {{}};
    for (const [k, v] of Object.entries(node)) {{
      out[k] = reviveFunctions(v);
    }}
    return out;
  }}
  if (typeof node === 'string') {{
    let fnText = null;
    if (node.startsWith('@@FUNCTION@@') && node.endsWith('@@ENDFUNCTION@@')) {{
      fnText = node.slice('@@FUNCTION@@'.length, -'@@ENDFUNCTION@@'.length);
    }} else if (node.startsWith('function(')) {{
      fnText = node;
    }}
    if (fnText) {{
      try {{
        return (0, eval)('(' + fnText + ')');
      }} catch (e) {{
        console.warn('Failed to revive chart callback function:', e);
      }}
    }}
  }}
  return node;
}}

const chartConfig = reviveFunctions(rawConfig);
chartConfig.options = chartConfig.options || {{}};
chartConfig.options.plugins = chartConfig.options.plugins || {{}};
chartConfig.options.plugins.tooltip = chartConfig.options.plugins.tooltip || {{}};
chartConfig.options.interaction = chartConfig.options.interaction || {{ mode: 'nearest', intersect: false }};

const prefix = '{safe_prefix}';
const tooltip = chartConfig.options.plugins.tooltip;
tooltip.callbacks = tooltip.callbacks || {{}};
if (typeof tooltip.callbacks.label !== 'function') {{
  tooltip.callbacks.label = function(c) {{
    const v = c.raw;
    const n = (typeof v === 'number')
      ? v.toLocaleString('en-IN', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})
      : String(v ?? '');
    const ds = c.dataset && c.dataset.label ? c.dataset.label + ': ' : '';
    return ' ' + ds + prefix + n;
  }};
}}

new Chart(ctx, chartConfig);
"""
    return _HTML_TEMPLATE.format(
        title=title, subtitle=subtitle, body=body, script=script
    )


async def get_chart_response(
    request: Any, ctx: Any
) -> tuple[int, Any, dict[str, str] | None]:
    path_params = getattr(request, "path_params", None)
    if not isinstance(path_params, dict):
        path_params = {}
    query_id = str(path_params.get("queryId", ""))

    if not query_id:
        return 400, {"error": "Missing queryId"}, None

    query_state = await ctx.state.get("queries", query_id)
    if not query_state:
        return 404, {"error": f"Query {query_id} not found"}, None

    if query_state.get("status") != "completed":
        return (
            202,
            {
                "error": "Query not yet completed",
                "status": query_state.get("status"),
            },
            None,
        )

    html = _build_html(query_state)
    return 200, html, {"Content-Type": "text/html; charset=utf-8"}
