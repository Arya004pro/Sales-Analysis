"""streamlit_app.py  Data Analytics Pipeline Dashboard

Layout changes:
  - Data source (file upload) stays in left panel, top position.
  - Schema viewer replaced with interactive ER diagram (schema_diagram.py).
  - Pipeline steps + query input remain in left panel.
  - Token usage + SQL remain in right panel.

Also includes PDF download button (unchanged).
"""

import streamlit as st
import streamlit.components.v1 as components
import requests
import time
import json
from datetime import datetime, timezone as _tz
from pathlib import Path

# ── Import ER diagram renderer ────────────────────────────────────────────────
try:
    from schema_diagram import render_schema_diagram

    _HAS_DIAGRAM = True
except ImportError:
    _HAS_DIAGRAM = False


def fetch_schema(view: str = "derived"):
    try:
        v = (view or "derived").strip().lower()
        if v not in {"derived", "raw", "all"}:
            v = "derived"
        nonce = int(st.session_state.get("schema_refresh_nonce", 0))
        r = requests.get(
            f"{API}/schema",
            params={"view": v, "_nonce": nonce},
            timeout=5,
            headers={"Cache-Control": "no-cache"},
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def suggestions_css() -> None:
    """Inject chip-bar styles."""
    st.markdown(
        """
        <style>
        .sug-header {
            font-size: 10px;
            font-weight: 700;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-bottom: 8px;
        }
        .sug-status {
            font-size: 10px;
            color: #64748b;
            margin-bottom: 8px;
            min-height: 14px;
        }
        div[data-testid="stButton"] > button[kind="secondaryFormSubmit"] {
            border-radius: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _fetch_suggestions(api_base: str, force_refresh: bool = False) -> dict:
    """Call GET /suggestions (optionally with ?refresh=1)."""
    url = f"{api_base.rstrip('/')}/suggestions"
    params = {"refresh": "1"} if force_refresh else {}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def _fetch_discovery(api_base: str, force_refresh: bool = False) -> dict:
    """Call GET /discover (optionally with ?refresh=1)."""
    url = f"{api_base.rstrip('/')}/discover"
    params = {"refresh": "1"} if force_refresh else {}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


_SUGS_KEY = "_sug_list"
_SUGS_AT_KEY = "_sug_generated_at"
_SUGS_CACHED = "_sug_from_cache"
_DISCOVERY_KEY = "_discovery_findings"
_DISCOVERY_AT = "_discovery_generated_at"
_DISCOVERY_CACHED = "_discovery_cached"


def render_suggestions(api_base: str) -> str | None:
    """Render suggestion chips and return the clicked suggestion, if any."""
    if _SUGS_KEY not in st.session_state:
        with st.spinner("Loading suggestions..."):
            try:
                data = _fetch_suggestions(api_base, force_refresh=False)
                st.session_state[_SUGS_KEY] = data.get("suggestions", [])
                st.session_state[_SUGS_AT_KEY] = data.get("generated_at", "")
                st.session_state[_SUGS_CACHED] = data.get("cached", False)
            except Exception:
                st.session_state[_SUGS_KEY] = []
                st.session_state[_SUGS_AT_KEY] = ""
                st.session_state[_SUGS_CACHED] = False

    suggestions: list[str] = st.session_state.get(_SUGS_KEY, [])
    if not suggestions:
        return None

    col_lbl, col_refresh = st.columns([5, 1])
    with col_lbl:
        st.markdown(
            '<div class="sug-header">Suggested questions</div>', unsafe_allow_html=True
        )
    with col_refresh:
        refresh_clicked = st.button(
            "Refresh",
            key="_sug_refresh_btn",
            help="Generate new suggestions for the current dataset",
            use_container_width=True,
            type="secondary",
        )

    if refresh_clicked:
        with st.spinner("Generating suggestions..."):
            try:
                data = _fetch_suggestions(api_base, force_refresh=True)
                st.session_state[_SUGS_KEY] = data.get("suggestions", [])
                st.session_state[_SUGS_AT_KEY] = data.get("generated_at", "")
                st.session_state[_SUGS_CACHED] = False
                st.rerun()
            except Exception as exc:
                st.warning(f"Could not refresh suggestions: {exc}")

    chosen: str | None = None
    chips_per_row = 2
    for row_start in range(0, len(suggestions), chips_per_row):
        row_chips = suggestions[row_start : row_start + chips_per_row]
        cols = st.columns(len(row_chips))
        for col, chip_text in zip(cols, row_chips):
            with col:
                label = chip_text if len(chip_text) <= 60 else chip_text[:57] + "..."
                if st.button(
                    label,
                    key=f"_sug_chip_{row_start}_{abs(hash(chip_text))}",
                    use_container_width=True,
                    help=chip_text,
                    type="secondary",
                ):
                    chosen = chip_text

    gen_at = st.session_state.get(_SUGS_AT_KEY, "")
    is_cached = st.session_state.get(_SUGS_CACHED, False)
    if gen_at:
        try:
            dt = datetime.fromisoformat(gen_at.replace("Z", "+00:00"))
            age_min = int((datetime.now(_tz.utc) - dt).total_seconds() / 60)
            cache_note = "cached" if is_cached else "fresh"
            st.caption(f"Suggestions {cache_note} - {age_min} min ago")
        except Exception:
            pass

    return chosen


def render_discovery(api_base: str) -> None:
    col_lbl, col_refresh = st.columns([5, 1])
    with col_lbl:
        st.markdown(
            '<div class="sug-header">What\'s interesting?</div>', unsafe_allow_html=True
        )
    with col_refresh:
        refresh_clicked = st.button(
            "Refresh",
            key="_discover_refresh_btn",
            help="Rescan all tables for surprising patterns",
            use_container_width=True,
            type="secondary",
        )

    run_clicked = st.button(
        "Surprise me",
        key="_discover_run_btn",
        use_container_width=True,
        type="primary",
    )

    if _DISCOVERY_KEY not in st.session_state:
        try:
            data = _fetch_discovery(api_base, force_refresh=False)
            st.session_state[_DISCOVERY_KEY] = data.get("findings", [])
            st.session_state[_DISCOVERY_AT] = data.get("generated_at", "")
            st.session_state[_DISCOVERY_CACHED] = bool(data.get("cached", False))
        except Exception:
            st.session_state[_DISCOVERY_KEY] = []
            st.session_state[_DISCOVERY_AT] = ""
            st.session_state[_DISCOVERY_CACHED] = False

    if run_clicked or refresh_clicked:
        with st.spinner("Scanning for surprising findings..."):
            try:
                data = _fetch_discovery(api_base, force_refresh=True)
                st.session_state[_DISCOVERY_KEY] = data.get("findings", [])
                st.session_state[_DISCOVERY_AT] = data.get("generated_at", "")
                st.session_state[_DISCOVERY_CACHED] = bool(data.get("cached", False))
            except Exception as exc:
                st.warning(f"Auto-discovery failed: {exc}")

    findings = st.session_state.get(_DISCOVERY_KEY, []) or []
    generated_at = st.session_state.get(_DISCOVERY_AT, "")
    is_cached = st.session_state.get(_DISCOVERY_CACHED, False)

    if generated_at:
        cache_note = "cached" if is_cached else "fresh"
        st.caption(f"Generated ({cache_note}): {generated_at}")

    if not findings:
        st.caption("No high-confidence findings yet.")
        return

    lines = []
    for i, f in enumerate(findings[:5], start=1):
        txt = str(f.get("text") or "").strip()
        if txt:
            lines.append(f"{i}. {txt}")
    st.markdown("\n".join(lines))


def _fetch_business_report(api_base: str, period: str = "weekly") -> dict:
    r = requests.get(
        f"{api_base.rstrip('/')}/reports/latest",
        params={"period": period},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def _trigger_business_report(api_base: str, period: str = "weekly") -> dict:
    r = requests.post(
        f"{api_base.rstrip('/')}/reports/run",
        json={"period": period, "requestedBy": "streamlit"},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


_DIGEST_CACHE_KEY = "_digest_cache"
_DIGEST_CACHE_TS_KEY = "_digest_cache_ts"


def _load_business_report(
    api_base: str, period: str, force: bool = False
) -> tuple[dict | None, str | None]:
    cache = st.session_state.setdefault(_DIGEST_CACHE_KEY, {})
    ts_map = st.session_state.setdefault(_DIGEST_CACHE_TS_KEY, {})
    now_ts = time.time()
    if not force and period in cache and (now_ts - float(ts_map.get(period, 0))) < 45:
        return cache.get(period), None

    try:
        data = _fetch_business_report(api_base, period=period)
        cache[period] = data
        ts_map[period] = now_ts
        return data, None
    except Exception as exc:
        return None, str(exc)


def render_business_digest(api_base: str) -> None:
    period = st.selectbox(
        "Digest period",
        options=["weekly", "monthly"],
        key="_digest_period",
        label_visibility="collapsed",
    )
    run_col, refresh_col = st.columns([1, 1])
    run_now = run_col.button(
        "Run now", key="_digest_run_now", use_container_width=True, type="secondary"
    )
    refresh = refresh_col.button(
        "Refresh", key="_digest_refresh", use_container_width=True, type="secondary"
    )

    if run_now:
        try:
            _trigger_business_report(api_base, period=period)
            st.info(f"{period.title()} digest queued. Refresh in a few seconds.")
            _load_business_report(api_base, period=period, force=True)
        except Exception as exc:
            st.warning(f"Could not trigger digest run: {exc}")

    report, err = _load_business_report(api_base, period=period, force=refresh)
    if not report:
        st.caption("No digest available yet. Use 'Run now' to generate one.")
        if err:
            st.caption(f"API message: {err}")
        return

    generated_at = str(report.get("generatedAt") or "")
    if generated_at:
        st.caption(f"Generated: {generated_at}")
    md = str(report.get("summaryMarkdown") or "").strip()
    if md:
        st.markdown(md)
    else:
        st.caption("Digest report is empty.")
        return

    safe_period = period.lower().strip()
    date_part = datetime.now().strftime("%Y-%m-%d")
    st.download_button(
        label="Download Markdown",
        data=md,
        file_name=f"{safe_period}_digest_{date_part}.md",
        mime="text/markdown",
        use_container_width=True,
        key="_digest_download_md",
    )
    try:
        digest_state = {
            "formattedText": md,
            "formattedItems": [],
            "chart_config": None,
            "token_totals": {},
        }
        pdf_bytes = _build_export_pdf(
            digest_state, f"{period.title()} Business Summary"
        )
        st.download_button(
            label="Download PDF",
            data=pdf_bytes,
            file_name=f"{safe_period}_digest_{date_part}.pdf",
            mime="application/pdf",
            use_container_width=True,
            key="_digest_download_pdf",
        )
    except Exception:
        pass


API = "http://127.0.0.1:3121"
POLL_INTERVAL = 0.7
SHARED_UPLOAD_DIR = Path("Qwen_llama/motia/data/uploads")
CONTAINER_UPLOAD_DIR = "/app/motia/data/uploads"

STEPS = [
    {"label": "Query Received", "sub": "REST API intake", "icon": "IN"},
    {"label": "Intent Parsing", "sub": "Understand metric and scope", "icon": "IP"},
    {"label": "Clarification Gate", "sub": "Resolve missing inputs", "icon": "CL"},
    {"label": "SQL Planning", "sub": "Generate safe DuckDB SQL", "icon": "SQL"},
    {"label": "Query Execution", "sub": "Run against dataset", "icon": "DB"},
    {"label": "Analysis", "sub": "Forecast + anomaly detection", "icon": "AN"},
    {"label": "Response Assembly", "sub": "Insights + formatting", "icon": "RS"},
]

STATUS_MAP = {
    "received": 0,
    "intent_parsed": 1,
    "ambiguity_checked": 2,
    "needs_clarification": 2,
    "sql_generated": 3,
    "executed": 4,
    "forecast_computed": 5,
    "anomaly_detected": 5,
    "insights_generated": 6,
    "completed": 6,
    "error": 6,
}

STEP_STATUS_OPTIONS = {
    0: ["received"],
    1: ["intent_parsed"],
    2: ["ambiguity_checked", "needs_clarification"],
    3: ["sql_generated"],
    4: ["executed"],
    5: ["forecast_computed", "anomaly_detected"],
    6: ["insights_generated", "completed", "error"],
}

for key, default in {
    "query_id": None,
    "polling": False,
    "conversation_session_id": None,
    "pending_session": None,
    "final_state": None,
    "current_status": "",
    "history": [],
    "step_times": {},
    "poll_start": None,
    "last_completed": -1,
    "bookmarks": [],
    "bm_loaded": False,
    "schema_refresh_nonce": 0,
    "schema_last_refresh": "",
    "_digest_period": "weekly",
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


def _on_enter():
    val = st.session_state.get("query_input_field", "").strip()
    if not val or st.session_state.polling:
        return
    st.session_state.polling = True
    st.session_state.final_state = None
    st.session_state.current_status = ""
    st.session_state.step_times = {}
    st.session_state.last_completed = -1
    st.session_state.poll_start = time.time()
    try:
        session_id = (
            st.session_state.pending_session or st.session_state.conversation_session_id
        )
        resp = submit_query(val, session_id=session_id)
        st.session_state.query_id = resp["queryId"]
        st.session_state.conversation_session_id = (
            resp.get("sessionId")
            or st.session_state.conversation_session_id
            or resp["queryId"]
        )
        st.session_state.pending_session = None
    except Exception as e:
        st.session_state.polling = False
        st.error(f"Failed to submit: {e}")


st.set_page_config(
    page_title="Data Analytics Pipeline",
    layout="wide",
)

st.markdown(
    """
<style>
.step-row { display:flex; align-items:flex-start; margin:8px 0 20px; }
.step-wrap { flex:1; display:flex; flex-direction:column; align-items:center; position:relative; }
.step-wrap:not(:last-child)::after {
    content:''; position:absolute; top:22px; left:50%; width:100%; height:2px;
    background:#2d3148; z-index:0;
}
.step-wrap.done:not(:last-child)::after   { background:#10b981; }
.step-wrap.active:not(:last-child)::after { background:linear-gradient(to right,#10b981,#2d3148); }
.step-circle {
    width:44px; height:44px; border-radius:50%;
    display:flex; align-items:center; justify-content:center;
    font-size:16px; font-weight:600; z-index:1; position:relative;
}
.step-circle.idle   { border:2px solid #2d3148; background:#0f1117; color:#64748b; }
.step-circle.active { border:2px solid #6366f1; background:rgba(99,102,241,.1); color:#818cf8;
                      animation:pulse 1.4s ease-in-out infinite; }
.step-circle.done   { border:2px solid #10b981; background:rgba(16,185,129,.1); color:#10b981; }
.step-circle.error  { border:2px solid #ef4444; background:rgba(239,68,68,.1); color:#ef4444; }
@keyframes pulse {
    0%,100%{box-shadow:0 0 0 4px rgba(99,102,241,.15);}
    50%{box-shadow:0 0 0 12px rgba(99,102,241,.30);}
}
.step-label { margin-top:8px; font-size:11px; font-weight:600; text-align:center;
              color:#64748b; max-width:80px; line-height:1.3; }
.step-sub   { font-size:10px; color:#475569; text-align:center; max-width:80px; line-height:1.3; margin-top:2px; }
.step-time  { font-size:10px; color:#10b981; text-align:center; margin-top:3px; }
.step-wrap.done   .step-label { color:#e2e8f0; }
.step-wrap.active .step-label { color:#e2e8f0; }
.step-wrap.active .step-sub   { color:#818cf8; }
.step-wrap.active .step-time  { color:#818cf8; }
.step-wrap.idle   .step-time  { display:none; }
.sql-box {
    background:#080a10; border:1px solid #2d3148; border-radius:8px;
    padding:12px 14px; font-family:"Cascadia Code",Consolas,monospace;
    font-size:12px; color:#93c5fd; white-space:pre-wrap; word-break:break-all;
    line-height:1.6; max-height:220px; overflow-y:auto;
}
.clarify-box {
    background:rgba(245,158,11,.07); border:1px solid rgba(245,158,11,.3);
    border-radius:10px; padding:14px 16px;
    color:#fbbf24; font-size:14px; line-height:1.5; margin:8px 0;
}
.result-box {
    background:#1a1d27; border:1px solid #2d3148; border-radius:10px;
    padding:16px 18px; font-size:14px; color:#e2e8f0;
    white-space:pre-wrap; line-height:1.75;
}
.result-error { border-color:rgba(239,68,68,.4); color:#fca5a5; }
.hist-item { background:#1a1d27; border:1px solid #2d3148; border-radius:8px;
             padding:10px 14px; margin-bottom:6px; font-size:12px; }
.hist-q { color:#94a3b8; margin-bottom:4px; }
.hist-a { color:#e2e8f0; white-space:pre-wrap; }
.section-lbl { font-size:10px; font-weight:700; color:#475569;
               text-transform:uppercase; letter-spacing:.1em; margin-bottom:10px; }
.schema-section {
    background:#1a1d27; border:1px solid #2d3148; border-radius:10px;
    padding:16px 18px; margin-top:8px;
}
/* bookmarks */
.bm-item {
    background:#1a1d27; border:1px solid #2d3148; border-radius:8px;
    padding:12px 14px; margin-bottom:8px;
}
.bm-query { font-size:12px; font-weight:600; color:#818cf8; margin-bottom:5px;
            white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.bm-text  { font-size:12px; color:#e2e8f0; line-height:1.6; white-space:pre-wrap;
            max-height:80px; overflow:hidden; }
.bm-ts    { font-size:10px; color:#475569; margin-top:5px; }
.bm-count { display:inline-block; background:rgba(245,158,11,.18); color:#f59e0b;
            border:1px solid rgba(245,158,11,.35); border-radius:10px;
            font-size:10px; font-weight:700; padding:1px 8px; margin-left:6px; }
</style>
""",
    unsafe_allow_html=True,
)


#  PDF export


def _safe_text(v) -> str:
    s = "" if v is None else str(v)
    return s


def _pretty_col(k: str) -> str:
    return (
        k.replace("_", " ")
        .replace("period1", "Period 1")
        .replace("period2", "Period 2")
        .title()
    )


def _to_num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def _to_rl_color(s, fallback):
    from reportlab.lib import colors

    if not isinstance(s, str):
        return fallback
    s = s.strip()
    try:
        if s.startswith("#"):
            return colors.HexColor(s)
        if s.startswith("rgba("):
            vals = s[5:-1].split(",")
            r, g, b = [max(0, min(255, int(float(x.strip())))) for x in vals[:3]]
            return colors.Color(r / 255.0, g / 255.0, b / 255.0)
        if s.startswith("rgb("):
            vals = s[4:-1].split(",")
            r, g, b = [max(0, min(255, int(float(x.strip())))) for x in vals[:3]]
            return colors.Color(r / 255.0, g / 255.0, b / 255.0)
    except Exception:
        return fallback
    return fallback


def _build_pdf_chart(chart_config):
    if not chart_config:
        return None
    cfg = chart_config.get("config", {}) or {}
    data = cfg.get("data", {}) or {}
    labels = list(data.get("labels", []) or [])
    datasets = list(data.get("datasets", []) or [])
    if not labels or not datasets:
        return None
    series = []
    for ds in datasets:
        vals = ds.get("data", []) or []
        if len(vals) < len(labels):
            vals = vals + [0] * (len(labels) - len(vals))
        series.append([_to_num(v) for v in vals[: len(labels)]])
    if not series:
        return None
    from reportlab.lib import colors
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics.charts.barcharts import HorizontalBarChart
    from reportlab.graphics.charts.linecharts import HorizontalLineChart

    max_label = max((len(str(x)) for x in labels), default=10)
    left_pad = 120 if max_label <= 14 else min(210, 120 + (max_label - 14) * 4)
    chart_h = max(180, min(380, 15 * len(labels) + 45))
    drawing = Drawing(500, chart_h + 35)
    chart_type = str((cfg.get("type") or "")).lower()
    is_line = ("line" in chart_type) or (
        "trend" in str((chart_config or {}).get("title", "")).lower()
    )

    chart = HorizontalLineChart() if is_line else HorizontalBarChart()
    chart.x = left_pad
    chart.y = 18
    chart.width = 480 - left_pad
    chart.height = chart_h
    chart.data = tuple(tuple(row) for row in series)
    display_labels = [
        str(x) if len(str(x)) <= 28 else f"{str(x)[:25]}..." for x in labels
    ]
    if len(display_labels) > 24:
        step = max(2, len(display_labels) // 12)
        display_labels = [
            lab if i % step == 0 else "" for i, lab in enumerate(display_labels)
        ]
    chart.categoryAxis.categoryNames = display_labels
    chart.categoryAxis.labels.boxAnchor = "e"
    chart.categoryAxis.labels.fontSize = 7 if len(labels) > 12 else 8
    chart.categoryAxis.labels.dx = -6
    chart.categoryAxis.labels.fillColor = colors.HexColor("#cbd5e1")
    chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.labels.fillColor = colors.HexColor("#94a3b8")
    chart.valueAxis.strokeColor = colors.HexColor("#334155")
    chart.categoryAxis.strokeColor = colors.HexColor("#334155")
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = colors.HexColor("#1f2937")

    if is_line:
        chart.joinedLines = 1
        for i, ds in enumerate(datasets):
            c = ds.get("borderColor") or ds.get("backgroundColor")
            if isinstance(c, list) and c:
                c = c[0]
            line_c = _to_rl_color(c, colors.HexColor("#60a5fa"))
            chart.lines[i].strokeColor = line_c
            chart.lines[i].strokeWidth = 1.7
            if hasattr(chart.lines[i], "symbol") and len(labels) > 36:
                chart.lines[i].symbol = None
    else:
        chart.groupSpacing = 5
        chart.barSpacing = 2
        for i, ds in enumerate(datasets):
            c = ds.get("backgroundColor")
            if isinstance(c, list) and c:
                c = c[0]
            fill = _to_rl_color(c, colors.HexColor("#60a5fa"))
            chart.bars[i].fillColor = fill
            chart.bars[i].strokeColor = fill
    drawing.add(chart)
    return drawing


def _build_pdf_chart_image(chart_config, max_width_pts):
    if not chart_config:
        return None
    cfg = chart_config.get("config") or {}
    data = cfg.get("data") or {}
    labels = list(data.get("labels", []) or [])
    datasets = list(data.get("datasets", []) or [])
    if not labels or not datasets:
        return None

    try:
        import matplotlib.pyplot as plt
        from io import BytesIO
        from reportlab.platypus import Image as RLImage
    except Exception:
        return None

    series = []
    names = []
    colors = []
    for ds in datasets:
        vals = ds.get("data", []) or []
        vals = [float(v) if v is not None else 0.0 for v in vals[: len(labels)]]
        if len(vals) < len(labels):
            vals += [0.0] * (len(labels) - len(vals))
        series.append(vals)
        names.append(str(ds.get("label") or "Series"))
        c = ds.get("borderColor") or ds.get("backgroundColor") or "#60a5fa"
        if isinstance(c, list):
            c = c[0] if c else "#60a5fa"
        colors.append(str(c))

    chart_type = str((cfg.get("type") or "")).lower()
    is_line = ("line" in chart_type) or (
        "trend" in str((chart_config or {}).get("title", "")).lower()
    )

    fig_w = max(6.4, min(8.8, float(max_width_pts) / 72.0))
    fig_h = 3.8 if len(labels) <= 36 else 4.4
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=170)
    fig.patch.set_facecolor("#020817")
    ax.set_facecolor("#0b1220")

    x = list(range(len(labels)))
    if is_line:
        for i, vals in enumerate(series):
            ax.plot(
                x,
                vals,
                color=colors[i % len(colors)],
                linewidth=1.4,
                marker="o" if len(labels) <= 36 else None,
                markersize=2.6,
                label=names[i],
            )
    else:
        base = series[0]
        ax.bar(x, base, color=colors[0], width=0.78, label=names[0])

    tick_step = max(1, len(labels) // 12) if len(labels) > 12 else 1
    tick_idx = x[::tick_step]
    tick_lbl = [str(labels[i]) for i in tick_idx]
    ax.set_xticks(tick_idx)
    ax.set_xticklabels(tick_lbl, rotation=45, ha="right", fontsize=7, color="#94a3b8")
    ax.tick_params(axis="y", labelsize=8, colors="#94a3b8")
    ax.grid(axis="y", color="#1f2937", linewidth=0.7, alpha=0.85)
    for spine in ax.spines.values():
        spine.set_color("#334155")
    if len(series) > 1:
        leg = ax.legend(loc="upper left", fontsize=7, frameon=False)
        for t in leg.get_texts():
            t.set_color("#cbd5e1")

    plt.tight_layout()
    img_buf = BytesIO()
    fig.savefig(img_buf, format="png", dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)
    img_buf.seek(0)

    width_pts = float(max_width_pts)
    height_pts = width_pts * (fig_h / max(0.1, fig_w))
    return RLImage(img_buf, width=width_pts, height=height_pts)


def _is_number_like(value) -> bool:
    if value is None:
        return False
    s = str(value).strip().replace(",", "")
    if not s:
        return False
    if s.startswith("Rs "):
        s = s[3:].strip()
    if s.startswith("-"):
        s = s[1:]
    try:
        float(s)
        return True
    except Exception:
        return False


def _build_export_pdf(state: dict, user_query: str) -> bytes:
    from io import BytesIO
    from xml.sax.saxutils import escape
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        SimpleDocTemplate,
        Paragraph,
        Spacer,
        Table,
        TableStyle,
        LongTable,
        KeepTogether,
        HRFlowable,
        CondPageBreak,
        ListFlowable,
        ListItem,
    )

    chart_config = state.get("chart_config")
    items = state.get("formattedItems", []) or []
    text_answer = state.get("formattedText", "") or ""
    token_totals = state.get("token_totals", {}) or {}
    generated_at = datetime.now().strftime("%d %B %Y, %H:%M")
    summary = _safe_text(text_answer.split(" Token Usage")[0].strip())
    query = _safe_text(user_query)

    styles = getSampleStyleSheet()
    title_s = ParagraphStyle(
        "title_s",
        parent=styles["Heading1"],
        fontSize=18,
        leading=22,
        spaceAfter=4,
        textColor=colors.HexColor("#f8fafc"),
    )
    meta_s = ParagraphStyle(
        "meta_s",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#94a3b8"),
    )
    h2_s = ParagraphStyle(
        "h2_s",
        parent=styles["Heading2"],
        fontSize=12,
        textColor=colors.HexColor("#e2e8f0"),
        spaceBefore=8,
        spaceAfter=6,
    )
    body_s = ParagraphStyle(
        "body_s",
        parent=styles["Normal"],
        fontSize=10.5,
        leading=14,
        textColor=colors.HexColor("#cbd5e1"),
    )

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.55 * inch,
        rightMargin=0.55 * inch,
        topMargin=0.55 * inch,
        bottomMargin=0.55 * inch,
        title=f"Analytics Report - {query[:80]}",
    )
    story = []
    story.append(Paragraph(escape(query), title_s))
    story.append(Paragraph(f"Generated {generated_at}", meta_s))
    story.append(Spacer(1, 6))
    story.append(
        HRFlowable(width="100%", thickness=0.6, color=colors.HexColor("#1e293b"))
    )
    story.append(Spacer(1, 10))

    summary_lines = summary.splitlines() if summary else ["-"]
    compact_lines = []
    prev_blank = False
    for ln in summary_lines:
        ln = ln.rstrip()
        blank = not ln.strip()
        if blank and prev_blank:
            continue
        compact_lines.append(ln)
        prev_blank = blank
    summary_lines = compact_lines
    summary_lower = summary.lower() if summary else ""
    summary_looks_structured = (
        (
            "period" in summary_lower
            and "value" in summary_lower
            and len(summary_lines) > 20
        )
        or ("insights:" in summary_lower)
        or (summary.count("\n") > 40 and (" - " in summary or "\n- " in summary))
    )
    if len(summary_lines) > 80:
        summary_lines = summary_lines[:80] + [
            "",
            f"... output truncated in PDF ({len(summary.splitlines()) - 80} more lines).",
        ]
    if summary_looks_structured and items:
        summary_lines = summary_lines[:6] + [
            "",
            "Detailed rows moved to chart/table to avoid repetition in PDF.",
        ]
    story.append(Paragraph("Answer", h2_s))
    bullets = [
        ln.strip()[2:].strip() for ln in summary_lines if ln.strip().startswith("- ")
    ]
    body_lines = [
        ln for ln in summary_lines if ln.strip() and not ln.strip().startswith("- ")
    ]
    summary_html = "<br/>".join(escape(x) for x in body_lines) if body_lines else "-"
    story.append(Paragraph(summary_html, body_s))
    if bullets:
        bullet_items = [
            ListItem(Paragraph(escape(b), body_s), leftIndent=10) for b in bullets
        ]
        story.append(Spacer(1, 4))
        story.append(ListFlowable(bullet_items, bulletType="bullet", leftIndent=12))
    story.append(Spacer(1, 8))

    chart_img = _build_pdf_chart_image(chart_config, doc.width)
    chart = chart_img or _build_pdf_chart(chart_config)
    if chart is not None:
        chart_title = (chart_config or {}).get("title")
        chart_nodes = [Paragraph("Chart", h2_s)]
        if chart_title:
            chart_nodes.append(Paragraph(escape(_safe_text(chart_title)), meta_s))
            chart_nodes.append(Spacer(1, 4))
        chart_nodes.append(chart)
        story.append(KeepTogether(chart_nodes))
        story.append(Spacer(1, 10))

    include_table = bool(items)
    cfg = (chart_config or {}).get("config", {}) or {}
    chart_type = str(cfg.get("type", "")).lower()
    is_time_series_pdf = (
        ("line" in chart_type)
        or ("trend" in str((chart_config or {}).get("title", "")).lower())
        or (items and "period" in items[0] and "value" in items[0])
    )
    if summary_looks_structured and items:
        if is_time_series_pdf:
            include_table = len(items) <= 500
        else:
            include_table = len(items) <= 25

    if include_table:
        skip_keys = {"raw_value", "raw_delta"}
        cols = [k for k in items[0].keys() if k not in skip_keys]
        table_items = items
        trimmed_note = None
        if len(items) > 240:
            head = items[:120]
            tail = items[-120:]
            table_items = head + tail
            trimmed_note = (
                f"Showing first 120 and last 120 rows out of {len(items)} rows."
            )

        table_data = [[_pretty_col(c) for c in cols]]
        for row in table_items:
            table_data.append([_safe_text(row.get(c, "")) for c in cols])

        avail_w = doc.width
        if len(cols) <= 1:
            col_widths = [avail_w]
        else:
            first_w = min(max(190, avail_w * 0.36), avail_w * 0.5)
            rest_w = (avail_w - first_w) / (len(cols) - 1)
            col_widths = [first_w] + [rest_w] * (len(cols) - 1)

        tbl = LongTable(table_data, repeatRows=1, hAlign="LEFT", colWidths=col_widths)
        align_cmds = [
            ("ALIGN", (0, 0), (-1, 0), "LEFT"),
            ("ALIGN", (0, 1), (0, -1), "LEFT"),
        ]
        for ci, col_name in enumerate(cols[1:], start=1):
            numeric_col = ("rank" in col_name.lower()) or all(
                _is_number_like(r.get(col_name)) for r in items[: min(20, len(items))]
            )
            align_cmds.append(
                ("ALIGN", (ci, 1), (ci, -1), "RIGHT" if numeric_col else "LEFT")
            )

        tbl.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 9.2),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 9),
                    ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor("#cbd5e1")),
                    ("LINEABOVE", (0, 0), (-1, 0), 0.8, colors.HexColor("#334155")),
                    ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#334155")),
                    ("GRID", (0, 1), (-1, -1), 0.35, colors.HexColor("#1f2937")),
                    (
                        "ROWBACKGROUNDS",
                        (0, 1),
                        (-1, -1),
                        [colors.HexColor("#020617"), colors.HexColor("#0b1220")],
                    ),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
                + align_cmds
            )
        )
        story.append(CondPageBreak(1.6 * inch))
        story.append(Paragraph("Data Table", h2_s))
        if trimmed_note:
            story.append(Paragraph(escape(trimmed_note), meta_s))
            story.append(Spacer(1, 3))
        story.append(tbl)
        story.append(Spacer(1, 8))

    if token_totals.get("total_tokens"):
        token_txt = (
            f"Token usage - prompt: {token_totals.get('prompt_tokens', 0)} | "
            f"completion: {token_totals.get('completion_tokens', 0)} | "
            f"total: {token_totals.get('total_tokens', 0)}"
        )
        story.append(Paragraph(escape(token_txt), meta_s))

    def _on_page(canv, _doc):
        canv.saveState()
        canv.setFillColor(colors.HexColor("#020817"))
        canv.rect(0, 0, letter[0], letter[1], stroke=0, fill=1)
        canv.setFont("Helvetica", 8)
        canv.setFillColor(colors.HexColor("#64748b"))
        canv.drawString(_doc.leftMargin, 0.30 * inch, "Data Analytics Report")
        canv.drawRightString(
            letter[0] - _doc.rightMargin, 0.30 * inch, f"Page {_doc.page}"
        )
        canv.restoreState()

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return buf.getvalue()


#  API helpers


def api_ok():
    try:
        r = requests.get(f"{API}/schema", timeout=5)
        r.raise_for_status()
        return True
    except Exception:
        return False


def submit_query(query, session_id=None):
    body = {"query": query}
    if session_id:
        body["sessionId"] = session_id
    r = requests.post(f"{API}/query", json=body, timeout=10)
    r.raise_for_status()
    return r.json()


def fetch_state(query_id):
    r = requests.get(f"{API}/query/{query_id}", timeout=10)
    r.raise_for_status()
    return r.json()


def _wait_for_ingest_ready(max_wait_seconds=20.0):
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            schema_r = requests.get(f"{API}/schema", timeout=3)
            if schema_r.status_code < 500:
                return True
        except Exception:
            pass
        time.sleep(0.6)
    return False


def ingest_uploaded_files(
    uploaded_files,
    reset_db=False,
    merge_confirm=False,
    discover_files=False,
    discover_dirs=None,
    llm_schema_infer=True,
):
    files_payload = []
    if uploaded_files:
        SHARED_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        for f in uploaded_files or []:
            raw = f.getvalue()
            if not raw:
                continue
            safe_name = Path(f.name).name
            local_path = SHARED_UPLOAD_DIR / safe_name
            local_path.write_bytes(raw)
            files_payload.append(
                {
                    "name": safe_name,
                    "path": f"{CONTAINER_UPLOAD_DIR}/{safe_name}",
                }
            )
    if not files_payload and not discover_files:
        raise ValueError("No valid file content to upload.")

    body = {
        "files": files_payload,
        "reset_db": bool(reset_db),
        "merge_confirm": bool(merge_confirm),
        "discover_files": bool(discover_files),
        "discover_latest_only": True,
        "llm_schema_infer": bool(llm_schema_infer),
    }
    if discover_dirs:
        body["discover_dirs"] = discover_dirs

    _wait_for_ingest_ready(max_wait_seconds=20.0)
    last_err = None
    for attempt in range(12):
        try:
            r = requests.post(f"{API}/ingest", json=body, timeout=300)
            if r.status_code >= 400:
                msg = None
                try:
                    msg = r.json().get("error")
                except Exception:
                    msg = r.text[:500]
                raise RuntimeError(f"HTTP {r.status_code}: {msg or 'ingest failed'}")
            return r.json()
        except Exception as exc:
            last_err = exc
            err_s = str(exc)
            if (
                ("HTTP 404" in err_s)
                or ("invocation_stopped" in err_s)
                or ("Connection aborted" in err_s)
            ):
                _wait_for_ingest_ready(max_wait_seconds=6.0)
                time.sleep(0.5 + attempt * 0.35)
                continue
            if attempt < 11:
                time.sleep(0.4 + attempt * 0.25)
                continue
            raise last_err
    raise last_err


def fetch_chart_html(query_id):
    try:
        r = requests.get(f"{API}/query/{query_id}/chart", timeout=10)
        r.raise_for_status()
        text = r.text
        if text.startswith('"') and text.endswith('"'):
            try:
                import json as _j

                return _j.loads(text)
            except Exception:
                return text
        return text
    except Exception as e:
        return f"<p style='color:#ef4444'>Could not load chart: {e}</p>"


def _parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _compute_backend_step_times(state):
    ts_map = state.get("status_timestamps", {}) or {}
    created_at = _parse_iso(state.get("createdAt"))
    step_at = {}
    for idx in range(len(STEPS)):
        for status_name in STEP_STATUS_OPTIONS.get(idx, []):
            dt = _parse_iso(ts_map.get(status_name))
            if dt:
                step_at[idx] = dt
                break
    if 0 not in step_at and created_at:
        step_at[0] = created_at
    times = {}
    prev = created_at or step_at.get(0)
    for idx in range(len(STEPS)):
        dt = step_at.get(idx)
        if not dt:
            continue
        if prev:
            times[idx] = max((dt - prev).total_seconds(), 0.0)
        else:
            times[idx] = 0.0
        prev = dt
    return times


def render_steps(completed_idx, is_error, step_times):
    parts = []
    for i, s in enumerate(STEPS):
        if is_error and i == completed_idx:
            cls, icon = "error", "✕"
        elif i <= completed_idx:
            cls, icon = "done", "✓"
        elif i == completed_idx + 1:
            cls, icon = "active", s["icon"]
        else:
            cls, icon = "idle", s["icon"]

        t = step_times.get(i)
        if t is not None:
            t_label = f"{t:.2f}s" if t < 1 else f"{t:.1f}s"
            time_html = f'<div class="step-time">{t_label}</div>'
        elif cls == "active":
            time_html = '<div class="step-time">…</div>'
        else:
            time_html = '<div class="step-time"></div>'

        parts.append(f"""
        <div class="step-wrap {cls}">
            <div class="step-circle {cls}">{icon}</div>
            <div class="step-label">{s["label"]}</div>
            <div class="step-sub">{s["sub"]}</div>
            {time_html}
        </div>""")

    st.markdown(f'<div class="step-row">{"".join(parts)}</div>', unsafe_allow_html=True)


def render_tokens(usage, totals):
    if not usage:
        return

    def _is_llm_entry(e: dict) -> bool:
        if "is_llm" in e:
            return bool(e.get("is_llm"))
        model = (e.get("model") or "").strip()
        return model not in ("rule_based", "adaptive_rule")

    llm_rows = 0
    rows = []
    for e in usage:
        is_llm = _is_llm_entry(e)
        if is_llm:
            llm_rows += 1
            prompt = e.get("prompt_tokens", 0)
            completion = e.get("completion_tokens", 0)
            total = e.get("total_tokens", 0)
        else:
            prompt = 0
            completion = 0
            total = 0
        rows.append(
            {
                "Step": e.get("step", "?"),
                "Model": (e.get("model") or "").split("/")[-1].replace("-instant", ""),
                "Prompt": prompt,
                "Completion": completion,
                "Total": total,
            }
        )
    st.dataframe(rows, width="stretch", hide_index=True)
    if llm_rows == 0:
        st.caption("LLM not used for this query (rule-based and deterministic steps).")
    elif totals and totals.get("total_tokens"):
        c1, c2, c3 = st.columns(3)
        c1.metric("Prompt tokens", totals.get("prompt_tokens", 0))
        c2.metric("Completion tokens", totals.get("completion_tokens", 0))
        c3.metric("Total tokens", totals.get("total_tokens", 0))


# ── Schema section — now uses ER diagram ─────────────────────────────────────


def render_schema_section():
    """
    Renders schema views in two tabs:
    - ER schema from derived tables only
    - Raw uploaded source tables in tabular view
    """
    tab_er, tab_raw = st.tabs(["ER schema (derived tables)", "Raw uploaded tables"])

    with tab_er:
        if _HAS_DIAGRAM:
            render_schema_diagram(
                view="derived",
                refresh_nonce=int(st.session_state.get("schema_refresh_nonce", 0)),
            )
        else:
            schema_data = fetch_schema(view="derived")
            if not schema_data or not schema_data.get("tables"):
                st.caption("No derived schema available yet.")
            else:
                tables = schema_data.get("tables", [])
                rels = schema_data.get("relationships", [])
                st.caption(
                    f"{len(tables)} derived table{'s' if len(tables) != 1 else ''}"
                )
                col_a, col_b = st.columns(2)
                for i, tbl in enumerate(tables):
                    tname = tbl.get("table", "unknown")
                    cols = tbl.get("columns", [])
                    target = col_a if i % 2 == 0 else col_b
                    with target:
                        with st.expander(
                            f"[ ] {tname} ({len(cols)} columns)", expanded=False
                        ):
                            st.dataframe(
                                [
                                    {"Column": c["name"], "Type": c["type"]}
                                    for c in cols
                                ],
                                hide_index=True,
                                width="stretch",
                            )
                if rels:
                    with st.expander(f"Relationships ({len(rels)})", expanded=False):
                        for r in rels:
                            st.markdown(
                                f"`{r.get('from_table')}.{r.get('from_column')}` -> "
                                f"`{r.get('to_table')}.{r.get('to_column')}` "
                                f"*({r.get('type', 'FK')}, {r.get('confidence', '?')})*"
                            )

    with tab_raw:
        raw_data = fetch_schema(view="raw")
        if not raw_data or not raw_data.get("tables"):
            st.caption("No raw tables loaded yet.")
        else:
            raw_tables = raw_data.get("tables", [])
            st.caption(
                f"{len(raw_tables)} raw table{'s' if len(raw_tables) != 1 else ''}"
            )
            for tbl in raw_tables:
                tname = tbl.get("table", "unknown")
                cols = tbl.get("columns", [])
                with st.expander(f"{tname} ({len(cols)} columns)", expanded=False):
                    st.dataframe(
                        [{"Column": c["name"], "Type": c["type"]} for c in cols],
                        hide_index=True,
                        width="stretch",
                    )

    if st.button("Refresh schema", key="refresh_schema_bottom", type="secondary"):
        st.session_state["schema_refresh_nonce"] = (
            int(st.session_state.get("schema_refresh_nonce", 0)) + 1
        )
        st.session_state["schema_last_refresh"] = datetime.now().strftime("%H:%M:%S")
        st.rerun()

    if st.session_state.get("schema_last_refresh"):
        st.caption(f"Last schema refresh: {st.session_state['schema_last_refresh']}")


#  Page layout

st.markdown("## Data Analytics - Live Pipeline")

if api_ok():
    st.success("Motia API connected (localhost:3121)")
else:
    st.error("Motia API not reachable - run `npm run dev` first")
    st.stop()

st.divider()

left, right = st.columns([3, 2], gap="large")

with left:
    #  Pipeline steps
    st.markdown('<div class="section-lbl">Pipeline steps</div>', unsafe_allow_html=True)
    steps_placeholder = st.empty()
    with steps_placeholder.container():
        render_steps(-1, False, {})

    st.divider()

    #  Data source (file upload)
    st.markdown('<div class="section-lbl">Data source</div>', unsafe_allow_html=True)
    with st.expander("Upload dataset files (CSV / JSON / Parquet)", expanded=False):
        upload_files = st.file_uploader(
            "Upload files",
            type=["csv", "tsv", "json", "jsonl", "parquet"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )
        same_business_merge = False
        n_uploads = len(upload_files or [])
        if n_uploads > 1:
            st.caption(
                "Multiple files detected. By default, ingest one file at a time."
            )
            same_business_merge = st.checkbox(
                "These files are from the same business schema (merge together)",
                value=False,
                key="same_business_merge_confirm",
            )
        reset_db = st.checkbox("Replace existing dataset", value=True)
        ingest_btn = st.button("Ingest files", type="secondary")
        stitch_drop_btn = st.button(
            "Stitch dropped file",
            type="secondary",
            help="Auto-discover the newest CSV/JSON/Parquet dropped in the project folder and ingest it.",
        )
        if ingest_btn:
            if not upload_files:
                st.warning("Select at least one file.")
            elif len(upload_files) > 1 and not same_business_merge:
                st.warning(
                    "Please upload one file only, or confirm same-business merge."
                )
            else:
                with st.spinner("Ingesting files into DuckDB..."):
                    try:
                        result = ingest_uploaded_files(
                            upload_files,
                            reset_db=reset_db,
                            merge_confirm=same_business_merge,
                        )
                        tnames = ", ".join(result.get("tables_created", []))
                        n_rels = len(result.get("relationships", []))
                        msg = (
                            f"Ingested: {tnames}" if tnames else "Ingestion completed."
                        )
                        if n_rels:
                            msg += f" - {n_rels} relationship{'s' if n_rels != 1 else ''} detected"
                        st.success(msg)
                        # Reset conversational context on schema change.
                        st.session_state.conversation_session_id = None
                        st.session_state.pending_session = None
                        for k in (_DISCOVERY_KEY, _DISCOVERY_AT, _DISCOVERY_CACHED):
                            st.session_state.pop(k, None)
                        skipped = result.get("skipped_files") or []
                        if skipped:
                            for s in skipped:
                                st.warning(
                                    f"Skipped `{s.get('file', 'unknown')}`: {s.get('reason', 'incompatible schema')}"
                                )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Ingestion failed: {e}")

        if stitch_drop_btn:
            with st.spinner("Scanning project directory for dropped files..."):
                try:
                    result = ingest_uploaded_files(
                        uploaded_files=None,
                        reset_db=reset_db,
                        merge_confirm=False,
                        discover_files=True,
                    )
                    d_files = result.get("discovered_files") or []
                    tnames = ", ".join(result.get("tables_created", []))
                    if tnames:
                        st.success(f"Stitched: {tnames}")
                    elif d_files:
                        st.success("Dropped file discovered and ingestion completed.")
                    else:
                        st.info("No new dropped data files found to ingest.")

                    # Reset conversational context on schema change.
                    if tnames:
                        st.session_state.conversation_session_id = None
                        st.session_state.pending_session = None
                        for k in (_DISCOVERY_KEY, _DISCOVERY_AT, _DISCOVERY_CACHED):
                            st.session_state.pop(k, None)
                        st.rerun()
                except Exception as e:
                    st.error(f"Stitch failed: {e}")

    st.divider()

    # Suggested question chips
    suggestions_css()
    chosen = render_suggestions(api_base=API)
    if chosen:
        st.session_state["query_input_field"] = chosen
        _on_enter()

    st.divider()
    render_discovery(api_base=API)

    #  Query input
    st.markdown('<div class="section-lbl">Ask a question</div>', unsafe_allow_html=True)

    query_input = st.text_input(
        "query",
        label_visibility="collapsed",
        placeholder="e.g. Top 5 entities by total value in last year",
        disabled=st.session_state.polling,
        on_change=_on_enter,
        key="query_input_field",
    )
    send_col, status_col = st.columns([1, 3])
    send_btn = send_col.button(
        "Send",
        disabled=st.session_state.polling,
        width="stretch",
        type="primary",
        on_click=_on_enter,
    )
    status_placeholder = status_col.empty()
    clarify_placeholder = st.empty()
    result_placeholder = st.empty()

with right:
    st.markdown('<div class="section-lbl">Token usage</div>', unsafe_allow_html=True)
    token_placeholder = st.empty()
    with token_placeholder.container():
        st.caption("No data yet — submit a query to begin.")

    st.divider()
    st.markdown('<div class="section-lbl">Generated SQL</div>', unsafe_allow_html=True)
    sql_placeholder = st.empty()
    with sql_placeholder.container():
        st.markdown(
            '<div class="sql-box">Waiting for SQL generation</div>',
            unsafe_allow_html=True,
        )

    st.divider()
    st.markdown(
        '<div class="section-lbl">Business digest</div>', unsafe_allow_html=True
    )
    render_business_digest(API)

# ── Bookmark localStorage bridge ──────────────────────────────────────────────


def bm_save_to_ls(bookmarks):
    import json as _json

    safe = _json.dumps(bookmarks, ensure_ascii=False)
    components.html(
        f"""
        <script>
        try {{
          localStorage.setItem('sales_analytics_bookmarks', {repr(safe)});
        }} catch(e) {{}}
        </script>
        """,
        height=0,
    )


def _star_button_key(qid):
    return f"star_{qid}"


def _render_bookmarks_panel():
    bms = st.session_state.bookmarks
    label = "⭐ Pinned Results"
    count_html = f'<span class="bm-count">{len(bms)}</span>'
    st.markdown(
        f'<div class="section-lbl">{label} {count_html}</div>',
        unsafe_allow_html=True,
    )
    if not bms:
        st.caption("No pinned results yet — star a result to save it here.")
        return

    with st.expander(
        f"Show {len(bms)} pinned result{'s' if len(bms) != 1 else ''}", expanded=True
    ):
        for i, bm in enumerate(bms):
            ts = bm.get("timestamp", "")
            q = bm.get("query", "")
            txt = bm.get("text", "")[:300]
            col_meta, col_del = st.columns([9, 1])
            with col_meta:
                st.markdown(
                    f'<div class="bm-item">'
                    f'<div class="bm-query">\u201c{q}\u201d</div>'
                    f'<div class="bm-text">{txt}</div>'
                    f'<div class="bm-ts">{ts}</div>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            with col_del:
                st.write("")
                if st.button("🗑", key=f"del_bm_{bm['query_id']}_{i}", help="Unpin"):
                    st.session_state.bookmarks = [
                        b
                        for b in st.session_state.bookmarks
                        if b["query_id"] != bm["query_id"]
                    ]
                    bm_save_to_ls(st.session_state.bookmarks)
                    st.rerun()


# ── Load bookmarks from localStorage on first render ─────────────────────────
if not st.session_state.bm_loaded:
    components.html(
        """
        <script>
        (function() {
          const bms = JSON.parse(localStorage.getItem('sales_analytics_bookmarks') || '[]');
          if (bms.length === 0) return;
          const encoded = encodeURIComponent(JSON.stringify(bms));
          const url = new URL(window.parent.location.href);
          if (!url.searchParams.has('_bm_init')) {
            url.searchParams.set('_bm_init', encoded);
            window.parent.history.replaceState({}, '', url.toString());
            window.parent.location.reload();
          }
        })();
        </script>
        """,
        height=0,
    )
    try:
        import urllib.parse as _ul

        raw = st.query_params.get("_bm_init", "")
        if raw:
            loaded = json.loads(_ul.unquote(raw))
            if isinstance(loaded, list):
                st.session_state.bookmarks = loaded
            st.query_params.pop("_bm_init", None)
    except Exception:
        pass
    st.session_state.bm_loaded = True


# ── Pinned Results panel ──────────────────────────────────────────────────────
_render_bookmarks_panel()

st.divider()

# ── History ───────────────────────────────────────────────────────────────────
if st.session_state.history:
    hist_search = st.text_input(
        "🔍 Filter history",
        placeholder="Type a keyword to search past queries…",
        key="hist_search_input",
        label_visibility="collapsed",
    )
    filtered_hist = (
        [
            item
            for item in st.session_state.history
            if hist_search.lower() in item["query"].lower()
        ]
        if hist_search.strip()
        else st.session_state.history
    )

    total = len(st.session_state.history)
    shown = len(filtered_hist)
    label = (
        f"History — {shown} of {total} match" + ("es" if shown != 1 else "")
        if hist_search.strip()
        else f"History — {total} quer" + ("ies" if total != 1 else "y")
    )
    with st.expander(label, expanded=bool(hist_search.strip())):
        if not filtered_hist:
            st.caption("No queries match that keyword.")
        for item in reversed(filtered_hist):
            st.markdown(
                f"""
            <div class="hist-item">
                <div class="hist-q">▸ {item["query"]}</div>
                <div class="hist-a">{item["result"][:300]}</div>
            </div>""",
                unsafe_allow_html=True,
            )

# ── Schema ER Diagram — BOTTOM OF PAGE ───────────────────────────────────────
st.divider()
st.markdown(
    '<div class="section-lbl">Dataset schema — ER diagram</div>', unsafe_allow_html=True
)
render_schema_section()


# ── Polling / final state ─────────────────────────────────────────────────────
state = None
if st.session_state.polling and st.session_state.query_id:
    try:
        state = fetch_state(st.session_state.query_id)
    except Exception as e:
        st.session_state.polling = False
        st.error(f"Polling error: {e}")
elif not st.session_state.polling and st.session_state.final_state:
    state = st.session_state.final_state

if state:
    status = state.get("status", "")
    target_idx = STATUS_MAP.get(status, -1)
    is_error = status == "error"
    backend_times = _compute_backend_step_times(state)
    if backend_times:
        st.session_state.step_times.update(backend_times)

    if target_idx > st.session_state.last_completed:
        st.session_state.last_completed = target_idx

    render_idx = (
        st.session_state.last_completed if st.session_state.polling else target_idx
    )
    with steps_placeholder.container():
        render_steps(render_idx, is_error, st.session_state.step_times)

    if st.session_state.polling:
        status_placeholder.caption(f"Status: `{status}`")
    elif is_error:
        status_placeholder.caption("Pipeline error — check Motia logs")
    else:
        status_placeholder.caption("Done — ask another question")

    if state.get("generated_sql"):
        with sql_placeholder.container():
            st.markdown(
                f'<div class="sql-box">{state["generated_sql"].strip()}</div>',
                unsafe_allow_html=True,
            )

    if state.get("token_usage"):
        with token_placeholder.container():
            render_tokens(state.get("token_usage"), state.get("token_totals"))

    #  Terminal states
    if status == "needs_clarification":
        if st.session_state.polling:
            st.session_state.polling = False
            st.session_state.pending_session = (
                st.session_state.conversation_session_id or st.session_state.query_id
            )
            st.session_state.final_state = state
            st.rerun()

        q = state.get("clarification", "Please clarify your query.")
        with clarify_placeholder.container():
            st.markdown(
                f'<div class="clarify-box">💬 {q}</div>', unsafe_allow_html=True
            )
            answer = st.text_input(
                "Your answer",
                key=f"clarify_{st.session_state.query_id}",
                placeholder="Type your answer and press Enter",
            )
            if st.button(
                "Reply", key=f"reply_{st.session_state.query_id}", type="primary"
            ):
                if answer.strip():
                    st.session_state.polling = True
                    st.session_state.step_times = {}
                    st.session_state.last_completed = -1
                    st.session_state.poll_start = time.time()
                    resp = submit_query(
                        answer.strip(), session_id=st.session_state.pending_session
                    )
                    st.session_state.query_id = resp["queryId"]
                    st.session_state.conversation_session_id = (
                        resp.get("sessionId")
                        or st.session_state.conversation_session_id
                        or resp["queryId"]
                    )
                    st.session_state.pending_session = None
                    st.rerun()

    elif status == "completed":
        if st.session_state.polling:
            st.session_state.polling = False
            st.session_state.final_state = state
            hist_query = query_input if query_input else "(clarification reply)"
            st.session_state.history.append(
                {
                    "query": hist_query,
                    "result": state.get("formattedText", ""),
                }
            )
            st.rerun()

        text = state.get("formattedText", "Query complete.")
        with result_placeholder.container():
            st.markdown('<div class="section-lbl">Result</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="result-box">{text}</div>', unsafe_allow_html=True)

            # ── Star / bookmark button ──────────────────────────────────────
            qid = st.session_state.query_id
            already_starred = any(
                b["query_id"] == qid for b in st.session_state.bookmarks
            )
            last_q = (
                st.session_state.history[-1]["query"]
                if st.session_state.history
                else "query"
            )

            star_col, chart_col, _ = st.columns([1, 1, 4])
            with star_col:
                star_label = "⭐ Pinned" if already_starred else "☆ Pin result"
                if st.button(
                    star_label, key=_star_button_key(qid), use_container_width=True
                ):
                    if already_starred:
                        st.session_state.bookmarks = [
                            b
                            for b in st.session_state.bookmarks
                            if b["query_id"] != qid
                        ]
                    else:
                        st.session_state.bookmarks.insert(
                            0,
                            {
                                "query_id": qid,
                                "query": last_q,
                                "text": text,
                                "timestamp": datetime.now().strftime("%d %b %Y, %H:%M"),
                            },
                        )
                    bm_save_to_ls(st.session_state.bookmarks)
                    st.rerun()

            if state.get("chart_config"):
                st.markdown("<br>", unsafe_allow_html=True)
                with st.expander("Interactive chart", expanded=True):
                    chart_html = fetch_chart_html(st.session_state.query_id)
                    components.html(chart_html, height=520, scrolling=False)

            # PDF download
            st.markdown("<br>", unsafe_allow_html=True)
            last_query = (
                st.session_state.history[-1]["query"]
                if st.session_state.history
                else "query"
            )
            safe_name = (
                "".join(
                    ch
                    for ch in last_query[:48]
                    if ch.isalnum() or ch in (" ", "_", "-")
                )
                .strip()
                .replace(" ", "_")
                or "report"
            )
            try:
                pdf_bytes = _build_export_pdf(state, last_query)
                st.download_button(
                    label="Download PDF",
                    data=pdf_bytes,
                    file_name=f"{safe_name}_{datetime.now().strftime('%Y-%m-%d')}.pdf",
                    mime="application/pdf",
                    use_container_width=False,
                )
            except ImportError:
                st.error("PDF export requires `reportlab`. Run: pip install reportlab")
            except Exception as e:
                st.warning(f"PDF export failed, using TXT fallback. ({e})")
                fallback_payload = {
                    "query": last_query,
                    "generated_at": datetime.now().isoformat(),
                    "formatted_text": state.get("formattedText", ""),
                    "formatted_items": state.get("formattedItems", []),
                    "generated_sql": state.get("generated_sql", ""),
                    "token_totals": state.get("token_totals", {}),
                }
                st.download_button(
                    label="Download TXT (fallback)",
                    data=json.dumps(fallback_payload, ensure_ascii=False, indent=2),
                    file_name=f"{safe_name}_{datetime.now().strftime('%Y-%m-%d')}.txt",
                    mime="text/plain",
                    use_container_width=False,
                )

    elif status == "error":
        if st.session_state.polling:
            st.session_state.polling = False
            st.session_state.final_state = state
            st.rerun()

        err = state.get("error", "Unknown error.")
        with result_placeholder.container():
            st.markdown('<div class="section-lbl">Result</div>', unsafe_allow_html=True)
            st.markdown(
                f'<div class="result-box result-error">⚠ {err}</div>',
                unsafe_allow_html=True,
            )

    else:
        if st.session_state.polling:
            time.sleep(POLL_INTERVAL)
            st.rerun()
