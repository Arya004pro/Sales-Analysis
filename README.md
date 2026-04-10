# Sales Analysis AI Pipeline (DuckDB + Motia + Streamlit)

This repository is an end-to-end sales analytics assistant that turns natural-language business questions into SQL, executes them on DuckDB, and returns formatted insights/charts.

It includes:
- A Motia workflow engine for query orchestration.
- A Streamlit app for interactive analytics and KPI tiles.
- A legacy HTML dashboard for pipeline visualization.
- Optional modern stack integrations: Instructor, Redis cache, Chroma SQL memory, MLflow tracking, dbt post-ingest hook, and a FastAPI websocket bridge.

## What This Project Does
- Accepts a query like "Top 5 products by revenue in 2023".
- Parses intent and maps it to schema-aware analytics logic.
- Generates and validates SQL.
- Executes SQL against DuckDB.
- Adds forecasting/anomaly/insight layers.
- Returns formatted text, metrics, and chart configuration.

## Runtime Architecture
- API/workflow: Motia engine (container)
- Database: DuckDB file (`Qwen_llama/motia/data/analytics.duckdb`)
- State/cache: file-based + optional Redis
- Retrieval memory: optional ChromaDB (few-shot SQL examples)
- Forecast experiment tracking: optional MLflow
- Transform hook: optional dbt-duckdb after ingestion
- Realtime updates: optional FastAPI websocket bridge

## Repository Layout
- `streamlit_app.py`: Main modern dashboard (`npm run dashboard`)
- `dashboard.html`: Legacy dashboard (works with websocket bridge + polling fallback)
- `server.py`: Static file server for `dashboard.html`
- `Qwen_llama/motia/steps/`: Motia workflow steps
- `Qwen_llama/services/`: Service layer used by steps
- `Qwen_llama/db/`: DuckDB access, schema context, ingestion, dbt runner
- `Qwen_llama/utils/`: Utilities (forecasting, cache, SQL memory, MLflow)
- `Qwen_llama/ws_bridge.py`: FastAPI websocket bridge service
- `Qwen_llama/motia/docker-compose.yml`: Motia + Redis + websocket bridge stack

## Prerequisites
1. Docker Desktop (running)
2. Node.js + npm
3. Python 3.10+ (3.11 recommended)
4. Groq API token

## Environment Setup
From repository root:

```powershell
copy Qwen_llama\.env.example Qwen_llama\.env
```

Set at least:

```env
GROQ_API_TOKEN=your_groq_api_token_here
```

Important optional toggles in `Qwen_llama/.env`:
- `PARSE_INTENT_USE_INSTRUCTOR=1`
- `REDIS_URL=redis://localhost:6379/0`
- `CHROMA_ENABLED=1`
- `MLFLOW_ENABLED=1`
- `DBT_AUTO_RUN=0`
- `WS_BRIDGE_URL=http://localhost:8001`

## Install Local Python Dependencies (for Streamlit)
The Docker stack handles Motia/container dependencies. For local Streamlit UI:

```powershell
pip install -r requirements.txt
pip install streamlit
```

If you want all optional Python tooling locally too:

```powershell
pip install -r Qwen_llama\requirements.txt
```

## Run Everything
Use separate terminals.

1. Start backend stack (Motia + Redis + websocket bridge):

```powershell
npm run dev
```

2. Start Streamlit dashboard:

```powershell
npm run dashboard
```

3. Open UI:
- Streamlit: `http://localhost:8501`

4. Optional: start legacy HTML dashboard server:

```powershell
python server.py
```

Legacy dashboard URL:
- `http://localhost:8080/dashboard.html`

5. Optional: run terminal chatbot client:

```powershell
npm run start
```

## Key Endpoints
- Motia REST API: `http://localhost:3121`
- Query submit: `POST http://localhost:3121/query`
- Query state: `GET http://localhost:3121/query/{queryId}`
- iii console: `http://localhost:3113`
- WS bridge health: `http://localhost:8001/health`
- WS query stream: `ws://localhost:8001/ws/query/{queryId}`

## Common Commands
- Start stack: `npm run dev`
- Stop stack: `npm run dev:down`
- Tail logs: `npm run dev:logs`
- Start Streamlit: `npm run dashboard`
- Start chatbot: `npm run start`

## Data Ingestion
Use the Streamlit "Data source" uploader to ingest CSV/JSON/Parquet files.

When `DBT_AUTO_RUN=1`, dbt models run automatically after ingestion.

## Notes On Optional Integrations
- Redis: used for schema prompt cache when available, falls back safely.
- ChromaDB: stores/retrieves successful SQL examples for few-shot prompting.
- MLflow: logs forecast experiment params/metrics when enabled.
- Websocket bridge: legacy dashboard receives push updates; polling remains fallback.

## Troubleshooting
- If Streamlit fails with `streamlit` not found, run `pip install streamlit`.
- If API looks unreachable, confirm containers are up with:

```powershell
docker compose -f Qwen_llama/motia/docker-compose.yml ps
```

- If dependency changes were made, rebuild with:

```powershell
npm run dev:down
npm run dev
```
