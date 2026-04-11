# Sales Analysis AI Pipeline

Single source of documentation for this repository.

## Overview

This project converts natural-language business questions into analytics results over DuckDB.

Core flow:
1. Receive query
2. Parse intent
3. Generate SQL
4. Execute SQL
5. Forecast / anomaly scan
6. Format response + chart

## Runtime Stack

- Orchestration/API: Motia engine
- Database: DuckDB (`Qwen_llama/motia/data/analytics.duckdb`)
- Cache/state: Redis + Motia state
- UI: Streamlit (`streamlit_app.py`) and legacy HTML dashboard (`dashboard.html`)

## Current Step Architecture

Step files are thin process wrappers so UI can show pipeline stages.

Step wrappers in `Qwen_llama/motia/steps/`:
- `receive_query_step.py`
- `parse_intent_step.py`
- `text_to_sql_step.py`
- `execute_query_step.py`
- `forecast_step.py`
- `anomaly_detection_step.py`
- `format_result_step.py`
- `business_report_step.py`
- `api_router_step.py`

Business logic lives in modules:
- `Qwen_llama/modules/pipeline/`
- `Qwen_llama/modules/reporting/`
- `Qwen_llama/modules/utilities/`

## Repository Map

- `streamlit_app.py`: Streamlit UI
- `dashboard.html`: Legacy dashboard view
- `server.py`: Static server for legacy dashboard
- `Qwen_llama/motia/`: Motia runtime, steps, compose files, local data
- `Qwen_llama/services/`: service layer called by modules
- `Qwen_llama/db/`: schema/context/query/ingest helpers
- `Qwen_llama/utils/`: forecasting, time/date parsing, shared utilities

## Setup

1. Copy env file:

```powershell
copy Qwen_llama\.env.example Qwen_llama\.env
```

2. Set required key in `Qwen_llama/.env`:

```env
GROQ_API_TOKEN=your_token_here
```

3. Install local UI dependencies (for Streamlit):

```powershell
pip install -r requirements.txt
```

## Run

Backend (Motia + Redis):

```powershell
npm run dev
```

Dashboard:

```powershell
npm run dashboard
```

Optional legacy HTML server:

```powershell
python server.py
```

## Endpoints

- API base: `http://localhost:3121`
- Submit query: `POST /query`
- Query result: `GET /query/{queryId}`
- Report latest: `GET /reports/latest`
- Report run: `POST /reports/run`

## Useful Commands

- Start stack: `npm run dev`
- Stop stack: `npm run dev:down`
- Tail logs: `npm run dev:logs`
- Streamlit: `npm run dashboard`
- Chatbot runner: `npm run start`
