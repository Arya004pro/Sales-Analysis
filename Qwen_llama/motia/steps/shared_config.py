"""Shared configuration for all Motia steps."""

import os
import sys
from dotenv import load_dotenv

STEPS_DIR = os.path.dirname(os.path.abspath(__file__))
MOTIA_DIR = os.path.dirname(STEPS_DIR)
PROJECT_ROOT = os.path.dirname(MOTIA_DIR)

for _p in [STEPS_DIR, MOTIA_DIR, PROJECT_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

for _env in [
    os.path.join(PROJECT_ROOT, ".env"),
    os.path.join(MOTIA_DIR, ".env"),
    "/app/.env",
]:
    if os.path.exists(_env):
        load_dotenv(_env)
        break

GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen/qwen3-32b")
SQL_GENERATOR_MODEL = os.getenv("SQL_GENERATOR_MODEL", QWEN_MODEL)
INSIGHTS_MODEL = os.getenv("INSIGHTS_MODEL", QWEN_MODEL)
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# Reasoning controls for Qwen. Reasoning is hidden from user output because
# downstream steps strip <think> blocks before parsing/formatting.
QWEN_ENABLE_REASONING = os.getenv("QWEN_ENABLE_REASONING", "1") == "1"
QWEN_REASONING_EFFORT = os.getenv("QWEN_REASONING_EFFORT", "low")

try:
    PARSE_INTENT_MAX_RETRIES = max(1, int(os.getenv("PARSE_INTENT_MAX_RETRIES", "1")))
except ValueError:
    PARSE_INTENT_MAX_RETRIES = 1

PARSE_INTENT_USE_INSTRUCTOR = os.getenv("PARSE_INTENT_USE_INSTRUCTOR", "1") == "1"

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "motia/data/analytics.duckdb")
