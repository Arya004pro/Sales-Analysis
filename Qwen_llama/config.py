import os
from dotenv import load_dotenv

load_dotenv()

GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen/qwen3-32b")
LLAMA_MODEL = os.getenv("LLAMA_MODEL", "llama-3.3-70b-versatile")
SQL_GENERATOR_MODEL = os.getenv("SQL_GENERATOR_MODEL", LLAMA_MODEL)

PARSE_INTENT_USE_INSTRUCTOR = os.getenv("PARSE_INTENT_USE_INSTRUCTOR", "1") == "1"

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "motia/data/analytics.duckdb")
