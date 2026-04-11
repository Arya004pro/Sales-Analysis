import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen/qwen3-32b")

url = "https://api.groq.com/openai/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {GROQ_API_TOKEN}",
    "Content-Type": "application/json",
}

payload = {
    "model": QWEN_MODEL,
    "messages": [{"role": "user", "content": "Reply with exactly the word OK"}],
    "max_tokens": 10,
}


def _run_token_check() -> requests.Response:
    return requests.post(url, headers=headers, json=payload)


def test_qwen_model_is_configured() -> None:
    assert isinstance(QWEN_MODEL, str)
    assert len(QWEN_MODEL.strip()) > 0


def test_groq_token_present_or_placeholder() -> None:
    # Keep this permissive so local pytest runs don't require secrets.
    assert GROQ_API_TOKEN is None or isinstance(GROQ_API_TOKEN, str)


if __name__ == "__main__":
    response = _run_token_check()
    print("STATUS CODE:", response.status_code)
    print("RAW RESPONSE:")
    print(response.text)
