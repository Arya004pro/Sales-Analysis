import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

GROQ_API_TOKEN = os.getenv("GROQ_API_TOKEN")
QWEN_MODEL     = os.getenv("QWEN_MODEL", "qwen/qwen3-32b")

url = "https://api.groq.com/openai/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {GROQ_API_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "model": QWEN_MODEL,
    "messages": [
        {"role": "user", "content": "Reply with exactly the word OK"}
    ],
    "max_tokens": 10
}

response = requests.post(url, headers=headers, json=payload)

print("STATUS CODE:", response.status_code)
print("RAW RESPONSE:")
print(response.text)
