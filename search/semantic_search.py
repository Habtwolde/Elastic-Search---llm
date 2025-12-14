import os
import sys
import json
import argparse
from typing import Any, Dict, List

import requests
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load .env (current directory or project root depending how you run)
load_dotenv()

# ---------- Elasticsearch ----------
ES_URL  = os.getenv("ES_URL", "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", os.getenv("ELASTIC_PASSWORD", "changeme"))

INDEX = os.getenv("ES_INDEX", "oracle_elser_index_v2")
MODEL = os.getenv("ES_MODEL", "elser-oracle")
ELSER_FIELD = os.getenv("ES_ELSER_FIELD", "ml.inference.body_expanded")

# ---------- Ollama ----------
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")

ES = Elasticsearch(
    ES_URL,
    basic_auth=(ES_USER, ES_PASS),
    request_timeout=120
)

def es_info() -> str:
    try:
        return ES.info()["version"]["number"]
    except Exception as e:
        return f"UNKNOWN (error: {e})"

def semantic_search(q: str, size: int = 5) -> List[Dict[str, Any]]:
    """
    ELSER semantic search using text_expansion against rank_features field.
    """
    body = {
        "size": size,
        "query": {
            "text_expansion": {
                ELSER_FIELD: {
                    "model_id": MODEL,
                    "model_text": q
                }
            }
        },
        "_source": ["id", "title", "body", "content", "updated_at"]
    }

    res = ES.search(index=INDEX, body=body)
    hits = res.get("hits", {}).get("hits", [])

    results: List[Dict[str, Any]] = []
    for h in hits:
        src = h.get("_source", {}) or {}
        results.append({
            "score": h.get("_score"),
            "id": src.get("id"),
            "title": src.get("title"),
            "body": src.get("body") or src.get("content"),
            "updated_at": src.get("updated_at"),
        })
    return results

def print_hits(q: str, results: List[Dict[str, Any]]) -> None:
    print(f"\nQuery: {q}")
    print(f"Hits: {len(results)}")

    for r in results:
        print("\n-------------------------")
        print("Score:", r.get("score"))
        print("ID:", r.get("id"))
        print("Title:", r.get("title"))
        print("Body:", (r.get("body") or ""))

def build_context(results: List[Dict[str, Any]], max_chars: int = 6000) -> str:
    """
    Build a compact context text for LLM grounding.
    """
    parts: List[str] = []
    for i, r in enumerate(results, start=1):
        parts.append(
            f"[Doc {i}] id={r.get('id')} | score={r.get('score')} | updated_at={r.get('updated_at')}\n"
            f"TITLE: {r.get('title')}\n"
            f"BODY: {((r.get('body') or '').strip())}\n"
        )
    ctx = "\n".join(parts).strip()
    return ctx[:max_chars]

def ollama_answer(user_question: str, context: str) -> str:
    """
    Calls Ollama /api/chat. Uses context-only instruction.
    """
    url = f"{OLLAMA_HOST.rstrip('/')}/api/chat"
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a precise assistant. Use ONLY the provided CONTEXT to answer. "
                    "If the answer is not in the context, say you don't have enough information."
                )
            },
            {
                "role": "user",
                "content": (
                    f"CONTEXT:\n{context}\n\n"
                    f"QUESTION:\n{user_question}\n\n"
                    "Return a concise answer. If multiple incidents apply, use bullet points."
                )
            }
        ],
        "stream": False
    }

    r = requests.post(url, json=payload, timeout=300)
    r.raise_for_status()
    data = r.json()
    return (data.get("message", {}) or {}).get("content", "").strip()

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ELSER semantic search (+ optional Ollama grounded answer)."
    )

    # UPDATED DEFAULT QUERY (incident-focused)
    parser.add_argument(
        "query",
        nargs="?",
        default=(
            "incident case description: service outage or system down; "
            "location and status; opened date; resolution or mitigation steps"
        ),
        help="Search query text (incident-focused)"
    )

    parser.add_argument("--size", type=int, default=5, help="Number of hits to return")
    parser.add_argument(
        "--answer",
        action="store_true",
        help="Also call Ollama to answer using the top hits as context"
    )
    parser.add_argument("--context-chars", type=int, default=6000, help="Max context length passed to LLM")
    args = parser.parse_args()

    print("ES VERSION:", es_info())
    print("INDEX:", INDEX)
    print("ELSER_MODEL:", MODEL)
    print("ELSER_FIELD:", ELSER_FIELD)
    if args.answer:
        print("OLLAMA_HOST:", OLLAMA_HOST)
        print("OLLAMA_MODEL:", OLLAMA_MODEL)

    results = semantic_search(args.query, size=args.size)
    print_hits(args.query, results)

    if args.answer:
        context = build_context(results, max_chars=args.context_chars)
        print("\n=========================")
        print("OLLAMA ANSWER (grounded)")
        print("=========================")
        try:
            ans = ollama_answer(args.query, context)
            print(ans)
        except requests.RequestException as e:
            print(f"ERROR calling Ollama: {e}")
            print("Tip: confirm Ollama is running: curl http://localhost:11434/api/tags")

if __name__ == "__main__":
    main()
