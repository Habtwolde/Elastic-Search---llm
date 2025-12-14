import os
import sys
import json
import time
import subprocess
from typing import Optional, Tuple

import requests


# -----------------------------
# helpers
# -----------------------------
def run(cmd: list[str]) -> tuple[int, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, shell=False)
    out = (p.stdout or "") + (p.stderr or "")
    return p.returncode, out.strip()

def heading(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

def ok(msg: str) -> None:
    print(f"[OK] {msg}")

def warn(msg: str) -> None:
    print(f"[WARN] {msg}")

def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")

def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    return "" if v is None else v

def http(method: str, url: str, auth: Optional[Tuple[str, str]] = None, payload: Optional[dict] = None, timeout: int = 20):
    fn = getattr(requests, method.lower())
    r = fn(url, auth=auth, json=payload, timeout=timeout)
    return r.status_code, r.text

def jdump(s: str, limit: int = 1600) -> str:
    try:
        return json.dumps(json.loads(s), indent=2)[:limit]
    except Exception:
        return s[:limit]

def wait_until(desc: str, fn, timeout_s: int = 300, sleep_s: int = 5) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        ok_flag, info = fn()
        if ok_flag:
            ok(f"{desc}: ready")
            return True
        print(f"[WAIT] {desc}: {info}")
        time.sleep(sleep_s)
    fail(f"{desc}: timed out after {timeout_s}s")
    return False


# -----------------------------
# core
# -----------------------------
def main() -> int:
    fix = "--fix" in sys.argv

    ES_URL = env("ES_URL", "http://localhost:9200").rstrip("/")
    ES_USER = env("ES_USER", "elastic")
    ES_PASS = env("ES_PASS", env("ELASTIC_PASSWORD", "changeme"))
    auth = (ES_USER, ES_PASS)

    INDEX = env("ES_INDEX", "oracle_elser_index")
    PIPELINE_ID = env("ES_INGEST_PIPELINE", "elser_oracle_pipeline")

    # Use correct ELSER v2 model id for ES 8.x
    ELSER_MODEL_ID = env("ELSER_MODEL_ID", ".elser_model_2")

    LS_SERVICE = env("LS_SERVICE", "ls01")  # docker compose service/container name
    ES_CONTAINER = env("ES_CONTAINER", "es01")

    heading("0) Mode")
    print("Fix mode:", "ON (--fix)" if fix else "OFF (read-only)")

    heading("1) Docker: containers & basic status")
    code, out = run(["docker", "ps", "--format", "{{.Names}}\t{{.Status}}\t{{.Ports}}"])
    if code != 0:
        fail("Docker command failed. Is Docker Desktop running?")
        print(out)
        return 2
    print(out)

    heading("2) Elasticsearch: reachability + license")
    st, bd = http("GET", f"{ES_URL}/", auth=auth)
    if st != 200:
        fail(f"Elasticsearch not reachable or auth failed (HTTP {st})")
        print(bd[:1200])
        return 3
    ok(f"Elasticsearch reachable at {ES_URL} (auth OK)")
    print(jdump(bd, 900))

    st, bd = http("GET", f"{ES_URL}/_license?pretty", auth=auth)
    print(jdump(bd, 1200))

    heading("3) ML: list models, ensure ELSER exists, download + deploy (if --fix)")
    st, bd = http("GET", f"{ES_URL}/_ml/trained_models?size=200&pretty", auth=auth)
    if st != 200:
        fail(f"Cannot list trained models (HTTP {st}).")
        print(bd[:1200])
        return 4

    models = []
    try:
        jj = json.loads(bd)
        models = [m.get("model_id") for m in jj.get("trained_model_configs", [])]
    except Exception:
        pass

    have_elser = ELSER_MODEL_ID in models
    print("Known trained models (first 40):")
    for m in models[:40]:
        print("  -", m)
    if have_elser:
        ok(f"ELSE R model found: {ELSER_MODEL_ID}")
    else:
        warn(f"ELSE R model NOT found: {ELSER_MODEL_ID}")

    if fix and not have_elser:
        # Download ELSER from Elastic model repository
        heading("3A) Download ELSER model")
        st, bd = http("POST", f"{ES_URL}/_ml/trained_models/{ELSER_MODEL_ID}/_download?pretty", auth=auth)
        print(jdump(bd, 1600))
        if st not in (200, 201):
            fail("ELSE R download request failed.")
            return 5

        def _download_ready():
            st2, bd2 = http("GET", f"{ES_URL}/_ml/trained_models/{ELSER_MODEL_ID}?pretty", auth=auth)
            if st2 != 200:
                return (False, f"HTTP {st2}")
            try:
                j2 = json.loads(bd2)
                # presence of model definition parts is enough to proceed
                return (True, "model exists")
            except Exception:
                return (False, "not json yet")

        if not wait_until("ELSE R model availability", _download_ready, timeout_s=600, sleep_s=10):
            return 6

    # Ensure deployment
    def _deploy_status():
        st2, bd2 = http("GET", f"{ES_URL}/_ml/trained_models/{ELSER_MODEL_ID}/_stats?pretty", auth=auth)
        if st2 != 200:
            return (False, f"HTTP {st2}: {bd2[:120]}")
        try:
            j2 = json.loads(bd2)
            ds = j2.get("trained_model_stats", [{}])[0].get("deployment_stats", {})
            state = ds.get("state")
            alloc = ds.get("allocation_status", {})
            # Treat STARTED as ready
            if state == "started":
                return (True, "started")
            return (False, f"state={state}, alloc={alloc}")
        except Exception:
            return (False, "could not parse stats")

    if fix:
        heading("3B) Start deployment (if not started)")
        st, bd = http(
            "POST",
            f"{ES_URL}/_ml/trained_models/{ELSER_MODEL_ID}/deployment/_start?pretty",
            auth=auth,
            payload={"number_of_allocations": 1, "threads_per_allocation": 1},
        )
        # It may return 409 if already started; that's fine.
        print(jdump(bd, 1600))
        if st not in (200, 201, 409):
            fail("Deployment start request failed.")
            return 7

        if not wait_until("ELSE R deployment", _deploy_status, timeout_s=600, sleep_s=10):
            return 8
    else:
        ok("Skipping deployment changes (read-only mode).")
        _ = _deploy_status()

    heading("4) Ingest pipeline: ensure it points to the correct model id (if --fix)")
    pipeline_payload = {
        "processors": [
            {
                "inference": {
                    "model_id": ELSER_MODEL_ID,
                    "input_output": [{"input_field": "content", "output_field": "ml.tokens"}],
                    "inference_config": {"text_expansion": {}},
                }
            }
        ]
    }

    if fix:
        st, bd = http("PUT", f"{ES_URL}/_ingest/pipeline/{PIPELINE_ID}?pretty", auth=auth, payload=pipeline_payload)
        print(jdump(bd, 1200))
        if st not in (200, 201):
            fail(f"Failed to create/update ingest pipeline {PIPELINE_ID}.")
            return 9
        ok(f"Ingest pipeline ready: {PIPELINE_ID}")
    else:
        st, bd = http("GET", f"{ES_URL}/_ingest/pipeline/{PIPELINE_ID}?pretty", auth=auth)
        if st == 200:
            ok(f"Ingest pipeline exists: {PIPELINE_ID}")
        else:
            warn(f"Ingest pipeline missing (HTTP {st}).")

    heading("5) Index mapping: ensure ml.tokens is rank_features (if --fix)")
    index_payload = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text"},
                "body": {"type": "text"},
                "content": {"type": "text"},
                "updated_at": {"type": "date"},
                "ml": {"properties": {"tokens": {"type": "rank_features"}}},
            }
        }
    }

    if fix:
        # Recreate index to ensure correct mapping (safe if you are still testing)
        st, bd = http("DELETE", f"{ES_URL}/{INDEX}?pretty", auth=auth)
        print(jdump(bd, 600))
        st, bd = http("PUT", f"{ES_URL}/{INDEX}?pretty", auth=auth, payload=index_payload)
        print(jdump(bd, 1200))
        if st not in (200, 201):
            fail(f"Failed to create index {INDEX}.")
            return 10
        ok(f"Index ready: {INDEX}")
    else:
        ok("Skipping index recreation (read-only mode).")

    heading("6) Restart Logstash (if --fix) and verify indexing")
    if fix:
        code, out = run(["docker", "compose", "restart", LS_SERVICE])
        print(out[:1200])
        if code != 0:
            warn("docker compose restart failed; trying docker restart")
            code2, out2 = run(["docker", "restart", LS_SERVICE])
            print(out2[:1200])

        time.sleep(20)

    st, bd = http("GET", f"{ES_URL}/{INDEX}/_count?pretty", auth=auth)
    print(jdump(bd, 800))

    heading("7) Recent Logstash evidence (tail)")
    code, out = run(["docker", "logs", "--tail", "160", LS_SERVICE])
    if code == 0:
        # show relevant lines
        needles = ["Could not index event", "status_exception", "pipeline", "inference", "ml", "ORA-", "SELECT"]
        lines = [ln for ln in out.splitlines() if any(n.lower() in ln.lower() for n in needles)]
        if lines:
            print("\n".join(lines[-140:]))
        else:
            print(out[-1200:])
    else:
        print(out)

    heading("DONE")
    ok("If count is still 0, paste sections 6 and 7 output only.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
