# Runbook: Oracle → Elasticsearch (ELSER) → Semantic Search → Ollama (RAG)

This repository implements an end-to-end semantic search and grounded LLM answering pipeline using:

- Excel as source data
- Oracle Database as system of record
- Logstash (JDBC) for ingestion
- Elasticsearch ELSER for semantic retrieval
- Local LLM via Ollama for grounded answers

---

Prerequisites:

- Docker Desktop running
- PowerShell (Windows)
- Elasticsearch + Logstash containers defined in docker-compose.yml
- Oracle table docs with: id, title, body, updated_at
- .env file contains at least:
    - ELASTIC_USER, ELASTIC_PASSWORD
    - Oracle connection variables used by Logstash (host/port/service/user/password)

## 0) Start Elasticsearch + Logstash

From the project root:

```powershell
docker compose up -d --force-recreate
```

Confirm containers:

```powershell
docker ps
docker compose ps
```
Check Elasticsearch health (use your credentials if security is enabled):

```powershell
curl -u elastic:changeme http://localhost:9200
curl -u elastic:changeme http://localhost:9200/_cluster/health?pretty
```
## 1) Create the ELSER Inference Endpoint

This creates an inference endpoint named elser-oracle using the built-in ELSER service.

```powershell
curl -u elastic:changeme -X PUT http://localhost:9200/_inference/sparse_embedding/elser-oracle `
  -H "Content-Type: application/json" `
  -d '{
    "service": "elser",
    "service_settings": {
      "num_allocations": 1,
      "num_threads": 1
    }
  }'
```

Verify it:

```powershell
curl -u elastic:changeme http://localhost:9200/_inference/sparse_embedding/elser-oracle?pretty
```
## 2) Create the Ingest Pipeline (ELSER Enrichment)

```powershell
curl -u elastic:changeme -X PUT "http://localhost:9200/_ingest/pipeline/elser_oracle_pipeline" `
  -H "Content-Type: application/json" `
  -d '{
    "description": "ELSER enrichment (clean old expanded field first)",
    "processors": [
      { "remove": { "field": "ml.inference.body_expanded", "ignore_missing": true } },
      { "remove": { "field": "ml.inference_error",        "ignore_missing": true } },
      {
        "inference": {
          "model_id": "elser-oracle",
          "input_output": [
            { "input_field": "body", "output_field": "ml.inference.body_expanded" }
          ],
          "on_failure": [
            { "set": { "field": "ml.inference_error", "value": "{{ _ingest.on_failure_message }}" } }
          ]
        }
      }
    ]
  }'
```
Verify pipeline exists:

```powershell
curl -u elastic:changeme "http://localhost:9200/_ingest/pipeline/elser_oracle_pipeline?pretty"
```

## 3) Create the Semantic Index (V2) with Correct Mapping

```powershell 
curl -u elastic:changeme -X PUT "http://localhost:9200/oracle_elser_index_v2" `
  -H "Content-Type: application/json" `
  -d '{
    "mappings": {
      "properties": {
        "id":         { "type": "keyword" },
        "title":      { "type": "text" },
        "body":       { "type": "text" },
        "content":    { "type": "text" },
        "updated_at": { "type": "date" },
        "ml": {
          "properties": {
            "inference": {
              "properties": {
                "body_expanded": { "type": "rank_features" }
              }
            }
          }
        }
      }
    }
  }'
```
Confirm mapping:

```powershell
curl -u elastic:changeme "http://localhost:9200/oracle_elser_index_v2/_mapping?pretty"
```
## 4) Reindex Old Documents into V2 (and Apply ELSER)

If you already have data in oracle_elser_index (old index), reindex into V2 while running the ingest pipeline:


```powershell
curl -u elastic:changeme -X POST "http://localhost:9200/_reindex?pretty" `
  -H "Content-Type: application/json" `
  -d '{
    "source": { "index": "oracle_elser_index" },
    "dest":   { "index": "oracle_elser_index_v2", "pipeline": "elser_oracle_pipeline" }
  }'
```

Confirm document count:
```powershell
curl -u elastic:changeme "http://localhost:9200/oracle_elser_index_v2/_count?pretty"
```

## 5) Point Logstash to the V2 Index (Ingestion from Oracle)

After confirming the config file is correct, rebuild and restart Logstash:

```powershell
docker compose up -d --build ls01
```

## 6) Validate ELSER Output is Actually Stored

Pick a known document ID and confirm ml.inference.body_expanded exists:

```powershell
curl -u elastic:changeme "http://localhost:9200/oracle_elser_index_v2/_search?pretty" `
  -H "Content-Type: application/json" `
  -d '{
    "size": 1,
    "_source": ["id","title","ml.inference.body_expanded"],
    "query": { "term": { "id": "0.5e1" } }
  }'
```

## 7) Load Excel into Oracle

```powershell
cd search
python load_excel_to_oracle.py --file "..\incidents.xlsx"
```

## 8) Start Stack

```powershell
docker compose up -d --build
```

## 9) Upload ELSER + Pipeline

```powershell
curl -X PUT http://localhost:9200/_ml/trained_models/elser-oracle -H "Content-Type: application/json" -d @put_elser_model_2.json
curl -X PUT http://localhost:9200/_ingest/pipeline/elser_oracle_pipeline -H "Content-Type: application/json" -d @elser_oracle_pipeline.json
```

## 10) Semantic Search + Ollama

```powershell
python semantic_search.py "summarize the open incidents and their locations" --answer
```
