---
name: bq-public-data-ingestion
description: >-
  Scaffolds, implements, and deploys production-grade GCP data pipelines (Cloud Functions 2nd Gen, Cloud Run Jobs, BigQuery LoadJobs)
  to ingest public and third-party datasets into BigQuery. Use when the user requests an ingestion script, Cloud Function template,
  or BigQuery pipeline for public data sources (e.g., UN Comtrade, IMF PortWatch, JODI, EIA, GIE AGSI+, NASA FIRMS, OFAC, CISA KEV, etc.).
---

# BigQuery Public Data Ingestion Skill

This skill guides the agent in generating modular, production-ready ingestion pipelines that extract public datasets, stage them in Google Cloud Storage (GCS), and load them into native BigQuery tables.

---

## 1. Workflow Overview

When asked to build an ingestion pipeline for a dataset:

1. **Consult the Catalog:**
   Look up the source in [references/data_sources_catalog.md](./references/data_sources_catalog.md) to identify:
   - Ingestion Archetype (A: Direct File, B: REST API, C: SDMX, D: Archive ZIP)
   - Source URL / Endpoint & Protocol
   - Authentication (None, API Key, Secret Manager)
   - Recommended update frequency (Cron expression)
   - Target BigQuery table schema, partitioning, and clustering strategies

2. **Select Archetype Template:**
   - **Archetype A (Static Bulk File / Direct URL):** Base code on [templates/direct_file_ingestion.py](./templates/direct_file_ingestion.py).
   - **Archetype B (Authenticated / Paginated REST API):** Base code on [templates/rest_api_ingestion.py](./templates/rest_api_ingestion.py).
   - **Archetype C (SDMX Central Bank / Macro):** Request SDMX-CSV and treat as Archetype A/B.
   - **Archetype D (Zipped Archive):** Implement streaming decompression via `zipfile` / `tarfile` without loading large files completely into memory.

3. **Generate Standard Deliverables:**
   For each pipeline, output a dedicated directory:
   - `main.py`: The entrypoint Cloud Function (`@functions_framework.http`) or Cloud Run script.
   - `requirements.txt`: Minimal, pinned Python dependencies.
   - `schema.json` (if static schema enforcement is preferred).
   - `deploy.sh`: Pre-configured `gcloud` deployment commands for the function and Cloud Scheduler.

---

## 2. Ingestion & Architecture Standards

### Google Cloud Storage (Staging)
- **Path Layout:** Always stage raw extractions using partitioned GCS paths:
  `gs://{GCS_BUCKET}/raw/{table_name}/dt={YYYY-MM-DD}/data.{ext}`
- **Memory Safety:** Never use `resp.content` or buffer multi-gigabyte payloads in memory. Always use `requests.get(..., stream=True)` and write to the GCS blob in 8MB chunks using `blob.open("wb")`.

### BigQuery Loading
- **Load Jobs Over Streaming Inserts:** Always use BigQuery `LoadJobs` (`client.load_table_from_uri`) instead of streaming inserts (`insert_rows_json`) to eliminate streaming costs and guarantee atomic loads.
- **Write Disposition:**
  - `WRITE_TRUNCATE`: For daily snapshot/reference lists where the source provides the entire current list (e.g., OFAC SDN, CISA KEV active catalog).
  - `WRITE_APPEND`: For incremental time-series (e.g., AGSI+ storage numbers, earthquake events, weather, trade flows).
- **Partitioning & Clustering:**
  - Date/Timestamp column partitioning (e.g., `_PARTITIONDATE` or source transaction/event date).
  - Cluster by primary entity IDs (e.g., country code, commodity code, airport code, CVE ID).

### Error Handling & Retries
- Use `urllib3.util.Retry` with status code retries (`429, 500, 502, 503, 504`) and exponential backoff.
- Set reasonable HTTP timeouts (e.g., 60–120s).

### Secret Management
- Do **NOT** hardcode API tokens.
- Map secrets from Secret Manager into environment variables during deployment:
  `--set-secrets 'API_KEY=projects/{PROJECT_ID}/secrets/{SECRET_NAME}:latest'`

---

## 3. Standard Deployment Commands (`deploy.sh`)

Every generated pipeline should include deployment commands matching this pattern:

```bash
#!/usr/bin/env bash
set -e

PROJECT_ID="${GCP_PROJECT:-your-project-id}"
REGION="${GCP_REGION:-us-central1}"
FUNCTION_NAME="ingest-<source_name>"
SCHEDULER_JOB_NAME="trigger-${FUNCTION_NAME}"
CRON_SCHEDULE="<CRON_FROM_CATALOG>"

# 1. Deploy Cloud Function (2nd Gen)
gcloud functions deploy "${FUNCTION_NAME}" \
    --gen2 \
    --runtime=python311 \
    --region="${REGION}" \
    --source=. \
    --entry-point="<ENTRYPOINT_NAME>" \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory=512MiB \
    --timeout=300s \
    --set-env-vars="GCP_PROJECT=${PROJECT_ID},GCS_BUCKET=my-lake-raw,BQ_DATASET=public_data,BQ_TABLE=<TABLE_NAME>"

# 2. Schedule via Cloud Scheduler
FUNCTION_URI=$(gcloud functions describe "${FUNCTION_NAME}" --gen2 --region="${REGION}" --format='value(serviceConfig.uri)')

gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
    --location="${REGION}" \
    --schedule="${CRON_SCHEDULE}" \
    --time-zone="UTC" \
    --uri="${FUNCTION_URI}" \
    --http-method=POST \
    --oidc-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com"
```
