#!/usr/bin/env bash
set -e

# Configuration
DEFAULT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "your-project-id")
GCP_PROJECT="${GCP_PROJECT:-${DEFAULT_PROJECT}}"
REGION="${GCP_REGION:-us-central1}"
GCS_BUCKET="${GCS_BUCKET:-cat_demo_imf_portwatch_bq_import}"
BQ_DATASET="${BQ_DATASET:-imf_portwatch}"
FUNCTION_NAME="ingest-imf-portwatch"
SCHEDULER_JOB="trigger-${FUNCTION_NAME}"

# IMF PortWatch publishes updates around 12:30 - 14:20 UTC (typically Wednesdays, with occasional mid-week revisions).
# Daily cron at 15:00 UTC ensures same-day ingestion with idempotent deduplication.
CRON_SCHEDULE="${CRON_SCHEDULE:-0 15 * * *}"

echo "==> Deploying Cloud Function (2nd Gen): ${FUNCTION_NAME}..."
gcloud functions deploy "${FUNCTION_NAME}" \
    --gen2 \
    --runtime=python311 \
    --region="${REGION}" \
    --source=. \
    --entry-point="ingest_portwatch" \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory=1GiB \
    --timeout=540s \
    --set-env-vars="GCP_PROJECT=${GCP_PROJECT},GCS_BUCKET=${GCS_BUCKET},BQ_DATASET=${BQ_DATASET}"

echo "==> Fetching deployed function URI..."
FUNCTION_URI=$(gcloud functions describe "${FUNCTION_NAME}" --gen2 --region="${REGION}" --format='value(serviceConfig.uri)')

echo "==> Setting up Cloud Scheduler (Daily at 15:00 UTC)..."
# Create or update Cloud Scheduler job
gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --schedule="${CRON_SCHEDULE}" \
    --time-zone="UTC" \
    --attempt-deadline=540s \
    --uri="${FUNCTION_URI}?target=all&days=14" \
    --http-method=POST \
    --oidc-service-account-email="${GCP_PROJECT}@appspot.gserviceaccount.com" \
    || gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --schedule="${CRON_SCHEDULE}" \
    --time-zone="UTC" \
    --attempt-deadline=540s \
    --uri="${FUNCTION_URI}?target=all&days=14" \
    --http-method=POST \
    --oidc-service-account-email="${GCP_PROJECT}@appspot.gserviceaccount.com"

echo "==> Deployment Complete!"
echo "Function URI: ${FUNCTION_URI}"
