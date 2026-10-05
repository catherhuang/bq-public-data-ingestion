#!/usr/bin/env bash
set -e

# Configuration
GCP_PROJECT="${GCP_PROJECT:-your-project-id}"
REGION="${GCP_REGION:-us-central1}"
GCS_BUCKET="${GCS_BUCKET:-your-gcs-raw-bucket}"
BQ_DATASET="${BQ_DATASET:-imf_portwatch}"
FUNCTION_NAME="ingest-imf-portwatch"
SCHEDULER_JOB="trigger-${FUNCTION_NAME}"

# PortWatch refreshes every Tuesday at 9:00 AM ET (13:00 / 14:00 UTC)
# Weekly cron: Every Tuesday at 14:00 UTC
CRON_SCHEDULE="0 14 * * 2"

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

echo "==> Setting up Cloud Scheduler (Weekly on Tuesdays)..."
# Create or update Cloud Scheduler job
gcloud scheduler jobs create http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --schedule="${CRON_SCHEDULE}" \
    --time-zone="UTC" \
    --uri="${FUNCTION_URI}?target=all&days=14" \
    --http-method=POST \
    --oidc-service-account-email="${GCP_PROJECT}@appspot.gserviceaccount.com" \
    || gcloud scheduler jobs update http "${SCHEDULER_JOB}" \
    --location="${REGION}" \
    --schedule="${CRON_SCHEDULE}" \
    --time-zone="UTC" \
    --uri="${FUNCTION_URI}?target=all&days=14" \
    --http-method=POST \
    --oidc-service-account-email="${GCP_PROJECT}@appspot.gserviceaccount.com"

echo "==> Deployment Complete!"
echo "Function URI: ${FUNCTION_URI}"
