#!/usr/bin/env bash
#
# Deployment Script for OpenSky Network (ADS-B) Ingestion Pipeline
# Deploys Cloud Function (2nd Gen) + Cloud Scheduler (Hourly Trigger)
#
set -e

# ==============================================================================
# 1. Configuration & Defaults
# ==============================================================================
PROJECT_ID="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${GCP_REGION:-us-central1}"
GCS_BUCKET="${GCS_BUCKET:-${PROJECT_ID}-raw-lake}"
BQ_DATASET="${BQ_DATASET:-opensky}"
BQ_TABLE="${BQ_TABLE:-aircraft_states}"

FUNCTION_NAME="ingest-opensky-network"
ENTRY_POINT="ingest_opensky"
SCHEDULER_JOB_NAME="trigger-${FUNCTION_NAME}"
CRON_SCHEDULE="0 * * * *"  # Runs hourly at minute 0

echo "=================================================================="
echo " Deploying OpenSky Network ADS-B Ingestion Pipeline to GCP"
echo "=================================================================="
echo " Project:       ${PROJECT_ID}"
echo " Region:        ${REGION}"
echo " GCS Bucket:    gs://${GCS_BUCKET}"
echo " BigQuery:      ${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}"
echo " Schedule:      ${CRON_SCHEDULE} (Hourly)"
echo "=================================================================="

if [ -z "${PROJECT_ID}" ]; then
    echo "ERROR: GCP_PROJECT is not set and could not be determined."
    exit 1
fi

# Ensure staging GCS bucket exists
if ! gsutil ls -b "gs://${GCS_BUCKET}" &>/dev/null; then
    echo "==> Creating GCS bucket gs://${GCS_BUCKET}..."
    gcloud storage buckets create "gs://${GCS_BUCKET}" --project="${PROJECT_ID}" --location="${REGION}" || true
fi

# Ensure BigQuery dataset exists
if ! bq show "${PROJECT_ID}:${BQ_DATASET}" &>/dev/null; then
    echo "==> Creating BigQuery dataset ${PROJECT_ID}:${BQ_DATASET}..."
    bq --location="${REGION}" mk --dataset "${PROJECT_ID}:${BQ_DATASET}" || true
fi

# ==============================================================================
# 2. Deploy Cloud Function (2nd Gen)
# ==============================================================================
echo "==> Deploying Cloud Function (2nd Gen): ${FUNCTION_NAME}..."

gcloud functions deploy "${FUNCTION_NAME}" \
    --gen2 \
    --runtime=python311 \
    --region="${REGION}" \
    --source=. \
    --entry-point="${ENTRY_POINT}" \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory=1GiB \
    --timeout=300s \
    --set-env-vars="GCP_PROJECT=${PROJECT_ID},GCS_BUCKET=${GCS_BUCKET},BQ_DATASET=${BQ_DATASET},BQ_TABLE=${BQ_TABLE}"

# Retrieve Function URI
FUNCTION_URI=$(gcloud functions describe "${FUNCTION_NAME}" --gen2 --region="${REGION}" --format='value(serviceConfig.uri)')
echo "==> Function Deployed Successfully: ${FUNCTION_URI}"

# ==============================================================================
# 3. Configure Cloud Scheduler
# ==============================================================================
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')
SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "==> Configuring Cloud Scheduler: ${SCHEDULER_JOB_NAME}..."

if gcloud scheduler jobs describe "${SCHEDULER_JOB_NAME}" --location="${REGION}" &>/dev/null; then
    echo "==> Updating existing Cloud Scheduler job..."
    gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --schedule="${CRON_SCHEDULE}" \
        --uri="${FUNCTION_URI}?corridor=global" \
        --http-method=POST \
        --oidc-service-account-email="${SERVICE_ACCOUNT}"
else
    echo "==> Creating new Cloud Scheduler job..."
    gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --schedule="${CRON_SCHEDULE}" \
        --uri="${FUNCTION_URI}?corridor=global" \
        --http-method=POST \
        --oidc-service-account-email="${SERVICE_ACCOUNT}"
fi

echo "=================================================================="
echo "==> Deployment Complete!"
echo "Function URI: ${FUNCTION_URI}"
echo ""
echo "To test trigger manually right now:"
echo "curl -X POST \"${FUNCTION_URI}?corridor=global\" -H \"Authorization: Bearer \$(gcloud auth print-identity-token)\""
echo "=================================================================="
