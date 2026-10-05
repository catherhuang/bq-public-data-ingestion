"""Template for Archetype B: Authenticated / Paginated REST API to BigQuery.

Use for: GIE AGSI+, EIA API v2, NASA FIRMS, GDACS, Open-Meteo.
"""

import os
import json
import logging
from datetime import datetime
import functions_framework
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from google.cloud import storage, bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE = os.environ.get("BQ_TABLE")
API_ENDPOINT = os.environ.get("API_ENDPOINT")
API_KEY = os.environ.get("API_KEY")  # Injected from Secret Manager


def get_retry_session(retries=3, backoff_factor=1.0):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


@functions_framework.http
def ingest_rest_api(request):
    """Fetches data from an external REST API, writes NDJSON to GCS, and loads into BigQuery."""
    try:
        execution_date = datetime.utcnow().strftime("%Y-%m-%d")
        destination_blob_name = f"raw/{BQ_TABLE}/dt={execution_date}/data.jsonl"

        session = get_retry_session()
        headers = {"Accept": "application/json"}
        if API_KEY:
            # Customize header/param depending on API (e.g. x-key for AGSI, or Bearer)
            headers["x-key"] = API_KEY

        logger.info(f"Fetching data from {API_ENDPOINT}")
        resp = session.get(API_ENDPOINT, headers=headers, timeout=60)
        resp.raise_for_status()
        payload = resp.json()

        # Handle various response envelopes (list vs dict with 'data' key)
        records = payload if isinstance(payload, list) else payload.get("data", payload.get("features", [payload]))

        # 1. Stream newline-delimited JSON (NDJSON) to GCS
        storage_client = storage.Client(project=GCP_PROJECT)
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(destination_blob_name)

        with blob.open("w") as writer:
            for item in records:
                # Add ingestion metadata
                item["_ingestion_timestamp"] = datetime.utcnow().isoformat()
                writer.write(json.dumps(item) + "\n")

        gcs_uri = f"gs://{GCS_BUCKET}/{destination_blob_name}"
        logger.info(f"Staged {len(records)} records to {gcs_uri}")

        # 2. Trigger BigQuery Load Job
        bq_client = bigquery.Client(project=GCP_PROJECT)
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=os.environ.get("BQ_WRITE_DISPOSITION", bigquery.WriteDisposition.WRITE_APPEND),
            ignore_unknown_values=True,
        )

        load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        logger.info(f"Starting BigQuery load job {load_job.job_id} into {table_ref}")
        load_job.result()

        return {"status": "success", "table": table_ref, "records_processed": len(records)}, 200

    except Exception as e:
        logger.exception("API Ingestion failed")
        return {"status": "error", "message": str(e)}, 500
