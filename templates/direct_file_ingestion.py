"""Template for Archetype A: Direct Static / Bulk File Download to BigQuery.

Use for: CISA KEV, OFAC SDN, JODI, IMF PortWatch, FAO Food Price Index.
"""

import os
import logging
from datetime import datetime
import functions_framework
import requests
from google.cloud import storage, bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE = os.environ.get("BQ_TABLE")
SOURCE_URL = os.environ.get("SOURCE_URL")


@functions_framework.http
def ingest_direct_file(request):
    """Downloads a public bulk file, streams to GCS, and loads into BigQuery."""
    try:
        execution_date = datetime.utcnow().strftime("%Y-%m-%d")
        file_ext = SOURCE_URL.split("?")[0].split(".")[-1].lower()
        if file_ext not in ["csv", "json", "parquet", "zip"]:
            file_ext = "csv"

        destination_blob_name = f"raw/{BQ_TABLE}/dt={execution_date}/data.{file_ext}"

        # 1. Stream from URL to GCS (memory-safe chunking)
        logger.info(f"Downloading from {SOURCE_URL} to gs://{GCS_BUCKET}/{destination_blob_name}")
        storage_client = storage.Client(project=GCP_PROJECT)
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(destination_blob_name)

        with requests.get(SOURCE_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with blob.open("wb") as blob_writer:
                for chunk in r.iter_content(chunk_size=1024 * 1024 * 8):  # 8MB chunks
                    if chunk:
                        blob_writer.write(chunk)

        gcs_uri = f"gs://{GCS_BUCKET}/{destination_blob_name}"
        logger.info(f"Uploaded successfully to {gcs_uri}")

        # 2. Trigger BigQuery Load Job
        bq_client = bigquery.Client(project=GCP_PROJECT)
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=(
                bigquery.SourceFormat.CSV
                if file_ext == "csv"
                else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            ),
            # Use WRITE_TRUNCATE for daily reference snapshot lists (e.g. OFAC/CISA),
            # or WRITE_APPEND for historical event time series
            write_disposition=os.environ.get("BQ_WRITE_DISPOSITION", bigquery.WriteDisposition.WRITE_TRUNCATE),
            ignore_unknown_values=True,
        )
        if file_ext == "csv":
            job_config.skip_leading_rows = 1
            job_config.allow_quoted_newlines = True

        load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        logger.info(f"Starting BigQuery load job {load_job.job_id} into {table_ref}")
        load_job.result()  # Wait for job completion

        loaded_table = bq_client.get_table(table_ref)
        logger.info(f"Loaded {loaded_table.num_rows} rows into {table_ref}")

        return (
            {
                "status": "success",
                "table": table_ref,
                "rows": loaded_table.num_rows,
                "gcs_uri": gcs_uri,
            },
            200,
        )

    except Exception as e:
        logger.exception("Ingestion failed")
        return {"status": "error", "message": str(e)}, 500
