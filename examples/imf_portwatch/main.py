"""IMF PortWatch Ingestion Pipeline for Google BigQuery.

Extracts maritime trade data (Daily Chokepoints and Daily Port Activity)
from the official IMF PortWatch ArcGIS FeatureServer REST API, stages NDJSON
to Google Cloud Storage, and loads into partitioned BigQuery tables.
"""

import os
import json
import logging
from datetime import datetime, timedelta
try:
    import functions_framework
except ImportError:
    class _MockFF:
        @staticmethod
        def http(func):
            return func
    functions_framework = _MockFF()

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from google.cloud import storage, bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Configuration
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
BQ_DATASET = os.environ.get("BQ_DATASET", "imf_portwatch")

# IMF PortWatch ArcGIS FeatureServer Endpoints
ENDPOINTS = {
    "chokepoints": {
        "url": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0/query",
        "table": os.environ.get("BQ_TABLE_CHOKEPOINTS", "daily_chokepoints"),
        "cluster_fields": ["portname", "portid"],
    },
    "ports": {
        "url": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Ports_Data/FeatureServer/0/query",
        "table": os.environ.get("BQ_TABLE_PORTS", "daily_ports"),
        "cluster_fields": ["country", "ISO3", "portname"],
    },
}


def get_retry_session(retries: int = 5, backoff_factor: float = 1.5) -> requests.Session:
    """Configures an HTTP session with exponential backoff retries."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_layer_records(
    session: requests.Session,
    endpoint_url: str,
    where_clause: str = "1=1",
    page_size: int = 2000,
):
    """Paginates through an ArcGIS FeatureServer layer using resultOffset."""
    offset = 0
    while True:
        params = {
            "where": where_clause,
            "outFields": "*",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }
        logger.info(f"Querying {endpoint_url} (offset={offset}, limit={page_size})...")
        resp = session.get(endpoint_url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            attrs = feat.get("attributes", {})
            yield attrs

        # ArcGIS returns exceededTransferLimit: true if more records exist
        if not data.get("exceededTransferLimit", False) and len(features) < page_size:
            break

        offset += len(features)


def stage_and_load(
    session: requests.Session,
    layer_key: str,
    where_clause: str,
    execution_date: str,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> dict:
    """Extracts, stages to GCS, and executes a BigQuery LoadJob for a specific layer."""
    config = ENDPOINTS[layer_key]
    table_name = config["table"]
    gcs_blob_name = f"raw/{table_name}/dt={execution_date}/data.jsonl"
    gcs_uri = f"gs://{GCS_BUCKET}/{gcs_blob_name}"

    storage_client = storage.Client(project=GCP_PROJECT)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_blob_name)

    # 1. Stream records into GCS as Newline-Delimited JSON (NDJSON)
    record_count = 0
    now_iso = datetime.utcnow().isoformat()

    logger.info(f"Starting extraction for {layer_key} into {gcs_uri}...")
    with blob.open("w") as writer:
        for record in fetch_layer_records(session, config["url"], where_clause=where_clause):
            record["_ingested_at"] = now_iso
            writer.write(json.dumps(record) + "\n")
            record_count += 1

    logger.info(f"Staged {record_count} records to {gcs_uri}")

    if record_count == 0:
        return {"layer": layer_key, "records": 0, "status": "no_data"}

    # 2. Trigger BigQuery LoadJob
    bq_client = bigquery.Client(project=GCP_PROJECT)
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        ),
        clustering_fields=config["cluster_fields"],
        ignore_unknown_values=True,
    )

    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    logger.info(f"Triggered BigQuery LoadJob {load_job.job_id} for {table_ref}...")
    load_job.result()  # Wait for load job to complete

    # 3. Idempotent Deduplication: Ensure unique (date, portid) keeping latest _ingested_at
    if write_disposition == bigquery.WriteDisposition.WRITE_APPEND:
        dedup_sql = f"""
        DELETE FROM `{table_ref}` t
        WHERE STRUCT(t.date, t.portid, t._ingested_at) IN (
            SELECT AS STRUCT date, portid, _ingested_at
            FROM (
                SELECT
                    date,
                    portid,
                    _ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY date, portid
                        ORDER BY _ingested_at DESC, ObjectId DESC
                    ) AS rn
                FROM `{table_ref}`
            )
            WHERE rn > 1
        )
        """
        logger.info(f"Running idempotent deduplication on {table_ref}...")
        dedup_job = bq_client.query(dedup_sql)
        dedup_job.result()

    return {
        "layer": layer_key,
        "table": table_ref,
        "records_loaded": record_count,
        "gcs_uri": gcs_uri,
        "status": "success",
    }


@functions_framework.http
def ingest_portwatch(request):
    """Cloud Function entrypoint.

    Query parameters:
    - target: 'chokepoints' | 'ports' | 'all' (default: 'all')
    - days: number of trailing days to fetch (e.g. '7' for incremental; 'all' for full history)
    """
    try:
        request_args = request.args or {}
        target = request_args.get("target", "all").lower()
        days_arg = request_args.get("days", "14")  # Default to last 14 days for weekly run
        write_disp = request_args.get("mode", "WRITE_APPEND").upper()

        execution_date = datetime.utcnow().strftime("%Y-%m-%d")

        if days_arg.lower() == "all":
            where_clause = "1=1"
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if write_disp == "WRITE_TRUNCATE" else bigquery.WriteDisposition.WRITE_APPEND
        else:
            cutoff = (datetime.utcnow() - timedelta(days=int(days_arg))).strftime("%Y-%m-%d")
            where_clause = f"date >= '{cutoff}'"
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        session = get_retry_session()
        results = []

        layers_to_run = ["chokepoints", "ports"] if target == "all" else [target]

        for layer in layers_to_run:
            if layer not in ENDPOINTS:
                continue
            res = stage_and_load(
                session=session,
                layer_key=layer,
                where_clause=where_clause,
                execution_date=execution_date,
                write_disposition=write_disposition,
            )
            results.append(res)

        return ({"status": "completed", "results": results}, 200)

    except Exception as e:
        logger.exception("Ingestion failed")
        return ({"status": "error", "message": str(e)}, 500)
