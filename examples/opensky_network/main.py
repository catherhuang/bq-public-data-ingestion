"""Cloud Function: Ingest OpenSky Network (ADS-B) Aircraft States into BigQuery.

Pipeline Workflow:
1. Calls OpenSky REST API (/api/states/all) with retry backoff & optional auth.
2. Normalizes raw 17-element aircraft arrays into structured records.
3. Stream-stages Newline-Delimited JSON (NDJSON) to Google Cloud Storage (GCS).
4. Executes native BigQuery LoadJob (day-partitioned on snapshot_date, clustered on origin_country, callsign, icao24).
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

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
from requests.auth import HTTPBasicAuth
from urllib3.util import Retry

from google.cloud import bigquery, storage

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingest_opensky")

# --- Configuration & Constants ---
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"
DEFAULT_TABLE = "aircraft_states"
GCS_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunk buffer

# Popular supply chain & logistics corridors (optional bounding boxes)
CORRIDOR_BBOXES = {
    "red_sea": {"lamin": 11.5, "lomin": 32.0, "lamax": 30.0, "lomax": 45.0},
    "panama": {"lamin": 7.0, "lomin": -83.0, "lamax": 10.5, "lomax": -77.0},
    "malacca": {"lamin": 1.0, "lomin": 95.0, "lamax": 6.0, "lomax": 105.0},
    "europe": {"lamin": 35.0, "lomin": -10.0, "lamax": 60.0, "lomax": 30.0},
    "north_america": {"lamin": 24.0, "lomin": -125.0, "lamax": 50.0, "lomax": -66.0},
}


def get_retry_session() -> requests.Session:
    """Configures a requests Session with exponential backoff on HTTP rate limits."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def load_bq_schema() -> list:
    """Loads BigQuery schema definition from adjacent schema.json file."""
    schema_path = Path(__file__).parent / "schema.json"
    if not schema_path.exists():
        logger.warning("schema.json not found, falling back to schema autodetect.")
        return None

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_json = json.load(f)

    return [bigquery.SchemaField.from_api_repr(field) for field in schema_json]


def transform_state_vector(raw_vector: list, snapshot_time_dt: datetime) -> dict:
    """Transforms a raw 17-element OpenSky state array into a typed dictionary."""
    def parse_ts(val):
        if val is not None and isinstance(val, (int, float)) and val > 0:
            return datetime.fromtimestamp(val, tz=timezone.utc).isoformat()
        return None

    def clean_str(val):
        if val is not None and isinstance(val, str):
            cleaned = val.strip()
            return cleaned if cleaned else None
        return None

    # OpenSky format specification:
    # 0: icao24, 1: callsign, 2: origin_country, 3: time_position, 4: last_contact,
    # 5: longitude, 6: latitude, 7: baro_altitude, 8: on_ground, 9: velocity,
    # 10: true_track, 11: vertical_rate, 12: sensors, 13: geo_altitude, 14: squawk,
    # 15: spi, 16: position_source
    return {
        "snapshot_time": snapshot_time_dt.isoformat(),
        "snapshot_date": snapshot_time_dt.strftime("%Y-%m-%d"),
        "icao24": raw_vector[0] if len(raw_vector) > 0 and raw_vector[0] else None,
        "callsign": clean_str(raw_vector[1]) if len(raw_vector) > 1 else None,
        "origin_country": clean_str(raw_vector[2]) if len(raw_vector) > 2 else None,
        "time_position": parse_ts(raw_vector[3]) if len(raw_vector) > 3 else None,
        "last_contact": parse_ts(raw_vector[4]) if len(raw_vector) > 4 else None,
        "longitude": float(raw_vector[5]) if len(raw_vector) > 5 and raw_vector[5] is not None else None,
        "latitude": float(raw_vector[6]) if len(raw_vector) > 6 and raw_vector[6] is not None else None,
        "baro_altitude_m": float(raw_vector[7]) if len(raw_vector) > 7 and raw_vector[7] is not None else None,
        "on_ground": bool(raw_vector[8]) if len(raw_vector) > 8 and raw_vector[8] is not None else None,
        "velocity_mps": float(raw_vector[9]) if len(raw_vector) > 9 and raw_vector[9] is not None else None,
        "true_track": float(raw_vector[10]) if len(raw_vector) > 10 and raw_vector[10] is not None else None,
        "vertical_rate_mps": float(raw_vector[11]) if len(raw_vector) > 11 and raw_vector[11] is not None else None,
        "geo_altitude_m": float(raw_vector[13]) if len(raw_vector) > 13 and raw_vector[13] is not None else None,
        "squawk": clean_str(raw_vector[14]) if len(raw_vector) > 14 else None,
        "spi": bool(raw_vector[15]) if len(raw_vector) > 15 and raw_vector[15] is not None else None,
        "position_source": int(raw_vector[16]) if len(raw_vector) > 16 and raw_vector[16] is not None else None,
    }


def fetch_and_stage_states(
    session: requests.Session,
    bucket_name: str,
    params: dict = None,
    auth: HTTPBasicAuth = None,
) -> tuple[str, int, datetime]:
    """Fetches states from OpenSky API and streams NDJSON directly into GCS."""
    logger.info("Requesting live aircraft states from OpenSky Network...")
    resp = session.get(OPENSKY_API_URL, params=params, auth=auth, timeout=60)
    resp.raise_for_status()

    payload = resp.json()
    server_time = payload.get("time", int(datetime.now(timezone.utc).timestamp()))
    snapshot_dt = datetime.fromtimestamp(server_time, tz=timezone.utc)
    raw_states = payload.get("states") or []
    total_states = len(raw_states)

    logger.info(f"Received {total_states} aircraft states at {snapshot_dt.isoformat()}.")

    # Construct partitioned GCS path: raw/opensky_states/dt=YYYY-MM-DD/states_{epoch}.jsonl
    dt_partition = snapshot_dt.strftime("%Y-%m-%d")
    blob_path = f"raw/opensky_states/dt={dt_partition}/states_{server_time}.jsonl"
    gcs_uri = f"gs://{bucket_name}/{blob_path}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path, chunk_size=GCS_CHUNK_SIZE)

    logger.info(f"Streaming NDJSON directly to {gcs_uri}...")
    written_count = 0
    with blob.open("w", encoding="utf-8") as gcs_writer:
        for raw_state in raw_states:
            record = transform_state_vector(raw_state, snapshot_dt)
            # Filter out records without valid ICAO address
            if record.get("icao24"):
                gcs_writer.write(json.dumps(record) + "\n")
                written_count += 1

    logger.info(f"Successfully staged {written_count} records to {gcs_uri}.")
    return gcs_uri, written_count, snapshot_dt


def load_gcs_to_bigquery(
    gcs_uri: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = "WRITE_APPEND",
) -> None:
    """Executes an atomic BigQuery LoadJob from staged GCS NDJSON."""
    bq_client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    schema = load_bq_schema()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="snapshot_date",
        ),
        clustering_fields=["origin_country", "callsign", "icao24"],
        ignore_unknown_values=True,
    )

    if schema:
        job_config.schema = schema
    else:
        job_config.autodetect = True

    logger.info(f"Triggering BigQuery LoadJob for {table_ref} from {gcs_uri}...")
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for completion
    logger.info(f"LoadJob {load_job.job_id} completed successfully.")


@functions_framework.http
def ingest_opensky(request):
    """Cloud Function entrypoint.

    Query parameters:
    - corridor: Optional preset bounding box ('red_sea', 'panama', 'malacca', 'europe', 'north_america', 'global')
    - lamin, lomin, lamax, lomax: Custom bounding box coordinates
    - mode: 'WRITE_APPEND' (default) or 'WRITE_TRUNCATE'
    """
    try:
        req_args = request.args or {}
        corridor = req_args.get("corridor", "global").lower()
        write_disp = req_args.get("mode", "WRITE_APPEND").upper()

        project_id = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
        bucket_name = os.environ.get("GCS_BUCKET")
        dataset_id = os.environ.get("BQ_DATASET", "opensky")
        table_id = os.environ.get("BQ_TABLE", DEFAULT_TABLE)

        # Optional OpenSky authentication from Secret Manager / env vars
        username = os.environ.get("OPENSKY_USERNAME")
        password = os.environ.get("OPENSKY_PASSWORD")
        auth = HTTPBasicAuth(username, password) if username and password else None

        if not bucket_name:
            return ({"error": "GCS_BUCKET environment variable must be set."}, 500, {"Content-Type": "application/json"})

        # Build query parameters
        params = {}
        if corridor in CORRIDOR_BBOXES:
            params.update(CORRIDOR_BBOXES[corridor])
            logger.info(f"Applying preset corridor bbox: {corridor}")
        elif "lamin" in req_args:
            params = {
                "lamin": float(req_args["lamin"]),
                "lomin": float(req_args["lomin"]),
                "lamax": float(req_args["lamax"]),
                "lomax": float(req_args["lomax"]),
            }

        session = get_retry_session()
        gcs_uri, record_count, snapshot_dt = fetch_and_stage_states(
            session=session,
            bucket_name=bucket_name,
            params=params if params else None,
            auth=auth,
        )

        load_gcs_to_bigquery(
            gcs_uri=gcs_uri,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            write_disposition=write_disp,
        )

        return (
            {
                "status": "success",
                "snapshot_time": snapshot_dt.isoformat(),
                "records_ingested": record_count,
                "gcs_uri": gcs_uri,
                "table": f"{project_id}.{dataset_id}.{table_id}",
                "corridor": corridor,
            },
            200,
            {"Content-Type": "application/json"},
        )

    except Exception as e:
        logger.exception("Pipeline execution failed.")
        return ({"status": "error", "message": str(e)}, 500, {"Content-Type": "application/json"})
