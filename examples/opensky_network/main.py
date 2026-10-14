"""Cloud Function: Ingest OpenSky Network (ADS-B) Aircraft States into BigQuery.

Pipeline Workflow:
1. Calls OpenSky REST API (/api/states/all) with fast connect timeout & optional auth.
   Falls back automatically to OpenData ADS-B (opendata.adsb.fi) across global supply-chain
   corridors when OpenSky blocks cloud provider egress IPs.
2. Normalizes raw aircraft state vectors into structured records.
3. Stream-stages Newline-Delimited JSON (NDJSON) to Google Cloud Storage (GCS).
4. Executes native BigQuery LoadJob (day-partitioned on snapshot_date, clustered on
   origin_country, callsign, icao24) followed by idempotent deduplication.
"""

import json
import logging
import os
import time
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
ADSB_FI_API_BASE = "https://opendata.adsb.fi/api/v2"
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

# Global logistics hub coverage points (lat, lon, dist_nm) for cloud fallback when OpenSky blocks GCP egress IPs
GLOBAL_ADSB_HUBS = [
    (40.0, -75.0, 250),   # US East Coast / Transatlantic Hub
    (38.0, -90.0, 250),   # US Midwest / Memphis & Louisville Cargo Hubs (FDX/UPS)
    (34.0, -118.0, 250),  # US West Coast / LAX & Ontario Cargo Hubs
    (61.2, -150.0, 250),  # Anchorage Air Cargo Hub
    (50.0, 8.5, 250),     # Central Europe / Frankfurt, Liege, Luxembourg, Amsterdam
    (51.5, -0.5, 250),    # UK / Western Europe
    (25.2, 51.5, 250),    # Persian Gulf / Doha & Dubai Air Cargo Hubs
    (22.0, 38.0, 250),    # Red Sea Corridor
    (1.3, 103.8, 250),    # Singapore / Strait of Malacca
    (22.3, 114.0, 250),   # Hong Kong / Pearl River Delta Cargo Hub
    (35.6, 139.7, 250),   # Tokyo / East Asia Hub
    (9.0, -79.5, 250),    # Panama Canal Corridor
]

# ICAO24 hex prefix -> Country mapping for ADS-B fallback normalization
ICAO_HEX_COUNTRY_MAP = {
    "a": "United States",
    "c0": "Canada", "c1": "Canada", "c2": "Canada", "c3": "Canada",
    "40": "United Kingdom", "41": "United Kingdom", "42": "United Kingdom", "43": "United Kingdom",
    "4c": "Ireland",
    "3c": "Germany", "3d": "Germany",
    "38": "France", "39": "France", "3a": "France", "3b": "France",
    "34": "Spain",
    "30": "Italy", "31": "Italy", "32": "Italy", "33": "Italy",
    "44": "Austria",
    "48": "Kingdom of the Netherlands",
    "49": "Portugal",
    "4a": "Sweden",
    "4b": "Turkey",
    "4d": "Malta",
    "e4": "Brazil",
    "78": "China", "79": "China", "7a": "China",
    "0d": "Mexico",
    "80": "India",
    "89": "United Arab Emirates",
    "06": "Qatar",
    "71": "Republic of Korea",
    "7c": "Australia",
    "84": "Japan", "85": "Japan", "86": "Japan",
    "76": "Singapore",
}


def get_retry_session() -> requests.Session:
    """Configures a requests Session with fast failure on TCP connect timeouts."""
    session = requests.Session()
    retries = Retry(
        total=2,
        connect=1,
        read=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "SupplyChainRiskPipeline/1.0"})
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


def infer_country_from_hex(icao24: str) -> str:
    """Infers origin_country from ICAO 24-bit hex prefix when not provided by fallback API."""
    if not icao24:
        return "Unknown"
    h = icao24.lower().strip()
    if h[:2] in ICAO_HEX_COUNTRY_MAP:
        return ICAO_HEX_COUNTRY_MAP[h[:2]]
    if h[:1] in ICAO_HEX_COUNTRY_MAP:
        return ICAO_HEX_COUNTRY_MAP[h[:1]]
    return "Other"


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


def transform_adsb_fi_record(ac: dict, snapshot_time_dt: datetime, server_epoch: int) -> dict:
    """Transforms an opendata.adsb.fi v2 aircraft object into the OpenSky BigQuery schema."""
    icao24 = str(ac.get("hex", "")).strip().lower() or None
    callsign = str(ac.get("flight", "")).strip() or None
    alt_baro_raw = ac.get("alt_baro")
    on_ground = True if alt_baro_raw == "ground" else False
    baro_alt_m = (
        0.0
        if alt_baro_raw == "ground"
        else (round(float(alt_baro_raw) * 0.3048, 2) if isinstance(alt_baro_raw, (int, float)) else None)
    )
    alt_geom_raw = ac.get("alt_geom")
    geo_alt_m = round(float(alt_geom_raw) * 0.3048, 2) if isinstance(alt_geom_raw, (int, float)) else None
    gs_knots = ac.get("gs")
    velocity_mps = round(float(gs_knots) * 0.514444, 2) if isinstance(gs_knots, (int, float)) else None
    baro_rate_fpm = ac.get("baro_rate")
    vertical_rate_mps = (
        round(float(baro_rate_fpm) * 0.00508, 2) if isinstance(baro_rate_fpm, (int, float)) else None
    )
    seen_pos = ac.get("seen_pos")
    time_position = (
        datetime.fromtimestamp(server_epoch - float(seen_pos), tz=timezone.utc).isoformat()
        if isinstance(seen_pos, (int, float))
        else snapshot_time_dt.isoformat()
    )
    seen = ac.get("seen")
    last_contact = (
        datetime.fromtimestamp(server_epoch - float(seen), tz=timezone.utc).isoformat()
        if isinstance(seen, (int, float))
        else snapshot_time_dt.isoformat()
    )

    return {
        "snapshot_time": snapshot_time_dt.isoformat(),
        "snapshot_date": snapshot_time_dt.strftime("%Y-%m-%d"),
        "icao24": icao24,
        "callsign": callsign,
        "origin_country": infer_country_from_hex(icao24),
        "time_position": time_position,
        "last_contact": last_contact,
        "longitude": float(ac["lon"]) if ac.get("lon") is not None else None,
        "latitude": float(ac["lat"]) if ac.get("lat") is not None else None,
        "baro_altitude_m": baro_alt_m,
        "on_ground": on_ground,
        "velocity_mps": velocity_mps,
        "true_track": float(ac["track"]) if isinstance(ac.get("track"), (int, float)) else None,
        "vertical_rate_mps": vertical_rate_mps,
        "geo_altitude_m": geo_alt_m,
        "squawk": str(ac["squawk"]).strip() if ac.get("squawk") else None,
        "spi": bool(ac.get("spi", 0)),
        "position_source": 0,
    }


def fetch_fallback_adsb_states(session: requests.Session, params: dict = None) -> tuple[list[dict], int, datetime]:
    """Fetches live ADS-B aircraft states from opendata.adsb.fi when OpenSky blocks cloud egress IPs."""
    server_time = int(datetime.now(timezone.utc).timestamp())
    snapshot_dt = datetime.fromtimestamp(server_time, tz=timezone.utc)
    seen_aircraft = {}

    for lat, lon, dist in GLOBAL_ADSB_HUBS:
        url = f"{ADSB_FI_API_BASE}/lat/{lat}/lon/{lon}/dist/{dist}"
        try:
            resp = session.get(url, timeout=(8, 20))
            if resp.status_code == 200:
                data = resp.json()
                aircraft_list = data.get("ac") or data.get("aircraft") or []
                for ac in aircraft_list:
                    hex_code = str(ac.get("hex", "")).strip().lower()
                    if not hex_code:
                        continue
                    if params and "lamin" in params:
                        ac_lat, ac_lon = ac.get("lat"), ac.get("lon")
                        if ac_lat is None or ac_lon is None:
                            continue
                        if not (
                            params["lamin"] <= float(ac_lat) <= params["lamax"]
                            and params["lomin"] <= float(ac_lon) <= params["lomax"]
                        ):
                            continue
                    seen_aircraft[hex_code] = transform_adsb_fi_record(ac, snapshot_dt, server_time)
            time.sleep(0.35)
        except Exception as exc:
            logger.warning(f"Fallback hub ({lat}, {lon}) fetch warning: {exc}")

    records = list(seen_aircraft.values())
    logger.info(f"Fallback ADS-B API retrieved {len(records)} unique aircraft states at {snapshot_dt.isoformat()}.")
    return records, server_time, snapshot_dt


def fetch_and_stage_states(
    session: requests.Session,
    bucket_name: str,
    params: dict = None,
    auth: HTTPBasicAuth = None,
) -> tuple[str, int, datetime]:
    """Fetches states from OpenSky API (with cloud ADS-B fallback) and streams NDJSON into GCS."""
    records = []
    server_time = int(datetime.now(timezone.utc).timestamp())
    snapshot_dt = datetime.fromtimestamp(server_time, tz=timezone.utc)

    try:
        logger.info("Requesting live aircraft states from OpenSky Network...")
        resp = session.get(OPENSKY_API_URL, params=params, auth=auth, timeout=(8, 45))
        resp.raise_for_status()

        payload = resp.json()
        server_time = payload.get("time", server_time)
        snapshot_dt = datetime.fromtimestamp(server_time, tz=timezone.utc)
        raw_states = payload.get("states") or []
        logger.info(f"Received {len(raw_states)} aircraft states from OpenSky at {snapshot_dt.isoformat()}.")
        for raw_state in raw_states:
            rec = transform_state_vector(raw_state, snapshot_dt)
            if rec.get("icao24"):
                records.append(rec)
    except Exception as exc:
        logger.warning(
            f"OpenSky primary endpoint unreachable from cloud IP ({exc}). "
            "Falling back to OpenData ADS-B global corridor sweep..."
        )
        records, server_time, snapshot_dt = fetch_fallback_adsb_states(session=session, params=params)

    dt_partition = snapshot_dt.strftime("%Y-%m-%d")
    blob_path = f"raw/opensky_states/dt={dt_partition}/states_{server_time}.jsonl"
    gcs_uri = f"gs://{bucket_name}/{blob_path}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path, chunk_size=GCS_CHUNK_SIZE)

    logger.info(f"Streaming {len(records)} NDJSON records directly to {gcs_uri}...")
    written_count = 0
    with blob.open("w", encoding="utf-8") as gcs_writer:
        for record in records:
            if record.get("icao24"):
                gcs_writer.write(json.dumps(record) + "\n")
                written_count += 1

    logger.info(f"Successfully staged {written_count} records to {gcs_uri}.")
    return gcs_uri, written_count, snapshot_dt


def deduplicate_table(bq_client: bigquery.Client, table_ref: str) -> None:
    """Deduplicates aircraft_states on (snapshot_date, icao24), keeping the latest snapshot_time."""
    dedup_sql = f"""
    CREATE OR REPLACE TABLE `{table_ref}`
    PARTITION BY snapshot_date
    CLUSTER BY origin_country, callsign, icao24
    AS
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, icao24
                ORDER BY
                    CASE WHEN callsign IS NOT NULL THEN 1 ELSE 0 END DESC,
                    snapshot_time DESC
            ) AS rn
        FROM `{table_ref}`
    )
    WHERE rn = 1
    """
    logger.info(f"Deduplicating {table_ref} on (snapshot_date, icao24)...")
    query_job = bq_client.query(dedup_sql)
    query_job.result()
    logger.info(f"Deduplication completed for {table_ref} (job {query_job.job_id}).")


def load_gcs_to_bigquery(
    gcs_uri: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    write_disposition: str = "WRITE_APPEND",
) -> None:
    """Executes an atomic BigQuery LoadJob from staged GCS NDJSON and deduplicates."""
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

    if write_disposition == "WRITE_APPEND":
        deduplicate_table(bq_client, table_ref)


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
