#!/usr/bin/env python3
"""Run OpenSky ingestion locally and load into BigQuery using native bq/gcloud CLI.

Uses zero external Python dependencies (pure standard library + bq CLI).
Bypasses OpenSky's cloud provider IP blocking while loading directly into GCP.
"""

import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ID = "cat-looker-core-argolis-demo"
BQ_DATASET = "opensky"
BQ_TABLE = "aircraft_states"
GCS_BUCKET = "opensky-staging-bucket-cat-demo"
OPENSKY_URL = "https://opensky-network.org/api/states/all"
LOCAL_TMP_FILE = "/tmp/opensky_states.jsonl"
SCRIPT_DIR = Path(__file__).parent.resolve()
SCHEMA_FILE = SCRIPT_DIR / "schema.json"


def transform_vector(raw, snapshot_dt):
    def clean_str(val):
        if val is not None and isinstance(val, str):
            c = val.strip()
            return c if c else None
        return None

    def parse_ts(val):
        if val and isinstance(val, (int, float)):
            return datetime.fromtimestamp(val, tz=timezone.utc).isoformat()
        return None

    return {
        "snapshot_time": snapshot_dt.isoformat(),
        "snapshot_date": snapshot_dt.strftime("%Y-%m-%d"),
        "icao24": raw[0] if len(raw) > 0 and raw[0] else None,
        "callsign": clean_str(raw[1]) if len(raw) > 1 else None,
        "origin_country": clean_str(raw[2]) if len(raw) > 2 else None,
        "time_position": parse_ts(raw[3]) if len(raw) > 3 else None,
        "last_contact": parse_ts(raw[4]) if len(raw) > 4 else None,
        "longitude": float(raw[5]) if len(raw) > 5 and raw[5] is not None else None,
        "latitude": float(raw[6]) if len(raw) > 6 and raw[6] is not None else None,
        "baro_altitude_m": float(raw[7]) if len(raw) > 7 and raw[7] is not None else None,
        "on_ground": bool(raw[8]) if len(raw) > 8 and raw[8] is not None else None,
        "velocity_mps": float(raw[9]) if len(raw) > 9 and raw[9] is not None else None,
        "true_track": float(raw[10]) if len(raw) > 10 and raw[10] is not None else None,
        "vertical_rate_mps": float(raw[11]) if len(raw) > 11 and raw[11] is not None else None,
        "geo_altitude_m": float(raw[13]) if len(raw) > 13 and raw[13] is not None else None,
        "squawk": clean_str(raw[14]) if len(raw) > 14 else None,
        "spi": bool(raw[15]) if len(raw) > 15 and raw[15] is not None else None,
        "position_source": int(raw[16]) if len(raw) > 16 and raw[16] is not None else None,
    }


def main():
    print("==================================================================")
    print(" Step 1: Fetching live aircraft states from OpenSky Network...")
    print("==================================================================")
    req = urllib.request.Request(
        OPENSKY_URL,
        headers={"User-Agent": "SupplyChainRiskResearch/1.0 (Student/Researcher)"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    server_time = data.get("time", int(datetime.now(timezone.utc).timestamp()))
    snapshot_dt = datetime.fromtimestamp(server_time, tz=timezone.utc)
    raw_states = data.get("states", [])
    print(f"Retrieved {len(raw_states)} aircraft states at {snapshot_dt.isoformat()}.")

    # Write normalized NDJSON locally
    valid_count = 0
    with open(LOCAL_TMP_FILE, "w", encoding="utf-8") as writer:
        for v in raw_states:
            rec = transform_vector(v, snapshot_dt)
            if rec.get("icao24"):
                writer.write(json.dumps(rec) + "\n")
                valid_count += 1
    print(f"Normalized and wrote {valid_count} records to {LOCAL_TMP_FILE}.")

    # Step 2: Upload to GCS
    dt_partition = snapshot_dt.strftime("%Y-%m-%d")
    gcs_uri = f"gs://{GCS_BUCKET}/raw/opensky_states/dt={dt_partition}/states_{server_time}.jsonl"
    print("\n==================================================================")
    print(f" Step 2: Uploading staged NDJSON to GCS: {gcs_uri}...")
    print("==================================================================")
    env = os.environ.copy()
    existing_metrics = env.get("CLOUDSDK_METRICS_ENVIRONMENT", "")
    if "datacloud.antigravity" not in existing_metrics:
        env["CLOUDSDK_METRICS_ENVIRONMENT"] = (
            f"{existing_metrics} datacloud.antigravity".strip()
        )
    subprocess.run(
        ["gcloud", "storage", "cp", LOCAL_TMP_FILE, gcs_uri], check=True, env=env
    )

    # Step 3: bq load into BigQuery
    table_target = f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}"
    print("\n==================================================================")
    print(f" Step 3: Executing BigQuery LoadJob into {table_target}...")
    print("==================================================================")
    bq_cmd = [
        "bq",
        "--location=US",
        "load",
        "--label=datacloud:antigravity",
        "--source_format=NEWLINE_DELIMITED_JSON",
        "--time_partitioning_field=snapshot_date",
        "--time_partitioning_type=DAY",
        "--clustering_fields=origin_country,callsign,icao24",
        table_target,
        gcs_uri,
        str(SCHEMA_FILE),
    ]
    subprocess.run(bq_cmd, check=True, env=env)

    print("\n==================================================================")
    print(f" ==> SUCCESS! BigQuery table {table_target} is populated!")
    print(f"     Total records loaded: {valid_count}")
    print("==================================================================")


if __name__ == "__main__":
    main()
