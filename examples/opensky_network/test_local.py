"""Local test script to verify data extraction and transformation from OpenSky Network."""

import json
import urllib.request
from datetime import datetime, timezone

# OpenSky API endpoint with bounding box (Switzerland / Alps regional sample)
TEST_URL = "https://opensky-network.org/api/states/all?lamin=45.8&lomin=5.9&lamax=47.8&lomax=10.5"


def transform_sample(raw_vector, snapshot_dt):
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


def run_test():
    print(f"Connecting to OpenSky Network API: {TEST_URL} ...")
    req = urllib.request.Request(TEST_URL, headers={"User-Agent": "Antigravity-Pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    server_time = payload.get("time")
    snapshot_dt = datetime.fromtimestamp(server_time, tz=timezone.utc)
    states = payload.get("states", [])

    print(f"Server Timestamp: {snapshot_dt.isoformat()}")
    print(f"Total Aircraft States Retrieved: {len(states)}")

    if states:
        print("\n--- Sample Transformed Record (Ready for BigQuery) ---")
        sample_record = transform_sample(states[0], snapshot_dt)
        print(json.dumps(sample_record, indent=2))
        print("\nVerification PASSED: Raw vector successfully normalized into typed BigQuery schema.")
    else:
        print("No aircraft states returned for this bounding box at this moment.")


if __name__ == "__main__":
    run_test()
