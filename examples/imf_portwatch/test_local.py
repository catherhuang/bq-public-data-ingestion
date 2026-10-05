"""Local test script to verify data extraction from IMF PortWatch ArcGIS REST API."""

import json
from main import get_retry_session, fetch_layer_records, ENDPOINTS


def test_extraction():
    session = get_retry_session()

    print("--- 1. Testing Chokepoints Layer (Latest 5 records) ---")
    chokepoints_cfg = ENDPOINTS["chokepoints"]
    records_chokepoints = []
    for record in fetch_layer_records(
        session, chokepoints_cfg["url"], where_clause="1=1", page_size=5
    ):
        records_chokepoints.append(record)
        if len(records_chokepoints) >= 5:
            break

    print(f"Successfully fetched {len(records_chokepoints)} chokepoint records.")
    print("Sample record:")
    print(json.dumps(records_chokepoints[0], indent=2))

    print("\n--- 2. Testing Ports Layer (Latest 5 records) ---")
    ports_cfg = ENDPOINTS["ports"]
    records_ports = []
    for record in fetch_layer_records(session, ports_cfg["url"], where_clause="1=1", page_size=5):
        records_ports.append(record)
        if len(records_ports) >= 5:
            break

    print(f"Successfully fetched {len(records_ports)} port activity records.")
    print("Sample record:")
    print(json.dumps(records_ports[0], indent=2))


if __name__ == "__main__":
    test_extraction()
