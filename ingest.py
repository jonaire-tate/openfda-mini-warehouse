"""
ingest.py

Phase 1 of the openFDA Drug Events Mini-Warehouse.

Pulls adverse event reports for narcolepsy medications from the openFDA API,
saves the raw JSON as a backup, and loads the records into a DuckDB table
called raw_adverse_events.
"""

import json
import time
from pathlib import Path

import requests
import duckdb


# ---------- Config ----------

# openFDA adverse events endpoint
API_URL = "https://api.fda.gov/drug/event.json"

# Narcolepsy medications (Jazz Pharmaceuticals therapeutic area)
DRUGS = ["XYREM", "XYWAV", "WAKIX", "SUNOSI"]

# Where the raw JSON backup goes
RAW_DIR = Path("raw")

# Where the DuckDB database file lives
DB_PATH = "data/warehouse.duckdb"

# Records per API call (openFDA caps this at 1000)
PAGE_SIZE = 100

# Total records to pull per drug (we'll stop when we hit this, or when the API runs out)
MAX_PER_DRUG = 500

# ---------- API fetch ----------

def fetch_drug_events(drug_name, max_records=MAX_PER_DRUG):
    """
    Pull adverse event reports from openFDA for a single drug.

    Uses pagination because the API returns a limited number of records per call.
    Returns a list of raw JSON records (each record is a dict).
    """
    all_records = []
    skip = 0

    while len(all_records) < max_records:
        params = {
            "search": f'patient.drug.medicinalproduct:"{drug_name}"',
            "limit": PAGE_SIZE,
            "skip": skip,
        }

        print(f"  Fetching {drug_name}: records {skip} to {skip + PAGE_SIZE}...")

        response = requests.get(API_URL, params=params, timeout=30)

        # openFDA returns 404 when there are no more results, which is normal
        if response.status_code == 404:
            print(f"  No more records for {drug_name}.")
            break

        response.raise_for_status()  # Raise an error if anything else went wrong
        data = response.json()

        results = data.get("results", [])
        if not results:
            break

        all_records.extend(results)
        skip += PAGE_SIZE

        # Be polite to the API
        time.sleep(0.5)

    # Trim to max in case the last page put us over
    return all_records[:max_records]

# ---------- Save raw JSON backup ----------

def save_raw_backup(drug_name, records):
    """
    Save the list of records to raw/<drug_name>.json.

    This is our immutable raw layer. If anything goes wrong downstream,
    we can re-read from these files instead of hitting the API again.
    """
    RAW_DIR.mkdir(exist_ok=True)
    output_path = RAW_DIR / f"{drug_name}.json"

    with open(output_path, "w") as f:
        json.dump(records, f, indent=2)

    print(f"  Saved {len(records)} records to {output_path}")

# ---------- Load into DuckDB ----------

def load_into_duckdb(all_records):
    """
    Load the combined list of records into DuckDB as raw_adverse_events.

    The raw layer stores each record as a JSON string in a single column.
    We deliberately don't flatten or parse here. That's the warehouse
    layer's job in Phase 2.
    """
    # Make sure the data folder exists
    Path("data").mkdir(exist_ok=True)

    con = duckdb.connect(DB_PATH)

    # Drop the table if it exists, so re-running the script starts clean
    con.execute("DROP TABLE IF EXISTS raw_adverse_events")

    # Create the raw table. One column: the full JSON record as a string.
    con.execute("""
        CREATE TABLE raw_adverse_events (
            record_json VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert each record as a JSON string
    rows = [(json.dumps(r),) for r in all_records]
    con.executemany(
        "INSERT INTO raw_adverse_events (record_json) VALUES (?)",
        rows
    )

    # Verify the load
    count = con.execute("SELECT COUNT(*) FROM raw_adverse_events").fetchone()[0]
    print(f"\nLoaded {count} records into raw_adverse_events.")

    con.close()

# ---------- Main ----------

def main():
    """
    Run the full Phase 1 ingestion:
    1. Fetch adverse event records for each drug from openFDA
    2. Save raw JSON backups
    3. Load everything into DuckDB as raw_adverse_events
    """
    print("Starting openFDA ingestion...\n")

    all_records = []

    for drug in DRUGS:
        print(f"Processing {drug}")
        records = fetch_drug_events(drug)

        if not records:
            print(f"  No records returned for {drug}. Skipping.\n")
            continue

        save_raw_backup(drug, records)
        all_records.extend(records)
        print(f"  Total records collected so far: {len(all_records)}\n")

    if not all_records:
        print("No records collected. Exiting without touching the database.")
        return

    load_into_duckdb(all_records)
    print("\nPhase 1 ingestion complete.")


if __name__ == "__main__":
    main()