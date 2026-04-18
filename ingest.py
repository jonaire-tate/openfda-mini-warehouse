"""
Pull adverse event reports for narcolepsy medications from openFDA,
save the raw JSON to disk, and load it into DuckDB as raw_adverse_events.
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

# Where the raw JSON backups go (immutable raw layer)
RAW_DIR = Path("raw")

# Where the DuckDB database file lives
DB_PATH = "data/warehouse.duckdb"

# openFDA caps responses at 1000 records per call; using 100 for politeness
PAGE_SIZE = 100

# Total records to pull per drug. 4 drugs * 500 = 2000 records total.
MAX_PER_DRUG = 500


# ---------- API fetch ----------

def fetch_drug_events(drug_name, max_records=MAX_PER_DRUG):
    """Fetch up to max_records adverse event reports for a single drug.

    Uses pagination via the skip parameter. Stops when max_records is hit
    or when the API runs out of results.
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

        # openFDA returns 404 when there are no more results, not an empty list
        if response.status_code == 404:
            print(f"  No more records for {drug_name}.")
            break

        response.raise_for_status()
        results = response.json().get("results", [])
        if not results:
            break

        all_records.extend(results)
        skip += PAGE_SIZE

        # Be polite to a free public API
        time.sleep(0.5)

    return all_records[:max_records]


# ---------- Raw JSON backup ----------

def save_raw_backup(drug_name, records):
    """Save records to raw/<drug>.json as an immutable backup.

    If anything breaks in the DuckDB load later, the raw JSON can be
    reloaded without hitting the API again.
    """
    RAW_DIR.mkdir(exist_ok=True)
    output_path = RAW_DIR / f"{drug_name}.json"

    with open(output_path, "w") as f:
        json.dump(records, f, indent=2)

    print(f"  Saved {len(records)} records to {output_path}")


# ---------- DuckDB load ----------

def load_into_duckdb(all_records):
    """Load records into raw_adverse_events as JSON strings.

    Schema-on-read pattern: the raw layer stores data exactly as openFDA
    returned it. Parsing and flattening happen in Phase 2 (warehouse layer).
    """
    Path("data").mkdir(exist_ok=True)
    con = duckdb.connect(DB_PATH)

    # Drop and recreate so re-running is idempotent
    con.execute("DROP TABLE IF EXISTS raw_adverse_events")
    con.execute("""
        CREATE TABLE raw_adverse_events (
            record_json VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # executemany with a parameterized insert is the safe way to bulk load
    rows = [(json.dumps(r),) for r in all_records]
    con.executemany(
        "INSERT INTO raw_adverse_events (record_json) VALUES (?)",
        rows
    )

    count = con.execute("SELECT COUNT(*) FROM raw_adverse_events").fetchone()[0]
    print(f"\nLoaded {count} records into raw_adverse_events.")
    con.close()


# ---------- Main ----------

def main():
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
    print("\nIngestion complete.")


if __name__ == "__main__":
    main()