import os
import json
import requests
from pathlib import Path
from datetime import datetime

DATASET = "usep-8jbt"
BASE = f"https://data.cityofnewyork.us/resource/{DATASET}.json"

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = RAW_DIR / "state.json"
OUT_PATH = RAW_DIR / "sales_increment.json"

# Update these after running inspect_schema.py
DATE_FIELD = "sale_date"      # e.g. "sale_date"
PRICE_FIELD = "sale_price"    # e.g. "sale_price"

def load_state():
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {}

def save_state(state: dict):
    STATE_PATH.write_text(json.dumps(state, indent=2))

def fetch_page(where: str | None, limit: int, offset: int):
    headers = {}
    token = os.getenv("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    params = {
        "$limit": limit,
        "$offset": offset,
    }
    if where:
        params["$where"] = where

    r = requests.get(BASE, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def main():
    state = load_state()
    last_date = state.get("last_date")  # ISO string

    where = None
    if last_date:
        # Socrata expects ISO timestamps; this works if DATE_FIELD is an ISO datetime in the dataset
        where = f"{DATE_FIELD} > '{last_date}'"

    all_rows = []
    limit = 50000
    offset = 0

    while True:
        rows = fetch_page(where, limit, offset)
        if not rows:
            break
        all_rows.extend(rows)
        offset += limit
        # safety: avoid runaway
        if offset > 500000:
            break

    if not all_rows:
        print("No new rows.")
        return

    OUT_PATH.write_text(json.dumps(all_rows))
    # update state to max date
    dates = [r.get(DATE_FIELD) for r in all_rows if r.get(DATE_FIELD)]
    max_date = max(dates)
    state["last_date"] = max_date
    state["last_refresh_utc"] = datetime.utcnow().isoformat()
    save_state(state)

    print(f"Fetched {len(all_rows)} rows; last_date={max_date}")

if __name__ == "__main__":
    main()
