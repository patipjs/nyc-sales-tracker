import os
import requests

DATASET = "usep-8jbt"  # Citywide Rolling Calendar Sales
BASE = f"https://data.cityofnewyork.us/resource/{DATASET}.json"

def main():
    headers = {}
    token = os.getenv("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    params = {"$limit": 5}
    r = requests.get(BASE, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        print("No rows returned.")
        return
    print("Sample keys:")
    print(sorted(rows[0].keys()))

if __name__ == "__main__":
    main()
