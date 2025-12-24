import json
from pathlib import Path
import pandas as pd

RAW_INC = Path("data/raw/sales_increment.json")
OUT_PARQUET = Path("data/processed/sales.parquet")
OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

# Update these after inspect_schema.py
COLUMN_MAP = {
    "sale_date": "sale_date",
    "sale_price": "sale_price",
    "zip_code": "zip_code",
    "borough": "borough",
    "neighborhood": "neighborhood",
    "address": "address",
    "gross_sqft": "gross_square_feet",
    "land_sqft": "land_square_feet",
    "building_class": "building_class_at_time_of",
    "tax_class": "tax_class_at_time_of_sale",
    "total_units": "total_units",
    "block": "block",
    "lot": "lot",
}

# Choose a dedupe strategy
DEDUPE_KEYS = ["block", "lot", "sale_date", "sale_price"]

def main():
    if not RAW_INC.exists():
        print("No increment file found; run ingest first.")
        return

    rows = json.loads(RAW_INC.read_text())
    df_new = pd.DataFrame(rows)

    # Rename to canonical names where possible
    rename = {v: k for k, v in COLUMN_MAP.items() if v in df_new.columns}
    df_new = df_new.rename(columns=rename)

    # Keep only canonical columns that exist
    keep = [c for c in COLUMN_MAP.keys() if c in df_new.columns]
    df_new = df_new[keep].copy()

    # Types
    if "sale_date" in df_new.columns:
        df_new["sale_date"] = pd.to_datetime(df_new["sale_date"], errors="coerce", utc=True).dt.tz_convert(None)
    if "sale_price" in df_new.columns:
        df_new["sale_price"] = pd.to_numeric(df_new["sale_price"], errors="coerce")

    df_new = df_new.dropna(subset=["sale_date", "sale_price"])
    df_new = df_new[df_new["sale_price"] > 0]

    # Load existing and append
    if OUT_PARQUET.exists():
        df_old = pd.read_parquet(OUT_PARQUET)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    # Dedupe (only on keys that exist)
    keys = [k for k in DEDUPE_KEYS if k in df.columns]
    if keys:
        df = df.drop_duplicates(subset=keys, keep="last")

    df.to_parquet(OUT_PARQUET, index=False)
    print(f"Wrote {len(df):,} rows to {OUT_PARQUET}")

if __name__ == "__main__":
    main()
