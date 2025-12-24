from pathlib import Path
import pandas as pd

INP = Path("data/processed/sales.parquet")

OUT_OVERALL = Path("data/processed/metrics_overall_monthly.parquet")
OUT_ZIP = Path("data/processed/metrics_zip_monthly.parquet")
OUT_PPSF_OVERALL = Path("data/processed/metrics_ppsf_monthly.parquet")
OUT_PPSF_ZIP = Path("data/processed/metrics_ppsf_zip_monthly.parquet")

ROLL_WINDOWS = [3, 6, 12]


def add_rolling_median(group: pd.DataFrame, col: str) -> pd.DataFrame:
    """Add rolling median columns for a metric at the monthly grain."""
    group = group.sort_values("month")
    for w in ROLL_WINDOWS:
        group[f"{col}_rolling_{w}m"] = group[col].rolling(window=w, min_periods=1).median()
    return group


def ensure_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def main():
    OUT_OVERALL.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(INP)

    # ---- Types / canonical month bucket ----
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df = ensure_numeric(df, ["sale_price", "gross_sqft"])

    df = df.dropna(subset=["sale_date", "sale_price"])
    df = df[df["sale_price"] > 0]

    df["month"] = df["sale_date"].dt.to_period("M").dt.to_timestamp()

    # =========================
    # 1) PRICE METRICS (overall)
    # =========================
    overall = (
        df.groupby("month")
        .agg(
            sales_count=("sale_price", "size"),
            median_price=("sale_price", "median"),
        )
        .reset_index()
    )
    overall = add_rolling_median(overall, "median_price")
    overall.to_parquet(OUT_OVERALL, index=False)

    # ======================
    # 2) PRICE METRICS (ZIP)
    # ======================
    if "zip_code" in df.columns:
        zip_monthly = (
            df.groupby(["zip_code", "month"])
            .agg(
                sales_count=("sale_price", "size"),
                median_price=("sale_price", "median"),
            )
            .reset_index()
        )
        zip_monthly = zip_monthly.groupby("zip_code", group_keys=False).apply(
            lambda g: add_rolling_median(g, "median_price")
        )
        zip_monthly.to_parquet(OUT_ZIP, index=False)

    # ==========================
    # 3) PPSF METRICS (coverage)
    # ==========================
    # PPSF only where gross_sqft is present and positive.
    has_sqft = "gross_sqft" in df.columns
    if has_sqft:
        df_ppsf = df[(df["gross_sqft"] > 0)].copy()
        df_ppsf["ppsf"] = df_ppsf["sale_price"] / df_ppsf["gross_sqft"]

        # Overall PPSF monthly + coverage
        ppsf_overall = (
            df_ppsf.groupby("month")
            .agg(
                ppsf_sales_count=("ppsf", "size"),
                median_ppsf=("ppsf", "median"),
            )
            .reset_index()
        )

        total_sales = df.groupby("month").size().reset_index(name="total_sales")
        ppsf_overall = ppsf_overall.merge(total_sales, on="month", how="left")
        ppsf_overall["ppsf_coverage"] = ppsf_overall["ppsf_sales_count"] / ppsf_overall["total_sales"]

        ppsf_overall = add_rolling_median(ppsf_overall, "median_ppsf")
        ppsf_overall.to_parquet(OUT_PPSF_OVERALL, index=False)

        # ZIP PPSF monthly + coverage
        if "zip_code" in df.columns:
            ppsf_zip = (
                df_ppsf.groupby(["zip_code", "month"])
                .agg(
                    ppsf_sales_count=("ppsf", "size"),
                    median_ppsf=("ppsf", "median"),
                )
                .reset_index()
            )

            # Add total sales per zip-month to compute coverage
            total_zip = (
                df.groupby(["zip_code", "month"])
                .size()
                .reset_index(name="total_sales")
            )

            ppsf_zip = ppsf_zip.merge(total_zip, on=["zip_code", "month"], how="left")
            ppsf_zip["ppsf_coverage"] = ppsf_zip["ppsf_sales_count"] / ppsf_zip["total_sales"]

            ppsf_zip = ppsf_zip.groupby("zip_code", group_keys=False).apply(
                lambda g: add_rolling_median(g, "median_ppsf")
            )
            ppsf_zip.to_parquet(OUT_PPSF_ZIP, index=False)

    # ---- Console output ----
    wrote = ["metrics_overall_monthly.parquet"]
    if "zip_code" in df.columns:
        wrote.append("metrics_zip_monthly.parquet")
    if has_sqft:
        wrote.append("metrics_ppsf_monthly.parquet")
        if "zip_code" in df.columns:
            wrote.append("metrics_ppsf_zip_monthly.parquet")

    print("Wrote:", ", ".join(wrote))


if __name__ == "__main__":
    main()
