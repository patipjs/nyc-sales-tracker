from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

OVERALL = Path("data/processed/metrics_overall_monthly.parquet")
ZIP = Path("data/processed/metrics_zip_monthly.parquet")
PPSF_OVERALL = Path("data/processed/metrics_ppsf_monthly.parquet")
PPSF_ZIP = Path("data/processed/metrics_ppsf_zip_monthly.parquet")
SALES = Path("data/processed/sales.parquet")

st.set_page_config(page_title="NYC Sales Tracker", layout="wide")
st.title("NYC Sales Tracker")

if not OVERALL.exists():
    st.error("Missing metrics. Run: python src/metrics.py")
    st.stop()

tab_overall, tab_zip = st.tabs(["Overall", "By ZIP"])


def line_chart(df, x, y, color=None):
    fig = px.line(df, x=x, y=y, color=color)
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data
def load_sales():
    if not SALES.exists():
        return None
    df = pd.read_parquet(SALES)
    if "sale_date" in df.columns:
        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
    return df


with tab_overall:
    df = pd.read_parquet(OVERALL)

    st.subheader("Median Sale Price (Monthly)")
    line_chart(df, "month", "median_price")

    st.subheader("Sales Count (Monthly)")
    line_chart(df, "month", "sales_count")

    st.subheader("Rolling Medians (3/6/12m)")
    metric = st.selectbox("Metric", ["median_price"], index=0)
    roll = st.selectbox("Window", ["3", "6", "12"], index=2)
    col = f"{metric}_rolling_{roll}m"
    line_chart(df, "month", col)

    # PPSF (only if present)
    if PPSF_OVERALL.exists():
        ppsf = pd.read_parquet(PPSF_OVERALL)

        st.divider()
        st.subheader("PPSF (Monthly Median)")
        st.caption("Computed on sales with reported gross_sqft > 0.")
        line_chart(ppsf, "month", "median_ppsf")

        st.subheader("Rolling PPSF (3/6/12m)")
        ppsf_roll = st.selectbox("Window (PPSF)", ["3", "6", "12"], index=2)
        ppsf_col = f"median_ppsf_rolling_{ppsf_roll}m"
        if ppsf_col in ppsf.columns:
            line_chart(ppsf, "month", ppsf_col)
        else:
            st.info("Rolling PPSF not available.")


with tab_zip:
    if not ZIP.exists():
        st.warning("ZIP metrics not found (zip_code missing).")
        st.stop()

    dfz = pd.read_parquet(ZIP)
    zips = sorted([z for z in dfz["zip_code"].dropna().unique().tolist()])

    default_zip = ["10027"] if "10027" in zips else (zips[:1] if zips else [])
    selected = st.multiselect("ZIP code(s)", options=zips, default=default_zip)

    if not selected:
        st.info("Select at least one ZIP.")
        st.stop()

    view = dfz[dfz["zip_code"].isin(selected)].copy()

    st.subheader("Median Sale Price by ZIP (Monthly)")
    line_chart(view, "month", "median_price", color="zip_code")

    st.subheader("Sales Count by ZIP (Monthly)")
    line_chart(view, "month", "sales_count", color="zip_code")

    st.subheader("Rolling Median Sale Price by ZIP (3/6/12m)")
    roll = st.selectbox("Window (ZIP)", ["3", "6", "12"], index=2)
    col = f"median_price_rolling_{roll}m"
    line_chart(view, "month", col, color="zip_code")

    # PPSF by ZIP (only if present)
    if PPSF_ZIP.exists():
        ppsf_z = pd.read_parquet(PPSF_ZIP)
        ppsf_view = ppsf_z[ppsf_z["zip_code"].isin(selected)].copy()

        st.divider()
        st.subheader("PPSF by ZIP (Monthly Median)")
        st.caption("Computed on sales with reported gross_sqft > 0.")
        line_chart(ppsf_view, "month", "median_ppsf", color="zip_code")

        st.subheader("Rolling PPSF by ZIP (3/6/12m)")
        ppsf_roll_zip = st.selectbox("Window (PPSF ZIP)", ["3", "6", "12"], index=2)
        ppsf_col_zip = f"median_ppsf_rolling_{ppsf_roll_zip}m"
        if ppsf_col_zip in ppsf_view.columns:
            line_chart(ppsf_view, "month", ppsf_col_zip, color="zip_code")
        else:
            st.info("Rolling PPSF not available for ZIP view.")

    st.divider()
    st.subheader("Transactions for selected ZIPs")
    st.caption("Download the latest transactions filtered by the ZIPs above. Bedrooms are not available in the source data.")

    sales_df = load_sales()
    if sales_df is None:
        st.info("Sales dataset not found. Run: python src/transform.py")
    else:
        transactions = sales_df[sales_df["zip_code"].isin(selected)].copy()
        if transactions.empty:
            st.info("No transactions found for the selected ZIPs.")
        else:
            preferred_columns = [
                "sale_date",
                "sale_price",
                "address",
                "neighborhood",
                "borough",
                "gross_sqft",
                "land_sqft",
                "total_units",
                "building_class",
                "tax_class",
                "block",
                "lot",
            ]
            columns = [c for c in preferred_columns if c in transactions.columns]
            transactions = transactions[columns].sort_values("sale_date", ascending=False)

            csv_bytes = transactions.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download CSV",
                data=csv_bytes,
                file_name="sales_transactions.csv",
                mime="text/csv",
            )
            st.dataframe(transactions, use_container_width=True)
