from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

OVERALL = Path("data/processed/metrics_overall_monthly.parquet")
ZIP = Path("data/processed/metrics_zip_monthly.parquet")
PPSF_OVERALL = Path("data/processed/metrics_ppsf_monthly.parquet")
PPSF_ZIP = Path("data/processed/metrics_ppsf_zip_monthly.parquet")

st.set_page_config(page_title="NYC Sales Tracker", layout="wide")
st.title("NYC Sales Tracker")

if not OVERALL.exists():
    st.error("Missing metrics. Run: python src/metrics.py")
    st.stop()

tab_overall, tab_zip = st.tabs(["Overall", "By ZIP"])


def line_chart(df, x, y, color=None):
    fig = px.line(df, x=x, y=y, color=color)
    st.plotly_chart(fig, use_container_width=True)


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
        st.subheader("PPSF (Monthly Median) with Coverage Gating")

        threshold = st.slider("Minimum PPSF coverage", 0.0, 1.0, 0.60, 0.05)
        view = ppsf[ppsf["ppsf_coverage"] >= threshold].copy()

        st.caption("Coverage = share of monthly sales with gross_sqft > 0. PPSF is only shown when coverage meets your threshold.")
        line_chart(view, "month", "median_ppsf")

        st.subheader("PPSF Coverage Over Time")
        line_chart(ppsf, "month", "ppsf_coverage")


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
        st.subheader("PPSF by ZIP (Monthly Median) with Coverage Gating")
        threshold = st.slider("Minimum PPSF coverage (ZIP)", 0.0, 1.0, 0.60, 0.05, key="zip_ppsf_cov")
        ppsf_view = ppsf_view[ppsf_view["ppsf_coverage"] >= threshold]

        st.caption("Coverage = share of zip-month sales with gross_sqft > 0.")
        line_chart(ppsf_view, "month", "median_ppsf", color="zip_code")

