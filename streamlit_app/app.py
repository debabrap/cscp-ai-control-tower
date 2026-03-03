import pandas as pd
import streamlit as st

st.set_page_config(page_title="CSCP AI Control Tower", layout="wide")

@st.cache_data
def load_data():
    demand = pd.read_csv("data/raw/demand_weekly.csv")
    supply = pd.read_csv("data/raw/supply_weekly.csv")
    forecast = pd.read_csv("data/processed/forecast_weekly.csv")
    risk_weekly = pd.read_csv("data/processed/shortage_risk_weekly.csv")
    risk_summary = pd.read_csv("data/processed/shortage_risk_summary.csv")

    demand["date"] = pd.to_datetime(demand["date"])
    forecast["date"] = pd.to_datetime(forecast["date"])
    risk_weekly["date"] = pd.to_datetime(risk_weekly["date"])

    return demand, supply, forecast, risk_weekly, risk_summary

demand, supply, forecast, risk_weekly, risk_summary = load_data()

st.title("CSCP AI Control Tower — Shortage Risk (Synthetic Demo)")

# --- Top alerts ---
crit = risk_summary[risk_summary["severity"] == "CRITICAL"].sort_values("risk_score", ascending=False)
high = risk_summary[risk_summary["severity"] == "HIGH"].sort_values("risk_score", ascending=False)

if not crit.empty:
    top = crit.iloc[0]
    st.error(
        f"CRITICAL RISK: {top['region']} / {top['component']} | "
        f"Risk={top['risk_score']:.1f} | Stockout={top['stockout_date']} | "
        f"Action: {top['recommended_action']}"
    )
elif not high.empty:
    top = high.iloc[0]
    st.warning(
        f"HIGH RISK: {top['region']} / {top['component']} | "
        f"Risk={top['risk_score']:.1f} | Stockout={top['stockout_date']} | "
        f"Action: {top['recommended_action']}"
    )
else:
    st.success("No HIGH/CRITICAL shortages detected in the current horizon.")

st.divider()

# --- Filters ---
col1, col2, col3 = st.columns(3)
region = col1.selectbox("Region", sorted(demand["region"].unique()))
component = col2.selectbox("Component", sorted(demand["component"].unique()))
history_weeks = col3.slider("History (weeks)", 12, 156, 52)

st.subheader(f"Selected: {region} — {component}")

# --- Demand history ---
d = demand[(demand["region"] == region) & (demand["component"] == component)].sort_values("date")
d_tail = d.tail(history_weeks)

st.caption("Historical demand (synthetic)")
st.line_chart(d_tail.set_index("date")["demand_units"], height=260)

# --- Forecast ---
f = forecast[(forecast["region"] == region) & (forecast["component"] == component)].sort_values("date")
st.caption("Forecast (next weeks) with confidence bounds")
st.line_chart(f.set_index("date")[["forecast_units", "lower", "upper"]], height=260)

# --- Risk projection ---
rw = risk_weekly[(risk_weekly["region"] == region) & (risk_weekly["component"] == component)].sort_values("date")

c1, c2 = st.columns(2)
with c1:
    st.caption("Projected inventory (units)")
    st.line_chart(rw.set_index("date")["projected_inventory_units"], height=260)

with c2:
    st.caption("Weeks of cover (WOC)")
    st.line_chart(rw.set_index("date")["weeks_of_cover"], height=260)

# --- Risk summary table ---
st.subheader("Shortage Risk Summary (All Region/Components)")
order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
risk_summary["_sev_rank"] = risk_summary["severity"].map(order).fillna(9).astype(int)

st.dataframe(
    risk_summary.sort_values(["_sev_rank", "risk_score"], ascending=[True, False]).drop(columns=["_sev_rank"]),
    use_container_width=True
)

st.caption("Next step: add mitigation options (expedite / rebalance / alternate supplier) as an agentic workflow.")