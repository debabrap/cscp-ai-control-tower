import pandas as pd
import streamlit as st
import json

st.set_page_config(page_title="CSCP AI Control Tower", layout="wide")


@st.cache_data
def load_data():
    demand = pd.read_csv("data/demo/demand_weekly.csv")
    supply = pd.read_csv("data/demo/supply_weekly.csv")
    forecast = pd.read_csv("data/demo/forecast_weekly.csv")
    risk_weekly = pd.read_csv("data/demo/shortage_risk_weekly.csv")
    risk_summary = pd.read_csv("data/demo/shortage_risk_summary.csv")

    try:
        mitigation_plan = pd.read_csv("data/demo/mitigation_plan.csv")
    except Exception:
        mitigation_plan = pd.DataFrame()

    try:
        with open("data/demo/mitigation_plan_explanations.json", "r") as f:
            explanations = json.load(f)
    except Exception:
        explanations = {}

    demand["date"] = pd.to_datetime(demand["date"])
    forecast["date"] = pd.to_datetime(forecast["date"])
    risk_weekly["date"] = pd.to_datetime(risk_weekly["date"])

    return demand, supply, forecast, risk_weekly, risk_summary, mitigation_plan, explanations


# Load all data
demand, supply, forecast, risk_weekly, risk_summary, mitigation_plan, explanations = load_data()

st.title("CSCP AI Control Tower — Hybrid AI Demo")

# ---- Alert Banner ----
critical = risk_summary[risk_summary["severity"] == "CRITICAL"]
high = risk_summary[risk_summary["severity"] == "HIGH"]

if not critical.empty:
    top = critical.sort_values("risk_score", ascending=False).iloc[0]
    st.error(
        f"CRITICAL RISK: {top['region']} / {top['component']} | "
        f"Risk={top['risk_score']:.1f} | "
        f"Stockout={top['stockout_date']}"
    )
elif not high.empty:
    top = high.sort_values("risk_score", ascending=False).iloc[0]
    st.warning(
        f"HIGH RISK: {top['region']} / {top['component']} | "
        f"Risk={top['risk_score']:.1f} | "
        f"Stockout={top['stockout_date']}"
    )
else:
    st.success("No HIGH/CRITICAL shortages detected.")

st.divider()

# ---- Filters ----
col1, col2 = st.columns(2)

region = col1.selectbox("Region", sorted(demand["region"].unique()))
component = col2.selectbox("Component", sorted(demand["component"].unique()))

st.subheader(f"{region} — {component}")

# ---- Demand History ----
d = demand[
    (demand["region"] == region) &
    (demand["component"] == component)
].sort_values("date")

st.caption("Historical Demand")
st.line_chart(d.set_index("date")["demand_units"], height=250)

# ---- Forecast ----
f = forecast[
    (forecast["region"] == region) &
    (forecast["component"] == component)
].sort_values("date")

st.caption("Forecast (Next Weeks)")
st.line_chart(f.set_index("date")[["forecast_units"]], height=250)

# ---- Risk Projection ----
rw = risk_weekly[
    (risk_weekly["region"] == region) &
    (risk_weekly["component"] == component)
].sort_values("date")

col1, col2 = st.columns(2)

with col1:
    st.caption("Projected Inventory")
    st.line_chart(rw.set_index("date")["projected_inventory_units"], height=250)

with col2:
    st.caption("Weeks of Cover")
    st.line_chart(rw.set_index("date")["weeks_of_cover"], height=250)

st.divider()

# ---- Mitigation Plan ----
st.subheader("Mitigation Plan")

if mitigation_plan.empty:
    st.info("No mitigation required.")
else:
    st.dataframe(mitigation_plan, use_container_width=True)

    # Decision card for selected region/component
    sel = mitigation_plan[
        (mitigation_plan["region"] == region) &
        (mitigation_plan["component"] == component)
    ]

    if not sel.empty:
        row = sel.iloc[0]

        st.error(
            f"DECISION REQUIRED — {row['severity']} | "
            f"Risk {float(row['risk_score']):.1f} → {float(row['risk_after']):.1f} "
            f"(↓{float(row['risk_reduction']):.1f}) | "
            f"Cost ${float(row['estimated_cost_usd']):,.0f}"
        )

        st.write("**Recommended Action:**", row["recommended_action"])

        key = f"{region}:{component}"
        if key in explanations:
            st.info(explanations[key])