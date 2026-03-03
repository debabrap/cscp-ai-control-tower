import os
import numpy as np
import pandas as pd

REGIONS = ["westus", "eastus", "northeurope"]
COMPONENTS = ["GPU", "NIC", "SWITCH"]
SUPPLIERS = ["SupplierA", "SupplierB", "SupplierC"]

def _seasonality(week_of_year: int) -> float:
    return 1.0 + 0.15 * np.sin(2 * np.pi * week_of_year / 52.0)

def _shock_factor(date: pd.Timestamp, region: str, component: str, shocks: list[dict]) -> float:
    factor = 1.0
    for s in shocks:
        if pd.Timestamp(s["start"]) <= date <= pd.Timestamp(s["end"]):
            if (s["region"] in (region, "*")) and (s["component"] in (component, "*")):
                factor *= float(s["mult"])
    return factor

def generate_synthetic(
    start="2024-01-01",
    end="2026-12-31",
    seed=7,
    out_dir="data/raw"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    os.makedirs(out_dir, exist_ok=True)

    dates = pd.date_range(start=start, end=end, freq="W-MON")

    base_demand = {"GPU": 120, "NIC": 220, "SWITCH": 90}
    region_multiplier = {"westus": 1.25, "eastus": 1.05, "northeurope": 0.85}

    shocks = [
        {"start": "2025-08-01", "end": "2025-10-15", "region": "westus", "component": "GPU", "mult": 1.35},
        {"start": "2026-02-01", "end": "2026-03-15", "region": "*", "component": "NIC", "mult": 0.85},
        {"start": "2025-12-01", "end": "2026-01-15", "region": "eastus", "component": "*", "mult": 1.20},
    ]

    demand_rows = []
    for d in dates:
        woy = int(d.isocalendar().week)
        for r in REGIONS:
            for c in COMPONENTS:
                seasonal = _seasonality(woy)
                shock = _shock_factor(d, r, c, shocks)
                noise = rng.normal(1.0, 0.08)

                level = base_demand[c] * region_multiplier[r] * seasonal * shock * noise
                units = max(0, int(round(level)))

                demand_rows.append({
                    "date": d.date().isoformat(),
                    "region": r,
                    "component": c,
                    "demand_units": units
                })

    demand_df = pd.DataFrame(demand_rows)

    supplier_lt = {
        "SupplierA": {"GPU": 10, "NIC": 7, "SWITCH": 8},
        "SupplierB": {"GPU": 12, "NIC": 6, "SWITCH": 9},
        "SupplierC": {"GPU": 14, "NIC": 8, "SWITCH": 10},
    }
    late_prob = {"SupplierA": 0.08, "SupplierB": 0.12, "SupplierC": 0.16}

    split = {
        "GPU": {"SupplierA": 0.45, "SupplierB": 0.35, "SupplierC": 0.20},
        "NIC": {"SupplierA": 0.30, "SupplierB": 0.45, "SupplierC": 0.25},
        "SWITCH": {"SupplierA": 0.25, "SupplierB": 0.35, "SupplierC": 0.40},
    }

    supply_rows = []
    demand_df["date_ts"] = pd.to_datetime(demand_df["date"])
    demand_df = demand_df.sort_values("date_ts")

    for r in REGIONS:
        for c in COMPONENTS:
            sub = demand_df[(demand_df["region"] == r) & (demand_df["component"] == c)].copy()
            sub["planned_po_units"] = (sub["demand_units"].rolling(4, min_periods=1).mean() * 1.02).round().astype(int)

            for _, row in sub.iterrows():
                order_date = row["date_ts"]
                planned_units = int(row["planned_po_units"])

                for s in SUPPLIERS:
                    s_units = int(round(planned_units * split[c][s]))
                    if s_units <= 0:
                        continue

                    lt_weeks = supplier_lt[s][c]
                    arrive_date = order_date + pd.Timedelta(weeks=lt_weeks)

                    if rng.random() < late_prob[s]:
                        arrive_date += pd.Timedelta(weeks=int(rng.integers(1, 4)))

                    if arrive_date < dates.min() or arrive_date > dates.max():
                        continue

                    supply_rows.append({
                        "order_date": order_date.date().isoformat(),
                        "arrival_date": arrive_date.date().isoformat(),
                        "region": r,
                        "component": c,
                        "supplier": s,
                        "arrival_units": s_units
                    })

    supply_df = pd.DataFrame(supply_rows)

    demand_path = os.path.join(out_dir, "demand_weekly.csv")
    supply_path = os.path.join(out_dir, "supply_weekly.csv")
    demand_df.drop(columns=["date_ts"]).to_csv(demand_path, index=False)
    supply_df.to_csv(supply_path, index=False)

    print(f"Wrote: {demand_path}")
    print(f"Wrote: {supply_path}")
    return demand_df.drop(columns=["date_ts"]), supply_df

if __name__ == "__main__":
    generate_synthetic()