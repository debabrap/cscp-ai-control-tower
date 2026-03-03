import os
import pandas as pd
import numpy as np

DEMAND_PATH = "data/raw/demand_weekly.csv"
SUPPLY_PATH = "data/raw/supply_weekly.csv"
FORECAST_PATH = "data/processed/forecast_weekly.csv"

OUT_DIR = "data/processed"
RISK_HORIZON_WEEKS = 12
DEFAULT_STARTING_WEEKS_OF_COVER = 6


def _severity(score: float) -> str:
    if score >= 80:
        return "CRITICAL"
    if score >= 60:
        return "HIGH"
    if score >= 35:
        return "MEDIUM"
    return "LOW"


def _recommendation(severity: str) -> str:
    if severity == "CRITICAL":
        return "Expedite + Rebalance + Activate alternate supplier"
    if severity == "HIGH":
        return "Rebalance inventory + Request expedite quote"
    if severity == "MEDIUM":
        return "Monitor + Pre-approve expedite thresholds"
    return "No action (watchlist)"


def _compute_risk_score(stockout_week_idx, min_woc, pipeline_fill_ratio) -> float:
    base = float(np.clip(20 + (1.0 - pipeline_fill_ratio) * 100, 0, 100))

    if min_woc <= 1:
        base += 35
    elif min_woc <= 2:
        base += 25
    elif min_woc <= 4:
        base += 15

    if stockout_week_idx is not None:
        urgency = 40 * (1 - (stockout_week_idx / max(1, (RISK_HORIZON_WEEKS - 1))))
        base += urgency

    return float(np.clip(base, 0, 100))


def build_shortage_risk():
    os.makedirs(OUT_DIR, exist_ok=True)

    demand = pd.read_csv(DEMAND_PATH)
    supply = pd.read_csv(SUPPLY_PATH)
    forecast = pd.read_csv(FORECAST_PATH)

    demand["date"] = pd.to_datetime(demand["date"])
    supply["arrival_date"] = pd.to_datetime(supply["arrival_date"])
    forecast["date"] = pd.to_datetime(forecast["date"])

    horizon_start = forecast["date"].min()
    horizon_end = forecast["date"].max()

    # Calculate starting inventory using recent demand
    recent = demand[demand["date"] < horizon_start].copy()
    recent = recent.sort_values("date")

    recent = recent.groupby(["region", "component"]).tail(8)
    avg_weekly = (
        recent.groupby(["region", "component"])["demand_units"]
        .mean()
        .reset_index()
    )

    avg_weekly.rename(columns={"demand_units": "avg_weekly_demand"}, inplace=True)
    avg_weekly["starting_inventory_units"] = (
        avg_weekly["avg_weekly_demand"] * DEFAULT_STARTING_WEEKS_OF_COVER
    ).round().astype(int)

    # Aggregate supply into weekly buckets
    supply_h = supply[
        (supply["arrival_date"] >= horizon_start)
        & (supply["arrival_date"] <= horizon_end)
    ].copy()

    if not supply_h.empty:
        supply_h["week"] = supply_h["arrival_date"].dt.to_period("W-MON").dt.start_time

        supply_w = (
            supply_h.groupby(["week", "region", "component"])["arrival_units"]
            .sum()
            .reset_index()
        )

        supply_w.rename(
            columns={"week": "date", "arrival_units": "supply_units"},
            inplace=True,
        )

        supply_w["date"] = pd.to_datetime(supply_w["date"])
    else:
        supply_w = pd.DataFrame(
            columns=["date", "region", "component", "supply_units"]
        )

    forecast = forecast.sort_values("date")

    keys = ["region", "component"]
    combinations = forecast[keys].drop_duplicates()

    combinations = combinations.merge(
        avg_weekly[keys + ["starting_inventory_units", "avg_weekly_demand"]],
        on=keys,
        how="left",
    )

    weekly_rows = []
    summary_rows = []

    for _, combo in combinations.iterrows():
        region = combo["region"]
        component = combo["component"]
        starting_inventory = int(combo["starting_inventory_units"])
        avg_demand = float(combo["avg_weekly_demand"])

        sub_fc = forecast[
            (forecast["region"] == region)
            & (forecast["component"] == component)
        ].sort_values("date").head(RISK_HORIZON_WEEKS)

        sub_sup = supply_w[
            (supply_w["region"] == region)
            & (supply_w["component"] == component)
        ]

        supply_map = dict(
            zip(
                pd.to_datetime(sub_sup["date"]).dt.normalize(),
                sub_sup["supply_units"],
            )
        )

        inventory = starting_inventory
        min_woc = float("inf")
        stockout_week_idx = None
        stockout_date = None

        total_supply = 0
        total_demand = 0

        for idx, row in enumerate(sub_fc.itertuples(index=False)):
            date = pd.to_datetime(row.date)
            demand_units = int(row.forecast_units)
            supply_units = int(supply_map.get(date.normalize(), 0))

            total_supply += supply_units
            total_demand += demand_units

            inventory = inventory + supply_units - demand_units
            weeks_of_cover = inventory / avg_demand if avg_demand > 0 else 0

            min_woc = min(min_woc, weeks_of_cover)

            if stockout_week_idx is None and inventory <= 0:
                stockout_week_idx = idx
                stockout_date = date.date().isoformat()

            weekly_rows.append(
                {
                    "date": date.date().isoformat(),
                    "region": region,
                    "component": component,
                    "projected_inventory_units": int(inventory),
                    "weeks_of_cover": float(weeks_of_cover),
                }
            )

        pipeline_fill_ratio = (
            total_supply / total_demand if total_demand > 0 else 0
        )

        risk_score = _compute_risk_score(
            stockout_week_idx, min_woc, pipeline_fill_ratio
        )

        severity = _severity(risk_score)

        summary_rows.append(
            {
                "region": region,
                "component": component,
                "starting_inventory_units": starting_inventory,
                "total_supply_horizon": int(total_supply),
                "total_demand_horizon": int(total_demand),
                "pipeline_fill_ratio": float(pipeline_fill_ratio),
                "min_weeks_of_cover": float(min_woc),
                "stockout_date": stockout_date,
                "risk_score": float(risk_score),
                "severity": severity,
                "recommended_action": _recommendation(severity),
            }
        )

    weekly_df = pd.DataFrame(weekly_rows)
    summary_df = pd.DataFrame(summary_rows)

    weekly_df.to_csv(os.path.join(OUT_DIR, "shortage_risk_weekly.csv"), index=False)
    summary_df.to_csv(os.path.join(OUT_DIR, "shortage_risk_summary.csv"), index=False)

    print("Wrote: data/processed/shortage_risk_weekly.csv")
    print("Wrote: data/processed/shortage_risk_summary.csv")


if __name__ == "__main__":
    build_shortage_risk()