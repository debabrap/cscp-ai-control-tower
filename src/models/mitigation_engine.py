import os
import json
import pandas as pd
import numpy as np

RISK_SUMMARY_PATH = "data/processed/shortage_risk_summary.csv"
OUT_DIR = "data/processed"
OUT_PLAN_PATH = os.path.join(OUT_DIR, "mitigation_plan.csv")
OUT_EXPLAIN_PATH = os.path.join(OUT_DIR, "mitigation_plan_explanations.json")

# ---- Tunables you can sell as “policy knobs” ----
HIGH_RISK_THRESHOLD = 60.0
CRITICAL_RISK_THRESHOLD = 80.0

# Cost assumptions (demo-safe, but realistic shape)
UNIT_COST = {
    "GPU": 25000,      # $/unit
    "NIC": 1200,
    "SWITCH": 8000,
}

# Expediting assumptions
EXPEDITE_FRACTION = 0.20          # expedite 20% of horizon demand
EXPEDITE_WEEKS_SAVED = 2          # improves time-to-arrive by ~2 weeks
EXPEDITE_COST_MULTIPLIER = 0.08   # expedite premium = 8% of unit cost

# Rebalance assumptions
REBALANCE_FRACTION = 0.15         # move 15% of horizon demand from another region
REBALANCE_COST_PER_UNIT = {
    "GPU": 300,     # intra-network logistics / handling
    "NIC": 30,
    "SWITCH": 120,
}

# Alternate supplier assumptions
ALT_SUPPLIER_FRACTION = 0.20      # shift 20% of horizon demand to alternate supplier
ALT_SUPPLIER_WEEKS_SAVED = 1      # modest lead time improvement
ALT_SUPPLIER_COST_MULTIPLIER = 0.03  # 3% premium

# Decision weights (policy)
WEIGHT_RISK_REDUCTION = 0.70
WEIGHT_COST = 0.30


def _clamp(x, lo=0.0, hi=100.0):
    return float(np.clip(x, lo, hi))


def _severity(score: float) -> str:
    if score >= CRITICAL_RISK_THRESHOLD:
        return "CRITICAL"
    if score >= HIGH_RISK_THRESHOLD:
        return "HIGH"
    if score >= 35:
        return "MEDIUM"
    return "LOW"


def _estimate_risk_after_action(current_risk: float, action: str) -> float:
    """
    Deterministic “risk improvement” model for demo:
    - expedite helps most when risk is critical
    - rebalance helps if shortage is near-term
    - alternate supplier helps modestly
    """
    if action == "EXPEDITE":
        # Bigger benefit at higher risk
        delta = 18 + 0.15 * (current_risk - 50)
        return _clamp(current_risk - delta)
    if action == "REBALANCE":
        delta = 12 + 0.10 * (current_risk - 50)
        return _clamp(current_risk - delta)
    if action == "ALT_SUPPLIER":
        delta = 8 + 0.07 * (current_risk - 50)
        return _clamp(current_risk - delta)
    return current_risk


def _cost_estimate(component: str, action: str, horizon_demand_units: int) -> float:
    unit_cost = float(UNIT_COST.get(component, 1000))

    if action == "EXPEDITE":
        units = int(round(horizon_demand_units * EXPEDITE_FRACTION))
        premium = unit_cost * EXPEDITE_COST_MULTIPLIER
        return float(units * premium)

    if action == "REBALANCE":
        units = int(round(horizon_demand_units * REBALANCE_FRACTION))
        per_unit = float(REBALANCE_COST_PER_UNIT.get(component, 20))
        return float(units * per_unit)

    if action == "ALT_SUPPLIER":
        units = int(round(horizon_demand_units * ALT_SUPPLIER_FRACTION))
        premium = unit_cost * ALT_SUPPLIER_COST_MULTIPLIER
        return float(units * premium)

    return 0.0


def _normalize_cost(cost: float, component: str, horizon_demand_units: int) -> float:
    """
    Convert cost into a 0–100 penalty.
    Penalty is relative to baseline “value at risk” ~ unit_cost * demand.
    """
    unit_cost = float(UNIT_COST.get(component, 1000))
    baseline = max(1.0, unit_cost * max(1, horizon_demand_units))
    ratio = cost / baseline
    # 0% → 0 penalty, 10% → 100 penalty (clamped)
    return _clamp(ratio * 1000, 0, 100)


def _choose_best_option(component: str, current_risk: float, horizon_demand_units: int):
    options = ["EXPEDITE", "REBALANCE", "ALT_SUPPLIER"]

    scored = []
    for a in options:
        new_risk = _estimate_risk_after_action(current_risk, a)
        risk_reduction = current_risk - new_risk

        cost = _cost_estimate(component, a, horizon_demand_units)
        cost_penalty = _normalize_cost(cost, component, horizon_demand_units)

        # Higher is better
        score = (WEIGHT_RISK_REDUCTION * risk_reduction) - (WEIGHT_COST * cost_penalty)

        scored.append({
            "action": a,
            "risk_after": new_risk,
            "risk_reduction": float(risk_reduction),
            "estimated_cost_usd": float(cost),
            "score": float(score),
        })

    scored = sorted(scored, key=lambda x: x["score"], reverse=True)
    return scored


def _templated_explanation(row: dict) -> str:
    return (
        f"Detected {_severity(row['risk_score'])} shortage risk for {row['region']} / {row['component']} "
        f"(risk={row['risk_score']:.1f}, stockout={row.get('stockout_date')}). "
        f"Recommended action: {row['recommended_action']} because it provides the best risk reduction "
        f"per dollar among evaluated options. Estimated cost=${row['estimated_cost_usd']:.0f}, "
        f"expected risk after={row['risk_after']:.1f}."
    )


def build_mitigation_plan():
    os.makedirs(OUT_DIR, exist_ok=True)

    rs = pd.read_csv(RISK_SUMMARY_PATH)

    # Safety: coerce types if needed
    rs["risk_score"] = pd.to_numeric(rs["risk_score"], errors="coerce").fillna(0.0)
    rs["total_demand_horizon"] = pd.to_numeric(rs.get("total_demand_horizon"), errors="coerce").fillna(0).astype(int)

    targets = rs[rs["risk_score"] >= HIGH_RISK_THRESHOLD].copy()
    if targets.empty:
        # Write empty outputs so dashboard doesn't break
        pd.DataFrame([]).to_csv(OUT_PLAN_PATH, index=False)
        with open(OUT_EXPLAIN_PATH, "w") as f:
            json.dump({}, f, indent=2)
        print("No HIGH/CRITICAL risks found. Wrote empty mitigation outputs.")
        return

    plan_rows = []
    explanations = {}

    for _, r in targets.iterrows():
        region = str(r["region"])
        component = str(r["component"])
        current_risk = float(r["risk_score"])
        horizon_demand = int(r.get("total_demand_horizon", 0))

        options = _choose_best_option(component, current_risk, horizon_demand)
        best = options[0]

        # Map action to recommendation text
        action_map = {
            "EXPEDITE": f"Expedite ~{int(round(horizon_demand * EXPEDITE_FRACTION))} units (est. {EXPEDITE_WEEKS_SAVED} weeks saved)",
            "REBALANCE": f"Rebalance ~{int(round(horizon_demand * REBALANCE_FRACTION))} units from another region",
            "ALT_SUPPLIER": f"Shift ~{int(round(horizon_demand * ALT_SUPPLIER_FRACTION))} units to alternate supplier (est. {ALT_SUPPLIER_WEEKS_SAVED} week saved)",
        }

        row_out = {
            "region": region,
            "component": component,
            "severity": _severity(current_risk),
            "risk_score": current_risk,
            "stockout_date": r.get("stockout_date", None),
            "recommended_action": action_map.get(best["action"], best["action"]),
            "risk_after": best["risk_after"],
            "risk_reduction": best["risk_reduction"],
            "estimated_cost_usd": best["estimated_cost_usd"],
            "option_1": json.dumps(options[0]),
            "option_2": json.dumps(options[1]),
            "option_3": json.dumps(options[2]),
        }

        plan_rows.append(row_out)
        explanations[f"{region}:{component}"] = _templated_explanation(row_out)

    plan_df = pd.DataFrame(plan_rows).sort_values(["severity", "risk_score"], ascending=[True, False])
    plan_df.to_csv(OUT_PLAN_PATH, index=False)

    with open(OUT_EXPLAIN_PATH, "w") as f:
        json.dump(explanations, f, indent=2)

    print(f"Wrote: {OUT_PLAN_PATH}")
    print(f"Wrote: {OUT_EXPLAIN_PATH}")


if __name__ == "__main__":
    build_mitigation_plan()