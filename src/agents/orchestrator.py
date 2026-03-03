import os
import json
import uuid
from datetime import datetime
import pandas as pd

RISK_SUMMARY = "data/demo/shortage_risk_summary.csv"
MITIGATION_PLAN = "data/demo/mitigation_plan.csv"

OUT_DIR = "data/processed"
INCIDENTS_OUT = os.path.join(OUT_DIR, "incidents.csv")
ACTIONS_OUT = os.path.join(OUT_DIR, "actions.csv")
APPROVALS_OUT = os.path.join(OUT_DIR, "approvals.csv")
PROPOSED_ORDERS_OUT = os.path.join(OUT_DIR, "proposed_orders.csv")
AUDIT_LOG_OUT = os.path.join(OUT_DIR, "audit_log.jsonl")

# --- Guardrails / Policy knobs ---
AUTO_CREATE_PO_DRAFTS = True
AUTO_NOTIFY = True

APPROVAL_COST_THRESHOLD_USD = 250_000
APPROVAL_ALWAYS_FOR_EXPEDITE = True
APPROVAL_ALWAYS_FOR_ALT_SUPPLIER = True

# Rough demo cost assumptions per unit
UNIT_COST = {"GPU": 25000, "NIC": 1200, "SWITCH": 8000}


def _now():
    return datetime.utcnow().isoformat() + "Z"


def _append_audit(event: dict):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(AUDIT_LOG_OUT, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")


def _incident_id():
    return "INC-" + uuid.uuid4().hex[:10].upper()


def _action_id():
    return "ACT-" + uuid.uuid4().hex[:10].upper()


def _approval_id():
    return "APR-" + uuid.uuid4().hex[:10].upper()


def _estimate_action_cost(component: str, action_text: str, total_demand_horizon: int) -> float:
    unit_cost = float(UNIT_COST.get(component, 1000))

    # Very simple parsing-based estimate for demo
    # We look for "Expedite ~X units" / "Rebalance ~X units" / "Shift ~X units"
    units = 0
    for token in action_text.replace(",", "").split():
        if token.isdigit():
            units = int(token)
            break

    # Fall back to 20% of horizon demand if units not parsed
    if units <= 0:
        units = max(1, int(round(total_demand_horizon * 0.20)))

    # Cost rules
    if action_text.upper().startswith("EXPEDITE"):
        return units * unit_cost * 0.08  # 8% premium
    if "Rebalance" in action_text or "REBALANCE" in action_text.upper():
        return units * 300  # handling / logistics (GPU-like)
    if "alternate supplier" in action_text.lower() or "Shift" in action_text:
        return units * unit_cost * 0.03  # 3% premium
    return units * unit_cost * 0.01  # default small premium


def run_agentic_workflow():
    os.makedirs(OUT_DIR, exist_ok=True)

    risk = pd.read_csv(RISK_SUMMARY)
    plan = pd.read_csv(MITIGATION_PLAN)

    # Filter to actionable incidents
    risk["risk_score"] = pd.to_numeric(risk["risk_score"], errors="coerce").fillna(0.0)
    actionable = risk[risk["severity"].isin(["CRITICAL", "HIGH"])].copy()

    incidents = []
    actions = []
    approvals = []
    proposed_orders = []

    for _, r in actionable.iterrows():
        region = str(r["region"])
        component = str(r["component"])
        risk_score = float(r["risk_score"])
        stockout_date = r.get("stockout_date", "")

        inc_id = _incident_id()
        created = _now()

        incident = {
            "incident_id": inc_id,
            "created_utc": created,
            "region": region,
            "component": component,
            "severity": r["severity"],
            "risk_score": risk_score,
            "stockout_date": stockout_date,
            "status": "OPEN",
            "summary": f"{r['severity']} shortage risk for {region}/{component} (risk={risk_score:.1f}, stockout={stockout_date})",
        }
        incidents.append(incident)
        _append_audit({"ts": created, "type": "INCIDENT_CREATED", "incident": incident})

        # Find mitigation recommendation
        rec = plan[(plan["region"] == region) & (plan["component"] == component)]
        if rec.empty:
            # If plan missing, create a safe default: notify + draft PO
            rec_action = "Create PO draft for additional supply (default)"
            risk_after = risk_score
            est_cost = _estimate_action_cost(component, rec_action, int(r.get("total_demand_horizon", 0)))
        else:
            rec_row = rec.iloc[0]
            rec_action = str(rec_row["recommended_action"])
            risk_after = float(rec_row.get("risk_after", risk_score))
            est_cost = float(rec_row.get("estimated_cost_usd", 0.0))

        act_id = _action_id()
        action = {
            "action_id": act_id,
            "incident_id": inc_id,
            "created_utc": created,
            "action_type": "MITIGATION_RECOMMENDATION",
            "recommended_action": rec_action,
            "estimated_cost_usd": est_cost,
            "risk_score_before": risk_score,
            "risk_score_after": risk_after,
            "execution_status": "PROPOSED",
        }
        actions.append(action)
        _append_audit({"ts": created, "type": "ACTION_PROPOSED", "action": action})

        # Determine if approval required
        needs_approval = False
        if est_cost >= APPROVAL_COST_THRESHOLD_USD:
            needs_approval = True
        if APPROVAL_ALWAYS_FOR_EXPEDITE and "Expedite" in rec_action:
            needs_approval = True
        if APPROVAL_ALWAYS_FOR_ALT_SUPPLIER and ("alternate supplier" in rec_action.lower() or "Shift" in rec_action):
            needs_approval = True

        if needs_approval:
            apr_id = _approval_id()
            approval = {
                "approval_id": apr_id,
                "incident_id": inc_id,
                "action_id": act_id,
                "created_utc": created,
                "status": "PENDING",
                "approver_role": "SupplyChainDirector",
                "approval_reason": f"Policy gate: cost/expedite/supplier-change for {region}/{component}",
            }
            approvals.append(approval)
            _append_audit({"ts": created, "type": "APPROVAL_REQUESTED", "approval": approval})
        else:
            # Auto-execute low-risk actions (demo)
            action["execution_status"] = "AUTO_EXECUTED"
            _append_audit({"ts": created, "type": "ACTION_AUTO_EXECUTED", "action_id": act_id})

        # Create proposed order drafts (never “real submit” in V1)
        if AUTO_CREATE_PO_DRAFTS:
            demand_h = int(r.get("total_demand_horizon", 0))
            qty = max(1, int(round(demand_h * 0.20)))  # 20% buffer draft

            po = {
                "incident_id": inc_id,
                "created_utc": created,
                "region": region,
                "component": component,
                "order_type": "PO_DRAFT",
                "quantity": qty,
                "preferred_supplier": "TBD",
                "status": "DRAFT",
                "notes": f"Auto-generated draft due to {r['severity']} risk. Link to approval/workflow required before submit.",
            }
            proposed_orders.append(po)
            _append_audit({"ts": created, "type": "PO_DRAFT_CREATED", "po": po})

    # Write outputs
    pd.DataFrame(incidents).to_csv(INCIDENTS_OUT, index=False)
    pd.DataFrame(actions).to_csv(ACTIONS_OUT, index=False)
    pd.DataFrame(approvals).to_csv(APPROVALS_OUT, index=False)
    pd.DataFrame(proposed_orders).to_csv(PROPOSED_ORDERS_OUT, index=False)

    print(f"Wrote: {INCIDENTS_OUT}")
    print(f"Wrote: {ACTIONS_OUT}")
    print(f"Wrote: {APPROVALS_OUT}")
    print(f"Wrote: {PROPOSED_ORDERS_OUT}")
    print(f"Wrote: {AUDIT_LOG_OUT}")


if __name__ == "__main__":
    run_agentic_workflow()