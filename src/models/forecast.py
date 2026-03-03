import os
import json
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

RAW_PATH = "data/raw/demand_weekly.csv"
OUT_DIR = "data/processed"
MODEL_DIR = "data/processed/models"

def train_and_forecast(horizon_weeks: int = 12):
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(MODEL_DIR, exist_ok=True)

    df = pd.read_csv(RAW_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    forecasts = []

    for (region, component), sub in df.groupby(["region", "component"]):
        ts = sub.set_index("date")["demand_units"].asfreq("W-MON")
        ts = ts.ffill().fillna(0)

        model = SARIMAX(
            ts,
            order=(1, 1, 1),
            seasonal_order=(1, 1, 1, 52),
            enforce_stationarity=False,
            enforce_invertibility=False
        )

        res = model.fit(disp=False)

        pred = res.get_forecast(steps=horizon_weeks)
        pred_mean = pred.predicted_mean
        pred_ci = pred.conf_int()

        for dt, val in pred_mean.items():
            forecasts.append({
                "date": dt.date().isoformat(),
                "region": region,
                "component": component,
                "forecast_units": float(max(0.0, val)),
                "lower": float(max(0.0, pred_ci.loc[dt, pred_ci.columns[0]])),
                "upper": float(max(0.0, pred_ci.loc[dt, pred_ci.columns[1]])),
            })

        meta = {
            "region": region,
            "component": component,
            "aic": float(res.aic),
            "bic": float(res.bic),
            "params": {k: float(v) for k, v in res.params.items()}
        }
        with open(os.path.join(MODEL_DIR, f"sarimax_{region}_{component}.json"), "w") as f:
            json.dump(meta, f, indent=2)

    fc_df = pd.DataFrame(forecasts)
    out_path = os.path.join(OUT_DIR, "forecast_weekly.csv")
    fc_df.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}")

if __name__ == "__main__":
    train_and_forecast()