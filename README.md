# CSCP AI Control Tower

AI-powered Cloud Datacenter Supply Chain Control Tower demo.

## Features
- Synthetic datacenter demand data
- SARIMAX forecasting model
- Streamlit dashboard
- Future: AI agents + mitigation workflows

## Run

Generate data:
python -m src.data.generate_synthetic


Train model:
python -m src.models.forecast


Launch dashboard:
streamlit run streamlit_app/app.py