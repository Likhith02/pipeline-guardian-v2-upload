
# Pipeline Guardian — v2 with CSV Upload

* 3 failure types (duplicates, NULL amounts, future dates)
* Streamlit UI now lets anyone upload a CSV into `dbt/seeds/`
* One‑click Smart Patch cleans & dedups, then re‑tests.
* Eval harness (`eval/run_suite.py`) auto‑checks pass rate.

Quick start:
```bash
python -m venv .venv && source .venv/bin/activate   # Win: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run ui/streamlit_app.py
```
