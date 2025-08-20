import os
import json
import subprocess
import pathlib
import re
import textwrap
import pandas as pd
import streamlit as st

# Paths
HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
DBT_DIR = ROOT / "dbt"

# Make sure dbt sees the project's profiles.yml inside ./dbt
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))

def run_cmd(cmd: str, cwd=None):
    """Run a shell command and stream output into Streamlit."""
    st.code(f"$ {cmd}")
    p = subprocess.Popen(
        cmd,
        cwd=cwd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines = []
    for line in p.stdout:
        lines.append(line)
        st.write(line.rstrip())
    p.wait()
    return p.returncode, "".join(lines)

st.set_page_config(page_title="Pipeline Guardian v2 (upload-enabled)", layout="wide")
st.title("🛡️ Pipeline Guardian — v2 with CSV uploader")
st.caption("Seed → Run → Test → Diagnose & Smart Patch → Re-run")

tab_seed, tab_run, tab_test, tab_diag = st.tabs(
    ["1) Seed", "2) Run", "3) Test", "4) Diagnose & Smart Patch"]
)

# ---------------- Seed tab ----------------
seed_dir = DBT_DIR / "seeds"
with tab_seed:
    st.subheader("Upload CSV into dbt/seeds/")
    uploaded = st.file_uploader("Choose a CSV", type="csv")
    if uploaded is not None:
        out_path = seed_dir / uploaded.name
        # Save uploaded file bytes
        with open(out_path, "wb") as f:
            f.write(uploaded.getbuffer())
        st.success(f"Saved `{uploaded.name}` to seeds/. Click **dbt seed (refresh)** below.")

    st.divider()
    if st.button("dbt seed (refresh)"):
        run_cmd("dbt seed --full-refresh", cwd=str(DBT_DIR))

# --------------- Run tab ------------------
with tab_run:
    st.subheader("Build models")
    if st.button("dbt deps (install dbt packages)"):
        run_cmd("dbt deps", cwd=str(DBT_DIR))
    if st.button("dbt run (build)"):
        run_cmd("dbt run", cwd=str(DBT_DIR))

# --------------- Test tab -----------------
with tab_test:
    st.subheader("Run tests")
    if st.button("dbt test"):
        code, _ = run_cmd("dbt test", cwd=str(DBT_DIR))
        if code == 0:
            st.success("All tests passed ✅")
        else:
            st.error("Some tests failed. Go to the Diagnose tab.")

# --------------- Diagnose tab -------------
with tab_diag:
    st.subheader("Diagnose failing tests and apply a Smart Patch")
    run_results = DBT_DIR / "target" / "run_results.json"

    if st.button("Analyze latest test results"):
        if not run_results.exists():
            st.warning("No run_results.json found. Run tests first.")
        else:
            data = json.loads(run_results.read_text(encoding="utf-8"))
            fails = [r for r in data.get("results", []) if r.get("status") == "fail"]

            if not fails:
                st.success("No failures 🎉")
            else:
                st.error("Failures:")
                st.json(fails, expanded=False)

                # Identify common issues for stg_orders
                issues = set()
                for f in fails:
                    node = (f.get("unique_id") or "").lower()
                    msg = (f.get("message") or "").lower()

                    # Duplicate order_id (unique test)
                    if "unique_stg_orders_order_id" in node or "unique" in node:
                        issues.add("duplicate order_id")

                    # NULL amount (not_null test)
                    if "not_null_stg_orders_amount" in node or ("not_null" in node and "amount" in node):
                        issues.add("NULL amount")

                    # Future dates (range/expectation tests)
                    if ("order_date" in node and "between" in node) or ("order_date" in msg and "future" in msg):
                        issues.add("future order_date")

                st.markdown("### Root cause")
                st.write(", ".join(sorted(issues)) if issues else "Could not classify automatically.")

                # Smart patch (clean + dedup)
                patch_sql = textwrap.dedent('''
                -- clean + dedup
                patched as (
                  with cleaned as (
                    select *
                    from source
                    where amount is not null
                      and cast(order_date as date) <= current_date
                  ),
                  ranked as (
                    select
                      order_id,
                      customer_id,
                      cast(amount as double) as amount,
                      cast(order_date as date) as order_date,
                      row_number() over (partition by order_id order by order_date desc) as rn
                    from cleaned
                  )
                  select * from ranked where rn = 1
                )
                ''')

                st.subheader("Suggested Patch")
                st.code(patch_sql, language="sql")

                if st.button("Apply Patch"):
                    model = DBT_DIR / "models" / "staging" / "stg_orders.sql"
                    txt = model.read_text(encoding="utf-8")
                    txt = re.sub(
                        r"-- PATCH_AREA_START.*?-- PATCH_AREA_END",
                        f"-- PATCH_AREA_START\n{patch_sql}\n-- PATCH_AREA_END",
                        txt,
                        flags=re.S,
                    )
                    model.write_text(txt, encoding="utf-8")
                    st.success("Patch applied. Now go to **Run → dbt run** and then **Test → dbt test**.")
