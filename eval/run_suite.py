
import subprocess, os, pathlib, re, textwrap

ROOT = pathlib.Path(__file__).resolve().parents[1]
DBT = ROOT / 'dbt'
os.environ['DBT_PROFILES_DIR'] = str(DBT)

def run(c): return subprocess.run(c, cwd=DBT, shell=True, capture_output=True, text=True)

def patch():
    model = DBT/'models/staging/stg_orders.sql'
    patch_sql = textwrap.dedent("""
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
    """)
    txt = model.read_text()
    txt = re.sub(r"-- PATCH_AREA_START.*?-- PATCH_AREA_END",
                 f"-- PATCH_AREA_START\n{patch_sql}\n-- PATCH_AREA_END",
                 txt, flags=re.S)
    model.write_text(txt)

run("dbt deps")
run("dbt seed --full-refresh")
run("dbt run")
first = run("dbt test")
print("initial status:", "pass" if first.returncode==0 else "fail")
if first.returncode!=0:
    patch()
    run("dbt run")
    second = run("dbt test")
    print("after patch:", "pass" if second.returncode==0 else "fail")
