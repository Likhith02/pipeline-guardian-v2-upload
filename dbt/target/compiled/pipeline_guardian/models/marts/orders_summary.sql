select count(*) total_orders, sum(amount) total_revenue
from "demo"."main"."stg_orders"