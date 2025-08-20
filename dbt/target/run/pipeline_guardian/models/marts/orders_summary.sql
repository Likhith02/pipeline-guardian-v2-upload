
  
    
    

    create  table
      "demo"."main"."orders_summary__dbt_tmp"
  
    as (
      select count(*) total_orders, sum(amount) total_revenue
from "demo"."main"."stg_orders"
    );
  
  