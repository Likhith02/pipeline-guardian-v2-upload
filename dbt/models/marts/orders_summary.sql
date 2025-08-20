
select count(*) total_orders, sum(amount) total_revenue
from {{ ref('stg_orders') }}
