
with source as (
    select * from {{ source('raw', 'orders') }}
),

-- PATCH_AREA_START
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
-- PATCH_AREA_END

select * from patched
