{{ config(
    materialized='table',
    tags=['gold','sales_summary']
) }}

with sales as (
    select
       sales_order_id,
       order_date,
       customer_id,
       coalesce(order_quantity,0) as order_qty,
       coalesce(line_total,0) as line_total,
       case when is_online_order = 1 then 'Online' else 'Offline' end as order_channel,
       case when unit_price_discount > 0 then 1 else 0 end as has_discount
    from {{ ref('int_sales_details') }}
),

daily as (
    select
      cast(order_date as date) as day,
      count(distinct sales_order_id) as total_orders,
      count(distinct customer_id) as unique_customers,
      sum(order_qty) as total_items_sold,
      sum(line_total) as total_revenue,
      case when count(*) = 0 then 0 else sum(line_total)*1.0 / nullif(count(distinct sales_order_id),0) end as avg_order_value,
      sum(case when order_channel = 'Online' then 1 else 0 end) as online_orders,
      sum(case when order_channel = 'Offline' then 1 else 0 end) as offline_orders,
      sum(case when has_discount = 1 then line_total else 0 end) as discounted_revenue,
      YEAR(order_date) as year,
      MONTH(order_date) as month
    from sales
    group by cast(order_date as date)
)

select
  day as order_date,
  year,
  month,
  total_orders,
  unique_customers,
  total_items_sold,
  round(total_revenue,2) as total_revenue,
  round(avg_order_value,2) as avg_order_value,
  online_orders,
  offline_orders,
  round(discounted_revenue,2) as discounted_revenue
from daily
