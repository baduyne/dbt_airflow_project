{{ config(
    materialized='table',
    tags=['gold','customer_metrics'],
) }}

with sales as (
    select
        order_detail_id,
        sales_order_id,
        product_id,
        sales_order_id as so_id,
        order_date,
        coalesce(line_total,0) as line_total,
        coalesce(order_quantity,0) as order_qty,
        case when is_online_order = 1 then 'Online' else 'Offline' end as order_channel,
        case when unit_price_discount > 0 then 1 else 0 end as has_discount,
        customer_id
    from {{ ref('int_sales_details') }}
),

customers as (
    select
      customer_id,
      coalesce(full_name, '') as full_name,
      coalesce(customer_type, 'Unknown') as customer_type,
      coalesce(marketing_segment, 'Unknown') as marketing_segment,
      territory_id
    from {{ ref('int_customer_profile') }}
),

customer_sales as (
    select
      c.customer_id,
      c.full_name,
      c.customer_type,
      c.marketing_segment,
      c.territory_id,
      count(distinct s.sales_order_id) as total_orders,
      sum(s.line_total) as total_revenue,
      case when count(distinct s.sales_order_id) = 0 then 0
           else sum(s.line_total) * 1.0 / nullif(count(distinct s.sales_order_id),0)
      end as avg_order_value,
      sum(s.order_qty) as total_items_purchased,
      min(s.order_date) as first_order_date,
      max(s.order_date) as last_order_date,
      sum(case when s.has_discount = 1 then 1 else 0 end) as orders_with_discount,
      sum(case when s.order_channel = 'Online' then 1 else 0 end) as online_orders
    from customers c
    left join sales s
      on c.customer_id = s.customer_id
    group by
      c.customer_id, c.full_name, c.customer_type, c.marketing_segment, c.territory_id
)

select
  customer_id,
  full_name,
  customer_type,
  marketing_segment,
  territory_id,
  total_orders,
  total_revenue,
  round(avg_order_value,2) as avg_order_value,
  total_items_purchased,
  first_order_date,
  last_order_date,
  orders_with_discount,
  online_orders
from customer_sales
order by total_revenue desc
