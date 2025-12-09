{{ config(
    materialized='table',
    tags=['gold','product_performance']
) }}

with sales as (
    select
      order_detail_id,
      sales_order_id,
      product_id,
      coalesce(order_quantity,0) as order_qty,
      coalesce(unit_price,0) as unit_price,
      coalesce(unit_price_discount,0) as unit_price_discount,
      coalesce(line_total,0) as line_total,
      order_date
    from {{ ref('int_sales_details') }} -- silver
),

products as (
    select
      p.product_id,
      p.product_name,
      p.product_number,
      p.subcategory_name,
      p.color,
      coalesce(p.list_price,0) as list_price,
      coalesce(p.standard_cost,0) as standard_cost
    from {{ ref('brnz_products') }} -- bronze (if silver lacks product attributes). If silver product dimension exists, ref that instead.
),

product_sales as (
    select
      p.product_id,
      p.product_name,
      p.subcategory_name,
      p.color,
      p.list_price,
      p.standard_cost,
      count(distinct s.sales_order_id) as total_orders,
      sum(s.order_qty) as total_quantity_sold,
      sum(s.line_total) as total_revenue,
      case when sum(s.order_qty) = 0 then 0 else sum(s.line_total)*1.0 / nullif(sum(s.order_qty),0) end as avg_selling_price,
      sum(s.line_total) - (sum(s.order_qty) * p.standard_cost) as total_profit,
      case when sum(s.line_total) > 0 then
        (sum(s.line_total) - (sum(s.order_qty) * p.standard_cost)) * 100.0 / sum(s.line_total)
      else 0
      end as profit_margin_pct
    from products p
    left join sales s
      on p.product_id = s.product_id
    group by
      p.product_id, p.product_name, p.subcategory_name, p.color, p.list_price, p.standard_cost
)

select
  product_id,
  product_name,
  subcategory_name,
  color,
  list_price,
  standard_cost,
  total_orders,
  total_quantity_sold,
  total_revenue,
  round(avg_selling_price,2) as avg_selling_price,
  round(total_profit,2) as total_profit,
  round(profit_margin_pct,2) as profit_margin_pct
from product_sales
order by total_revenue desc
