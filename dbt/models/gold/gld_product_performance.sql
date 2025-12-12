{{ config(
    materialized='table',
    tags=['gold','product_performance']
) }}

with sales as (
    select
        order_detail_id,
        sales_order_id,
        product_id,
        cast(coalesce(order_quantity,0) as decimal(18,2)) as order_qty,
        cast(coalesce(unit_price,0) as decimal(18,2)) as unit_price,
        cast(coalesce(unit_price_discount,0) as decimal(18,4)) as unit_price_discount,
        cast(coalesce(line_total,0) as decimal(18,2)) as line_total,
        order_date
    from {{ ref('int_sales_details') }}
),

products as (
    select
        product_id,
        product_name,
        product_number,
        subcategory_name,
        color,
        cast(coalesce(list_price,0) as decimal(18,2)) as list_price,
        cast(coalesce(standard_cost,0) as decimal(18,2)) as standard_cost
    from {{ ref('brnz_products') }}
),

product_sales as (
    select
        p.product_id,
        p.product_name,
        p.subcategory_name,
        p.color,
        p.list_price,
        p.standard_cost,

        coalesce(count(distinct s.sales_order_id),0) as total_orders,
        coalesce(sum(s.order_qty), 0) as total_quantity_sold,
        coalesce(sum(s.line_total), 0) as total_revenue,

        case
            when sum(s.order_qty) = 0 then 0
            else sum(s.line_total) * 1.0 / nullif(sum(s.order_qty), 0)
        end as avg_selling_price,

        sum(s.line_total) - (sum(s.order_qty) * p.standard_cost) as total_profit,

        case
            when sum(s.line_total) <= 0 then 0
            when (sum(s.line_total) - (sum(s.order_qty) * p.standard_cost)) < 0 then 0
            else (sum(s.line_total) - (sum(s.order_qty) * p.standard_cost)) * 100.0 / sum(s.line_total)
        end as profit_margin_pct


    from products p
    join sales s
        on p.product_id = s.product_id
    group by
        p.product_id,
        p.product_name,
        p.subcategory_name,
        p.color,
        p.list_price,
        p.standard_cost
)

select
    product_id,
    product_name,
    subcategory_name,
    color,
    list_price,
    standard_cost,
    total_revenue,
    total_quantity_sold,
    total_orders,

    round(avg_selling_price, 2) as avg_selling_price,
    round(total_profit, 2) as total_profit,
    round(profit_margin_pct, 2) as profit_margin_pct
from product_sales
