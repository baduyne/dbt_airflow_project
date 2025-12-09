{{ config(materialized='table') }}

select
    c.territory_id,
    count(distinct so.sales_order_id) as total_orders,
    sum(sd.gross_amount) as total_revenue,
    avg(sd.gross_amount) as avg_line_value
from {{ ref('slvr_customers') }} c
join {{ ref('slvr_sales_orders') }} so
    on c.customer_id = so.customer_id
join {{ ref('int_sales_details') }} sd
    on sd.sales_order_id = so.sales_order_id
group by
    c.territory_id
