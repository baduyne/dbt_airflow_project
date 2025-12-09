{{ config(materialized='table') }}

select
    c.territory_id,
    t.territory_name,
    count(distinct s.sales_order_id) as total_orders,
    sum(s.line_total) as total_revenue,
    avg(s.line_total) as avg_line_value
from {{ ref('int_sales_details') }} s
join {{ ref('brnz_customers') }} c using (customer_id)
join {{ ref('slvr_territories') }} t using (territory_id)
group by c.territory_id, t.territory_name
