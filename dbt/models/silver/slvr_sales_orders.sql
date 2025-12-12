{{
    config(
        materialized='table'
    )
}}

with bronze_sales as (
    select * from {{ ref('brnz_sales_orders') }}
),

cleaned as (
    select
        sales_order_id,
        order_detail_id,
        order_date,
        due_date,
        ship_date,
        order_status,
        case
            when is_online_order = 1 then 'Online'
            else 'Offline'
        end as order_channel,
        purchase_order_number,
        customer_id,
        sales_person_id,
        territory_id,
        product_id,
        order_quantity as order_qty,
        unit_price,
        unit_price_discount,
        line_total,
        -- Calculated fields
        unit_price * order_quantity as gross_amount,
        line_total / nullif(order_quantity, 0) as effective_unit_price,
        case
            when unit_price_discount > 0 then 1
            else 0
        end as has_discount
    from bronze_sales
    where order_quantity > 0
        and unit_price >= 0
)

select * from cleaned
