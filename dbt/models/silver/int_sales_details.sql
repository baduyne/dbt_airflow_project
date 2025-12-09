{{
    config(
        materialized='view',
        tags=['silver', 'sales']
    )
}}

/*
    Silver Layer: Enriched Sales Details

    Business Logic:
    1. JOINS: Sales Order Lines + Product Information
    2. CALCULATIONS:
       - Line Item Gross Amount (Qty * UnitPrice)
       - Discount Amount
       - Net Amount
    3. TRANSFORMATION:
       - Decode Order Status (Int -> String)
       - Flag Bulk Orders (>10 items)
*/

with sales_orders as (
    select * from {{ ref('brnz_sales_orders') }}
),

products as (
    select * from {{ ref('brnz_products') }}
),

final as (
    select
        -- Key IDs
        so.order_detail_id,
        so.sales_order_id,
        so.customer_id,
        so.is_online_order,
        so.product_id,
        so.line_total,
        -- Dimensions
        so.order_date,
        so.ship_date,
        p.product_name,
        p.product_number,
        p.color as product_color,
        p.subcategory_name,

        -- Business Logic: Order Status Decoding
        case so.order_status
            when 1 then 'In Process'
            when 2 then 'Approved'
            when 3 then 'Backordered'
            when 4 then 'Rejected'
            when 5 then 'Shipped'
            when 6 then 'Cancelled'
            else 'Unknown'
        end as order_status_description,

        -- Transaction Metrics
        so.order_quantity,
        so.unit_price,
        so.unit_price_discount,

        -- Business Logic: Calculated Financials
        cast((so.order_quantity * so.unit_price) as decimal(12,2)) as gross_amount,

        cast((so.order_quantity * so.unit_price * so.unit_price_discount) as decimal(12,2)) as discount_amount,

        cast((so.order_quantity * so.unit_price * (1 - so.unit_price_discount)) as decimal(12,2)) as net_line_amount,

        -- Business Logic: Flags
        case
            when so.order_quantity >= 10 then 1
            else 0
        end as is_bulk_order

    from sales_orders so
    left join products p
        on so.product_id = p.product_id
)

select * from final
