{{
    config(
        materialized='table'
    )
}}

with bronze_products as (
    select * from {{ ref('brnz_products') }}
),

cleaned as (
    select
        product_id,
        product_name,
        product_number,
        coalesce(Color, 'N/A') as color,
        standard_cost,
        list_price,
        coalesce(Size, 'N/A') as size,
        coalesce(Weight, 0) as weight,
        product_line,
        product_class as class,
        product_style as style,
        product_subcategory_id as subcategory_id,
        coalesce(subcategory_name, 'Uncategorized') as subcategory_name,
        product_category_id as category_id,
        sell_start_date,
        sell_end_date,
        case
            when discontinued_date is not null then 1
            else 0
        end as is_discontinued,
        last_modified_date
    from bronze_products
)

select * from cleaned
