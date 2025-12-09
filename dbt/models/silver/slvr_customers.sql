{{
    config(
        materialized='table'
    )
}}

with bronze_customers as (
    select * from {{ ref('brnz_customers') }}
),

cleaned as (
    select
        customer_id,
        coalesce(first_name, 'Unknown') as first_name,
        coalesce(last_name, 'Unknown') as last_name,
        concat(coalesce(first_name, 'Unknown'), ' ', coalesce(last_name, 'Unknown')) as full_name,
        email_promotion_preference as email_promotion,
        store_id,
        territory_id,
        last_modified_date
    from bronze_customers
)

select * from cleaned
