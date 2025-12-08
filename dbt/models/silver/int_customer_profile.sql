{{
    config(
        materialized='view',
        tags=['silver', 'customers']
    )
}}

/*
    Silver Layer: Customer Profiles
    
    Business Logic:
    1. SEGMENTATION:
       - Customer Type: Store vs Individual
       - Marketing Segment: Based on email promotion preference
*/

with customers as (
    select * from {{ ref('brnz_customers') }}
),

final as (
    select
        customer_id,
        person_id,
        store_id,
        territory_id,
        
        -- Identity
        first_name,
        last_name,
        full_name,
        
        -- Business Logic: Customer Type Segmentation
        case 
            when store_id is not null and store_id > 0 then 'Store'
            else 'Individual'
        end as customer_type,

        -- Business Logic: Marketing Segmentation
        case 
            when email_promotion_preference = 0 then 'Opt-Out'
            when email_promotion_preference = 1 then 'AdventureWorks Only'
            when email_promotion_preference = 2 then 'Partners & AdventureWorks'
            else 'Unknown'
        end as marketing_segment

    from customers
)

select * from final
