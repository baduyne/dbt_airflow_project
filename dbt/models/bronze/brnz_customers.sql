{{
    config(
        materialized='view',
        tags=['bronze', 'customers']
    )
}}

/*
    Bronze layer for Customer data
    
    This model combines customer master data with person information.
    - Cleans and standardizes column names
    - Removes NULL values in critical fields
    - Adds audit columns for tracking
*/

with source_customer as (
    select 
        CustomerID,
        PersonID,
        StoreID,
        TerritoryID,
        ModifiedDate
    from {{ source('adventureworks', 'Customer') }}
),

source_person as (
    select 
        BusinessEntityID,
        FirstName,
        MiddleName,
        LastName,
        EmailPromotion,
        ModifiedDate
    from {{ source('adventureworks_person', 'Person') }}
),

final as (
    select
        c.CustomerID as customer_id,
        c.PersonID as person_id,
        coalesce(p.FirstName, 'UNKNOWN') as first_name,
        coalesce(p.MiddleName, '') as middle_name,
        coalesce(p.LastName, 'UNKNOWN') as last_name,
        concat(
            coalesce(p.FirstName, 'UNKNOWN'), ' ',
            coalesce(p.LastName, 'UNKNOWN')
        ) as full_name,
        coalesce(p.EmailPromotion, 0) as email_promotion_preference,
        coalesce(c.StoreID, 0) as store_id,
        coalesce(c.TerritoryID, 0) as territory_id,
        cast(c.ModifiedDate as date) as last_modified_date,
        CURRENT_TIMESTAMP as dbt_load_timestamp
    from source_customer c
    left join source_person p
        on c.PersonID = p.BusinessEntityID
)

select * from final
