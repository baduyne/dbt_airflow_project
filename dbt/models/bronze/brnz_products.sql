{{
    config(
        materialized='view',
        tags=['bronze', 'products']
    )
}}

/*
    Bronze layer for Product data
    
    This model combines product master data with category information.
    - Cleans and standardizes column names
    - Handles NULL values in optional fields
    - Converts numeric and date fields
    - Adds audit columns
*/

with product as (
    select
        ProductID,
        Name,
        ProductNumber,
        MakeFlag,
        FinishedGoodsFlag,
        Color,
        SafetyStockLevel,
        ReorderPoint,
        StandardCost,
        ListPrice,
        Size,
        SizeUnitMeasureCode,
        WeightUnitMeasureCode,
        Weight,
        DaysToManufacture,
        ProductLine,
        Class,
        Style,
        ProductSubcategoryID,
        ProductModelID,
        SellStartDate,
        SellEndDate,
        DiscontinuedDate,
        ModifiedDate
    from {{ source('adventureworks_production', 'Product') }}
),

product_subcategory as (
    select
        ProductSubcategoryID,
        Name as SubcategoryName,
        ProductCategoryID,
        ModifiedDate
    from {{ source('adventureworks_production', 'ProductSubcategory') }}
),

final as (
    select
        p.ProductID as product_id,
        p.Name as product_name,
        p.ProductNumber as product_number,
        p.MakeFlag as is_manufactured,
        p.FinishedGoodsFlag as is_finished_good,
        coalesce(p.Color, 'UNKNOWN') as color,
        p.SafetyStockLevel as safety_stock_level,
        p.ReorderPoint as reorder_point,
        cast(p.StandardCost as decimal(12, 2)) as standard_cost,
        cast(p.ListPrice as decimal(12, 2)) as list_price,
        coalesce(p.Size, 'N/A') as size,
        coalesce(p.SizeUnitMeasureCode, 'N/A') as size_unit_measure_code,
        coalesce(p.WeightUnitMeasureCode, 'N/A') as weight_unit_measure_code,
        coalesce(cast(p.Weight as decimal(8, 2)), 0) as weight,
        p.DaysToManufacture as days_to_manufacture,
        coalesce(p.ProductLine, 'UNKNOWN') as product_line,
        coalesce(p.Class, 'UNKNOWN') as product_class,
        coalesce(p.Style, 'UNKNOWN') as product_style,
        coalesce(p.ProductSubcategoryID, 0) as product_subcategory_id,
        coalesce(p.ProductModelID, 0) as product_model_id,
        coalesce(ps.SubcategoryName, 'UNKNOWN') as subcategory_name,
        coalesce(ps.ProductCategoryID, 0) as product_category_id,
        cast(p.SellStartDate as date) as sell_start_date,
        cast(p.SellEndDate as date) as sell_end_date,
        cast(p.DiscontinuedDate as date) as discontinued_date,
        case
            when cast(p.SellEndDate as date) is null then 'ACTIVE'
            when cast(p.SellEndDate as date) < cast(getdate() as date) then 'DISCONTINUED'
            else 'ACTIVE'
        end as product_status,
        cast(p.ModifiedDate as date) as last_modified_date,
        CURRENT_TIMESTAMP as dbt_load_timestamp
    from product p
    left join product_subcategory ps
        on p.ProductSubcategoryID = ps.ProductSubcategoryID
)

select * from final
