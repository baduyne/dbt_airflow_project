{{
    config(
        materialized='view',
        tags=['bronze', 'sales']
    )
}}

/*
    Bronze layer for Sales Orders
    
    This model combines sales order headers with line items.
    - Standardizes column names
    - Handles NULL values in optional fields
    - Converts data types appropriately
    - Adds audit columns
*/

with sales_order_header as (
    select
        SalesOrderID,
        OrderDate,
        DueDate,
        ShipDate,
        Status,
        OnlineOrderFlag,
        SalesOrderNumber,
        PurchaseOrderNumber,
        CustomerID,
        SalesPersonID,
        TerritoryID,
        ShipMethodID,
        CreditCardID,
        CurrencyRateID,
        SubTotal,
        TaxAmt,
        Freight,
        TotalDue,
        ModifiedDate
    from {{ source('adventureworks', 'SalesOrderHeader') }}
),

sales_order_detail as (
    select
        SalesOrderDetailID,
        SalesOrderID,
        ProductID,
        OrderQty,
        UnitPrice,
        UnitPriceDiscount,
        LineTotal,
        ModifiedDate
    from {{ source('adventureworks', 'SalesOrderDetail') }}
),

final as (
    select
        h.SalesOrderID as sales_order_id,
        cast(h.OrderDate as date) as order_date,
        cast(h.DueDate as date) as due_date,
        cast(h.ShipDate as date) as ship_date,
        h.Status as order_status,
        h.OnlineOrderFlag as is_online_order,
        h.SalesOrderNumber as order_number,
        coalesce(h.PurchaseOrderNumber, 'NONE') as purchase_order_number,
        h.CustomerID as customer_id,
        coalesce(h.SalesPersonID, 0) as sales_person_id,
        coalesce(h.TerritoryID, 0) as territory_id,
        coalesce(h.ShipMethodID, 0) as ship_method_id,
        d.SalesOrderDetailID as order_detail_id,
        d.ProductID as product_id,
        d.OrderQty as order_quantity,
        d.UnitPrice as unit_price,
        d.UnitPriceDiscount as unit_price_discount,
        d.LineTotal as line_total,
        cast(h.SubTotal as decimal(12, 2)) as order_subtotal,
        cast(h.TaxAmt as decimal(12, 2)) as tax_amount,
        cast(h.Freight as decimal(12, 2)) as freight_amount,
        cast(h.TotalDue as decimal(12, 2)) as order_total,
        cast(h.ModifiedDate as date) as last_modified_date,
        CURRENT_TIMESTAMP as dbt_load_timestamp
    from sales_order_header h
    left join sales_order_detail d
        on h.SalesOrderID = d.SalesOrderID
)

select * from final
