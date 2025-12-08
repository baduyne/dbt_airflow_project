{% test line_total_calculation(model) %}

/*
    Custom test to validate sales order line total calculation.
    
    Business Logic:
    line_total = order_quantity * unit_price * (1 - unit_price_discount)
    
    This test checks if the calculated line_total matches the expected value
    with a tolerance of 0.01 for rounding differences.
    
    Usage in schema.yml:
    tests:
      - line_total_calculation
*/

select
    order_detail_id,
    order_quantity,
    unit_price,
    unit_price_discount,
    line_total,
    (order_quantity * unit_price * (1 - unit_price_discount)) as calculated_total,
    abs(line_total - (order_quantity * unit_price * (1 - unit_price_discount))) as difference
from {{ model }}
where abs(line_total - (order_quantity * unit_price * (1 - unit_price_discount))) > 0.01

{% endtest %}
