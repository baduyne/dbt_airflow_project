{% test date_range_valid(model, start_date_column, end_date_column) %}

/*
    Custom test to validate that start date is before or equal to end date.

    Usage in schema.yml:
    tests:
      - date_range_valid:
          start_date_column: order_date
          end_date_column: ship_date

    This test is useful for:
    - Order date vs ship date validation
    - Start date vs end date validation
    - Temporal consistency checks
*/

select
    {{ start_date_column }},
    {{ end_date_column }}
from {{ model }}
where {{ start_date_column }} is not null
  and {{ end_date_column }} is not null
  and {{ start_date_column }} > {{ end_date_column }}

{% endtest %}
