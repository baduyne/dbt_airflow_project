{% test positive_values(model, column_name) %}

/*
    Custom test to validate that all values in a column are positive (> 0).

    Usage in schema.yml:
    tests:
      - positive_values:
          column_name: price

    This test is useful for:
    - Prices that cannot be negative or zero
    - Quantities that must be positive
    - Financial amounts that should be positive
*/

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
