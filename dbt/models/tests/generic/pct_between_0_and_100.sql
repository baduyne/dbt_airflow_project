{% test pct_between_0_and_100(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null and ({{ column_name }} < 0 or {{ column_name }} > 100)
{% endtest %}
{% test pct_between_0_and_100(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null and ({{ column_name }} < 0 or {{ column_name }} > 100)
{% endtest %}
