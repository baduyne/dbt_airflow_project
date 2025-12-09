{% test non_future_date(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null and cast({{ column_name }} as date) > current_date
{% endtest %}
