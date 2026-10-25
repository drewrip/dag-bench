{% test not_empty(model) %}
{#
    Universal data quality check: the model must return at least one row.
    Fails (returns 1 row) when the model's result set is empty.
#}
select 1 as issue
from {{ model }}
having count(*) = 0

{% endtest %}
