
{% macro percentage(part, whole) %}
    case
        when {{ whole }} = 0 then 0
        else (({{ part }}::decimal(18,3) / {{ whole }}::decimal(18,3)) * 100)::decimal(10,2)
    end
{% endmacro %}
