
{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 then null
        else ({{ numerator }}::decimal(18,3) / {{ denominator }}::decimal(18,3))
    end
{% endmacro %}
