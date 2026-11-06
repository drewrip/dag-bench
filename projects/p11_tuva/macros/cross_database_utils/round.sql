{#

    Rounds a numeric expression to `precision` decimal places.

    Every dialect Tuva targets today spells this the same way. The wrapper
    exists so a future adapter that does not can be handled in one place
    instead of at every call site.

#}

{% macro round(expression, precision=0) %}
  {{ return(adapter.dispatch('round', 'p11_tuva')(expression, precision)) }}
{% endmacro %}

{% macro default__round(expression, precision=0) %}
  round( {{ expression }}, {{ precision }} )
{% endmacro %}
