{#
    dag-bench: relations are named after their (unique) node name. Upstream
    aliases such as core__patient -> core.patient and input_layer__patient ->
    input_layer.patient would collide once all schemas collapse into one.
#}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {{ node.name }}
{%- endmacro %}
