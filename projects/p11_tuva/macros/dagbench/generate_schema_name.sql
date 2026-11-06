{#
    dag-bench: every relation lives in the target schema (`main` on DuckDB,
    `synth_multi_p11_tuva` on Postgres), matching the other benchmark projects.
    Upstream routes models and seeds to input_layer, core, terminology, ...
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
