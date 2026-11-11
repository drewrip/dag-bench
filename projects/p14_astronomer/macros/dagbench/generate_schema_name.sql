{#
    dag-bench: every relation lives in the target schema (`main` on DuckDB,
    `synth_multi_p14_astronomer` on Postgres), matching the other benchmark
    projects. Upstream lands models in the `+schema` names verbatim (stg, int,
    marts, ops_analytics).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
