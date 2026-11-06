{#
    dag-bench: every relation lives in the target schema (`main` on DuckDB,
    `synth_multi_p12_stripe` on Postgres), matching the other benchmark projects.
    Upstream routes models to `<target>_stripe` and `<target>_stg_stripe`.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
