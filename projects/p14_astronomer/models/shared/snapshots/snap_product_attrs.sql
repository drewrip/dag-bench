{{ config(materialized='table') }}

{#- dag-bench: was snapshots/snap_product_attrs.sql (check strategy, unique_key sku).
    This model is what a first `dbt snapshot` builds: every row current. -#}

with snapshot_query as (
{#-
    Product attributes, versioned on change.

    `check` strategy rather than `timestamp`, because the PIM stamps
    `updated_at` on every write including the ones that change nothing. A
    timestamp strategy over this feed mints a version row for a no-op, and half
    the versions in the old table were exactly that.

    Reads the source for the same reason `snap_product_price` does: a snapshot
    over a model has to wait for the model, and dim_product is on the far side
    of the snapshot.
-#}

with ranked as (

    select
        sku,
        product_name,
        category_id,
        brand,
        supplier_id,
        status,
        {{ dagbench_json_extract_string("attributes_json", "'$.color'") }}  as attr_color,
        {{ dagbench_json_extract_string("attributes_json", "'$.size'") }}   as attr_size,
        updated_at,
        row_number() over (
            partition by sku
            order by
                updated_at desc nulls last,
                case source
                    when 'pim_ui' then 1
                    when 'supplier_feed' then 2
                    when 'bulk_load' then 3
                    else 4
                end,
                change_id
        ) as version_seq
    from {{ source('product', 'pim_product_versions') }}
    where operation = 'upsert'

)

select
    sku,
    product_name,
    category_id,
    brand,
    supplier_id,
    status,
    attr_color,
    attr_size,
    updated_at
from ranked
where version_seq = 1
)

select
    s.*,
    md5(cast(s.sku as varchar) || '|' || cast(cast({{ ds() }} as timestamp) as varchar)) as dbt_scd_id,
    cast({{ ds() }} as timestamp) as dbt_updated_at,
    cast({{ ds() }} as timestamp) as dbt_valid_from,
    cast(null as timestamp) as dbt_valid_to
from snapshot_query s
