{{
    config(
        materialized='table',
        tags=['dimension', 'tags', 'scd_type_1']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_product_tags') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tag_id']) }} AS tags_key,
        tag_id AS tags_id,
        
        -- Attributes
        product_id,
        tag_name,
        tag_type,
        created_at,
        
        -- Audit
        {{ dagbench_current_timestamp() }} AS dw_created_at,
        {{ dagbench_current_timestamp() }} AS dw_updated_at
        
    FROM source
    WHERE tag_id IS NOT NULL
)

SELECT * FROM final
