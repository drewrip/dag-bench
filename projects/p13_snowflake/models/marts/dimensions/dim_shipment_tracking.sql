{{
    config(
        materialized='table',
        tags=['dimension', 'tracking', 'scd_type_1']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_shipment_tracking') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['tracking_id']) }} AS tracking_key,
        tracking_id AS tracking_id,
        
        -- Attributes
        shipment_id,
        status,
        location,
        description,
        tracked_at,
        created_at,
        
        -- Audit
        {{ dagbench_current_timestamp() }} AS dw_created_at,
        {{ dagbench_current_timestamp() }} AS dw_updated_at
        
    FROM source
    WHERE tracking_id IS NOT NULL
)

SELECT * FROM final
