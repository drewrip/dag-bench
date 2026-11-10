{{
    config(
        materialized='table',
        tags=['dimension', 'issues', 'scd_type_1']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg__lookup_dq_issues') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id']) }} AS issues_key,
        id AS issues_id,
        
        -- Attributes
        text_value,
        
        -- Audit
        {{ dagbench_current_timestamp() }} AS dw_created_at,
        {{ dagbench_current_timestamp() }} AS dw_updated_at
        
    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM final
