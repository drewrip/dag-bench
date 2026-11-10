{{
    config(
        materialized='table',
        tags=['dimension', 'compensation', 'scd_type_1']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_employee_compensation') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['compensation_id']) }} AS compensation_key,
        compensation_id AS compensation_id,
        
        -- Attributes
        employee_id,
        compensation_type,
        amount,
        currency_code,
        frequency,
        effective_from,
        effective_to,
        created_at,
        
        -- Audit
        {{ dagbench_current_timestamp() }} AS dw_created_at,
        {{ dagbench_current_timestamp() }} AS dw_updated_at
        
    FROM source
    WHERE compensation_id IS NOT NULL
)

SELECT * FROM final
