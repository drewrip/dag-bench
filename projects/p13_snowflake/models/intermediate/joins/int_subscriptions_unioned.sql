{{
    config(
        materialized='view',
        tags=['intermediate', 'union']
    )
}}

/*
    Union model: int_subscriptions_unioned
    Combines data from 2 sources
*/

WITH 
source_1 AS (
    SELECT
        'payments' AS _source_system,
        _id, _loaded_at, _source_system AS _source_system_1, _source_table, _row_hash
    FROM {{ ref('stg_payments__subscriptions') }}
),
source_2 AS (
    SELECT
        'sfdc' AS _source_system,
        _id, _loaded_at, _source_system AS _source_system_1, _source_table, _row_hash
    FROM {{ ref('stg_sfdc__subscriptions') }}
),

combined AS (
    SELECT * FROM source_1 UNION ALL SELECT * FROM source_2
)

SELECT * FROM combined
