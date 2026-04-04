{{
  config(
    materialized = "table"
  )
}}
select
    batchdate,
    batchid
from
    {{ source('tpcdi', 'raw_batchdate') }}
