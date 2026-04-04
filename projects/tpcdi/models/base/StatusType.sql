{{
    config(
        materialized = 'table'
    )
}}
select
  st_id,
  st_name
from
  {{ source('tpcdi', 'raw_statustype') }}
