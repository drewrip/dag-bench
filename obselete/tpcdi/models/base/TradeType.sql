select
  tt_id,
  tt_name,
  tt_is_sell,
  tt_is_mrkt
from
  {{ source('tpcdi', 'raw_tradetype') }}
