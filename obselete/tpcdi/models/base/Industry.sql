select
  in_id,
  in_name,
  in_sc_id
from
  {{ source('tpcdi', 'raw_industry') }}
