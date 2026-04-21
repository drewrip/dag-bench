select
  tx_id,
  tx_name,
  tx_rate
from
  {{ source('tpcdi', 'raw_taxrate') }}
