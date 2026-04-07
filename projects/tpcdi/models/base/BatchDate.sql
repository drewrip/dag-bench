select
    batchdate,
    batchid
from
    {{ source('tpcdi', 'raw_batchdate') }}
