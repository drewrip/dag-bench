select
  sk_timeid,
  timevalue,
  hourid,
  hourdesc,
  minuteid,
  minutedesc,
  secondid,
  seconddesc,
  markethoursflag,
  officehoursflag
from
  {{ source('tpcdi', 'raw_time') }}
