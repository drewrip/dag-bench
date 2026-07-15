select wh_id, name as wh_name, country, region, capacity_m3, is_active
from {{ source('sc','warehouses') }}
