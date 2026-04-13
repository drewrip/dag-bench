select wh_id, name as wh_name, country as wh_country, region,
    capacity_m3, is_active
from {{ source('sc','warehouses') }}
