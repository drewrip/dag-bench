select sub_id, name as sub_name, region, capacity_mw, voltage_kv, lat, lon
from {{ source('grid','substations') }}
