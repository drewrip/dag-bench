select sub_id,
    upper(substr(trim(name),1,1)) || lower(substr(trim(name),2)) as sub_name,
    region, capacity_mw, voltage_kv, lat, lon
from {{ source('grid','substations') }}
