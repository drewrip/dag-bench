select d.device_id, d.site_id, d.device_type, d.model,
    d.firmware, d.installed_date, d.is_active,
    s.name as site_name, s.region, s.latitude, s.longitude
from {{ source('iot','devices') }} d
join {{ source('iot','sites') }} s using (site_id)
