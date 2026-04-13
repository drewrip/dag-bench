select provider_id, name as provider_name, specialty, state as prov_state, is_in_network, npi
from {{ source('hc','providers') }}
