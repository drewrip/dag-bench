{{ config(
     enabled = p11_tuva.tuva_boolean_var('clinical_enabled', false)
   )
}}

select
      location_id
    , npi
    , name
    , facility_type
    , parent_organization
    , address
    , city
    , state
    , zip_code
    , latitude
    , longitude
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
    , data_source
from {{ source('tuva', 'synthetic_data__location') }}
