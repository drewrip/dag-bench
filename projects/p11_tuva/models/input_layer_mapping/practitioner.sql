{{ config(
     enabled = p11_tuva.tuva_boolean_var('clinical_enabled', false)
   )
}}

select
      practitioner_id
    , npi
    , first_name
    , last_name
    , practice_affiliation
    , specialty
    , sub_specialty
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
    , data_source
from {{ source('tuva', 'synthetic_data__practitioner') }}
