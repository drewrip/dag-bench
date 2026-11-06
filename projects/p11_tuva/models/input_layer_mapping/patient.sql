{{ config(
     enabled = p11_tuva.tuva_boolean_var('clinical_enabled', false)
   )
}}

{%- set tuva_columns -%}
      person_id
    , patient_id
    , first_name
    , middle_name
    , last_name
    , name_suffix
    , sex
    , race
    , ethnicity
    , birth_date
    , death_date
    , death_flag
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , county
    , latitude
    , longitude
    , phone
    , email
{%- endset -%}

{%- set tuva_metadata -%}
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_metadata }}
from {{ source('tuva', 'synthetic_data__patient') }}
