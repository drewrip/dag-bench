{{ config(
     enabled = p11_tuva.tuva_boolean_var('clinical_enabled', false)
   )
}}

{%- set tuva_columns -%}
      immunization_id
    , person_id
    , patient_id
    , encounter_id
    , source_code_type
    , source_code
    , source_description
    , status
    , status_reason
    , occurrence_date
    , source_dose
    , lot_number
    , body_site
    , route
    , location_id
    , practitioner_id
{%- endset -%}

{%- set tuva_metadata -%}
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_metadata }}
from {{ source('tuva', 'synthetic_data__immunization') }}
