{{ config(
     enabled = p11_tuva.tuva_boolean_var('clinical_enabled', false)
   )
}}

select
      source_procedure_id
    , person_id
    , patient_id
    , encounter_id
    , procedure_date
    , code_system
    , source_code
    , source_description
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , practitioner_id
    , ingest_datetime
    , data_source
from {{ source('tuva', 'synthetic_data__procedure') }}
