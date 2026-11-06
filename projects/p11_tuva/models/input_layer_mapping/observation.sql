{{ config(
     enabled = p11_tuva.tuva_boolean_var('clinical_enabled', false)
   )
}}

{%- set tuva_columns -%}
      observation_id
    , person_id
    , patient_id
    , encounter_id
    , panel_id
    , observation_date
    , observation_type
    , source_code_type
    , source_code
    , source_description
    , result
    , source_units
    , normalized_units
    , source_reference_range_low
    , source_reference_range_high
    , normalized_reference_range_low
    , normalized_reference_range_high
{%- endset -%}

{%- set tuva_metadata -%}
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_metadata }}
from {{ source('tuva', 'synthetic_data__observation') }}
