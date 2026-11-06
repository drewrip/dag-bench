{{ config(
     enabled = p11_tuva.tuva_boolean_var('claims_enabled', false)
   )
}}

{%- set tuva_columns -%}
      claim_id
    , claim_line_number
    , person_id
    , member_id
    , payer
    , {{ p11_tuva.quote_column('plan') }}
    , prescribing_provider_npi
    , dispensing_provider_npi
    , dispensing_date
    , ndc_code
    , quantity
    , days_supply
    , refills
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , in_network_flag
{%- endset -%}

{# Extension columns for testing passthrough to core.pharmacy_claim #}
{%- set tuva_metadata -%}
    , data_source
    , file_date
    , file_name
    , ingest_datetime
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_metadata }}
from {{ source('tuva', 'synthetic_data__pharmacy_claim') }}
