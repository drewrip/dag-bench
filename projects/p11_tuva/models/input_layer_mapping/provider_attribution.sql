{{ config(
     enabled = (
         p11_tuva.tuva_boolean_var('provider_attribution_enabled', false) == True and
         p11_tuva.tuva_boolean_var('claims_enabled', false)
     )
   )
}}

select
      person_id
    , member_id
    , year_month
    , payer
    , {{ p11_tuva.quote_column('plan') }}
    , payer_attributed_provider
    , payer_attributed_provider_practice
    , payer_attributed_provider_organization
    , payer_attributed_provider_lob
    , custom_attributed_provider
    , custom_attributed_provider_practice
    , custom_attributed_provider_organization
    , custom_attributed_provider_lob
    , file_name
    , ingest_datetime
    , data_source
from {{ source('tuva', 'synthetic_data__provider_attribution') }}
