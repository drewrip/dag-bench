{{ config(
     enabled = p11_tuva.tuva_boolean_var('claims_enabled', false)
   )
}}
select *
from {{ ref('pharmacy_claim') }}
