select diag_id, claim_id, icd_code, is_primary, chronic_flag
from {{ source('hc','diagnoses') }}
