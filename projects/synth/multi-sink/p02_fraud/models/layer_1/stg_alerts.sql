select alert_id, txn_id, alert_type, severity, created_ts, resolved, resolution,
    severity='critical' as is_critical,
    resolved and resolution='confirmed_fraud' as is_confirmed_fraud
from {{ source('src','alerts') }}
