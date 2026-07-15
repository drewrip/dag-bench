select purchase_id, player_id, purchase_ts, item_type, item_name, price_usd, is_refunded,
    not is_refunded as is_valid
from {{ source('game','purchases') }}
