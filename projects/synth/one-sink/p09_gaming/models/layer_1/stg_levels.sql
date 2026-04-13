select level_id, level_name, world, difficulty, par_time_sec,
    reward_coins, unlock_level
from {{ source('game','levels') }}
