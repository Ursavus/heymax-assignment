select event_time
  , user_id
  , platform
  , transaction_category
from warehouse.public_event_stream
where event_type in ('reward_search')