select event_time
  , user_id
  , platform
  , event_type
from warehouse.public_event_stream
where event_type in ('like', 'share')