select transaction_category
  , min(event_time) as first_event_time
  , max(event_time) as last_event_time
from warehouse.public_event_stream
where event_type in ('reward_search', 'miles_earned', 'miles_redeemed')
group by transaction_category