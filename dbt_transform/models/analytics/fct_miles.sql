select event_time
  , user_id
  , transaction_category
  , platform
  , case 
      when event_type = 'miles_earned' then 1
      when event_type = 'miles_redeemed' then -1
    end * NULLIF(miles_amount, '')::float::int as miles_amount
from warehouse.public_event_stream
where event_type in ('miles_earned', 'miles_redeemed')
