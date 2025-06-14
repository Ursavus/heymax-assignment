select user_id
  , date_trunc('month', event_time) as month
  , sum(case when event_type = 'like' then 1 end) as total_likes
  , sum(case when event_type = 'share' then 1 end) as total_shares
from warehouse.fct_engagement
group by user_id, month