with total_user_cte as (
select date_trunc('month', event_time) as month
  , count(distinct user_id) as total_user_count
from warehouse.public_event_stream
group by 1
),

miles_user_cte as (
select date_trunc('month', event_time) as month
  , count(distinct user_id) as miles_user_count
  , count(distinct case when miles_amount < 0 then user_id end) as redeeming_user_count
  , count(distinct case when miles_amount > 0 then user_id end) as earning_user_count
from warehouse.fct_miles
group by 1
),

searching_user_cte as (
select date_trunc('month', event_time) as month
  , count(distinct user_id) as searching_user_count
from warehouse.fct_search
group by 1
),

engaging_user_cte as (
select date_trunc('month', event_time) as month
  , count(distinct user_id) as engaging_user_count
  , count(distinct case when event_type = 'like' then user_id end) as liking_user_count
  , count(distinct case when event_type = 'share' then user_id end) as sharing_user_count
from warehouse.fct_engagement
group by 1
)

select *
from total_user_cte
left join miles_user_cte using (month)
left join searching_user_cte using (month)
left join engaging_user_cte using (month)