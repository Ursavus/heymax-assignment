with unique_week_user as (
select distinct date_trunc('day', event_time) as date
  , user_id
  , date_trunc('day', du.join_time) as join_date
from warehouse.public_event_stream fm
join warehouse.dim_user du using (user_id)
),

user_status as (
select *
  , case 
      when date = join_date then 'NEW'
      when EXTRACT(EPOCH FROM (date - lag(date) over (partition by user_id order by date)))/86400 <= 1 then 'RETAINED'
      when EXTRACT(EPOCH FROM (date - lag(date) over (partition by user_id order by date)))/86400 > 1 then 'RESURRECTED'
    end as status
from unique_week_user
),

daily_users as (
select date
  , count(distinct case when status = 'NEW' then user_id end) as new_users
  , count(distinct case when status = 'RETAINED' then user_id end) as retained_users
  , count(distinct case when status = 'RESURRECTED' then user_id end) as resurrected_users
from user_status
group by date
)

select *
  , sum(new_users) over (order by date) - new_users - retained_users - resurrected_users as churned_users
from daily_users