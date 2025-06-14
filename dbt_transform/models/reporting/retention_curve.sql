with individual_retention as (
select du.user_id
  , date(join_time) as join_cohort
  , (date(event_time) - date(join_time)) days_from
from warehouse.dim_user du
join warehouse.public_event_stream pes on pes.user_id = du.user_id
group by 1,2,3
),

cohort_size as (
select join_cohort
    , count(distinct user_id) as base_size
from individual_retention
group by 1
)

select distinct ir.join_cohort
  , days_from
  , base_size
  , count(distinct user_id) as returning_user
from individual_retention ir
join cohort_size cs on ir.join_cohort = cs.join_cohort
group by 1,2,3
