with ranked_cte as (
select user_id
    , event_time as join_time
    , utm_source
    , country as origin_country
    , platform as first_platform
    , rank() over (partition by user_id order by event_time) as rank
from warehouse.public_event_stream
)

select user_id
    , join_time
    , utm_source
    , origin_country
    , first_platform
from ranked_cte
where rank = 1