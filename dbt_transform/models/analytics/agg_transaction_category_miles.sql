select transaction_category
  , date_trunc('month', event_time) as month
  , sum(case when miles_amount > 0 then 1 end) as total_earn_count
  , sum(case when miles_amount < 0 then 1 end) as total_redemption_count
  , sum(case when miles_amount > 0 then miles_amount end) as total_miles_earned
  , sum(case when miles_amount < 0 then miles_amount end) as total_miles_redeemed
from warehouse.fct_miles
group by transaction_category, month