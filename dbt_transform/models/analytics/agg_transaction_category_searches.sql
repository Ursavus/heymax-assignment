select transaction_category
  , date_trunc('month', event_time) as month
  , count(*) as total_search_count
from warehouse.fct_search
group by transaction_category, month