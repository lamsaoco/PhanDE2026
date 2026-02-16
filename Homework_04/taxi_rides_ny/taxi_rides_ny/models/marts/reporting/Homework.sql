-- Question 4
select * 
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green' and revenue_month >= '2020-01-01' and revenue_month <= '2020-12-31'
order by revenue_monthly_total_amount desc
limit 1

-- Question 5
select sum(total_monthly_trips) as total_trips
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green' and revenue_month >= '2019-10-01' and revenue_month <= '2019-10-31'