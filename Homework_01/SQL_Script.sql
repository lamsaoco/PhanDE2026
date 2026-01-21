-- question 3
select count(1)
from green_taxi
where lpep_pickup_datetime between '2025-11-01' and '2025-12-01'
	and trip_distance <= 1;

-- question 4
select lpep_pickup_datetime, trip_distance
from green_taxi
where trip_distance < 100
order by trip_distance desc
limit 1;

-- question 5
select gt."PULocationID", lc."Zone", SUM(gt.total_amount) as total_amount
from green_taxi gt
	left outer join location_category lc ON gt."PULocationID" = lc."LocationID"
where lpep_pickup_datetime::date = '2025-11-18'
group by gt."PULocationID", lc."Zone"
order by SUM(gt.total_amount) desc
limit 1;

-- question 6
select pu."Zone" as pu_zone, doff."Zone" as doff_zone, gt.tip_amount
from green_taxi gt
	left outer join location_category pu ON gt."PULocationID" = pu."LocationID"
	left outer join location_category doff ON gt."DOLocationID" = doff."LocationID"
where pu."Zone" = 'East Harlem North'
order by gt.tip_amount desc
limit 1;
