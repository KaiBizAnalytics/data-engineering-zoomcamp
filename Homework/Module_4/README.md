## Question 3
select count(*) from taxi_rides_ny.prod.fct_monthly_zone_revenue

## Question 4
select pickup_zone, sum(revenue_monthly_total_amount) as total_revenue
from taxi_rides_ny.prod.fct_monthly_zone_revenue
where service_type = 'Green' AND year(revenue_month) = 2020
group by pickup_zone
order by total_revenue desc 
limit 1

## Question 5
select sum(total_monthly_trips) as total_trips
from taxi_rides_ny.prod.fct_monthly_zone_revenue
where service_type = 'Green' AND year(revenue_month) = 2019 AND month(revenue_month) = 10

## Question 6
ingest_fhv.py
stg_fhv_tripdata.sql
sources.yml
