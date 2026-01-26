#Question 1
> docker run -it --rm --entrypoint bash python:3.13
> root@99ee8827ac50:/# pip --version
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)

#Question 2
db:5432

#Question 3
> mkdir data
> cd data
> wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

> docker compose build ingest
> docker compose run --rm ingest
> docker compose up -d

SELECT COUNT(*) AS short_trip_count
FROM public.green_trips_2025_11
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1;
--answer: 8,007

#Question 4
SELECT
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS max_trip_distance
FROM public.green_trips_2025_11
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_trip_distance DESC
LIMIT 1;
--answer: 2025-11-14 88.03

#Question 5
SELECT
    z."Zone" AS pickup_zone,
    SUM(t."total_amount") AS total_amount_sum
FROM public.green_trips_2025_11 t
JOIN public.zones z
  ON t."PULocationID" = z."LocationID"
WHERE t."lpep_pickup_datetime" >= '2025-11-18'
  AND t."lpep_pickup_datetime" <  '2025-11-19'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;
--answer: East Harlem North 9281.919999999996

#Question 6
SELECT 
	z_do."Zone" AS dropoff_zone,
	MAX(t."tip_amount") AS max_tip
FROM public.green_trips_2025_11 t
JOIN public.zones z_pu
  ON t."PULocationID" = z_pu."LocationID"
JOIN public.zones z_do
  ON t."DOLocationID" = z_do."LocationID"
WHERE z_pu."Zone" = 'East Harlem North'
 AND t."lpep_pickup_datetime" >= '2025-11-01'
 AND t."lpep_pickup_datetime" <  '2025-12-01'
GROUP BY z_do."Zone"
ORDER BY max_tip DESC
LIMIT 1;
--answer: "Yorkville West"	81.89
