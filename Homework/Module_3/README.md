# Create External Table
CREATE OR REPLACE EXTERNAL TABLE `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2026_kai/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(*)
FROM `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi_external`;

# Create Regular table
CREATE OR REPLACE TABLE `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi`
AS
SELECT *
FROM `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi_external`;

SELECT COUNT(*)
FROM `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi`;

SELECT
  MIN(tpep_pickup_datetime) AS min_pickup,
  MAX(tpep_pickup_datetime) AS max_pickup
FROM `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi`;




CREATE OR REPLACE EXTERNAL TABLE `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2026_kai/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(*)
FROM `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi_external`;


CREATE OR REPLACE TABLE `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi`
AS
SELECT *
FROM `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi_external`;


SELECT COUNT(*)
FROM `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi`;

SELECT
  MIN(tpep_pickup_datetime) AS min_pickup,
  MAX(tpep_pickup_datetime) AS max_pickup
FROM `sanguine-mark-366002.zoomcamp_hw3.yellow_taxi`;


# Question 1
SELECT COUNT(*) AS cnt
FROM `zoomcamp_hw3.yellow_taxi`;


# Question 2
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu
FROM `zoomcamp_hw3.yellow_taxi`;  

SELECT COUNT(DISTINCT PULocationID) AS distinct_pu
FROM `zoomcamp_hw3.yellow_taxi_external`;  


# Question 3
SELECT PULocationID
FROM `zoomcamp_hw3.yellow_taxi`;

SELECT PULocationID, DOLocationID
FROM `zoomcamp_hw3.yellow_taxi`;


# Question 4
SELECT COUNT(*) AS zero_fare_cnt
FROM `zoomcamp_hw3.yellow_taxi`
WHERE fare_amount = 0;


# Question 5
CREATE OR REPLACE TABLE `zoomcamp_hw3.yellow_taxi_part_clust`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `zoomcamp_hw3.yellow_taxi`;


# Question 6
SELECT COUNT(DISTINCT VendorID) AS distinct_vendors
FROM `zoomcamp_hw3.yellow_taxi`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime <  '2024-03-16';


SELECT COUNT(DISTINCT VendorID) AS distinct_vendors
FROM `zoomcamp_hw3.yellow_taxi_part_clust`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime <  '2024-03-16';


# Question 9
SELECT COUNT(*) 
FROM `zoomcamp_hw3.yellow_taxi_part_clust`;

-- 0 bytes are estimated because BigQuery can answer COUNT(*) using table metadata (row counts stored in storage statistics) without scanning any data for materialized table.










