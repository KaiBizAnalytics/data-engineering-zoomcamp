## Q1

docker compose down -v
docker compose build
docker compose up -d

docker exec -it module_7-redpanda-1  rpk version
rpk version: v25.3.9
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

## Q2
took 7.08 seconds


## Q3
total messages read: 49416
trip_distance > 5: 8506

## Q4
docker exec -it module_7-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_pu_location.py
docker exec -it module_7-postgres-1 psql -U postgres -d postgres -c "SELECT PULocationID, num_trips FROM green_trip_window_counts ORDER BY num_trips DESC LIMIT 3;"

## Q5
docker exec -it module_7-jobmanager-1 flink run -py /opt/src/job/q5_session_longest_streak.py
docker exec -it module_7-postgres-1 psql -U postgres -d postgres -c "SELECT PULocationID, num_trips FROM green_trip_session_counts ORDER BY num_trips DESC LIMIT 5;"
 pulocationid | num_trips 
--------------+-----------
           74 |        81
           74 |        72
           74 |        69
           74 |        56
           74 |        54

## Q6
docker exec -it module_7-postgres-1 psql -U postgres -d postgres -c "SELECT window_start, total_tip_amount FROM green_trip_hourly_tips ORDER BY total_tip_amount DESC LIMIT 5;"
    window_start     |  total_tip_amount  
---------------------+--------------------
 2025-10-16 18:00:00 |  510.8599999999999
 2025-10-30 16:00:00 |             494.41
 2025-10-09 18:00:00 | 472.01000000000016
 2025-10-10 17:00:00 |  470.0800000000002
 2025-10-16 17:00:00 | 445.01000000000005
