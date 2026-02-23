"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime, timezone

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"

def _month_starts(start: pd.Timestamp, end: pd.Timestamp):
    """
    Yield the first day of each month that overlaps [start, end).
    Example: start=2022-01-01, end=2022-03-01 -> 2022-01-01, 2022-02-01
    """
    cur = pd.Timestamp(start.year, start.month, 1, tz=start.tz)
    while cur < end:
        yield cur
        cur = (cur + pd.offsets.MonthBegin(1)).normalize()

def materialize():
    # Bruin provides these as ISO strings, typically with Z (UTC)
    start_date = pd.to_datetime(os.environ["BRUIN_START_DATE"], utc=True)
    end_date = pd.to_datetime(os.environ["BRUIN_END_DATE"], utc=True)

    vars_json = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_json.get("taxi_types", ["yellow"])

    dfs = []

    for taxi_type in taxi_types:
        for m in _month_starts(start_date, end_date):
            url = BASE_URL.format(taxi_type=taxi_type, year=m.year, month=m.month)

            # Read parquet directly from URL (requires pyarrow installed)
            df = pd.read_parquet(url, engine="pyarrow")

            rename_map = {}

            # location IDs
            if "PULocationID" in df.columns:
                rename_map["PULocationID"] = "pickup_location_id"
            if "DOLocationID" in df.columns:
                rename_map["DOLocationID"] = "dropoff_location_id"

            # (optional) if vendor-specific naming shows up
            if "pulocationid" in df.columns:
                rename_map["pulocationid"] = "pickup_location_id"
            if "dolocationid" in df.columns:
                rename_map["dolocationid"] = "dropoff_location_id"

            df = df.rename(columns=rename_map)

            # Standardize datetime columns across taxi types
            # Yellow/green usually: tpep_pickup_datetime / lpep_pickup_datetime
            # We'll map to pickup_datetime/dropoff_datetime expected by your asset schema
            if "tpep_pickup_datetime" in df.columns:
                df = df.rename(columns={
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                })
            elif "lpep_pickup_datetime" in df.columns:
                df = df.rename(columns={
                    "lpep_pickup_datetime": "pickup_datetime",
                    "lpep_dropoff_datetime": "dropoff_datetime",
                })

            # Ensure timezone-aware timestamps (UTC)
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], utc=True, errors="coerce")
            df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], utc=True, errors="coerce")

            # Filter to interval [start_date, end_date)
            df = df[(df["pickup_datetime"] >= start_date) & (df["pickup_datetime"] < end_date)]

            # Add taxi_type for lineage/debugging (optional but useful)
            df["taxi_type"] = taxi_type

            dfs.append(df)

    # IMPORTANT: always define what you return
    if not dfs:
        # Return an empty dataframe with expected columns (helps materialization not crash)
        return pd.DataFrame(columns=[
            "pickup_datetime","dropoff_datetime",
            "pickup_location_id","dropoff_location_id",
            "fare_amount","payment_type","taxi_type"
        ])

    final_dataframe = pd.concat(dfs, ignore_index=True)

    # Optional: drop rows with missing critical timestamps
    final_dataframe = final_dataframe.dropna(subset=["pickup_datetime", "dropoff_datetime"])

    return final_dataframe