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
    description: Pickup datetime
  - name: dropoff_datetime
    type: timestamp
    description: Dropoff datetime
  - name: passenger_count
    type: integer
    description: Number of passengers
  - name: trip_distance
    type: double
    description: Trip distance (miles)
  - name: payment_type
    type: integer
    description: Payment type id
  - name: taxi_type
    type: string
    description: Taxi type (yellow/green)
  - name: extracted_at
    type: timestamp
    description: Ingestion extraction timestamp

@bruin"""

import os
import json
import io
import datetime
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta


def _month_range(start_date, end_date):
    cur = start_date.replace(day=1)
    while cur < end_date:
        yield cur.year, cur.month
        cur += relativedelta(months=1)


def _find_and_rename(df, candidates, target):
    for c in candidates:
        if c in df.columns:
            if c != target:
                df = df.rename(columns={c: target})
            return df
    return df


def materialize():
    """Fetch parquet files from the TLC public endpoint and return a concatenated DataFrame.

    Uses BRUIN_START_DATE / BRUIN_END_DATE and BRUIN_VARS to determine taxi types and run window.
    Keeps data raw; staging layer handles dedup/cleanup.
    """
    start = os.environ.get("BRUIN_START_DATE")
    end = os.environ.get("BRUIN_END_DATE")
    if not start or not end:
        raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set by the pipeline runtime")

    start_date = datetime.datetime.fromisoformat(start).date()
    end_date = datetime.datetime.fromisoformat(end).date()

    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    try:
        vars_json = json.loads(bruin_vars)
    except Exception:
        vars_json = {}

    taxi_types = vars_json.get("taxi_types", ["yellow"])

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    frames = []
    extracted_at = datetime.datetime.utcnow()

    for taxi in taxi_types:
        for year, month in _month_range(start_date, end_date):
            filename = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
            url = f"{base_url}/{filename}"
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code != 200:
                    # skip missing months
                    continue
                data = resp.content
                df = pd.read_parquet(io.BytesIO(data), engine="pyarrow")
            except Exception:
                # skip problematic files but don't fail the whole run
                continue

            # normalize common column names
            df = _find_and_rename(df, ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"], "pickup_datetime")
            df = _find_and_rename(df, ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"], "dropoff_datetime")
            df = _find_and_rename(df, ["payment_type", "payment_type_id", "paymenttype"], "payment_type")

            # add taxi_type and extracted_at
            df["taxi_type"] = taxi
            df["extracted_at"] = extracted_at

            # ensure datetimes parsed
            if "pickup_datetime" in df.columns:
                df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
            if "dropoff_datetime" in df.columns:
                df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

            frames.append(df)

    if not frames:
        # return empty dataframe with expected columns so materialization can create table schema
        return pd.DataFrame(columns=["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "payment_type", "taxi_type", "extracted_at"])

    out = pd.concat(frames, ignore_index=True, sort=False)

    # select a stable subset of columns if available
    preferred = ["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "fare_amount", "payment_type", "taxi_type", "extracted_at"]
    cols = [c for c in preferred if c in out.columns]
    return out[cols]
