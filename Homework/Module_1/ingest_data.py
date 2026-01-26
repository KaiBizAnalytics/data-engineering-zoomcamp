#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

green_parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

zones_dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database")

@click.option("--green-path", default="/data/green_tripdata_2025-11.parquet", help="Green parquet path (in container)")
@click.option("--zones-path", default="/data/taxi_zone_lookup.csv", help="Zones csv path (in container)")

@click.option("--green-table", default="green_taxi_trips", help="Target table name for green trips")
@click.option("--zones-table", default="zones", help="Target table name for zones")

@click.option("--chunksize", default=100000, type=int, help="Chunk size for inserts")
def run(pg_user, pg_pass, pg_host, pg_port, pg_db,
        green_path, zones_path, green_table, zones_table, chunksize):

    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    # ---------------------------
    # 1) Ingest zones (CSV)
    # ---------------------------
    click.echo(f"Loading zones from {zones_path}")

    zones_iter = pd.read_csv(
        zones_path,
        dtype=zones_dtype,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    for df_chunk in tqdm(zones_iter, desc="zones"):
        df_chunk.columns = [c.strip().replace(" ", "_") for c in df_chunk.columns]

        if first:
            df_chunk.head(0).to_sql(name=zones_table, con=engine, if_exists="replace", index=False)
            first = False

        df_chunk.to_sql(name=zones_table, con=engine, if_exists="append", index=False, method="multi")

    # ---------------------------
    # 2) Ingest green trips (Parquet)
    # ---------------------------
    click.echo(f"Loading green trips from {green_path}")

    df = pd.read_parquet(green_path)
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    # Parse datetimes if present
    for c in green_parse_dates:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Create table then append, chunked
    df.head(0).to_sql(name=green_table, con=engine, if_exists="replace", index=False)

    n = len(df)
    for start in tqdm(range(0, n, chunksize), desc="green_trips"):
        df.iloc[start:start+chunksize].to_sql(
            name=green_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi"
        )

    click.echo("Ingestion completed.")


if __name__ == "__main__":
    run()
