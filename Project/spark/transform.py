#!/usr/bin/env python
# coding: utf-8

"""
Spark Transform: Clean, enrich, and join NBA game + attendance data.

Steps:
  1. Read raw game.parquet and game_info.parquet locally
  2. Filter to regular season, add season_year / home_win / is_bubble_season
  3. Join attendance from game_info
  4. Write processed Parquet locally, then upload to GCS

Usage:
    python spark/transform.py [OPTIONS]
"""

import os
from pathlib import Path

import click
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import storage

DEFAULT_GAME_INPUT  = "./data/raw/csv/game.parquet"
DEFAULT_INFO_INPUT  = "./data/raw/csv/game_info.parquet"
DEFAULT_OUTPUT      = "./data/processed/game_cleaned.parquet"
GCS_PREFIX          = "processed"


def upload_to_gcs(local_path: Path, bucket_name: str, blob_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    print(f"Uploading to gs://{bucket_name}/{blob_name} ...")
    blob.upload_from_filename(str(local_path))
    print("Upload complete.")


@click.command()
@click.option('--game-input',  default=DEFAULT_GAME_INPUT,  help='Local path to raw game.parquet')
@click.option('--info-input',  default=DEFAULT_INFO_INPUT,  help='Local path to raw game_info.parquet')
@click.option('--output-path', default=DEFAULT_OUTPUT,      help='Local path to write processed Parquet')
@click.option('--gcs-bucket',  default='sanguine-mark-366002-nba-lake', help='GCS bucket for upload')
def run(game_input, info_input, output_path, gcs_bucket):
    """Transform NBA game + attendance data with PySpark and upload to GCS."""

    spark = (
        SparkSession.builder
        .appName("NBA Home Court Advantage - Transform")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Read ──────────────────────────────────────────────────────────────────
    print(f"Reading game data from {game_input}")
    df_game = spark.read.parquet(game_input)
    print(f"  Raw game rows: {df_game.count():,}")

    print(f"Reading attendance data from {info_input}")
    df_info = spark.read.parquet(info_input).select(
        "game_id",
        F.col("attendance").cast("integer").alias("attendance"),
    )

    # ── Transform game data ───────────────────────────────────────────────────
    df = df_game.select(
        "game_id",
        F.to_date("game_date").alias("game_date"),
        F.col("season_id").cast("string").alias("season_id"),
        "team_id_home",
        "team_abbreviation_home",
        "team_name_home",
        "wl_home",
        F.col("pts_home").cast("integer").alias("pts_home"),
        "team_id_away",
        "team_abbreviation_away",
        "team_name_away",
        "wl_away",
        F.col("pts_away").cast("integer").alias("pts_away"),
    )

    # Regular season only (season_id starts with "2")
    df = df.filter(F.col("season_id").startswith("2"))

    # Extract season_year from last 4 digits of season_id (e.g. "22019" → 2019)
    df = df.withColumn("season_year", F.substring("season_id", -4, 4).cast("integer"))

    # Home win flag
    df = df.withColumn("home_win", F.when(F.col("wl_home") == "W", 1).otherwise(0))

    # COVID bubble flag: 2019-20 season played with zero fans at Disney World
    df = df.withColumn(
        "is_bubble_season",
        F.when(F.col("season_year") == 2019, True).otherwise(False),
    )

    # Drop rows missing critical fields
    df = df.dropna(subset=["game_date", "wl_home", "team_abbreviation_home"])

    # ── Join attendance ───────────────────────────────────────────────────────
    df = df.join(df_info, on="game_id", how="left")

    print(f"  Processed rows: {df.count():,}")

    # ── Write locally via pandas (avoids winutils on Windows) ─────────────────
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_parquet(str(out), index=False)
    print(f"Saved Parquet → {out}")

    spark.stop()

    # ── Upload to GCS ─────────────────────────────────────────────────────────
    upload_to_gcs(out, gcs_bucket, f"{GCS_PREFIX}/game_cleaned.parquet")


if __name__ == '__main__':
    run()
