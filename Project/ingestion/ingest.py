#!/usr/bin/env python
# coding: utf-8

import os
import json
from pathlib import Path

import pandas as pd
import click
from tqdm.auto import tqdm
from google.cloud import storage

DATASET = "wyattowalsh/basketball"

# Columns and types for game.csv
GAME_DTYPE = {
    "game_id":                  "string",
    "season_id":                "string",
    "team_id_home":             "Int64",
    "team_abbreviation_home":   "string",
    "team_name_home":           "string",
    "wl_home":                  "string",
    "pts_home":                 "Int64",
    "team_id_away":             "Int64",
    "team_abbreviation_away":   "string",
    "team_name_away":           "string",
    "wl_away":                  "string",
    "pts_away":                 "Int64",
}

# Columns and types for game_info.csv (attendance data)
GAME_INFO_DTYPE = {
    "game_id":    "string",
    "attendance": "Int64",
    "game_time":  "string",
}

PARSE_DATES = []  # keep date columns as strings — Spark handles date parsing


def setup_kaggle_creds(username: str, key: str):
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(exist_ok=True)
    creds_file = kaggle_dir / "kaggle.json"
    creds_file.write_text(json.dumps({"username": username, "key": key}))
    creds_file.chmod(0o600)


def upload_to_gcs(local_path: Path, bucket_name: str, blob_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    print(f"Uploading to gs://{bucket_name}/{blob_name} ...")
    blob.upload_from_filename(str(local_path))
    print("Upload complete.")


def find_csv(dest: Path, filename: str) -> Path:
    """Find a CSV file — checks csv/ subdirectory first, then searches recursively."""
    direct = dest / "csv" / filename
    if direct.exists():
        return direct
    matches = list(dest.rglob(filename))
    if not matches:
        raise FileNotFoundError(f"{filename} not found anywhere under {dest}")
    return matches[0]


def read_csv_chunked(csv_path: Path, dtype: dict, chunksize: int) -> pd.DataFrame:
    """Read a CSV in chunks with a tqdm progress bar — course pattern."""
    df_iter = pd.read_csv(
        csv_path,
        dtype=dtype,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunksize,
        low_memory=False,
    )
    chunks = [chunk for chunk in tqdm(df_iter, desc=csv_path.name)]
    return pd.concat(chunks, ignore_index=True)


@click.command()
@click.option('--gcs-bucket',   default='sanguine-mark-366002-nba-lake', help='GCS bucket name')
@click.option('--gcs-prefix',   default='raw',                           help='GCS prefix (folder) for raw data')
@click.option('--kaggle-user',  default=None,                            help='Kaggle username (or set KAGGLE_USERNAME env var)')
@click.option('--kaggle-key',   default=None,                            help='Kaggle API key (or set KAGGLE_KEY env var)')
@click.option('--download-dir', default='./data/raw',                    help='Local directory to download data into')
@click.option('--chunksize',    default=50000, type=int,                 help='Chunk size for reading CSVs')
def run(gcs_bucket, gcs_prefix, kaggle_user, kaggle_key, download_dir, chunksize):
    """Download NBA game + attendance data from Kaggle and upload raw Parquet to GCS."""

    username = kaggle_user or os.environ.get("KAGGLE_USERNAME")
    key      = kaggle_key  or os.environ.get("KAGGLE_KEY")
    if not username or not key:
        raise click.UsageError("Kaggle credentials required via --kaggle-user/--kaggle-key or env vars")

    setup_kaggle_creds(username, key)

    from kaggle.api.kaggle_api_extended import KaggleApi

    dest = Path(download_dir)
    dest.mkdir(parents=True, exist_ok=True)

    # Download dataset from Kaggle
    api = KaggleApi()
    api.authenticate()
    print(f"Downloading {DATASET} → {dest} ...")
    api.dataset_download_files(DATASET, path=str(dest), unzip=True)
    print("Download complete.")

    # ── game.csv ──────────────────────────────────────────────────────────────
    game_csv = find_csv(dest, "game.csv")
    print(f"Found game.csv at {game_csv}")
    df_game = read_csv_chunked(game_csv, GAME_DTYPE, chunksize)
    print(f"  game: {len(df_game):,} rows")

    game_parquet = game_csv.parent / "game.parquet"
    df_game.to_parquet(game_parquet, index=False)
    upload_to_gcs(game_parquet, gcs_bucket, f"{gcs_prefix}/game.parquet")

    # ── game_info.csv (attendance) ────────────────────────────────────────────
    info_csv = find_csv(dest, "game_info.csv")
    print(f"Found game_info.csv at {info_csv}")
    df_info = read_csv_chunked(info_csv, GAME_INFO_DTYPE, chunksize)
    print(f"  game_info: {len(df_info):,} rows")

    info_parquet = info_csv.parent / "game_info.parquet"
    df_info.to_parquet(info_parquet, index=False)
    upload_to_gcs(info_parquet, gcs_bucket, f"{gcs_prefix}/game_info.parquet")

    print("Ingestion complete.")


if __name__ == '__main__':
    run()
