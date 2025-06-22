"""
Job 3: Load 3 sets of KPI data from S3 into DynamoDB

Tables:
- genre_kpis_table      ← genre_daily_stats/
- top_songs_table       ← top_songs_per_genre/
- top_genres_table      ← top_genres_per_day/

Each item is inserted with proper primary keys.
Float values are converted to Decimal to meet DynamoDB requirements.
"""

import boto3
import pandas as pd
import pyarrow.parquet as pq
import os, glob, subprocess
from decimal import Decimal

# Base S3 folder for KPIs and local scratch directory
s3_base = "s3://lab3dynamo/processed/kpis/"
local_base = "/tmp/kpi_data"

# Table-to-folder mapping with their handling functions
tables = {
    "genre_kpis_table": {
        "s3_folder": "genre_daily_stats",
        "handler": "handle_genre_kpis"
    },
    "top_songs_table": {
        "s3_folder": "top_songs_per_genre",
        "handler": "handle_top_songs"
    },
    "top_genres_table": {
        "s3_folder": "top_genres_per_day",
        "handler": "handle_top_genres"
    }
}

# Init DynamoDB client
dynamodb = boto3.resource("dynamodb")

def download_data(s3_folder):
    """
    Downloads all files from a given S3 folder to local /tmp.
    """
    path = os.path.join(local_base, s3_folder)
    os.makedirs(path, exist_ok=True)
    s3_path = f"{s3_base}{s3_folder}/"
    print(f"[INFO] Downloading data from {s3_path} to {path}")
    subprocess.run(["aws", "s3", "cp", s3_path, path, "--recursive"], check=True)
    return path

def read_parquet_files(folder):
    """
    Reads all Parquet files from the specified folder into a single DataFrame.
    """
    files = glob.glob(folder + "/*.parquet")
    print(f"[INFO] Found {len(files)} Parquet files.")
    return pd.concat([pq.read_table(f).to_pandas() for f in files])

def handle_genre_kpis(df, table_name):
    """
    Inserts genre-level KPIs into genre_kpis_table.
    """
    print(f"[INFO] Writing genre KPIs to {table_name}")
    table = dynamodb.Table(table_name)
    success = 0

    for _, row in df.iterrows():
        try:
            item = {
                "date": str(row["date"]),
                "track_genre": str(row["track_genre"]),
                "listen_count": int(row["listen_count"]),
                "unique_listeners": int(row["unique_listeners"]),
                "total_listening_time_ms": int(row["total_listening_time_ms"]),
                "avg_listening_time_per_user_ms": Decimal(str(row["avg_listening_time_per_user_ms"]))
            }
            table.put_item(Item=item)
            success += 1
        except Exception as e:
            print(f"[ERROR: genre_kpis_table] Row failed: {row.to_dict()} → {str(e)}")

    print(f"[SUCCESS] {success} genre KPI records inserted.")

def handle_top_songs(df, table_name):
    """
    Inserts top songs data into top_songs_table.
    """
    print(f"[INFO] Writing top songs to {table_name}")
    table = dynamodb.Table(table_name)
    success = 0

    for _, row in df.iterrows():
        try:
            item = {
                "date": str(row["date"]),
                "track_id": str(row["track_id"]),
                "track_name": str(row["track_name"]),
                "track_genre": str(row["track_genre"]),
                "play_count": int(row["play_count"]),
                "rank": int(row["rank"])
            }
            table.put_item(Item=item)
            success += 1
        except Exception as e:
            print(f"[ERROR: top_songs_table] Row failed: {row.to_dict()} → {str(e)}")

    print(f"[SUCCESS] {success} top songs records inserted.")

def handle_top_genres(df, table_name):
    """
    Inserts top genres into top_genres_table.
    """
    print(f"[INFO] Writing top genres to {table_name}")
    table = dynamodb.Table(table_name)
    success = 0

    for _, row in df.iterrows():
        try:
            item = {
                "date": str(row["date"]),
                "track_genre": str(row["track_genre"]),
                "genre_listens": int(row["genre_listens"]),
                "rank": int(row["rank"])
            }
            table.put_item(Item=item)
            success += 1
        except Exception as e:
            print(f"[ERROR: top_genres_table] Row failed: {row.to_dict()} → {str(e)}")

    print(f"[SUCCESS] {success} top genres records inserted.")

# ========== MAIN EXECUTION ==========
print("[INFO] Starting multi-table KPI DynamoDB loader job...")

for table_name, meta in tables.items():
    print(f"\n[STEP] Processing {table_name}")
    try:
        local_path = download_data(meta["s3_folder"])
        df = read_parquet_files(local_path)
        print(f"[INFO] Loaded {len(df)} records for {table_name}")
        handler = globals()[meta["handler"]]
        handler(df, table_name)
    except Exception as e:
        print(f"[FATAL ERROR] Could not process {table_name}: {str(e)}")

print("\n✅ [COMPLETE] All KPI tables processed.")
