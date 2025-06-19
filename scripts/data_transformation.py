# scripts/data_transformation.py (place in my_music_etl_project/scripts/)

import pandas as pd
import logging
from datetime import datetime
import os
from typing import Union, Tuple # Import Union and Tuple for type hinting in Python < 3.10

# Import functions from other local scripts.
# These imports work because 'scripts/' is in the Python path.
from extract_load_s3 import extract_csv_from_s3, upload_dataframe_to_s3_parquet
from data_validator import validate_dataframe, USERS_SCHEMA, SONGS_SCHEMA, STREAM_SCHEMA
from redshift_loader import get_redshift_connection, load_data_to_redshift_from_s3

# Configure logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Retrieve environment variables for S3 paths
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_STATIC_KEYS_PREFIX = os.getenv('S3_STATIC_KEYS_PREFIX')
S3_STREAM_KEYS_PREFIX = os.getenv('S3_STREAM_KEYS_PREFIX')
S3_PROCESSED_PREFIX = os.getenv('S3_PROCESSED_PREFIX')

def load_static_data_to_redshift_task():
    """
    Airflow task (python_callable) to extract, validate, and load static users and songs data to Redshift.
    This function is intended for a one-time initial load.
    """
    logger.info("Starting initial load of static data to Redshift.")
    users_s3_key = f"{S3_STATIC_KEYS_PREFIX}users.csv"
    songs_s3_key = f"{S3_STATIC_KEYS_PREFIX}songs.csv"

    # Extract static data from S3
    users_df = extract_csv_from_s3(users_s3_key)
    songs_df = extract_csv_from_s3(songs_s3_key)

    if users_df is None or songs_df is None:
        raise ValueError("Static users or songs data not found in S3. Please ensure they are uploaded.")

    # Validate extracted data against predefined schemas
    validated_users_df = validate_dataframe(users_df, USERS_SCHEMA, "users")
    validated_songs_df = validate_dataframe(songs_df, SONGS_SCHEMA, "songs")

    # Ensure 'id' in songs_df is renamed to 'song_id' to match Redshift schema
    if 'id' in validated_songs_df.columns:
        validated_songs_df = validated_songs_df.rename(columns={'id': 'song_id'})
    
    # Ensure 'track_id' is present in songs_df, fallback to 'song_id' if only 'id' was in CSV
    if 'track_id' not in validated_songs_df.columns:
        # Assuming that if track_id is not explicit, it should be the same as song_id
        validated_songs_df['track_id'] = validated_songs_df['song_id'] 

    # Upload validated DataFrames to S3 as Parquet files for Redshift COPY command
    users_parquet_key = f"{S3_PROCESSED_PREFIX}users_dim/users_dim.parquet"
    songs_parquet_key = f"{S3_PROCESSED_PREFIX}songs_dim/songs_dim.parquet"
    upload_dataframe_to_s3_parquet(validated_users_df, users_parquet_key)
    upload_dataframe_to_s3_parquet(validated_songs_df, songs_parquet_key)

    # Load data from S3 Parquet files into Redshift dimension tables
    load_data_to_redshift_from_s3("users_dim", f"{S3_PROCESSED_PREFIX}users_dim/", primary_key_cols=['user_id'])
    load_data_to_redshift_from_s3("songs_dim", f"{S3_PROCESSED_PREFIX}songs_dim/", primary_key_cols=['song_id'])

    logger.info("Initial load of static data to Redshift completed.")

def process_stream_data_task(stream_file_name: str):
    """
    Airflow task (python_callable) to process a single stream file.
    Extracts, validates, transforms, computes KPIs, and loads to Redshift.
    """
    logger.info(f"Starting processing for stream file: {stream_file_name}")

    stream_s3_key = f"{S3_STREAM_KEYS_PREFIX}{stream_file_name}"

    # Extract Stream Data from S3
    stream_df = extract_csv_from_s3(stream_s3_key)

    if stream_df is None or stream_df.empty:
        logger.warning(f"No data or empty data in {stream_file_name}. Skipping transformation and load.")
        return # Gracefully exit if no data to process

    # Validate Stream Data
    validated_stream_df = validate_dataframe(stream_df, STREAM_SCHEMA, "stream")
    if validated_stream_df.empty:
        logger.warning(f"Stream data from {stream_file_name} became empty after validation. Skipping.")
        return

    # Read Dimension Data from Redshift (for joining and enrichment)
    users_dim_df, songs_dim_df = read_dimensions_from_redshift()
    if users_dim_df.empty or songs_dim_df.empty:
        raise ValueError("Dimension tables (users_dim, songs_dim) are empty in Redshift. Ensure static data is loaded.")

    # Transformation & KPI Computation
    genre_kpis_df, hourly_kpis_df = transform_and_compute_kpis(
        validated_stream_df, users_dim_df, songs_dim_df
    )

    if genre_kpis_df.empty and hourly_kpis_df.empty:
        logger.info("No KPIs computed for this stream batch. Exiting task.")
        return

    # Upload computed KPIs to S3 as Parquet files
    timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
    
    if not genre_kpis_df.empty:
        genre_kpis_parquet_key = f"{S3_PROCESSED_PREFIX}genre_kpis/genre_kpis_{timestamp_str}.parquet"
        upload_dataframe_to_s3_parquet(genre_kpis_df, genre_kpis_parquet_key)
        # Load to Redshift
        load_data_to_redshift_from_s3("genre_kpis_fact", f"{S3_PROCESSED_PREFIX}genre_kpis/", primary_key_cols=['track_genre'])
    else:
        logger.info("No genre KPIs to load.")

    if not hourly_kpis_df.empty:
        hourly_kpis_parquet_key = f"{S3_PROCESSED_PREFIX}hourly_kpis/hourly_kpis_{timestamp_str}.parquet"
        upload_dataframe_to_s3_parquet(hourly_kpis_df, hourly_kpis_parquet_key)
        # Load to Redshift
        load_data_to_redshift_from_s3("hourly_kpis_fact", f"{S3_PROCESSED_PREFIX}hourly_kpis/", primary_key_cols=['listen_date', 'listen_hour'])
    else:
        logger.info("No hourly KPIs to load.")

    logger.info(f"Processing for stream file {stream_file_name} completed successfully.")

def read_dimensions_from_redshift():
    """Reads users_dim and songs_dim from Redshift into Pandas DataFrames."""
    conn = None
    try:
        conn = get_redshift_connection()
        # Ensure that pandas is able to read from the psycopg2 connection
        users_dim_df = pd.read_sql("SELECT * FROM users_dim", conn)
        songs_dim_df = pd.read_sql("SELECT * FROM songs_dim", conn)
        logger.info(f"Read {len(users_dim_df)} rows from users_dim and {len(songs_dim_df)} rows from songs_dim from Redshift.")
        return users_dim_df, songs_dim_df
    except Exception as e:
        logger.error(f"Error reading dimension tables from Redshift: {e}")
        raise # Re-raise the exception to propagate the error
    finally:
        if conn:
            conn.close()

def transform_and_compute_kpis(
    stream_df: pd.DataFrame,
    users_df: pd.DataFrame, # Will come from Redshift for incremental runs
    songs_df: pd.DataFrame  # Will come from Redshift for incremental runs
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transforms stream data, merges with dimensions, and computes KPIs using Pandas.
    Returns genre_kpis_df and hourly_kpis_df.
    """
    if stream_df.empty:
        logger.warning("Stream DataFrame is empty. Skipping transformation and KPI computation.")
        return pd.DataFrame(), pd.DataFrame() # Return empty DataFrames to avoid downstream errors

    logger.info("Starting data transformation and KPI computation...")

    # Merge stream with users_dim on 'user_id'
    merged_df = pd.merge(stream_df, users_df, on='user_id', how='inner')
    
    # Merge with songs_dim using 'track_id' from stream and 'track_id' from songs_dim
    # We assume songs_dim now contains 'track_id' based on initial load logic.
    if 'track_id' not in songs_df.columns:
        logger.warning("Songs dimension table does not have 'track_id' column for joining. This might lead to data loss if 'track_id' is the correct key.")
        # Fallback to 'song_id' from songs_df if 'track_id' is missing.
        # This part should ideally not be hit if initial load maps 'id' to 'track_id' correctly.
        final_df = pd.merge(merged_df, songs_df, left_on='track_id', right_on='song_id', how='inner', suffixes=('_user', '_song'))    
    else:
        final_df = pd.merge(merged_df, songs_df, on='track_id', how='inner', suffixes=('_user', '_song'))

    logger.info(f"Merged data count: {len(final_df)}")

    # Convert 'listen_time' to datetime if not already (validation should handle this)
    if not pd.api.types.is_datetime64_any_dtype(final_df['listen_time']):
        final_df['listen_time'] = pd.to_datetime(final_df['listen_time'], errors='coerce')
        final_df.dropna(subset=['listen_time'], inplace=True) # Drop rows where conversion failed

    # Add an hourly column for hourly KPIs
    final_df['listen_hour'] = final_df['listen_time'].dt.hour
    final_df['listen_date'] = final_df['listen_time'].dt.date # For hourly KPIs primary key


    # --- KPI Computations ---

    logger.info("Computing Genre-Level KPIs...")
    genre_kpis_df = final_df.groupby('track_genre').agg(
        listen_count=('track_id', 'count'),
        avg_track_duration_ms=('duration_ms', 'mean'),
        genre_popularity_index=('popularity', 'mean')
    ).reset_index()

    # Most Popular Track per Genre
    # Use idxmax to find the index of the max popularity within each genre group
    idx_max_popularity = final_df.loc[final_df.groupby('track_genre')['popularity'].idxmax()]
    most_popular_track_per_genre = idx_max_popularity[[
        'track_genre', 'track_name', 'artists', 'popularity' # FIX: Removed _song suffix from 'track_name' and 'artists'
    ]].rename(columns={
        'track_genre': 'track_genre',
        'track_name': 'most_popular_track_name', # FIX: Renamed
        'artists': 'most_popular_track_artists',  # FIX: Renamed
        'popularity': 'most_popular_track_popularity'
    })

    # Merge most popular track info back to genre KPIs
    genre_kpis_df = pd.merge(
        genre_kpis_df,
        most_popular_track_per_genre,
        on='track_genre',
        how='left'
    )
    genre_kpis_df['last_updated'] = datetime.now()


    logger.info("Genre KPIs computed.")

    logger.info("Computing Hourly KPIs...")
    hourly_kpis_df = final_df.groupby(['listen_date', 'listen_hour']).agg(
        unique_listeners=('user_id', 'nunique'),
        total_plays=('track_id', 'count') # Temporary for diversity index calculation
    ).reset_index()

    # Top Artists per Hour
    # FIX: Changed 'artists_song' to 'artists' here as well for consistency
    artists_hourly_plays = final_df.groupby(['listen_date', 'listen_hour', 'artists']).agg(
        artist_hourly_plays=('track_id', 'count')
    ).reset_index()
    # Rank artists within each hour based on plays
    artists_hourly_plays['rank'] = artists_hourly_plays.groupby(['listen_date', 'listen_hour'])['artist_hourly_plays'].rank(method='max', ascending=False)
    # Select only the top-ranked artist(s)
    top_artists_hourly = artists_hourly_plays[artists_hourly_plays['rank'] == 1].drop(columns='rank')
    top_artists_hourly = top_artists_hourly.rename(columns={
        'artists': 'top_artist_per_hour', # FIX: Renamed
        'artist_hourly_plays': 'top_artist_plays'
    })

    # Merge top artist info back to hourly KPIs
    hourly_kpis_df = pd.merge(
        hourly_kpis_df,
        top_artists_hourly[['listen_date', 'listen_hour', 'top_artist_per_hour', 'top_artist_plays']],
        on=['listen_date', 'listen_hour'],
        how='left'
    )

    # Track Diversity Index: unique tracks / total plays per hour
    hourly_unique_tracks = final_df.groupby(['listen_date', 'listen_hour']).agg(
        unique_tracks_count_hourly=('track_id', 'nunique')
    ).reset_index()

    # Merge unique tracks count to hourly KPIs
    hourly_kpis_df = pd.merge(
        hourly_kpis_df,
        hourly_unique_tracks,
        on=['listen_date', 'listen_hour'],
        how='left'
    )
    # Calculate diversity index
    hourly_kpis_df['track_diversity_index'] = hourly_kpis_df['unique_tracks_count_hourly'] / hourly_kpis_df['total_plays']
    # Clean up temporary columns
    hourly_kpis_df = hourly_kpis_df.drop(columns=['total_plays', 'unique_tracks_count_hourly'])
    
    hourly_kpis_df['last_updated'] = datetime.now()

    logger.info("Hourly KPIs computed.")
    logger.info("Transformation and KPI computation complete.")
    return genre_kpis_df, hourly_kpis_df
