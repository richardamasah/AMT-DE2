# scripts/data_validator.py (place in my_music_etl_project/scripts/)

import pandas as pd
import logging
from typing import Union # Import Union for type hinting in Python < 3.10

# Configure logger for this module.
# This logger will output messages to Airflow task logs.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def validate_dataframe(df: pd.DataFrame, expected_columns: dict, df_name: str) -> pd.DataFrame:
    """
    Validates a Pandas DataFrame against a defined schema (expected columns and data types).
    Performs the following checks and operations:
    1. Checks for missing critical columns.
    2. Enforces specified data types, coercing errors.
    3. Drops rows with null values in critical columns after type coercion.

    Args:
        df (pd.DataFrame): The input DataFrame to validate.
        expected_columns (dict): A dictionary where keys are column names and values are desired Pandas dtypes.
        df_name (str): A descriptive name for the DataFrame (used in logging).

    Returns:
        pd.DataFrame: The validated and cleaned DataFrame.

    Raises:
        ValueError: If critical columns are missing from the input DataFrame.
    """
    if df.empty:
        logger.warning(f"DataFrame '{df_name}' is empty after initial load. Skipping validation.")
        return df

    # 1. Check for missing critical columns
    missing_cols = [col for col in expected_columns.keys() if col not in df.columns]
    if missing_cols:
        logger.error(f"DataFrame '{df_name}' is missing critical columns: {missing_cols}")
        raise ValueError(f"Missing critical columns in {df_name}: {missing_cols}")

    # 2. Enforce data types and handle potential conversion errors
    for col, dtype in expected_columns.items():
        if col in df.columns: # Ensure the column exists before attempting conversion
            try:
                if dtype == 'datetime64[ns]':
                    # Convert to datetime format. 'errors='coerce'' will turn invalid dates into NaT (Not a Time).
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif dtype == 'boolean':
                    # Convert to boolean. Handles common string representations (e.g., 'TRUE', '1', 'y', 'yes').
                    df[col] = df[col].astype(str).str.lower().isin(['true', '1', 't', 'yes', 'y'])
                    df[col] = df[col].astype(dtype) # Ensure the final dtype is boolean
                elif 'float' in str(dtype) or 'int' in str(dtype):
                    # Convert to numeric. 'errors='coerce'' will turn invalid numbers into NaN.
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Use nullable integer types like 'Int64' to prevent NaN for integer columns
                    if 'int' in str(dtype) and not pd.isna(df[col]).any():
                        df[col] = df[col].astype('Int64') # Capital 'I' for nullable Int64
                    else:
                        df[col] = df[col].astype(dtype)
                else:
                    # For other types (e.g., 'object' for strings, or other specific types)
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                logger.error(f"Error converting column '{col}' in '{df_name}' to {dtype}: {e}")
                # Depending on your ETL strictness, you might re-raise the exception here
                # or just log the warning and continue processing.

    # 3. Drop rows where critical columns became null (e.g., due to 'coerce' during type conversion)
    initial_rows_count = len(df)
    # Define which columns are 'critical' for dropping nulls.
    # Here, we assume numeric and datetime columns must be non-null for data integrity.
    critical_cols_for_null_check = [col for col in expected_columns.keys() if expected_columns[col] not in ['object', 'boolean']]
    
    # Drop rows where any of the identified critical columns have null values
    df_cleaned = df.dropna(subset=critical_cols_for_null_check)
    dropped_rows_count = initial_rows_count - len(df_cleaned)
    if dropped_rows_count > 0:
        logger.warning(f"Dropped {dropped_rows_count} rows from '{df_name}' due to nulls in critical columns after type coercion.")
    
    logger.info(f"DataFrame '{df_name}' validated. Final rows: {len(df_cleaned)}")
    return df_cleaned

# Define expected schemas for each type of DataFrame.
# These dictionaries specify column names and their expected Pandas data types.
USERS_SCHEMA = {
    'user_id': 'object',           # Matches 'user_id' in your CSV
    'user_name': 'object',         # NEW: Added 'user_name' as it's in your CSV
    'user_age': 'Int64',           # Matches 'user_age' in your CSV
    'user_country': 'object',      # Matches 'user_country' in your CSV
    'created_at': 'datetime64[ns]' # CHANGED: Renamed from 'registration_date' to 'created_at'
}

SONGS_SCHEMA = {
    'id': 'object',                # Original ID from CSV (could be string)
    'track_id': 'object',          # Actual unique track ID (e.g., Spotify ID)
    'track_name': 'object',
    'artists': 'object',
    'album_name': 'object',
    'track_genre': 'object',
    'popularity': 'Int64',
    'duration_ms': 'Int64',
    'explicit': 'boolean',
    'danceability': 'float64',
    'energy': 'float64',
    'key': 'Int64',
    'loudness': 'float64',
    'mode': 'Int64',
    'speechiness': 'float64',
    'acousticness': 'float64',
    'instrumentalness': 'float64',
    'liveness': 'float64',
    'valence': 'float64',
    'tempo': 'float64',
    'time_signature': 'object'
}

STREAM_SCHEMA = {
    'user_id': 'object',
    'track_id': 'object',
    'listen_time': 'datetime64[ns]'
}
