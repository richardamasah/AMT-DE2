"""
Job 1: Ingest + Validate + Clean + Join

- Loads user, song, and stream data from Glue Catalog
- Validates schema (required columns)
- Cleans bad rows (nulls, invalid values)
- Logs bad records to 'bad-records/' folder by category
- Joins all tables
- Writes cleaned data to 'validated/' S3 folder
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Config
database_name = "lab3"
validated_path = "s3://lab3dynamo1/validated"
bad_base_path = "s3://lab3dynamo1/bad-records"

# Required schemas
required_users = {'user_id', 'user_name', 'user_age', 'user_country', 'created_at'}
required_songs = {'track_id', 'track_name', 'track_genre', 'duration_ms'}
required_stream = {'user_id', 'track_id', 'listen_time'}

def check_columns(df, required, table_name):
    """Check if required columns exist in dataframe"""
    actual = set(df.columns)
    missing = required - actual
    if missing:
        print(f"[ERROR] Missing columns in {table_name}: {missing}")
        raise Exception(f"Schema mismatch in {table_name}")
    print(f"[VALIDATION] {table_name} schema is valid.")

try:
    print("[INFO] Loading tables from Glue Catalog...")
    users = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="users_csv")
    songs = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="songs_csv")
    stream = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="streams1_csv")

    # Convert to Spark DataFrames
    users_df = users.toDF()
    songs_df = songs.toDF()
    stream_df = stream.toDF()

    print("[INFO] Validating schemas...")
    check_columns(users_df, required_users, "users_csv")
    check_columns(songs_df, required_songs, "songs_csv")
    check_columns(stream_df, required_stream, "streams1_csv")

    # Filter bad rows (nulls) per table
    print("[INFO] Filtering null rows...")
    bad_users = users_df.filter("user_id IS NULL OR user_name IS NULL OR user_age IS NULL OR user_country IS NULL OR created_at IS NULL")
    good_users = users_df.dropna(subset=["user_id", "user_name", "user_age", "user_country", "created_at"])

    bad_songs = songs_df.filter("track_id IS NULL OR track_genre IS NULL OR duration_ms IS NULL")
    good_songs = songs_df.dropna(subset=["track_id", "track_genre", "duration_ms"])

    bad_streams = stream_df.filter("user_id IS NULL OR track_id IS NULL OR listen_time IS NULL")
    good_streams = stream_df.dropna(subset=["user_id", "track_id", "listen_time"])

    # Perform joins
    print("[INFO] Joining stream and song data...")
    stream_songs_df = good_streams.join(good_songs, on="track_id", how="inner")

    print("[INFO] Joining with user data...")
    joined_df = stream_songs_df.join(good_users, on="user_id", how="inner")

    # Filter invalid values
    print("[INFO] Filtering invalid values...")
    bad_joined = joined_df.filter((joined_df["duration_ms"] <= 0) | (joined_df["user_age"] <= 0))
    clean_df = joined_df.filter((joined_df["duration_ms"] > 0) & (joined_df["user_age"] > 0))

    # Save validated/clean data
    print("[INFO] Writing cleaned data to S3...")
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(clean_df, glueContext, "cleaned_dyf"),
        connection_type="s3",
        connection_options={"path": validated_path},
        format="parquet"
    )

    # Write bad records separately
    print("[INFO] Writing bad records to S3 folders...")

    if bad_users.count() > 0:
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(bad_users, glueContext, "bad_users"),
            connection_type="s3",
            connection_options={"path": f"{bad_base_path}/users/"},
            format="parquet"
        )

    if bad_songs.count() > 0:
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(bad_songs, glueContext, "bad_songs"),
            connection_type="s3",
            connection_options={"path": f"{bad_base_path}/songs/"},
            format="parquet"
        )

    if bad_streams.count() > 0:
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(bad_streams, glueContext, "bad_streams"),
            connection_type="s3",
            connection_options={"path": f"{bad_base_path}/streams/"},
            format="parquet"
        )

    if bad_joined.count() > 0:
        glueContext.write_dynamic_frame.from_options(
            frame=DynamicFrame.fromDF(bad_joined, glueContext, "bad_joined"),
            connection_type="s3",
            connection_options={"path": f"{bad_base_path}/joined/"},
            format="parquet"
        )

    print("[SUCCESS] Job 1 finished. Cleaned and bad data written successfully.")

except Exception as e:
    print(f"[FAILURE] Job 1 failed: {str(e)}")
    raise

job.commit()
