import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

# Init

from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Parameters
database_name = "lab3"  # replace with your database name
output_path = "s3://lab3dynamo/validated"  # change to your bucket

# Define required columns
required_users = {'user_id', 'user_name', 'user_age', 'user_country', 'created_at'}
required_songs = {'track_id', 'track_name', 'track_genre', 'duration_ms'}
required_stream = {'user_id', 'track_id', 'listen_time'}

# Load tables
users = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="users_csv")
songs = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="songs_csv")
stream = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name="streams1_csv")

# Convert to DataFrames
users_df = users.toDF()
songs_df = songs.toDF()
stream_df = stream.toDF()

# Validate required columns
def check_columns(df, required, table_name):
    actual = set(df.columns)
    missing = required - actual
    if missing:
        raise Exception(f"[ERROR] Missing columns in {table_name}: {missing}")
    print(f"[SUCCESS] {table_name} has all required columns.")

check_columns(users_df, required_users, "users_csv")
check_columns(songs_df, required_songs, "songs_csv")
check_columns(stream_df, required_stream, "streams1_csv")

# Drop rows with nulls in important columns
users_df = users_df.dropna(subset=["user_id", "user_name", "user_age", "user_country", "created_at"])
songs_df = songs_df.dropna(subset=["track_id", "track_genre", "duration_ms"])
stream_df = stream_df.dropna(subset=["user_id", "track_id", "listen_time"])

# Join stream + songs on track_id
stream_songs_df = stream_df.join(songs_df, on="track_id", how="inner")

# Join with users on user_id
full_df = stream_songs_df.join(users_df, on="user_id", how="inner")

# Optional cleaning
# Remove rows with negative durations or age
full_df = full_df.filter((full_df["duration_ms"] > 0) & (full_df["user_age"] > 0))

# Convert to DynamicFrame
cleaned_dyf = DynamicFrame.fromDF(full_df, glueContext, "cleaned_dyf")

# Write to S3 as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dyf,
    connection_type="s3",
    connection_options={"path": output_path},
    format="Parquet"
)

job.commit()
