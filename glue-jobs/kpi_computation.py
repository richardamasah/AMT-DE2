"""
Job 2: Compute Daily Genre-Level KPIs from cleaned stream data
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, count, countDistinct, sum, avg, row_number, to_date
from pyspark.sql.window import Window

# Init
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Paths
input_path = "s3://lab3dynamo/validated/"
output_base = "s3://lab3dynamo/processed/kpis/"

try:
    print("[INFO] Reading validated stream data...")
    df = spark.read.parquet(input_path)

    print("[INFO] Extracting date from listen_time...")
    df = df.withColumn("date", to_date("listen_time"))

    # ========== 🎧 1. Genre-Level Daily KPIs ==========
    print("[INFO] Calculating genre-level daily KPIs...")

    genre_kpis = df.groupBy("date", "track_genre").agg(
        count("*").alias("listen_count"),
        countDistinct("user_id").alias("unique_listeners"),
        sum("duration_ms").alias("total_listening_time_ms")
    ).withColumn(
        "avg_listening_time_per_user_ms",
        col("total_listening_time_ms") / col("unique_listeners")
    )

    genre_kpis.write.mode("overwrite").parquet(output_base + "genre_daily_stats/")
    print("[OK] Genre KPIs written to genre_daily_stats/")

    # ========== 🏆 2. Top 3 Songs per Genre per Day ==========
    print("[INFO] Calculating top 3 songs per genre per day...")

    song_play_counts = df.groupBy("date", "track_genre", "track_name") \
        .agg(count("*").alias("play_count"))

    window_spec_song = Window.partitionBy("date", "track_genre").orderBy(col("play_count").desc())
    top_songs = song_play_counts.withColumn("rank", row_number().over(window_spec_song)) \
        .filter(col("rank") <= 3)

    top_songs.write.mode("overwrite").parquet(output_base + "top_songs_per_genre/")
    print("[OK] Top songs written to top_songs_per_genre/")

    # ========== 🔝 3. Top 5 Genres per Day ==========
    print("[INFO] Calculating top 5 genres per day...")

    genre_counts = df.groupBy("date", "track_genre").agg(count("*").alias("genre_listens"))
    window_spec_genre = Window.partitionBy("date").orderBy(col("genre_listens").desc())

    top_genres = genre_counts.withColumn("rank", row_number().over(window_spec_genre)) \
        .filter(col("rank") <= 5)

    top_genres.write.mode("overwrite").parquet(output_base + "top_genres_per_day/")
    print("[OK] Top genres written to top_genres_per_day/")

    print("[SUCCESS] All KPIs computed and saved successfully.")

except Exception as e:
    print(f"[FAILURE] KPI job failed: {str(e)}")
    raise

job.commit()
