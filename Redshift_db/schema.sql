-- 1. Staging table for incremental loads and Upsert operations 
CREATE TABLE IF NOT EXISTS staging_streams (
    user_id INT,
    track_id VARCHAR(64),
    listen_time TIMESTAMP,
    user_name VARCHAR(128),
    user_age INT,
    user_country VARCHAR(64),
    created_at DATE,
    id INT,
    artist VARCHAR(1024),
    album_name VARCHAR(512),
    title VARCHAR(512),
    popularity VARCHAR(16),      -- store as string for flexibility
    duration INT,
    explicit VARCHAR(8),         
    danceability FLOAT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,
    genre VARCHAR(128)
);

-- 2. Final table to store the processed and merged streaming data 
CREATE TABLE IF NOT EXISTS final_streams (
    user_id INT,
    track_id VARCHAR(64),
    listen_time TIMESTAMP,
    user_name VARCHAR(128),
    user_age INT,
    user_country VARCHAR(64),
    created_at DATE,
    id INT,
    artist VARCHAR(1024),
    album_name VARCHAR(512),
    title VARCHAR(512),
    popularity VARCHAR(16),      -- store as string for flexibility
    duration INT,
    explicit VARCHAR(8),         
    danceability FLOAT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,
    genre VARCHAR(128)
);

-- 3. KPI tables for genre-level and hourly metrics 
CREATE TABLE IF NOT EXISTS genre_kpis (
    genre VARCHAR,
    listen_count INT,
    avg_duration FLOAT,
    most_popular_track VARCHAR,
    popularity_index FLOAT
);

CREATE TABLE IF NOT EXISTS hourly_kpis (
    hour_bucket TIMESTAMP,
    unique_listeners INT,
    top_artist VARCHAR,
    track_diversity_index FLOAT
);

