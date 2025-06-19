

#  Music Streaming ETL Pipeline (Airflow + S3 + Redshift)

##  Overview

This project implements an end-to-end ETL pipeline for a music streaming service using:

* **Amazon S3** – for raw, staged, and transformed data storage
* **Apache Airflow (MWAA)** – for orchestrating and scheduling tasks
* **Amazon Redshift** – for storing analytics-ready data and KPIs

The pipeline processes streaming events, user data, and song metadata to compute business-critical KPIs like **top artists per hour**, **track diversity index**, and **genre-level popularity**.

![ETL Architecture](images/Architectural.jpeg)
---

## 🧠 Key Components

### 1. Purpose

This ETL pipeline:

* Collects raw logs and static data from S3
* Cleans and validates the data
* Transforms it to extract insights
* Loads KPIs into Redshift for analytics and reporting

### 2. Python Modules

#### ✅ `helper_functions.py`

* `read_s3_csv_to_df` – Load CSVs from S3 into Pandas
* `write_df_to_s3_csv` – Save DataFrames back to S3

#### 📥 `extraction.py`

* Detects and stages **new stream files** and **static data** (users, songs)
* Tracks processed files via a log in S3

#### ✔️ `validation.py`

* Ensures required columns exist
* Logs nulls in critical fields
* Validated data is saved to S3 `validated/` prefix

#### 🔁 `transformation.py`

* Applies logic for streams, users, and songs
* Calculates:

  * `unique_listeners` & `track_diversity_index` per hour
  * `genre-level KPIs` (popularity, duration, top tracks)
* Results saved to S3 `transformed/`

#### 📦 `load.py`

* Loads final KPIs from S3 into Redshift using COPY
* Performs **UPSERT** to avoid duplicates
* Cleans up loaded files from S3

#### 🧠 `mwaa_dag.py`

* Defines DAG flow and task dependencies
* Uses XComs to pass S3 paths between tasks
* Ensures Redshift schema is prepared before loading

---

## 🔁 ETL Workflow Summary

1. **Extract**

   * Checks for new streaming logs in `lab1-etl-landingzone/streams/`
   * Reads static user/song data from `etl-lab1-rds-landingzone/processed/`

2. **Stage**

   * Moves data to `staging-bucket-lab1test/raw/` for further processing

3. **Validate**

   * Ensures schema consistency and logs nulls
   * Moves data to `validated/` S3 paths

4. **Transform**

   * Stream: Parses timestamps, calculates partial hourly KPIs
   * User: Converts data types
   * Song: Casts columns, cleans strings
   * Saves to `transformed/` paths

5. **Calculate KPIs**

   * Combines stream + song data for:

     * Hourly KPIs (e.g., top artists per hour)
     * Genre KPIs (e.g., popularity index)

6. **Load to Redshift**

   * Stages data via COPY
   * Executes UPSERT to final tables
   * Deletes temporary files from S3

---

## 🧮 KPI Examples

### 🎵 Hourly KPIs

* `listen_hour`
* `top_artists_per_hour`
* `unique_listeners`
* `track_diversity_index`

### 🎼 Genre KPIs

* `listen_count`
* `average_track_duration`
* `popularity_index`
* `most_popular_track_per_genre`

---

## 📂 S3 Folder Structure

```plaintext
raw/
  └── streams/, users/, songs/
validated/
  └── streams/, users/, songs/
transformed/
  ├── streams/, users/, songs/
  ├── hourly_kpis/
  └── genre_kpis/
```

---

## 🏗️ Redshift Tables

Each table has a **staging** and a **final** version.

| Table Type  | Example Columns                                          |
| ----------- | -------------------------------------------------------- |
| Hourly KPIs | listen\_hour, unique\_listeners, top\_artists\_per\_hour |
| Genre KPIs  | track\_genre, listen\_count, popularity\_index           |

---

## 🛠️ Technologies Used

* Python (Pandas, boto3)
* Apache Airflow (MWAA)
* Amazon S3
* Amazon Redshift
* IAM, S3Hook, XCom

---

