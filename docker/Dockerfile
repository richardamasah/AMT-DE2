# docker/Dockerfile (place in my_music_etl_project/docker/)

# Use a specific Airflow version with Python 3.10 as the base image.
# This ensures compatibility with newer Python packages like Pandas 2.2.2.
ARG AIRFLOW_VERSION=2.8.1
FROM apache/airflow:${AIRFLOW_VERSION}-python3.10

# Install Python dependencies from requirements.txt
# COPY requirements.txt: Copies the requirements file from your local 'docker' folder
# to the '/requirements.txt' path inside the Docker image.
# RUN pip install --no-cache-dir: Installs the Python packages listed in requirements.txt.
# --no-cache-dir prevents pip from storing package caches, reducing image size.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the PostgreSQL JDBC driver JAR.
# This JAR is essential for Redshift to communicate with your Airflow tasks via psycopg2.
# IMPORTANT: You MUST download 'postgresql-42.7.3.jar' (or the latest 42.x.x version)
# from the official PostgreSQL JDBC driver website (e.g., https://jdbc.postgresql.org/download/)
# and place it in the 'my_music_etl_project/docker/' directory alongside this Dockerfile.
COPY postgresql-42.7.3.jar /usr/share/java/postgresql-jdbc.jar

# (Optional) Set SPARK_HOME environment variable.
# This is typically needed if you plan to integrate with Apache Spark.
# While not directly used in the current Python tasks, it's good practice
# if your ETL involves Spark processing down the line.
ENV SPARK_HOME /opt/spark
