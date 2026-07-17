"""NYC Taxi analytics pipeline.

Week 1: land one month of raw data into the MinIO "raw" bucket.
Week 2: clean/enrich that month with PySpark and write it to "processed".
Week 3: load the cleaned month into Snowflake and build dbt marts.

The DAG is scheduled monthly and is backfill-friendly: each DAG run's
data_interval_start maps to exactly one NYC TLC monthly file
(yellow_tripdata_YYYY-MM.parquet), so `airflow dags backfill` can replay
any range of months later once the rest of the pipeline exists.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

RAW_BUCKET = "raw"
PROCESSED_BUCKET = "processed"
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
S3_CONN_ID = "minio_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
SPARK_JOB_PATH = "/opt/airflow/spark_jobs/clean_trips.py"
# hadoop-aws must match the Hadoop client version bundled inside the pinned
# pyspark version (3.5.3 -> Hadoop 3.3.4); aws-java-sdk-bundle must match
# what that hadoop-aws release was built against.
SPARK_S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


@dag(
    dag_id="taxi_pipeline",
    description="NYC Yellow Taxi batch pipeline: extract -> (Spark) -> (Snowflake) -> (dbt)",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    # Airflow treats end_date as *inclusive* of data_interval_start, so
    # 2025-01-01 here would still schedule a Jan-2025 run. Use the last day
    # of Dec 2024 to guarantee exactly 12 runs (Jan-Dec 2024).
    end_date=pendulum.datetime(2024, 12, 31, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["taxi", "week1-extract"],
)
def taxi_pipeline():
    @task
    def extract_month(data_interval_start=None) -> str:
        """Download this run's monthly trip file straight into MinIO raw/."""
        year_month = data_interval_start.strftime("%Y-%m")
        key = (
            f"yellow_tripdata/year={data_interval_start.year}/"
            f"month={data_interval_start.month:02d}/"
            f"yellow_tripdata_{year_month}.parquet"
        )

        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        if hook.check_for_key(key, bucket_name=RAW_BUCKET):
            return key

        url = f"{TLC_BASE_URL}/yellow_tripdata_{year_month}.parquet"
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        hook.load_bytes(response.content, key=key, bucket_name=RAW_BUCKET, replace=True)
        return key

    @task
    def extract_zone_lookup() -> str:
        """Land the static zone dimension once; skip if already present."""
        key = "dimensions/taxi_zone_lookup.csv"

        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        if hook.check_for_key(key, bucket_name=RAW_BUCKET):
            return key

        response = requests.get(ZONE_LOOKUP_URL, timeout=60)
        response.raise_for_status()
        hook.load_bytes(response.content, key=key, bucket_name=RAW_BUCKET, replace=True)
        return key

    @task
    def spark_clean_month(data_interval_start=None) -> str:
        """spark-submit clean_trips.py for this run's year/month.

        Credentials come from the same Airflow Connection extract_month
        uses, not duplicated env vars — single source of truth.
        """
        year, month = data_interval_start.year, data_interval_start.month
        conn = S3Hook(aws_conn_id=S3_CONN_ID).get_connection(S3_CONN_ID)
        endpoint_url = conn.extra_dejson["endpoint_url"]

        cmd = [
            "spark-submit",
            "--packages",
            SPARK_S3A_PACKAGES,
            SPARK_JOB_PATH,
            "--year",
            str(year),
            "--month",
            str(month),
            "--endpoint-url",
            endpoint_url,
            "--access-key",
            conn.login,
            "--secret-key",
            conn.password,
            "--raw-bucket",
            RAW_BUCKET,
            "--processed-bucket",
            PROCESSED_BUCKET,
        ]
        # conn.password is a known Airflow secret, so its value is
        # automatically redacted from task logs by Airflow's secrets masker.
        subprocess.run(cmd, check=True)
        return f"{PROCESSED_BUCKET}/trips/year={year}/month={month:02d}/"

    raw_key = extract_month()
    zone_key = extract_zone_lookup()
    processed_key = spark_clean_month()
    [raw_key, zone_key] >> processed_key


taxi_pipeline()
