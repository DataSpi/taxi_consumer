"""NYC Taxi analytics pipeline.

Week 1 scope: land one month of raw data into the MinIO "raw" bucket.
Later weeks add Spark cleaning, Snowflake loading, and dbt build/test as
further tasks in this same DAG.

The DAG is scheduled monthly and is backfill-friendly: each DAG run's
data_interval_start maps to exactly one NYC TLC monthly file
(yellow_tripdata_YYYY-MM.parquet), so `airflow dags backfill` can replay
any range of months later once the rest of the pipeline exists.
"""

from __future__ import annotations

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_BUCKET = "raw"
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
S3_CONN_ID = "minio_default"


@dag(
    dag_id="taxi_pipeline",
    description="NYC Yellow Taxi batch pipeline: extract -> (Spark) -> (Snowflake) -> (dbt)",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
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

    extract_month()
    extract_zone_lookup()


taxi_pipeline()
