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
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

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
DBT_PROJECT_DIR = "/opt/airflow/dbt/taxi_dbt"
DBT_VENV_DIR = "/opt/airflow/dbt_venv"

# Cosmos derives dbt's profiles.yml from the same Airflow Connection the
# Python tasks above use -- one source of truth for Snowflake credentials,
# no separate copy of them for dbt.
profile_config = ProfileConfig(
    profile_name="taxi_dbt",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={"database": "TAXI_CONSUMERS", "schema": "ANALYTICS"},
    ),
)

# VIRTUALENV, not LOCAL: installing dbt-snowflake into the same environment as
# apache-airflow causes an unresolvable pip dependency conflict (confirmed by
# trying it -- see requirements.txt). Cosmos builds an isolated venv for dbt
# at task runtime instead.
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.VIRTUALENV,
    virtualenv_dir=DBT_VENV_DIR,
)


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

    @task
    def load_zone_lookup_to_snowflake() -> str:
        """Load the static zone dimension into Snowflake once; skip if already loaded.

        CSV, not Parquet, so this uses a plain positional COPY INTO instead of
        MATCH_BY_COLUMN_NAME (there's no column-name metadata in a CSV file).
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        (count,) = hook.get_first("SELECT COUNT(*) FROM TAXI_CONSUMERS.RAW.ZONE_LOOKUP")
        if count > 0:
            return f"skipped, {count} rows already present"

        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / "taxi_zone_lookup.csv"
            s3.get_conn().download_file(RAW_BUCKET, "dimensions/taxi_zone_lookup.csv", str(local_path))

            conn = hook.get_conn()
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"PUT file://{local_path} @TAXI_CONSUMERS.RAW.ZONE_LOOKUP_STAGE OVERWRITE=TRUE"
                )
                cursor.execute(
                    """
                    COPY INTO TAXI_CONSUMERS.RAW.ZONE_LOOKUP
                    FROM @TAXI_CONSUMERS.RAW.ZONE_LOOKUP_STAGE
                    FILE_FORMAT = (FORMAT_NAME = TAXI_CONSUMERS.RAW.CSV_FORMAT)
                    PURGE = TRUE
                    """
                )
            finally:
                cursor.close()
                conn.close()
        return "loaded taxi_zone_lookup.csv"

    @task
    def load_to_snowflake(data_interval_start=None) -> str:
        """Download this month's Spark output from MinIO, PUT + COPY INTO Snowflake.

        Snowflake's PUT only accepts local files, not remote s3a:// paths, so
        the processed Parquet has to pass through local disk here even though
        it never needed to in Week 2's Spark-to-MinIO step.
        """
        year, month = data_interval_start.year, data_interval_start.month
        prefix = f"trips/year={year}/month={month:02d}/"

        s3 = S3Hook(aws_conn_id=S3_CONN_ID)
        keys = [k for k in (s3.list_keys(bucket_name=PROCESSED_BUCKET, prefix=prefix) or []) if k.endswith(".parquet")]
        if not keys:
            raise AirflowException(f"No processed Parquet files found at {prefix}")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        with tempfile.TemporaryDirectory() as tmpdir:
            for key in keys:
                s3.get_conn().download_file(PROCESSED_BUCKET, key, str(Path(tmpdir) / Path(key).name))

            conn = hook.get_conn()
            cursor = conn.cursor()
            try:
                # Idempotency: PUT ... OVERWRITE=TRUE re-uploads the file with
                # a new checksum each run, so Snowflake's COPY INTO load-history
                # dedup doesn't recognize a rerun as "already loaded" and would
                # silently double the month's rows. Delete this partition first
                # so reruns (retries, backf-replay, manual clear) are safe.
                cursor.execute(
                    "DELETE FROM TAXI_CONSUMERS.RAW.TRIPS WHERE PICKUP_YEAR = %s AND PICKUP_MONTH = %s",
                    (year, month),
                )
                stage_path = f"@TAXI_CONSUMERS.RAW.TRIPS_STAGE/year={year}/month={month:02d}/"
                cursor.execute(f"PUT file://{tmpdir}/*.parquet {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
                cursor.execute(
                    f"""
                    COPY INTO TAXI_CONSUMERS.RAW.TRIPS
                    FROM {stage_path}
                    FILE_FORMAT = (FORMAT_NAME = TAXI_CONSUMERS.RAW.PARQUET_FORMAT)
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    PURGE = TRUE
                    """
                )
            finally:
                cursor.close()
                conn.close()
        return f"loaded TAXI_CONSUMERS.RAW.TRIPS year={year} month={month:02d}"

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
        # CUSTOM: parse the project's SQL/YAML directly instead of shelling
        # out to a local `dbt ls` at DAG-parse time -- there is no dbt
        # executable in this environment (see execution_config above).
        render_config=RenderConfig(load_method=LoadMode.CUSTOM),
        operator_args={
            "py_requirements": ["dbt-snowflake==1.8.4"],
            # All dbt tasks share one virtualenv dir; running them concurrently
            # races on the first `pip install` into it (confirmed by hitting
            # it). This pool caps concurrency to 1 so they queue instead.
            "pool": "dbt_pool",
        },
    )

    raw_key = extract_month()
    zone_key = extract_zone_lookup()
    processed_key = spark_clean_month()
    zone_loaded = load_zone_lookup_to_snowflake()
    trips_loaded = load_to_snowflake()

    [raw_key, zone_key] >> processed_key
    zone_key >> zone_loaded
    processed_key >> trips_loaded
    [zone_loaded, trips_loaded] >> dbt_build


taxi_pipeline()
