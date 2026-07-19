"""Clean and enrich one month of NYC Yellow Taxi trip data.

Reads raw parquet from MinIO `raw/`, drops invalid/garbage rows, joins in
the taxi zone dimension (broadcast join since it's tiny), and writes the
result to MinIO `processed/` partitioned by pickup year/month.

Invoked via spark-submit from the `spark_clean_month` Airflow task in
taxi_pipeline_dag.py — not meant to be run standalone in production, but
can be for local debugging, e.g.:

    spark-submit clean_trips.py --year 2024 --month 1 \\
        --endpoint-url http://minio:9000 \\
        --access-key minioadmin --secret-key minioadmin
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(endpoint_url: str, access_key: str, secret_key: str) -> SparkSession:
    return (
        SparkSession.builder.appName("clean_trips")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def clean_and_enrich(spark: SparkSession, raw_path: str, zone_lookup_path: str, year: int, month: int):
    trips = spark.read.parquet(raw_path)
    zones = spark.read.option("header", True).csv(zone_lookup_path)

    trips = (
        trips.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .dropDuplicates()
        .filter(F.col("pickup_datetime").isNotNull() & F.col("dropoff_datetime").isNotNull())
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        # TLC's source files always contain a handful of mis-keyed trips whose
        # pickup_datetime falls outside the file's nominal month (taxi meter
        # data-entry errors, a known quirk of this dataset -- confirmed by
        # hitting one dated 2002 inside the "January 2024" file). Downstream,
        # dim_date's spine only covers the project's declared 2024 scope, so
        # stray rows like that fail the fct_trips -> dim_date relationship
        # test. Filtering to the file's own year/month here is the standard
        # TLC-data-cleaning step for this, not an arbitrary cutoff.
        .filter((F.year("pickup_datetime") == year) & (F.month("pickup_datetime") == month))
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
        )
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
    )

    pickup_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    dropoff_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )

    return trips.join(F.broadcast(pickup_zones), on="PULocationID", how="left").join(
        F.broadcast(dropoff_zones), on="DOLocationID", how="left"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--endpoint-url", required=True)
    parser.add_argument("--access-key", required=True)
    parser.add_argument("--secret-key", required=True)
    parser.add_argument("--raw-bucket", default="raw")
    parser.add_argument("--processed-bucket", default="processed")
    args = parser.parse_args()

    raw_path = f"s3a://{args.raw_bucket}/yellow_tripdata/year={args.year}/month={args.month:02d}/"
    zone_lookup_path = f"s3a://{args.raw_bucket}/dimensions/taxi_zone_lookup.csv"
    output_path = f"s3a://{args.processed_bucket}/trips/year={args.year}/month={args.month:02d}/"

    spark = build_spark(args.endpoint_url, args.access_key, args.secret_key)
    try:
        enriched = clean_and_enrich(spark, raw_path, zone_lookup_path, args.year, args.month)
        enriched.write.mode("overwrite").parquet(output_path)
        print(f"Wrote cleaned trips to {output_path} ({enriched.count()} rows)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
