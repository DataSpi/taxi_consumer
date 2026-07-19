-- Run once in a Snowflake worksheet (logged in as your trial account's default user,
-- which normally has SYSADMIN among its roles). Idempotent — safe to re-run.

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS TAXI_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS TAXI_DB;
CREATE SCHEMA IF NOT EXISTS TAXI_DB.RAW;

CREATE FILE FORMAT IF NOT EXISTS TAXI_DB.RAW.PARQUET_FORMAT
  TYPE = PARQUET;

-- Internal stage: Airflow PUTs the Spark-cleaned Parquet files here before COPY INTO.
-- No external cloud storage integration needed (see CLAUDE.md for why).
CREATE STAGE IF NOT EXISTS TAXI_DB.RAW.TRIPS_STAGE
  FILE_FORMAT = TAXI_DB.RAW.PARQUET_FORMAT;

CREATE TABLE IF NOT EXISTS TAXI_DB.RAW.TRIPS (
  VENDORID               NUMBER,
  PICKUP_DATETIME         TIMESTAMP_NTZ,
  DROPOFF_DATETIME        TIMESTAMP_NTZ,
  PASSENGER_COUNT         NUMBER,
  TRIP_DISTANCE           FLOAT,
  RATECODEID              NUMBER,
  STORE_AND_FWD_FLAG      VARCHAR,
  PULOCATIONID            NUMBER,
  DOLOCATIONID            NUMBER,
  PAYMENT_TYPE            NUMBER,
  FARE_AMOUNT             FLOAT,
  EXTRA                   FLOAT,
  MTA_TAX                 FLOAT,
  TIP_AMOUNT              FLOAT,
  TOLLS_AMOUNT            FLOAT,
  IMPROVEMENT_SURCHARGE   FLOAT,
  TOTAL_AMOUNT            FLOAT,
  CONGESTION_SURCHARGE    FLOAT,
  AIRPORT_FEE             FLOAT,
  TRIP_DURATION_MINUTES   FLOAT,
  PICKUP_YEAR             NUMBER,
  PICKUP_MONTH            NUMBER,
  PICKUP_BOROUGH          VARCHAR,
  PICKUP_ZONE             VARCHAR,
  DROPOFF_BOROUGH         VARCHAR,
  DROPOFF_ZONE            VARCHAR
);
-- Spark also attaches PICKUP_BOROUGH/PICKUP_ZONE/DROPOFF_BOROUGH/DROPOFF_ZONE
-- (Week 2 enrichment). dbt's staging/marts layer intentionally ignores these
-- in favor of its own dim_zone join off ZONE_LOOKUP below -- see CLAUDE.md
-- for why (real dimensional modeling + relationships tests, not a pass-through).

CREATE FILE FORMAT IF NOT EXISTS TAXI_DB.RAW.CSV_FORMAT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE STAGE IF NOT EXISTS TAXI_DB.RAW.ZONE_LOOKUP_STAGE
  FILE_FORMAT = TAXI_DB.RAW.CSV_FORMAT;

-- Column order must match taxi_zone_lookup.csv (LocationID,Borough,Zone,service_zone) --
-- COPY INTO for CSV is positional, unlike the MATCH_BY_COLUMN_NAME used for Parquet above.
CREATE TABLE IF NOT EXISTS TAXI_DB.RAW.ZONE_LOOKUP (
  LOCATIONID    NUMBER,
  BOROUGH       VARCHAR,
  ZONE          VARCHAR,
  SERVICE_ZONE  VARCHAR
);

-- Sanity check after running:
-- SHOW WAREHOUSES LIKE 'TAXI_WH';
-- SHOW TABLES IN TAXI_DB.RAW;
