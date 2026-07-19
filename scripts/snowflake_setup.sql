-- Adjusted to match the objects already created for this project:
--   database TAXI_CONSUMERS, schema RAW, warehouse TAXI_CONSUMER, role SPYNO_ANALYST.
-- SPYNO_ANALYST only had USAGE (not CREATE) on these -- the grants below fix that.
--
-- Run this ENTIRE script as whatever role your Snowflake worksheet is
-- already logged in with (e.g. ACCOUNTADMIN). Do NOT try to `USE ROLE
-- SPYNO_ANALYST` -- that role is only assigned to the separate service user
-- SPYNO_MAC that Airflow connects as, not to your own web UI login, so
-- switching to it here fails with "role is not assigned to the executing
-- user". The FUTURE grants below let SPYNO_ANALYST use the tables/stages/
-- file formats created here even though a different role owns them.

USE WAREHOUSE TAXI_CONSUMER;
USE DATABASE TAXI_CONSUMERS;

GRANT CREATE SCHEMA ON DATABASE TAXI_CONSUMERS TO ROLE SPYNO_ANALYST;
GRANT ALL PRIVILEGES ON SCHEMA TAXI_CONSUMERS.RAW TO ROLE SPYNO_ANALYST;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE TAXI_CONSUMERS TO ROLE SPYNO_ANALYST;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA TAXI_CONSUMERS.RAW TO ROLE SPYNO_ANALYST;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA TAXI_CONSUMERS.RAW TO ROLE SPYNO_ANALYST;
GRANT ALL PRIVILEGES ON FUTURE FILE FORMATS IN SCHEMA TAXI_CONSUMERS.RAW TO ROLE SPYNO_ANALYST;

-- USE_LOGICAL_TYPE = TRUE is required, not optional: without it Snowflake
-- misreads the INT64-encoded microsecond timestamps that Spark writes into
-- Parquet, producing garbage dates (e.g. year 3728527) instead of an error --
-- confirmed by hitting it. See docs/errors_and_fixes.md.
CREATE FILE FORMAT IF NOT EXISTS TAXI_CONSUMERS.RAW.PARQUET_FORMAT
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE;

-- Internal stage: Airflow PUTs the Spark-cleaned Parquet files here before COPY INTO.
-- No external cloud storage integration needed (see CLAUDE.md for why).
CREATE STAGE IF NOT EXISTS TAXI_CONSUMERS.RAW.TRIPS_STAGE
  FILE_FORMAT = TAXI_CONSUMERS.RAW.PARQUET_FORMAT;

CREATE TABLE IF NOT EXISTS TAXI_CONSUMERS.RAW.TRIPS (
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

CREATE FILE FORMAT IF NOT EXISTS TAXI_CONSUMERS.RAW.CSV_FORMAT
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE STAGE IF NOT EXISTS TAXI_CONSUMERS.RAW.ZONE_LOOKUP_STAGE
  FILE_FORMAT = TAXI_CONSUMERS.RAW.CSV_FORMAT;

-- Column order must match taxi_zone_lookup.csv (LocationID,Borough,Zone,service_zone) --
-- COPY INTO for CSV is positional, unlike the MATCH_BY_COLUMN_NAME used for Parquet above.
CREATE TABLE IF NOT EXISTS TAXI_CONSUMERS.RAW.ZONE_LOOKUP (
  LOCATIONID    NUMBER,
  BOROUGH       VARCHAR,
  ZONE          VARCHAR,
  SERVICE_ZONE  VARCHAR
);

-- Sanity check after running:
-- SHOW TABLES IN TAXI_CONSUMERS.RAW;
