# NYC Taxi Analytics Engineering Platform

End-to-end batch analytics engineering pipeline on NYC TLC Yellow Taxi trip data — built to demonstrate Airflow, Docker, PySpark, Snowflake, and dbt working together.

> Status: Weeks 1-4 done — full pipeline (extract → Spark clean → Snowflake load → dbt marts) verified end-to-end through Airflow for 2024, plus a live Streamlit dashboard on top of the marts. See [CLAUDE.md](./CLAUDE.md) for full architecture, decisions, and roadmap.

## Architecture

```
NYC TLC parquet + zone CSV
  -> Airflow extract_month -> MinIO raw bucket
  -> Airflow spark_clean_month (PySpark) -> MinIO processed bucket
  -> Airflow load_to_snowflake (PUT + COPY INTO) -> Snowflake RAW schema
  -> Airflow dbt_build/dbt_test -> staging -> intermediate -> marts
  -> Streamlit dashboard + dbt docs site
```

Batch pipeline, medallion architecture, incremental by pickup month (each Airflow run/backfill processes one month end-to-end: extract → Spark clean → Snowflake load → dbt incremental merge).

## Stack

- **Orchestration**: Apache Airflow
- **Containers**: Docker Compose
- **Processing**: PySpark
- **Object storage (data lake)**: MinIO (S3-compatible)
- **Warehouse**: Snowflake
- **Transformation/testing**: dbt
- **Dashboard**: Streamlit

## Repo layout

```
taxi_consumer/
├── docker-compose.yml
├── airflow/dags/            # taxi_pipeline_dag.py
├── spark_jobs/              # clean_trips.py, enrich_zones.py, tests/
├── dbt/taxi_dbt/            # staging / intermediate / marts
├── dashboard/               # Streamlit app
├── docs/                    # architecture diagram, ERD, case study
└── scripts/                 # download_data.sh, init_minio_buckets.sh
```

## Running locally (Weeks 1-2: Airflow + MinIO + PySpark)

```bash
cp .env.example .env
docker compose up --build
```

Wait for `airflow-init` and `minio-createbuckets` to exit `0` (one-shot jobs), then:

- **Airflow UI**: http://localhost:8080 (login: `admin` / `admin`) — unpause the `taxi_pipeline` DAG and trigger a run, or let the scheduler catch up on its own (it's set to backfill from `2024-01-01`, one run per month, `max_active_runs=1`).
- **MinIO console**: http://localhost:9001 (login: `minioadmin` / `minioadmin`) — after a run succeeds, check the `raw` bucket for `yellow_tripdata/year=2024/month=01/...parquet` and `dimensions/taxi_zone_lookup.csv`.

To backfill a specific range instead of waiting on the scheduler:

```bash
docker compose exec airflow-scheduler airflow dags backfill taxi_pipeline \
  --start-date 2024-01-01 --end-date 2024-03-01
```

### What this stage does

- `docker-compose.yml` builds a custom Airflow image (`airflow/Dockerfile`) with the AWS provider installed (S3Hook works against any S3-compatible endpoint, including MinIO), runs Postgres as the Airflow metadata DB, and MinIO as the S3-compatible data lake.
- `taxi_pipeline_dag.py` has two tasks: `extract_month` (downloads that run's `yellow_tripdata_YYYY-MM.parquet` from the NYC TLC public bucket straight into MinIO `raw/`) and `extract_zone_lookup` (lands the static zone dimension once). Both are idempotent — they skip re-uploading if the key already exists.
- The MinIO connection is declared declaratively via the `AIRFLOW_CONN_MINIO_DEFAULT` env var (JSON format) instead of being clicked in through the UI, so a fresh clone works with zero manual setup.
- Scheduling is monthly with `catchup=True` and `end_date=2024-12-31`, so the DAG backfills exactly the 12 months of 2024 on its own.

## Week 2: PySpark cleaning

A third task, `spark_clean_month`, runs after `extract_month` and `extract_zone_lookup` (`[raw_key, zone_key] >> processed_key` in the DAG). It shells out to `spark-submit` — no separate Spark service/cluster; PySpark runs in `local[*]` mode inside the same Airflow container (simplest setup that's still 100% real PySpark; a real spark-master/worker cluster is a possible later upgrade, not required to show the skill).

`spark_jobs/clean_trips.py`:
- Reads the raw month's parquet + the zone lookup CSV straight from MinIO via the `s3a://` filesystem connector (`hadoop-aws` + `aws-java-sdk-bundle`, pulled at submit time via `--packages` rather than baked into the image, so versions are easy to bump without a rebuild).
- Drops bad rows: null/out-of-order timestamps, non-positive distance/fare/total/passenger_count, exact duplicates.
- Broadcast-joins the (tiny) zone lookup twice — once for pickup, once for dropoff — to enrich each trip with `pickup_borough`/`pickup_zone`/`dropoff_borough`/`dropoff_zone`.
- Writes the result to MinIO `processed/trips/year=YYYY/month=MM/` as Parquet.

Credentials for both the Airflow task and the Spark job come from the same `minio_default` Airflow Connection (no duplicated secrets) — `spark_clean_month` reads it via `S3Hook` and passes it to `spark-submit` as CLI args (Airflow's secrets masker redacts the password value from task logs automatically).

Verified end-to-end through Airflow itself (not just a manual run): triggering `taxi_pipeline` for Feb 2024 ran all 3 tasks to `success` and produced `processed/trips/year=2024/month=02/` with 11 partitioned Parquet files.

## Week 3: Snowflake + dbt

Two more tasks land the cleaned month in Snowflake and build the star schema on top of it: `load_zone_lookup_to_snowflake`/`load_to_snowflake` (`PUT` the processed Parquet to an internal named stage, then `COPY INTO` `TAXI_CONSUMERS.RAW`, deleting the target partition first so reruns replace rather than duplicate), followed by a Cosmos `DbtTaskGroup` that runs the full `dbt/taxi_dbt` project (`stg_trips`/`stg_zones` → `dim_date`/`dim_zone`/`fct_trips`/`fct_trips_daily_summary`, each with schema + singular tests) against `TAXI_CONSUMERS.ANALYTICS`.

`fct_trips` is dbt `incremental` (`delete+insert` by `pickup_year`/`pickup_month`), matched to the DAG's monthly schedule — the whole pipeline backfills/replays one month at a time rather than a single full-refresh dump. Cosmos derives dbt's `profiles.yml` from the same `snowflake_default` Airflow Connection the Python tasks use, so there's one credential source for the whole DAG.

Verified end-to-end for full-year 2024 backfill: `taxi_pipeline` completed all 17 tasks per run (extract → Spark clean → Snowflake load → 12 Cosmos `dbt_build.*` run/test tasks), landing `FCT_TRIPS` at 2,723,733 rows, `DIM_ZONE` at 265, `DIM_DATE` at 366, `FCT_TRIPS_DAILY_SUMMARY` at 6,662 — all dbt tests passing. 14 real bugs hit and fixed along the way are logged in `docs/errors_and_fixes.md`; design decisions that generalize beyond this project are in `docs/best_practices.md`.

## Week 4: Streamlit dashboard

`dashboard/app.py` reads straight from the `TAXI_CONSUMERS.ANALYTICS` marts (`fct_trips_daily_summary` joined to `dim_zone`/`dim_date`) via `snowflake-connector-python`, reusing the same `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` credential blob the pipeline already uses — no separate copy of the Snowflake password for the dashboard. It's its own service in `docker-compose.yml`.

Filters (date range, borough) drive four views: a daily trip-count trend, revenue by borough, the top 10 pickup zones, and a weekday-vs-weekend comparison, plus KPI tiles (total trips, revenue, avg fare, avg trip distance) and a raw-data expander.

```bash
docker compose up -d streamlit
```

- **Dashboard**: http://localhost:8501

### Not built yet (see [CLAUDE.md](./CLAUDE.md))

- GitHub Actions CI, architecture diagram image, demo recording (capture before the Snowflake trial expires).

## What I learned

_(to be filled in at the end — trade-offs, what would change for a real production version)_
