# NYC Taxi Analytics Engineering Platform

End-to-end batch analytics engineering pipeline on NYC TLC Yellow Taxi trip data — built to demonstrate Airflow, Docker, PySpark, Snowflake, and dbt working together.

> Status: scaffolding phase. See [CLAUDE.md](./CLAUDE.md) for full architecture, decisions, and roadmap.

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

## Running locally (Week 1: Airflow + MinIO + extract task)

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
- Scheduling is monthly with `catchup=True`, so the same DAG that runs live going forward can also backfill any historical month later — this is what Week 3's Spark/Snowflake/dbt tasks will hook into.

### Not built yet (upcoming weeks — see [CLAUDE.md](./CLAUDE.md))

- Week 2: PySpark job to clean/enrich the raw parquet and write partitioned output to MinIO `processed/`.
- Week 3: Snowflake internal-stage load + dbt project (staging/intermediate/incremental marts), wired into this same DAG.
- Week 4: Streamlit dashboard, GitHub Actions CI, architecture diagram, case study.

## What I learned

_(to be filled in at the end — trade-offs, what would change for a real production version)_
