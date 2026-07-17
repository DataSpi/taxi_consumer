# Project: NYC Taxi Analytics Engineering Platform

## Purpose
Portfolio project built to close 4 skill gaps for Analytics Engineering job applications: **Airflow, Docker, Apache Spark/PySpark, Snowflake**. dbt and GitHub Actions CI/CD are already strong (see the user's other project at `/Users/spinokiem/Documents/wrk/DataSpi` for reference patterns: medallion architecture, staging/mart layering, schema+custom tests).

## Key decisions (do not re-litigate without a good reason)
- **No Apache Flink.** Considered and explicitly rejected — streaming/Flink isn't what Analytics Engineering JDs ask for, and it would blow up timeline/complexity (JVM, Kafka/Kinesis, checkpointing) for no proportional benefit. Batch-only stack.
- **Data scope**: NYC TLC Yellow Taxi Trip Records, one full recent calendar year (12 monthly parquet files, ~35-40M trips, ~2GB raw), plus `taxi_zone_lookup.csv` dimension. Deliberately NOT multi-year / multi-service-type (green, FHV) — that's a stretch goal only, to keep Snowflake trial costs and dev time bounded. Confirm exact year against what's currently published on the TLC site when starting ingestion.
- **Snowflake trial is a hard 30-day clock** — it was already running as of 2026-07-17. Sequence work so Docker/Airflow/Spark (no Snowflake needed) happens first, Snowflake+dbt work is concentrated in the middle/end. Use X-Small warehouse with short auto-suspend (~60s). Before the trial expires: capture screenshots, export `dbt docs generate` as a static site, and record a demo video/GIF — these are the only proof that survives after the trial account is gone.
- **Snowflake loading pattern**: internal named stage + `PUT` + `COPY INTO`, not an external stage. External stages need Snowflake to reach real cloud storage (S3/Azure/GCS) over the network — MinIO on localhost isn't reachable that way. Internal stage avoids that entirely at zero extra cost while still exercising the real `COPY INTO` skill.
- **MinIO** (S3-compatible object storage, in Docker) sits between raw ingestion and Spark processing, acting as the "data lake" layer — raw bucket and processed bucket. If MinIO setup becomes a time sink, the fallback is Spark reading/writing local filesystem volumes directly.
- **Spark runs in local mode (`local[*]`)** inside a single container for v1 — simpler to build/debug than a real spark-master/worker cluster. A standalone cluster is a possible stretch goal to demonstrate distributed cluster understanding, not required for v1.
- **Incremental design is the core narrative**: `fct_trips` is a dbt incremental model (`is_incremental()`, filtered by pickup month), matched to an Airflow DAG parameterized by execution month so the whole pipeline can backfill/replay one month at a time (Jan → Dec) rather than a single full-refresh dump. This is the strongest interview talking point in the project — don't collapse it into a single full-load job.
- **dbt-in-Airflow**: prefer `astronomer-cosmos` for the dbt task group (modern, portfolio-relevant pattern); `BashOperator` calling `dbt build` is the acceptable fallback if Cosmos setup eats too much time.

## Architecture (batch, medallion, hybrid Spark + Snowflake)
```
NYC TLC parquet + zone CSV
  -> Airflow extract_month -> MinIO raw bucket
  -> Airflow spark_clean_month (PySpark: normalize schema, dedupe, filter bad rows,
       join taxi_zone_lookup, partition by pickup year/month) -> MinIO processed bucket
  -> Airflow load_to_snowflake (PUT to internal stage + COPY INTO) -> Snowflake RAW schema
  -> Airflow dbt_build/dbt_test (via Cosmos) -> dbt staging -> intermediate -> marts
  -> Streamlit dashboard (reads marts) + dbt docs static site
```

## Repo layout
```
taxi_consumer/
├── docker-compose.yml          # airflow-webserver, airflow-scheduler, airflow-postgres, minio, spark, (streamlit)
├── airflow/dags/taxi_pipeline_dag.py   # parameterized by execution month, backfillable
├── spark_jobs/{clean_trips.py, enrich_zones.py, tests/}
├── dbt/taxi_dbt/models/{staging,intermediate,marts}
├── dashboard/                   # Streamlit app
├── docs/                        # architecture diagram, ERD, case study writeup
└── scripts/                     # download_data.sh, init_minio_buckets.sh
```

## Data model
- Dims: `dim_date`, `dim_zone` (from taxi_zone_lookup — borough/zone/service_zone), `dim_vendor`, `dim_rate_code`, `dim_payment_type`
- Fact: `fct_trips` (grain = one trip), incremental by pickup month
- Aggregate: `fct_trips_daily_summary` for dashboard performance

## Roadmap (full-time, ~3-4 weeks, must fit inside the 30-day Snowflake trial)
1. **Week 1** — Docker Compose (Airflow + Postgres + MinIO) up; extract DAG lands raw month in MinIO. No Snowflake needed yet.
2. **Week 2** — PySpark clean/enrich job wired into Airflow; partitioned processed output in MinIO.
3. **Week 3** — Snowflake account setup, internal-stage load task, full dbt project (sources/staging/intermediate/marts/tests/docs), Cosmos integration, backfill all 12 months end-to-end.
4. **Week 4** — Streamlit dashboard, README + architecture diagram, case study writeup, GitHub Actions CI, capture all demo evidence before the Snowflake trial expires.

## Definition of done
- `docker compose up` from a clean clone stands up the whole stack.
- Manually triggered/backfilled Airflow DAG run completes end-to-end with dbt tests passing.
- `dbt docs generate && dbt docs serve` shows full staging→marts lineage.
- Streamlit dashboard reads live data from Snowflake marts.
- README has an architecture diagram, a from-scratch run guide, and a "what I learned / trade-offs" section.

## Full plan reference
The complete planning conversation and rationale lives at `/Users/spinokiem/.claude/plans/tao-ang-mu-n-l-m-velvet-thimble.md` (auto-generated filename from plan mode — not meant to be human-searchable; this CLAUDE.md is the durable reference going forward).
