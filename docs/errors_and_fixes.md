# Errors hit while building this project, and how they were fixed

Chronological log of every real bug encountered during Weeks 1-3, kept for learning purposes — each one is a concept worth understanding, not just a line of code to copy.

---

## 1. Airflow scheduler auto-backfilled far beyond the intended date range

**Symptom:** DAG kept creating new monthly runs (2024-05, 2024-06, ...) well past what was expected, on its very first trigger.

**Root cause:** `catchup=True` + no `end_date` means Airflow backfills from `start_date` all the way to *today*, not to some implicit "recent" cutoff. This is correct, documented behavior — the DAG had genuinely no upper bound.

**Fix:** Add an explicit `end_date` to the `@dag(...)` decorator.

**Follow-up gotcha:** `end_date` turned out to be **inclusive** of `data_interval_start`, not exclusive. Setting `end_date=2025-01-01` still scheduled a Jan-2025 run. Fixed by using the *last day of the last wanted month* (`2024-12-31`) instead of the first day of the month after.

`airflow/dags/taxi_pipeline_dag.py`

---

## 2. MinIO connection silently blank because `.env` wasn't created

**Symptom:** `docker compose` printed `The "AIRFLOW_CONN_MINIO_DEFAULT" variable is not set. Defaulting to a blank string.` — not a hard error, so it was easy to miss and move on.

**Root cause:** The setup step `cp .env.example .env` was skipped, so Airflow had no way to authenticate to MinIO. Docker Compose doesn't fail the build when a referenced env var is missing from `.env` — it just substitutes an empty string.

**Fix:** Create `.env`, then **recreate** (not just restart) the Airflow containers — env vars are baked in at container creation, so `docker compose restart` isn't enough after changing `.env`; needs `--force-recreate` or `up -d --build`.

---

## 3. PySpark couldn't cast a TLC timestamp column to BIGINT

**Symptom:**
```
pyspark.errors.exceptions.captured.AnalysisException: [DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]
Cannot resolve "CAST(dropoff_datetime AS BIGINT)" ... cannot cast "TIMESTAMP_NTZ" to "BIGINT".
```

**Root cause:** Spark 3.5 reads the TLC parquet's datetime columns as `TIMESTAMP_NTZ` (no timezone), and that type can't be `.cast("long")` directly the way an ordinary `TIMESTAMP` can.

**Fix:** Use `F.unix_timestamp(col)` instead of `col.cast("long")` to get epoch seconds.

`spark_jobs/clean_trips.py`

---

## 4. Spark launch warning: `ps: command not found`

**Symptom:** Harmless-looking warning during `spark-submit` startup.

**Root cause:** The Debian-slim-based Airflow image doesn't ship `procps` (the package that provides `ps`), which Spark's own launch scripts shell out to.

**Fix:** Add `procps` alongside `default-jdk-headless` in the `apt-get install` line.

`airflow/Dockerfile`

---

## 5. `pip install` timing out mid-build

**Symptom:** `ReadTimeoutError: HTTPSConnectionPool(host='files.pythonhosted.org', ...)` killing the Docker build partway through downloading a large wheel (pyspark is ~317MB).

**Root cause:** Plain network flakiness, no code bug — but the default pip timeout is short enough that a large-package build is more exposed to it.

**Fix:** `pip install --timeout 120 --retries 5 ...` in the Dockerfile.

`airflow/Dockerfile`

---

## 6. Installing `dbt-snowflake` in the same environment as Airflow: unresolvable pip conflict

**Symptom:**
```
pip._vendor.resolvelib.resolvers.ResolutionTooDeep: 200000
```
after several minutes of pip repeatedly trying different `dbt-adapters` versions.

**Root cause:** `dbt-core`'s dependency tree and `apache-airflow`'s dependency tree pin overlapping packages (jinja2, click, etc.) at incompatible version ranges. This is a well-known, real conflict in the Airflow+dbt ecosystem — not something to "just pin harder."

**Fix:** Don't install `dbt-snowflake`/`dbt-core` into the Airflow image at all. Use `astronomer-cosmos` with `ExecutionMode.VIRTUALENV`, which builds an **isolated** Python virtualenv for dbt at task runtime, completely separate from Airflow's own site-packages.

`airflow/requirements.txt`, `airflow/dags/taxi_pipeline_dag.py`

---

## 7. DAG failed to even parse: "Unable to find the dbt executable"

**Symptom:**
```
cosmos.config.CosmosConfigException: Unable to find the dbt executable, attempted: <dbt> and <dbt>.
```
This happened at **DAG parse time**, before any task ran.

**Root cause:** Cosmos's default load mode (`LoadMode.AUTOMATIC`) tries to run `dbt ls` locally to discover the project's model graph *when Airflow parses the DAG file* — but by design (see #6) there's no `dbt` binary installed outside the per-task virtualenv, so this always fails.

**Fix:** Explicitly set `RenderConfig(load_method=LoadMode.CUSTOM)` — Cosmos parses the project's `.sql`/`.yml` files directly (lightweight regex/AST parsing) instead of shelling out to a real dbt install.

`airflow/dags/taxi_pipeline_dag.py`

---

## 8. Snowflake object names didn't match what the setup script assumed

**Symptom:** Connection worked, but `SHOW DATABASES LIKE '%TAXI%'` showed `TAXI_CONSUMERS`, not the `TAXI_DB` the original setup script created.

**Root cause:** The initial `scripts/snowflake_setup.sql` assumed a generic `TAXI_DB`/`TAXI_WH`/`SYSADMIN` setup, but the actual Snowflake trial account already had its own objects created by hand: database `TAXI_CONSUMERS`, warehouse `TAXI_CONSUMER`, role `SPYNO_ANALYST`.

**Fix:** Rather than force a second, parallel database into existence, every SQL reference in the codebase (DAG task SQL, dbt `sources.yml`, Cosmos `profile_args`) was renamed to match the real object names. Lesson: verify what actually exists in the target system before writing code against assumed names — `SHOW DATABASES` / `SHOW GRANTS` are cheap ways to check.

---

## 9. Role only had `USAGE`, not `CREATE` — couldn't run the setup script

**Symptom:**
```
SQL access control error: Requested role 'SPYNO_ANALYST' is not assigned to the executing user.
```

**Root cause, part 1:** `SPYNO_ANALYST` only had `USAGE` on the database/schema/warehouse — not enough to `CREATE TABLE`/`CREATE STAGE`/`CREATE FILE FORMAT`.

**Root cause, part 2 (the actual error message):** The setup script tried to `USE ROLE SPYNO_ANALYST` from a Snowflake worksheet — but that role was granted only to the separate service user (`SPYNO_MAC`) that Airflow connects as, not to whatever personal login the worksheet was open as. A role has to be *assigned to your user* before you can switch to it.

**Fix:** Run the whole setup script as the already-logged-in role (e.g. `ACCOUNTADMIN`) instead of switching roles mid-script, and add `GRANT ... ON FUTURE TABLES/STAGES/FILE FORMATS IN SCHEMA ... TO ROLE SPYNO_ANALYST` so the service role can use objects it didn't personally create.

`scripts/snowflake_setup.sql`

---

## 10. Concurrent dbt tasks corrupted a shared virtualenv install

**Symptom:**
```
subprocess.CalledProcessError: Command '['/opt/airflow/dbt_venv/bin/pip', 'install', 'dbt-snowflake==1.8.4']' returned non-zero exit status 1.
```
...but only on the *first-ever* run; manually re-running the same task afterward succeeded immediately.

**Root cause:** `stg_trips.run` and `stg_zones.run` are the first two dbt models with no dependency on each other, so Airflow's LocalExecutor started them **at the same time**. Both tried to `pip install` into the *same* shared `virtualenv_dir` simultaneously — a classic race condition on first-time setup. Once the venv was fully populated (from whichever task "won"), later runs just saw "Requirement already satisfied" and succeeded, which is why a plain retry looked like it "fixed itself."

**Fix:** Created a dedicated Airflow **Pool** (`dbt_pool`, 1 slot) and assigned every Cosmos-generated task to it via `operator_args={"pool": "dbt_pool"}`. This serializes all dbt tasks so the shared venv is never written to by two processes at once. Trade-off: the dbt portion of each run is slower (fully sequential) — an acceptable cost for correctness at this project's scale.

`docker-compose.yml` (creates the pool in `airflow-init`), `airflow/dags/taxi_pipeline_dag.py`

---

## 11. A model ran its tests before a table it depended on even existed

**Symptom:**
```
002003 (42S02): SQL compilation error:
Object 'TAXI_CONSUMERS.ANALYTICS.DIM_ZONE' does not exist or not authorized.
```
in `fct_trips`'s `relationships` test — even though `dim_zone` was a real model in the project.

**Root cause:** Cosmos's `LoadMode.CUSTOM` parser (needed per #7) builds the task dependency graph by scanning for `ref()`/`source()` calls. dbt's newer recommended test syntax nests the `ref()` call one level deeper (`relationships: arguments: to: ref(...)`) to silence a deprecation warning — but Cosmos's lightweight parser didn't detect `ref()` calls in that nested form, so it silently dropped the dependency edge from `dim_zone.run` to `fct_trips.test`. The test then got scheduled with no ordering constraint against `dim_zone`, and happened to run first.

**Fix:** Reverted the `relationships` tests back to the flat `to:`/`field:` syntax (not nested under `arguments:`). This produces a soft deprecation warning from dbt itself, which is a much smaller cost than the ordering bug it fixes. If Cosmos adds support for the nested syntax in a future version, this can be revisited.

`dbt/taxi_dbt/models/marts/schema.yml`

---

## 12. Re-running the Snowflake load task silently duplicated a month's data

**Symptom:** A `relationships` test failed with "Got 5447500 results" — suspiciously close to (and turned out to be *exactly*) double the real January row count (2,723,750 × 2).

**Root cause:** `load_to_snowflake` re-uploads that month's Parquet files with `PUT ... OVERWRITE=TRUE` every run. Snowflake's `COPY INTO` normally skips files it recognizes as already-loaded (by filename+checksum) — but `OVERWRITE=TRUE` gives the re-uploaded file a new checksum, so a rerun (e.g. from `airflow tasks clear`, or a retry) looks like a brand-new file to `COPY INTO` and gets loaded a second time, silently duplicating that month's rows in `RAW.TRIPS`.

**Fix, two layers of defense:**
- **Raw load task:** `DELETE FROM RAW.TRIPS WHERE PICKUP_YEAR = %s AND PICKUP_MONTH = %s` immediately before the `COPY INTO`, so the task is idempotent per month regardless of how many times it's retried.
- **dbt mart:** changed `fct_trips` from `incremental_strategy='append'` to `'delete+insert'` with `unique_key=['pickup_year', 'pickup_month']`, so even if upstream data for an already-loaded month changes, rebuilding it replaces that month's fact rows instead of blindly appending more.

`airflow/dags/taxi_pipeline_dag.py`, `dbt/taxi_dbt/models/marts/fct_trips.sql`

---

## 13. Every single row landed with a garbage timestamp (`year 3728527 is out of range`)

**Symptom:** A `relationships` test failed with **all 2,723,750 rows** — not a subset, the entire table. Trying to query `MIN(pickup_datetime)` from the client crashed with `InterfaceError: ... year 3728527 is out of range` — the underlying stored value was a real, enormous number, not a display glitch.

**Root cause:** Snowflake's `COPY INTO` for Parquet doesn't reliably use the file's embedded logical-type metadata for `TIMESTAMP` columns unless the file format explicitly sets `USE_LOGICAL_TYPE = TRUE`. Without it, Snowflake can misread the raw INT64 microsecond-since-epoch encoding that Spark writes (e.g. reading it at the wrong time unit), producing a wildly wrong date instead of erroring — this is a documented Snowflake+Parquet gotcha, not something obviously visible from the Parquet file itself. Confirmed the corruption started at the raw load layer (`RAW.TRIPS` already had 100% bad timestamps) before dbt ever touched the data, which narrowed it down to the `COPY INTO` step rather than any Spark or dbt logic.

**Fix:** `CREATE OR REPLACE FILE FORMAT ... TYPE = PARQUET USE_LOGICAL_TYPE = TRUE`.

**Follow-up permissions wrinkle:** the fix itself hit `Insufficient privileges ... must have OWNERSHIP granted on FILE FORMAT`, because the file format had originally been created by `ACCOUNTADMIN` — the `GRANT ... ON FUTURE FILE FORMATS` from bug #9 only auto-applies to objects created *after* the grant, not retroactively to ones that already existed. Fixed by re-running the `CREATE OR REPLACE` as `ACCOUNTADMIN` (a `CREATE OR REPLACE` re-creates the object, so the future-grant then applies to the new instance and `SPYNO_ANALYST` regains access).

`scripts/snowflake_setup.sql`

---

## 14. A few real TLC rows have pickup dates outside the file's own month

**Symptom:** After fixing #13, `fct_trips.test` still failed the `relationships` test to `dim_date` — but this time only a handful of rows, not the whole table. Querying the actual min/max showed a pickup_datetime of `2002-12-31` sitting inside the "January 2024" trip file.

**Root cause:** This isn't a pipeline bug at all — it's a genuine, documented quirk of the NYC TLC dataset itself. Taxi meters occasionally record a wrong date (data-entry/hardware error at the source), so a small number of rows in any given month's file have a `pickup_datetime` that doesn't actually fall in that month. `dim_date`'s spine only covers the project's declared 2024 scope, so those stray rows have nowhere to join to.

**Fix:** Added a filter to `clean_trips.py`: keep only rows where `year(pickup_datetime)` and `month(pickup_datetime)` match the year/month the Spark job was invoked for (which Airflow already passes in as `--year`/`--month`). This is the standard, expected cleaning step for this dataset, not an arbitrary cutoff invented to make the test pass.

`spark_jobs/clean_trips.py`

---

## Pattern across most of these

Almost every real bug here came from the same shape of problem: **something is safe the first time but not the Nth time** (reruns, retries, backfills, concurrent tasks) — idempotency and concurrency, not one-shot "happy path" logic. That's a fair reflection of what production data engineering debugging actually looks like, and worth internalizing as the recurring lesson, not just each individual fix.
