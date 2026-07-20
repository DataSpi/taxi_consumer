# Best Practices áp dụng trong project này

Đây là các quyết định thiết kế có chủ đích, đúc kết lại để tái dùng cho các project khác — khác với `docs/errors_and_fixes.md` (log các bug đã gặp và cách fix). Mỗi mục có: pattern là gì, dùng ở đâu trong code, và tại sao nó tốt hơn cách làm "ngây thơ".

## 1. Data lake layout

### 1.1 Hive-style partition path + tên file tự mô tả
**Ở đâu:** [airflow/dags/taxi_pipeline_dag.py:83-87](../airflow/dags/taxi_pipeline_dag.py#L83-L87), [spark_jobs/clean_trips.py:95-97](../spark_jobs/clean_trips.py#L95-L97)

```python
key = (
    f"yellow_tripdata/year={data_interval_start.year}/"
    f"month={data_interval_start.month:02d}/"
    f"yellow_tripdata_{year_month}.parquet"
)
```

Path dùng dạng `year=.../month=.../` (Hive partitioning) để Spark/query engine tự suy ra cột partition từ path, không cần đọc nội dung file, và cho phép partition pruning khi query filter theo year/month. Tên file bên trong vẫn giữ nguyên convention gốc (`yellow_tripdata_2024-01.parquet`) để tự mô tả được ngay cả khi tách rời khỏi folder context — dễ debug khi browse trực tiếp trong MinIO console.

**Bài học chung:** path phục vụ máy (partition discovery/pruning), filename phục vụ người (traceability). Cả 2 bucket `raw` và `processed` đều theo đúng 1 quy ước này — nhất quán xuyên suốt data lake, không phải mỗi chỗ một kiểu.

## 2. Idempotency — mọi bước ghi đều an toàn khi rerun

Đây là chủ đề xuyên suốt cả pipeline, áp dụng khác nhau ở từng layer vì mỗi layer có ràng buộc khác nhau:

### 2.1 Check-before-write ở tầng extract
**Ở đâu:** [taxi_pipeline_dag.py:90-91](../airflow/dags/taxi_pipeline_dag.py#L90-L91), [taxi_pipeline_dag.py:105-106](../airflow/dags/taxi_pipeline_dag.py#L105-L106)

`extract_month` và `extract_zone_lookup` đều `check_for_key()` trước khi tải — file tĩnh (immutable từ nguồn TLC) thì rerun = skip, không tải lại tốn băng thông/thời gian.

### 2.2 DELETE trước khi COPY INTO ở tầng Snowflake load
**Ở đâu:** [taxi_pipeline_dag.py:209-217](../airflow/dags/taxi_pipeline_dag.py#L209-L217)

```python
# PUT ... OVERWRITE=TRUE re-uploads with a new checksum each run, so
# Snowflake's COPY INTO load-history dedup doesn't recognize a rerun —
# DELETE the partition first so reruns are safe.
cursor.execute("DELETE FROM ... WHERE PICKUP_YEAR = %s AND PICKUP_MONTH = %s", (year, month))
```

Snowflake tự dedup COPY INTO dựa trên checksum file trong load history — nhưng vì file được re-upload mỗi lần (checksum đổi), cơ chế dedup đó không cứu được. Giải pháp: xoá đúng partition (year/month) trước khi load lại, biến "rerun" thành full-replace thay vì append-trùng.

### 2.3 delete+insert incremental thay vì append trong dbt
**Ở đâu:** [dbt/taxi_dbt/models/marts/fct_trips.sql:1-15](../dbt/taxi_dbt/models/marts/fct_trips.sql#L1-L15)

```sql
{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key=['pickup_year','pickup_month']) }}
```

High-water-mark incremental (`where pickup_datetime > max(pickup_datetime)`) nhưng chiến lược ghi là `delete+insert` theo `unique_key`, không phải `append`. Cùng 1 tư tưởng như mục 2.2: khi backfill/replay 1 tháng, delete+insert tự triệt tiêu bản ghi cũ trước khi chèn — an toàn khi rerun. `append` thuần đã từng nhân đôi row khi task Snowflake bị rerun (xem `errors_and_fixes.md`).

**Bài học chung:** đừng chỉ hỏi "task này làm gì khi chạy lần đầu" — luôn hỏi thêm "task này làm gì nếu Airflow retry nó, hoặc user backfill lại đúng tháng đó lần 2". 3 layer trên xử lý idempotency theo 3 cơ chế khác nhau (skip / delete-before-write / delete+insert) tuỳ vào bản chất dữ liệu (static vs. partition-replaceable).

## 3. Một nguồn credential duy nhất, dùng lại xuyên suốt

**Ở đâu:** [taxi_pipeline_dag.py:43-53](../airflow/dags/taxi_pipeline_dag.py#L43-L53), [taxi_pipeline_dag.py:120-122](../airflow/dags/taxi_pipeline_dag.py#L120-L122)

`S3Hook`/`SnowflakeHook` (Python tasks), `spark-submit` (Spark job nhận credential qua CLI args lấy từ `S3Hook.get_connection()`), và Cosmos's `SnowflakeUserPasswordProfileMapping` (dbt) — cả 3 đều đọc credential từ **cùng một Airflow Connection** (`minio_default`, `snowflake_default`), không có bản copy riêng nào trong dbt `profiles.yml` hay biến môi trường Spark. Đổi password 1 chỗ, cả pipeline nhận được ngay.

**Bài học chung:** với hệ thống nhiều công cụ (Airflow + Spark + dbt) cùng cần nói chuyện với 1 hệ thống ngoài (Snowflake/S3), hãy tìm cách để 1 công cụ (ở đây là Airflow Connections) làm nguồn sự thật duy nhất, các công cụ khác đọc lại qua nó — thay vì mỗi công cụ tự có config credential riêng.

## 4. Secret không lộ trong log

**Ở đâu:** [taxi_pipeline_dag.py:144-146](../airflow/dags/taxi_pipeline_dag.py#L144-L146)

`conn.password` được truyền thẳng vào `subprocess.run(cmd)`, nhưng vì nó đến từ Airflow Connection (được Airflow's secrets masker theo dõi theo giá trị, không phải theo tên biến), giá trị này tự động bị redact khỏi task log dù xuất hiện trong argument list của command. Không cần code gì thêm để che nó.

## 5. Broadcast join cho dimension nhỏ

**Ở đâu:** [spark_jobs/clean_trips.py:79-81](../spark_jobs/clean_trips.py#L79-L81)

```python
trips.join(F.broadcast(pickup_zones), on="PULocationID", how="left")
```

`taxi_zone_lookup` chỉ ~265 dòng — ép Spark broadcast nó tới tất cả executor thay vì shuffle join, tránh shuffle tốn kém cho 1 bảng dimension bé xíu join với fact table hàng triệu dòng.

## 6. Defense in depth: data quality check ở 2 layer độc lập

**Ở đâu:** [spark_jobs/clean_trips.py:45-59](../spark_jobs/clean_trips.py#L45-L59) (Spark filter) và [dbt/taxi_dbt/tests/assert_no_invalid_amounts.sql](../dbt/taxi_dbt/tests/assert_no_invalid_amounts.sql) (dbt singular test)

```sql
-- Independent check on top of what Spark's clean_trips.py already filters
-- (defense in depth between the two layers, not blind trust that upstream
-- cleaning worked).
```

Spark đã filter `fare_amount > 0`, `trip_distance > 0`, v.v. Nhưng dbt vẫn có test riêng assert lại đúng điều kiện đó trên data trong Snowflake. Không tin tưởng mù quáng rằng "layer trước đã lọc rồi thì chắc chắn sạch" — 2 layer độc lập kiểm tra cùng 1 invariant để bắt được lỗi nếu 1 trong 2 bên có bug hoặc bị bypass (ví dụ ai đó load thẳng data vào Snowflake không qua Spark).

## 7. dbt: staging mỏng, transform thật ở marts (star schema)

**Ở đâu:** [dbt/taxi_dbt/models/staging/stg_trips.sql](../dbt/taxi_dbt/models/staging/stg_trips.sql), so với [scripts/snowflake_setup.sql:64-67](../scripts/snowflake_setup.sql#L64-L67)

Spark output đã có sẵn `pickup_borough`/`pickup_zone`/`dropoff_borough`/`dropoff_zone` (join sẵn), nhưng `stg_trips.sql` **cố tình bỏ qua** các cột đó, chỉ rename/cast. Việc join zone thật sự được làm lại ở marts qua `dim_zone` + `relationships` test. Staging layer chỉ làm việc nhẹ (rename, cast kiểu dữ liệu) — không transform nghiệp vụ; toàn bộ modeling thật (dimensional join, business logic) dồn về marts, nơi có thể test được quan hệ khoá ngoại.

**Bài học chung:** dù upstream có "tiện" cho sẵn data đã join, vẫn nên tự dựng lại star schema đúng chuẩn ở dbt nếu mục tiêu là thể hiện dimensional modeling — pass-through column từ upstream thì không test được referential integrity.

## 8. Materialization theo đúng mục đích sử dụng

**Ở đâu:** [dbt/taxi_dbt/dbt_project.yml:17-24](../dbt/taxi_dbt/dbt_project.yml#L17-L24)

```yaml
staging:
  +materialized: view
marts:
  +materialized: table
  fct_trips:
    +materialized: incremental
```

Staging = view (rẻ, luôn phản ánh data mới nhất, không tốn storage). Marts = table (query nhanh cho dashboard, không compile lại mỗi lần). `fct_trips` riêng = incremental vì bảng fact lớn, full-refresh mỗi lần sẽ quá chậm/tốn credit Snowflake khi data đã hàng triệu dòng.

## 9. Nguồn dữ liệu ngoài khai báo tường minh, kèm freshness

**Ở đâu:** [dbt/taxi_dbt/models/staging/src_taxi.yml](../dbt/taxi_dbt/models/staging/src_taxi.yml)

```yaml
sources:
  - name: raw
    tables:
      - name: trips
        loaded_at_field: PICKUP_DATETIME
        freshness:
          warn_after: { count: 45, period: day }
```

Khai báo `source()` thay vì hardcode tên bảng raw trong SQL — cho phép dbt lineage graph vẽ đúng, và cho phép `dbt source freshness` cảnh báo nếu pipeline load bị đứng quá lâu, tách biệt hẳn với logic transform.

## 10. Infra: healthcheck + `depends_on: condition` thay vì sleep/retry mù

**Ở đâu:** [docker-compose.yml](../docker-compose.yml) toàn file

Mọi service phụ thuộc (`airflow-init` chờ `postgres` healthy, `airflow-webserver`/`scheduler` chờ `airflow-init` completed successfully, `minio-createbuckets` chờ `minio` healthy) đều dùng `depends_on: condition:` gắn với `healthcheck`, không phải "hy vọng service kia lên kịp" hay sleep cứng. Các one-shot job (`minio-createbuckets`, `airflow-init`) tách riêng khỏi long-running service, chạy xong thì exit — không lẫn logic init 1 lần vào entrypoint của service chạy mãi.

## 11. Airflow Pool để serialize truy cập tài nguyên dùng chung

**Ở đâu:** [docker-compose.yml:81](../docker-compose.yml#L81), [taxi_pipeline_dag.py:243-249](../airflow/dags/taxi_pipeline_dag.py#L243-L249)

Tất cả task Cosmos dbt dùng chung 1 virtualenv dir (`DBT_VENV_DIR`) — nếu chạy song song, `pip install` đầu tiên vào dir đó sẽ race. Giải pháp không phải là ép toàn DAG chạy tuần tự, mà tạo riêng 1 `Pool` dung lượng 1 chỉ áp cho nhóm task đó (`operator_args={"pool": "dbt_pool"}`) — giới hạn đúng phạm vi cần serialize, các task khác trong DAG vẫn song song bình thường.

## 12. Snowflake: FUTURE GRANTS để role phụ dùng được object của role khác tạo

**Ở đâu:** [scripts/snowflake_setup.sql:16-21](../scripts/snowflake_setup.sql#L16-L21)

```sql
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA TAXI_CONSUMERS.RAW TO ROLE SPYNO_ANALYST;
```

Script setup chạy bằng role admin (tạo table/stage/file format), nhưng Airflow/dbt kết nối bằng role `SPYNO_ANALYST` khác. `GRANT ... ON FUTURE ...` đảm bảo mọi object *sẽ được tạo sau này* trong schema đó tự động cấp quyền cho role kia — không phải chạy `GRANT` thủ công lại mỗi lần có object mới.

---
**Ghi chú:** không liệt kê lại các mục đã có trong `docs/errors_and_fixes.md` (Parquet `USE_LOGICAL_TYPE`, Cosmos virtualenv race gốc rễ, cú pháp `relationships` phẳng cho Cosmos parser) — những cái đó là bug cụ thể đã hit, còn tài liệu này là pattern thiết kế chủ động, có thể áp dụng cho project khác từ đầu chứ không cần đợi hit bug mới biết.
