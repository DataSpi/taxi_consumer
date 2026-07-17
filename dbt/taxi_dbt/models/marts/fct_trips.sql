{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

-- Append-only, high-water-mark incremental: each Airflow run loads exactly one
-- new month into raw.trips, so on every dbt run we only need rows newer than
-- whatever's already in this table. Matches the pipeline's month-by-month
-- backfill design (see CLAUDE.md) rather than a single full-refresh load.

select
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    cast(pickup_datetime as date)  as pickup_date,
    passenger_count,
    trip_distance,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    trip_duration_minutes,
    pickup_year,
    pickup_month
from {{ ref('stg_trips') }}

{% if is_incremental() %}
where pickup_datetime > (select coalesce(max(pickup_datetime), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
