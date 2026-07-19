{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['pickup_year', 'pickup_month']
    )
}}

-- High-water-mark incremental (only pull rows newer than what's already
-- here), but delete+insert by (pickup_year, pickup_month) rather than plain
-- append: makes replaying/backfilling a given month idempotent -- rerunning
-- for a month whose data hasn't advanced past the existing watermark is a
-- safe no-op instead of duplicating rows. (Plain 'append' silently doubled
-- rows the first time this pipeline's Snowflake load task got rerun for an
-- already-loaded month -- see docs/errors_and_fixes.md.)

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
