-- Light renaming/casting only; the star-schema join to zone descriptions happens
-- in the marts layer via dim_zone (see README/CLAUDE.md for why Spark's own
-- pre-joined borough/zone columns are intentionally not used here).

select
    vendorid                as vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid               as rate_code_id,
    store_and_fwd_flag,
    pulocationid              as pickup_location_id,
    dolocationid              as dropoff_location_id,
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
from {{ source('raw', 'trips') }}
