-- Pre-aggregated so the dashboard doesn't scan fct_trips on every query.

select
    pickup_date,
    pickup_location_id,
    count(*)                           as trip_count,
    sum(fare_amount)                   as total_fare_amount,
    sum(tip_amount)                    as total_tip_amount,
    sum(total_amount)                  as total_revenue,
    avg(trip_distance)                 as avg_trip_distance,
    avg(trip_duration_minutes)         as avg_trip_duration_minutes
from {{ ref('fct_trips') }}
group by 1, 2
