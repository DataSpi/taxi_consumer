-- Independent check on top of what Spark's clean_trips.py already filters
-- (defense in depth between the two layers, not blind trust that upstream
-- cleaning worked). Should return zero rows.

select *
from {{ ref('fct_trips') }}
where fare_amount <= 0
   or total_amount <= 0
   or trip_distance <= 0
   or dropoff_datetime <= pickup_datetime
