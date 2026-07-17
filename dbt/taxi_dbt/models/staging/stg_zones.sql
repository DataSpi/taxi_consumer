select
    locationid    as location_id,
    borough,
    zone,
    service_zone
from {{ source('raw', 'zone_lookup') }}
