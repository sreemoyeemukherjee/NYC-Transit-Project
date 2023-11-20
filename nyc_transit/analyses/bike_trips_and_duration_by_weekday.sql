select
    weekday(started_at_ts) as weekday,
    count(*) as total_trips, -- counting total trips
    sum(duration_sec) as total_trip_duration_secs -- total time
from {{ ref('mart__fact_all_bike_trips') }}
group by all