WITH joined_data AS (
  SELECT
    r.zone AS pickup_zone,
    DATEDIFF('second', l.pickup_datetime, LEAD(l.pickup_datetime, 1) OVER (PARTITION BY r.zone ORDER BY l.pickup_datetime)) AS time_diff_sec
  FROM {{ ref('mart__fact_all_taxi_trips') }} l
  LEFT JOIN {{ ref('mart__dim_locations') }} r ON l.pulocationid = r.locationid
)
SELECT
  pickup_zone,
  AVG(time_diff_sec) AS avg_time_between_pickups_in_sec
FROM joined_data
GROUP BY ALL;
