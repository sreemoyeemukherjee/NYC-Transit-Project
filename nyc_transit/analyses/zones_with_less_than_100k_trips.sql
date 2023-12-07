SELECT
  zone,
  COUNT(t.*) AS trip_count
FROM
  {{ ref('mart__fact_all_taxi_trips') }} t
JOIN
  {{ ref('mart__dim_locations') }} l ON t.pulocationid = l.locationid
GROUP BY ALL
HAVING
  trip_count < 100000;
