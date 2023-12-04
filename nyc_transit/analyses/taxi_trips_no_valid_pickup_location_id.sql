SELECT
  t.*
FROM
  {{ ref('mart__fact_all_taxi_trips') }} t
LEFT JOIN
  {{ ref('mart__dim_locations') }} l ON t.pulocationid = l.locationid
WHERE
  l.locationid IS NULL;
