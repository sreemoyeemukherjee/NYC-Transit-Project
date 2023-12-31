SELECT
    l.borough AS borough,
    COUNT(t.*) AS trip_count
FROM {{ ref('mart__fact_all_taxi_trips') }} t
    JOIN {{ ref('mart__dim_locations') }} l ON t.pulocationid = l.locationid
GROUP BY ALL

