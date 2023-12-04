SELECT
    r.fare_amount AS individual_fare,
    AVG(r.fare_amount) OVER (PARTITION BY l.zone) AS zone_avg_fare,
    AVG(r.fare_amount) OVER (PARTITION BY l.borough) AS borough_avg_fare,
    AVG(r.fare_amount) OVER () AS overall_avg_fare
FROM {{ ref('stg__yellow_tripdata') }} r
LEFT JOIN {{ ref('mart__dim_locations') }} l ON r.pulocationid = l.locationid