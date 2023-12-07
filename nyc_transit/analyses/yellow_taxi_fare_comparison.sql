SELECT
    fare_amount AS individual_fare,
    zone,
    AVG(fare_amount) OVER (PARTITION BY zone) AS zone_avg_fare,
    borough,
    AVG(fare_amount) OVER (PARTITION BY borough) AS borough_avg_fare,
    AVG(fare_amount) OVER () AS overall_avg_fare
FROM {{ ref('stg__yellow_tripdata') }} r
LEFT JOIN {{ ref('mart__dim_locations') }} l ON r.pulocationid = l.locationid