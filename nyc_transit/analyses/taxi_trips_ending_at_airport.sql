SELECT
    COUNT(*) AS total_trips -- count total number of trips
FROM
    {{ ref('mart__fact_all_taxi_trips') }} trips
JOIN
    {{ ref('taxi+_zone_lookup') }} zones
ON
    trips.dolocationid = zones.LocationID   -- checking drop location
WHERE
    zones.service_zone IN ('EWR', 'Airports'); -- matching the service_zones 'Airports' or 'EWR'