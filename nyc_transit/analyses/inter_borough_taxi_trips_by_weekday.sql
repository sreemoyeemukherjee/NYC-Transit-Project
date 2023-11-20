SELECT
    weekday(pickup_datetime) as weekday,
    COUNT(*) as total_trips, -- counting total number of trips
    SUM(CASE WHEN start_borough <> end_borough THEN 1 ELSE 0 END) as trips_different_borough, -- counting trips starting and ending in different borough
    (SUM(CASE WHEN start_borough <> end_borough THEN 1 ELSE 0 END) / COUNT(*)) * 100 as percentage_different_borough -- % of trips starting and ending in different borough
FROM (
    SELECT
        all_taxi.*,
        trip_start.Borough AS start_borough,
        trip_end.Borough AS end_borough
    FROM
        {{ ref('mart__fact_all_taxi_trips') }} all_taxi
    LEFT JOIN
        {{ ref('taxi+_zone_lookup') }} trip_start
    ON
        all_taxi.pulocationid = trip_start.LocationID -- joining all the trip start borough information
    LEFT JOIN
        {{ ref('taxi+_zone_lookup') }} trip_end
    ON
        all_taxi.dolocationid = trip_end.LocationID -- joining all the trip end borough information
) AS joined_data
GROUP BY
    weekday(pickup_datetime);