WITH weather AS (
    SELECT
         CAST(date AS DATE) AS weather_date,
        (CASE WHEN prcp > 0 OR snow > 0 THEN TRUE ELSE FALSE END) AS is_precip
    FROM {{ ref('stg__central_park_weather') }}
),
bike_trips AS (
    SELECT
        CAST(started_at_ts AS DATE) AS bike_date,
        COUNT(*) AS num_trips
    FROM {{ ref('mart__fact_all_bike_trips') }}
    GROUP BY bike_date
),
weather_trips AS (
    SELECT
        b.bike_date,
        b.num_trips,
        COALESCE(w.is_precip, FALSE) AS is_precip,
        COALESCE(LAG(w.is_precip) OVER (ORDER BY b.bike_date), FALSE) AS is_prev_precip
    FROM bike_trips b
    LEFT JOIN weather w ON b.bike_date = w.weather_date
)

SELECT
    AVG(CASE WHEN is_precip THEN num_trips ELSE NULL END) AS avg_on_precip_days,
    AVG(CASE WHEN is_prev_precip AND NOT is_precip THEN num_trips ELSE NULL END) AS avg_trips_on_prev_precip_days
FROM weather_trips;