WITH is_prcp_by_day AS
(
    SELECT
        date,
        (prcp + snow) > 0 AS is_prcp
    FROM {{ ref('stg__central_park_weather') }}
),
data AS (
    SELECT
        i.date,
        is_prcp,
        LEAD(is_prcp, 1) OVER (ORDER BY i.date) AS is_prcp_next,
        t.trips AS trips_today,
        t.trips - n.trips AS trips_next
    FROM is_prcp_by_day i
    JOIN {{ ref('mart__fact_all_trips_daily') }} t ON i.date = t.date AND t.type = 'bike'
    JOIN {{ ref('mart__fact_all_trips_daily') }} n ON (i.date + 1) = n.date AND n.type = 'bike'
)
SELECT AVG(trips_next/trips_today) trips_reduction
FROM data
WHERE is_prcp_next AND NOT is_prcp;
