SELECT
    date,
    AVG(prcp) OVER (ORDER BY date RANGE BETWEEN INTERVAL '3' DAY PRECEDING AND INTERVAL '3' DAY FOLLOWING) AS prcp_7_day_avg
FROM {{ ref('stg__central_park_weather') }}