SELECT
    date,
    AVG(prcp) OVER (ORDER BY date RANGE BETWEEN INTERVAL '3' DAYS PRECEDING AND INTERVAL '3' DAYS FOLLOWING) AS prcp_7_day_avg
FROM {{ ref('stg__central_park_weather') }}
ORDER BY date;