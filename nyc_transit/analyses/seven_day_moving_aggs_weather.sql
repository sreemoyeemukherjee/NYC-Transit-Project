SELECT date,
       MIN(snow) OVER seven_days as window_min_snow,
       MAX(snow) OVER seven_days as window_max_snow,
       AVG(snow) OVER seven_days as window_avg_snow,
       SUM(snow) OVER seven_days as window_sum_snow,
       MIN(prcp) OVER seven_days as window_min_prcp,
       MAX(prcp) OVER seven_days as window_max_prcp,
       AVG(prcp) OVER seven_days as window_avg_prcp,
       SUM(prcp) OVER seven_days as window_sum_prcp
FROM {{ ref('stg__central_park_weather') }}
WINDOW seven_days AS (
    ORDER BY date ASC
    RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND
        INTERVAL 3 DAYS FOLLOWING)
ORDER BY date;
