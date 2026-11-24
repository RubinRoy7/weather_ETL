/*
===============================================================================
Script 04: Analysis queries on dw.vw_city_daily_environment
===============================================================================
Run after several days of weather and air-quality data have been loaded.
===============================================================================
*/
USE WeatherDW;
GO

-- 1. Compare daily PM2.5 and AQI averages by city.
SELECT city_name,
       COUNT(*) AS days,
       ROUND(AVG(pm2_5_avg), 1) AS avg_pm2_5,
       MAX(aqi_max) AS worst_aqi
FROM dw.vw_city_daily_environment
WHERE pm2_5_avg IS NOT NULL
GROUP BY city_name
ORDER BY avg_pm2_5 DESC;

-- 2. Compare PM2.5 on rainy and dry/light-rain city-days.
SELECT CASE WHEN precip_mm >= 1 THEN 'Rainy (>= 1 mm)' ELSE 'Dry / light' END AS day_type,
       COUNT(*) AS city_days,
       ROUND(AVG(pm2_5_avg), 1) AS avg_pm2_5
FROM dw.vw_city_daily_environment
WHERE pm2_5_avg IS NOT NULL AND precip_mm IS NOT NULL
GROUP BY CASE WHEN precip_mm >= 1 THEN 'Rainy (>= 1 mm)' ELSE 'Dry / light' END;

-- 3. Day-over-day PM2.5 change for each city.
SELECT city_name, obs_date, pm2_5_avg,
       pm2_5_avg - LAG(pm2_5_avg) OVER (PARTITION BY city_name ORDER BY obs_date) AS change_vs_prev_day
FROM dw.vw_city_daily_environment
ORDER BY city_name, obs_date;

-- 4. Weather days with no matching air-quality fact row or no PM2.5 average.
SELECT city_name, obs_date
FROM dw.vw_city_daily_environment
WHERE pm2_5_avg IS NULL
ORDER BY obs_date, city_name;

-- 5. Rank cities by daily maximum US AQI.
SELECT obs_date, city_name, aqi_max,
       RANK() OVER (PARTITION BY obs_date ORDER BY aqi_max DESC) AS aqi_rank
FROM dw.vw_city_daily_environment
WHERE aqi_max IS NOT NULL
ORDER BY obs_date DESC, aqi_rank;

-- 6. Inspect missing PM10 rejections by timestamp, not row position.
SELECT reject_id, batch_id, city_name, obs_time, pm10, pm2_5, us_aqi, reason
FROM etl.rejected_air_quality_rows
WHERE reason LIKE '%missing_pm10%'
ORDER BY reject_id DESC;
GO
