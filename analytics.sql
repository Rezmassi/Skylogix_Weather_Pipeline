-- SkyLogix Weather Analytics & Logistics Insights
-- 1. EXTREME CONDITIONS ALERT
-- Identifies cities where weather poses a safety risk to the 1,200 trucks.
SELECT 
    city, 
    temp_c, 
    wind_speed_ms, 
    visibility_m, 
    condition_main
FROM weather_readings 
WHERE wind_speed_ms > 10 OR visibility_m < 2000
ORDER BY observed_at DESC;

-- 2. TEMPERATURE TRENDS BY CITY
-- Calculates average temperatures to help with fuel consumption analysis.
SELECT 
    city, 
    ROUND(AVG(temp_c), 2) as avg_temp, 
    MAX(temp_c) as max_temp, 
    MIN(temp_c) as min_temp
FROM weather_readings 
GROUP BY city;

-- 3. RAIN IMPACT ANALYSIS
-- Identifies cities experiencing rainfall that might delay logistics.
SELECT 
    city, 
    rain_1h_mm, 
    condition_description,
    observed_at
FROM weather_readings 
WHERE rain_1h_mm > 0
ORDER BY rain_1h_mm DESC;

-- 4. RECENT INGESTION AUDIT
-- Verification query to ensure the Airflow pipeline is running correctly.
SELECT 
    city, 
    ingested_at 
FROM weather_readings 
ORDER BY ingested_at DESC 
LIMIT 10;
