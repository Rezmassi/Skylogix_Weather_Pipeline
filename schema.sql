-- SkyLogix Weather Warehouse Schema

CREATE TABLE IF NOT EXISTS weather_readings (
    -- Surrogate Key 
    id SERIAL PRIMARY KEY,
    
    -- Geographic Information 
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    lat NUMERIC,
    lon NUMERIC,
    
    -- Time of weather observation 
    observed_at TIMESTAMP NOT NULL,
    
    -- Core Metrics 
    temp_c NUMERIC,
    feels_like_c NUMERIC,
    pressure_hpa INTEGER,
    humidity_pct INTEGER,
    
    -- Safety metrucs
    wind_speed_ms NUMERIC,
    wind_deg INTEGER,
    cloud_pct INTEGER,
    visibility_m INTEGER,
    rain_1h_mm NUMERIC DEFAULT 0.0,
    snow_1h_mm NUMERIC DEFAULT 0.0,
    
    -- Qualitative Conditions 
    condition_main VARCHAR(50),
    condition_description VARCHAR(255),
    
    -- Metadata [cite: 81]
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints to prevent duplicate entries for the same city at the same time
    UNIQUE(city, observed_at)
);

-- Indexes for performance optimization 
CREATE INDEX IF NOT EXISTS idx_city_observed ON weather_readings (city, observed_at);