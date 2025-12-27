This technical report summarizes the development of the real-time weather data pipeline designed for SkyLogix Transportation.



**1. Executive Summary**

SkyLogix Transportation manages a fleet of 1,200 trucks across major African logistics corridors. This project replaces manual weather monitoring with an automated, centralized data pipeline that integrates real-time conditions into a structured analytics warehouse.





**2. Problem Statement**



Manual Monitoring: ad-hoc checks caused inconsistent information and delays.



Data Fragmentation: A lack of a central repository made trend analysis and proactive routing impossible.



Safety Risks: Poor visibility into regional weather increased accident risks and insurance costs.





3\. **Data Architecture \& Design**

Ingestion Layer (MongoDB): Acts as a staging layer where raw JSON payloads from OpenWeatherMap are upserted. This ensures data lineage by keeping unmodified API responses.



Orchestration Layer (Apache Airflow): Schedules tasks to fetch, transform, and load data, ensuring reliability and error handling.



Analytics Layer (PostgreSQL): A tabular warehouse model that stores flattened metrics such as visibility, wind speed, and precipitation for long-term analysis.





**4. Key Technical Implementation**



JSON Flattening: Developed Python logic to extract nested values (e.g., main.temp, wind.speed) into a 17-column relational schema.



Incremental Processing: Implemented indexes on 'updatedAt' in MongoDB to ensure only new or modified documents are processed by the transformation task.



Upsert: Used ON CONFLICT (city, observed\_at) in PostgreSQL to prevent data duplication while allowing for retroactive updates to weather readings.





**5. Business Findings**

The warehouse now enables dispatchers to query for "Extreme Conditions":



High-Risk Detection: Identifying wind speeds > 10m/s or visibility < 2000m to trigger route adjustments.



Route Optimization: Correlating historical weather patterns with logistics data to adjust SLAs for specific corridors.



Correlation with Logistics Data

While this pipeline currently focuses on weather, it is designed to be joined with SkyLogix’s existing logistics tables:



Joining on City \& Time: By joining weather\_readings.observed\_at with trip\_logs.departure\_time, analysts can calculate the exact "weather-related delay" factor for every truck in the fleet.



Safety Scorecards: Weather data can be merged with vehicle telematics to create safety scorecards, identifying drivers who successfully navigate difficult conditions versus those at higher risk.







**6. Conclusion**

By automating the ingestion and transformation of weather data, SkyLogix can now make data-driven decisions. This system minimizes operational risks and provides the foundation for integrating weather insights directly into trip and vehicle data.

