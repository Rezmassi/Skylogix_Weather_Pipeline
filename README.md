# SkyLogix Real-Time Weather Pipeline

## Project Overview
This pipeline automates the collection of weather data for SkyLogix Transportation's primary logistics corridors: Nairobi, Lagos, Accra, and Johannesburg.

## Data Architecture Diagram
```mermaid
graph LR
    A[OpenWeather API] --> B[Python Ingestor]
    B --> C[(MongoDB Staging)]
    C --> D{Airflow DAG}
    D --> E[Transform Logic]
    E --> F[(PostgreSQL Warehouse)]
    F --> G[Analytics & Reports]


## Tech Stack
- **Source:** OpenWeatherMap API 
- **Staging Layer:** MongoDB (weather_raw) 
- **Warehouse Layer:** PostgreSQL (weather_readings) 
- **Orchestration:** Apache Airflow 

## Setup Instructions
1. **Environment:** Create a `.env` file with your `MONGO_URI`, `POSTGRES_DB`, and OpenWeather `API_KEY`.
2. **Database:** Run `schema.sql` to create the PostgreSQL warehouse model.
3. **Airflow:** Place `skylogix_dag.py` in your Airflow DAGs folder.
4. **Execution:** Trigger the DAG via the Airflow UI to start the hourly ingestion.

## Analytics
Sample queries for extreme condition detection (High wind/Rain) are included in the `analytics/` folder.