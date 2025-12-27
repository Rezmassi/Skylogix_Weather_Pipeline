import os
import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

# for environment variables (.env file)
load_dotenv()

def transform_and_load():

    try:
        # Connect to MongoDB (Source)
        mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
        mongodb = mongo_client["skylogix_db"]
        raw_collection = mongodb["weather_raw"]

        # Connect to PostgreSQL (Destination)
        pg_conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT")
        )
        pg_cursor = pg_conn.cursor()

        # 2. Extract: Getting all raw data from MongoDB
        raw_data = raw_collection.find()

        for doc in raw_data:
            for doc in raw_data:
            # 3. Transform: Mapping MongoDB JSON fields to flat variables [cite: 49, 78]
                raw_payload = doc.get("raw_payload", {})  
                main = raw_payload.get("main", {})
                wind = raw_payload.get("wind", {})
                clouds = raw_payload.get("clouds", {})
                rain = raw_payload.get("rain", {})
                snow = raw_payload.get("snow", {})
                weather_list = raw_payload.get("weather", [{}])[0]

            # Convert 'dt' from MongoDB to a proper Python datetime [cite: 59]
            observed_at = datetime.fromtimestamp(doc.get("dt"))

            # 4. Load: Insert into PostgreSQL with full SkyLogix Schema [cite: 81, 109]
            upsert_query = """
            INSERT INTO weather_readings (
                city, country, observed_at, lat, lon, temp_c, feels_like_c, 
                pressure_hpa, humidity_pct, wind_speed_ms, wind_deg, 
                cloud_pct, visibility_m, rain_1h_mm, snow_1h_mm, 
                condition_main, condition_description
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, observed_at) DO NOTHING;
            """
            
            # Prepare the data tuple in the exact same order as the columns above [cite: 92]
            data_to_insert = (
                doc.get("city"),
                raw_payload.get("sys", {}).get("country"),
                observed_at,
                raw_payload.get("coord", {}).get("lat"),
                raw_payload.get("coord", {}).get("lon"),
                main.get("temp"),
                main.get("feels_like"),
                main.get("pressure"),
                main.get("humidity"),
                wind.get("speed"),
                wind.get("deg"),
                clouds.get("all"),
                raw_payload.get("visibility"),
                rain.get("1h", 0.0) if rain else 0.0,
                snow.get("1h", 0.0) if snow else 0.0,
                weather_list.get("main"),
                weather_list.get("description")
            )

            pg_cursor.execute(upsert_query, data_to_insert)

        
        pg_conn.commit()
        print(f"✅ Successfully transformed and loaded data into PostgreSQL!")

    except Exception as e:
        print(f"❌ Error during ETL: {e}")
    
    finally:
        if 'pg_conn' in locals():
            pg_cursor.close()
            pg_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    transform_and_load()