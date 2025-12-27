import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# 1. Loading configuration
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")  #hidden: used .env file
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
CITIES = ["Nairobi", "Lagos", "Accra", "Johannesburg"]

def fetch_and_upsert():

    # 2. Connecting to MongoDB
    client = MongoClient(MONGO_URI)
    db = client["skylogix_db"]
    collection = db["weather_raw"]

    # creating  index on city and timestamp to make upserts fast so that we don't insert duplicates
    collection.create_index([("name", 1), ("dt", 1)], unique=True)

    for city in CITIES:
        print(f"Fetching weather for {city}...")
        
        # 3. Calling OpenWeather API
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        
        if response.status_code == 200: #if request is successful, then proceed
            data = response.json()
            
            # Add a local 'ingested_at' timestamp for our own tracking
            data['ingested_at'] = datetime.utcnow()

            # 4. Performing the Upsert. upsert is a combination of update and insert operation.
            # We use city name and the API'sdt as the unique key
            query = {"name": data["name"], "dt": data["dt"]}
            
            result = collection.replace_one(
                query, 
                data, 
                upsert=True
            )
            
            if result.matched_count > 0:
                print(f" -> Updated existing record for {city}.")
            else:
                print(f" -> Inserted new record for {city}.")
        else:
            print(f" ❌ Failed to fetch {city}: {response.status_code}")

    client.close()

if __name__ == "__main__":
    fetch_and_upsert()