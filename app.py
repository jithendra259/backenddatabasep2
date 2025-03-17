import asyncio
import aiohttp
import json
import nest_asyncio
import pymongo
import re
import time
from datetime import datetime, timedelta

# Apply nest_asyncio to allow nested event loops (necessary in Colab/Jupyter)
nest_asyncio.apply()

# --- Configuration ---
API_KEY = "c69bd9a20bccfbbe7b4f2e37a17b1a2f2332b423"
MAX_UID = 5          # For testing, use 5; change to 15000 for full run
BATCH_SIZE = 5       # For testing, use 5; change to 10000 for full run
CONCURRENCY = 5      # Adjust as needed

# MongoDB connection string (provided)
MONGO_URI = "mongodb+srv://kandulas:7WiHXWMQZH3DVvyr@cluster0.jsark.mongodb.net/"
DATABASE_NAME = "aqidb"             # Your database name
COLLECTION_NAME = "waqi_stations"    # Collection name

# --- MongoDB Setup ---
try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    print("MongoDB connected.")
except Exception as e:
    print("Error connecting to MongoDB:", e)
    exit(1)

# --- Helper Functions ---

def parse_station_country(station_name: str) -> str:
    """
    Parse the station name (a comma-separated string) to extract the country.
    Removes any parenthesized text.
    """
    parts = [p.strip() for p in station_name.split(",") if p.strip()]
    if len(parts) >= 2:
        country = parts[-1]
        country = re.sub(r"\s*\(.*\)", "", country)
        return country
    return "Unknown"

def get_reading_date(iso_time: str) -> str:
    """Extract the date portion (YYYY-MM-DD) from an ISO timestamp string."""
    try:
        return iso_time.split("T")[0]
    except Exception:
        return ""

async def fetch_station(session: aiohttp.ClientSession, uid: int):
    """
    Fetch station details for a given UID using the WAQI feed endpoint.
    Returns the station data if the API response status is "ok", otherwise None.
    """
    url = f"https://api.waqi.info/feed/@{uid}/?token={API_KEY}"
    try:
        async with session.get(url) as response:
            data = await response.json()
            if data.get("status") == "ok":
                print(f"UID {uid}: Data fetched successfully.")
                return data["data"]
            else:
                print(f"UID {uid}: Invalid response. Status: {data.get('status')}")
    except Exception as e:
        print(f"Error fetching uid {uid}: {e}")
    return None

async def bound_fetch(sem: asyncio.Semaphore, session: aiohttp.ClientSession, uid: int):
    async with sem:
        return await fetch_station(session, uid)

async def fetch_batch(start: int, end: int, sem: asyncio.Semaphore, session: aiohttp.ClientSession):
    tasks = [bound_fetch(sem, session, uid) for uid in range(start, end + 1)]
    results = await asyncio.gather(*tasks)
    return [res for res in results if res is not None]

async def run_batches():
    sem = asyncio.Semaphore(CONCURRENCY)
    all_results = []
    async with aiohttp.ClientSession() as session:
        for batch_start in range(1, MAX_UID + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE - 1, MAX_UID)
            print(f"\nProcessing UIDs from {batch_start} to {batch_end}...")
            batch_results = await fetch_batch(batch_start, batch_end, sem, session)
            for station_data in batch_results:
                update_mongo_with_station(station_data)
            all_results.extend(batch_results)
            print(f"  Valid stations found so far: {len(all_results)}")
            await asyncio.sleep(1)  # brief pause between batches
    return all_results

def update_mongo_with_station(station_data: dict):
    """
    Update or insert a station document in MongoDB.
    - If a document with the given uid exists, update its current reading and:
      * If the reading is from the current day, append the new reading to a 'readings' array.
      * Otherwise, reset the readings array with the new reading.
    - If no document exists, insert a new document with station metadata, current reading,
      and an initial 'readings' array.
    """
    # Use the uid from data if available; otherwise, fallback to station name.
    uid = station_data.get("uid")
    if uid is None:
        uid = station_data.get("station", {}).get("name")
    uid_str = str(uid).strip()
    if not uid_str:
        print("Skipping station with no uid or station name.")
        return

    # Update station_data with uid so that it's stored in the document
    station_data["uid"] = uid_str

    new_reading = {
        "time": station_data.get("time", {}),
        "aqi": station_data.get("aqi"),
        "iaqi": station_data.get("iaqi")
    }
    new_time_iso = station_data.get("time", {}).get("iso", "")
    new_date = get_reading_date(new_time_iso)

    try:
        doc = collection.find_one({"uid": uid_str})
    except Exception as e:
        print(f"Error querying MongoDB for uid {uid_str}: {e}")
        return

    if doc:
        print(f"Updating document for uid {uid_str}.")
        update_fields = {"current": station_data}
        readings = doc.get("readings", [])
        if readings:
            last_reading = readings[-1]
            last_date = get_reading_date(last_reading.get("time", {}).get("iso", ""))
            if last_date == new_date:
                result = collection.update_one(
                    {"uid": uid_str},
                    {"$set": update_fields, "$push": {"readings": new_reading}}
                )
                print(f"Appended new reading for uid {uid_str} on {new_date}. (Matched: {result.matched_count}, Modified: {result.modified_count})")
            else:
                result = collection.update_one(
                    {"uid": uid_str},
                    {"$set": {"current": station_data, "readings": [new_reading]}}
                )
                print(f"New day detected. Reset readings for uid {uid_str} to new day {new_date}. (Matched: {result.matched_count}, Modified: {result.modified_count})")
        else:
            result = collection.update_one(
                {"uid": uid_str},
                {"$set": {"current": station_data, "readings": [new_reading]}}
            )
            print(f"Created readings array for uid {uid_str}. (Matched: {result.matched_count}, Modified: {result.modified_count})")
    else:
        new_doc = {
            "uid": uid_str,
            "station": station_data.get("station", {}),
            "current": station_data,
            "readings": [new_reading]
        }
        try:
            result = collection.insert_one(new_doc)
            print(f"Inserted new document for uid {uid_str}. Inserted ID: {result.inserted_id}")
        except Exception as e:
            print(f"Error inserting new document for uid {uid_str}: {e}")

def main():
    try:
        while True:
            print("\nStarting data fetch at", datetime.now().isoformat())
            results = asyncio.run(run_batches())
            print(f"Total valid stations fetched in this run: {len(results)}")
            # Sleep until the start of the next hour
            now = datetime.now()
            next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            sleep_seconds = (next_hour - now).total_seconds()
            print(f"Sleeping for {sleep_seconds} seconds until next run...")
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    from datetime import timedelta  # Import timedelta here
    main()
