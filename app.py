import asyncio
import aiohttp
import json
import nest_asyncio
import pymongo
import re
import time
from datetime import datetime

# Apply nest_asyncio to allow nested event loops (necessary in Colab/Jupyter)
nest_asyncio.apply()

# --- Configuration ---
API_KEY = "c69bd9a20bccfbbe7b4f2e37a17b1a2f2332b423"
MAX_UID = 100  # For testing, use a lower value; change to 15000 for full-scale run
BATCH_SIZE = 100  # Adjust accordingly for testing; full run might use 10000
CONCURRENCY = 50  # Adjust for testing if needed

# MongoDB connection string (provided)
MONGO_URI = "mongodb+srv://kandulas:7WiHXWMQZH3DVvyr@cluster0.jsark.mongodb.net/"
DATABASE_NAME = "aqidb"           # Updated database name
COLLECTION_NAME = "waqi_stations"  # Collection name

# --- MongoDB Setup ---
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

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
                return data["data"]
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
            print(f"Processing UIDs from {batch_start} to {batch_end}...")
            batch_results = await fetch_batch(batch_start, batch_end, sem, session)
            for station_data in batch_results:
                update_mongo_with_station(station_data)
            all_results.extend(batch_results)
            print(f"  Valid stations found so far: {len(all_results)}")
            # For testing, we can comment out the sleep or reduce it:
            await asyncio.sleep(0.5)
    return all_results

def update_mongo_with_station(station_data: dict):
    """
    If a document with the given uid exists, update its current reading and append
    the new reading to a 'readings' array (if the reading is from the current day).
    Otherwise, insert a new document with the station metadata, current reading, and
    an initial 'readings' array.
    """
    uid = station_data.get("uid")
    if uid is None:
        uid = station_data.get("station", {}).get("name")
    if not uid:
        return

    # Prepare the new reading data
    new_reading = {
        "time": station_data.get("time", {}),
        "aqi": station_data.get("aqi"),
        "iaqi": station_data.get("iaqi")
    }
    # Get the date of the new reading (YYYY-MM-DD)
    new_time_iso = station_data.get("time", {}).get("iso", "")
    new_date = get_reading_date(new_time_iso)

    # Check if document exists
    doc = collection.find_one({"uid": uid})
    if doc:
        # Update current reading
        update_fields = {"current": station_data}
        readings = doc.get("readings", [])
        if readings:
            last_reading = readings[-1]
            last_date = get_reading_date(last_reading.get("time", {}).get("iso", ""))
            if last_date == new_date:
                # Append the new reading
                collection.update_one(
                    {"uid": uid},
                    {"$set": update_fields, "$push": {"readings": new_reading}}
                )
            else:
                # New day: reset the readings array
                collection.update_one(
                    {"uid": uid},
                    {"$set": {"current": station_data, "readings": [new_reading]}}
                )
        else:
            # Create readings array if missing
            collection.update_one(
                {"uid": uid},
                {"$set": {"current": station_data, "readings": [new_reading]}}
            )
    else:
        # Insert new document
        new_doc = {
            "uid": uid,
            "station": station_data.get("station", {}),
            "current": station_data,
            "readings": [new_reading]
        }
        collection.insert_one(new_doc)

def main():
    results = asyncio.run(run_batches())
    print(f"\nTotal valid stations fetched: {len(results)}")
    filename = "waqi_stations_test.json"
    try:
        with open(filename, "w") as f:
            json.dump(results, f, indent=2)
        print(f"All fetched station data saved to {filename}")
    except Exception as e:
        print(f"Error saving data: {e}")

if __name__ == "__main__":
    main()
