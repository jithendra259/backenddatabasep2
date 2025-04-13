import asyncio
import aiohttp
import pymongo
import re
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# --- Configuration ---
API_KEY = "c69bd9a20bccfbbe7b4f2e37a17b1a2f2332b423"
MAX_UID = 15000          # Change as needed
BATCH_SIZE = 10000       # Adjust based on performance
CONCURRENCY = 800

MONGO_URI = "mongodb+srv://kandulas:7WiHXWMQZH3DVvyr@cluster0.jsark.mongodb.net/"
DATABASE_NAME = "aqidb"
COLLECTION_NAME = "waqi_stations"

# --- MongoDB Setup ---
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# --- Helper Functions ---

def get_date_from_iso(iso_str: str) -> str:
    """Extract the YYYY-MM-DD part from an ISO timestamp."""
    try:
        return iso_str.split("T")[0]
    except Exception:
        return None

async def fetch_station(session: aiohttp.ClientSession, uid: int):
    """
    Fetch station details for a given UID.
    Returns station data if API returns status ok.
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

async def fetch_batch(start: int, end: int, sem: asyncio.Semaphore, session: aiohttp.ClientSession):
    tasks = [asyncio.create_task(fetch_station(session, uid)) for uid in range(start, end + 1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    valid_results = []
    for res in results:
        if isinstance(res, Exception):
            print(f"Error in fetch_batch: {res}")
        elif res is not None:
            valid_results.append(res)
    return valid_results

async def run_batches():
    timeout = aiohttp.ClientTimeout(total=30)  # 30-second timeout for each request
    sem = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for batch_start in range(1, MAX_UID + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE - 1, MAX_UID)
            print(f"\nProcessing UIDs from {batch_start} to {batch_end}...")
            batch_results = await fetch_batch(batch_start, batch_end, sem, session)
            for station_data in batch_results:
                await update_mongo_with_station(station_data)
            await asyncio.sleep(1)  # Pause briefly between batches

async def update_mongo_with_station(station_data: dict):
    """
    Update or insert a station document.
      - If a document exists for a given idx and city name, push the new reading.
      - Otherwise, insert a new document (with forecastdaily field initialized).
    """
    idx = station_data.get("idx")
    city = station_data.get("city", {}).get("name", "")
    geo = station_data.get("city", {}).get("geo", [])
    if not idx or not city:
        print("Skipping station due to missing idx or city name.")
        return

    query = {"idx": idx, "city.name": city}
    try:
        existing_doc = collection.find_one(query)
    except Exception as e:
        print(f"Error querying MongoDB for {query}: {e}")
        return

    timestamp = datetime.now(timezone.utc)
    new_reading = {
        "time": station_data.get("time", {}),
        "aqi": station_data.get("aqi"),
        "iaqi": station_data.get("iaqi"),
        "updated_at": timestamp
    }
    if existing_doc:
        collection.update_one(
            query,
            {"$set": {"current": station_data, "updated_at": timestamp},
             "$push": {"readings": new_reading}}
        )
        print(f"Updated station idx {idx} ({city}).")
    else:
        new_doc = {
            "idx": idx,
            "city": {"name": city, "geo": geo},
            "station": station_data.get("station", {}),
            "current": station_data,
            "readings": [new_reading],
            "forecastdaily": {},  # Initialize forecastdaily field
            "created_at": timestamp,
            "updated_at": timestamp
        }
        collection.insert_one(new_doc)
        print(f"Inserted new station idx {idx} ({city}).")

def prune_old_readings():
    """
    Remove any readings older than 48 hours from all documents.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    result = collection.update_many({}, {"$pull": {"readings": {"updated_at": {"$lt": cutoff}}}})
    print(f"Pruned old readings from {result.modified_count} documents.")

def compute_daily_summary(readings: list, day: str, param: str) -> dict:
    """
    Compute average, min, and max for a given parameter from readings on a specific day.
    For top-level 'aqi', use reading['aqi']. For other parameters, check within 'iaqi'.
    """
    values = []
    for reading in readings:
        iso_time = reading.get("time", {}).get("iso")
        reading_day = get_date_from_iso(iso_time) if iso_time else None
        if reading_day == day:
            if param == "aqi":
                value = reading.get("aqi")
                if isinstance(value, (int, float)):
                    values.append(value)
            else:
                iaqi = reading.get("iaqi", {})
                if param in iaqi:
                    param_obj = iaqi.get(param)
                    if isinstance(param_obj, dict) and "v" in param_obj:
                        value = param_obj["v"]
                    elif isinstance(param_obj, (int, float)):
                        value = param_obj
                    else:
                        value = None
                    if isinstance(value, (int, float)):
                        values.append(value)
    if not values:
        return None
    avg_val = sum(values) / len(values)
    return {"day": day, "avg": round(avg_val, 2), "min": min(values), "max": max(values)}

def update_daily_forecast():
    """
    For each station document, aggregate the day's readings (for days before today) and
    update the forecastdaily field with summary stats (avg, min, max) for defined parameters.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    parameters = ["aqi", "o3", "pm10", "pm25", "uvi", "no2", "dew", "p", "t", "w", "wg"]

    # Convert the cursor to a list to prevent cursor timeout
    docs = list(collection.find({}))
    for doc in docs:
        readings = doc.get("readings", [])
        forecastdaily = doc.get("forecastdaily", {})
        days = {get_date_from_iso(reading.get("time", {}).get("iso"))
                for reading in readings if reading.get("time", {}).get("iso")
                and get_date_from_iso(reading.get("time", {}).get("iso")) < today}
        if not days:
            continue
        for day in days:
            for param in parameters:
                summaries = forecastdaily.get(param, [])
                if any(summary.get("day") == day for summary in summaries):
                    continue
                summary = compute_daily_summary(readings, day, param)
                if summary:
                    summaries.append(summary)
                    forecastdaily[param] = summaries
        collection.update_one({"_id": doc["_id"]}, {"$set": {"forecastdaily": forecastdaily}})
        print(f"Updated forecast for station idx {doc.get('idx')} ({doc.get('city', {}).get('name', '')}).")

def main():
    try:
        print("\nStarting data fetch at", datetime.now(timezone.utc).isoformat())
        # Run asynchronous data fetch and update operations once
        asyncio.run(run_batches())
        # Prune readings older than 48 hours
        prune_old_readings()
        # Update daily forecasts
        update_daily_forecast()
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    main()
