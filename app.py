import asyncio
import aiohttp
import pymongo
from datetime import datetime, timezone

# --- Configuration ---
API_KEY = "c69bd9a20bccfbbe7b4f2e37a17b1a2f2332b423"
START_UID = 1     # Start fetching from UID 1000
MAX_UID = 15000
BATCH_SIZE = 10000
CONCURRENCY = 800

MONGO_URI = "mongodb+srv://kandulas:nW2kmyupZDuAw751@cluster0.gv74jnl.mongodb.net/"
DATABASE_NAME = "aqidb2"
COLLECTION_NAME = "waqi_stations2"

# --- MongoDB Setup ---
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def get_date_from_iso(iso_str: str) -> str:
    """Extract the YYYY-MM-DD part from an ISO timestamp."""
    try:
        return iso_str.split("T")[0]
    except Exception:
        return None

async def fetch_station(session: aiohttp.ClientSession, uid: int) -> dict:
    url = f"https://api.waqi.info/feed/@{uid}/?token={API_KEY}"
    try:
        async with session.get(url) as response:
            data = await response.json()
            if data.get("status") == "ok":
                return data["data"]
    except Exception as e:
        print(f"Error fetching uid {uid}: {e}")
    return None

async def limited_fetch_station(session: aiohttp.ClientSession, uid: int, sem: asyncio.Semaphore) -> dict:
    async with sem:
        return await fetch_station(session, uid)

async def fetch_batch(start: int, end: int, sem: asyncio.Semaphore, session: aiohttp.ClientSession) -> list:
    tasks = [asyncio.create_task(limited_fetch_station(session, uid, sem))
             for uid in range(start, end + 1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # Filter out exceptions and None results
    valid_results = [res for res in results if res and not isinstance(res, Exception)]
    return valid_results

async def run_batches():
    timeout = aiohttp.ClientTimeout(total=30)
    sem = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for batch_start in range(START_UID, MAX_UID + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE - 1, MAX_UID)
            print(f"\nProcessing UIDs from {batch_start} to {batch_end}...")
            batch_results = await fetch_batch(batch_start, batch_end, sem, session)
            for station_data in batch_results:
                await update_mongo_with_station(station_data)
            await asyncio.sleep(1)

async def update_mongo_with_station(station_data: dict):
    idx = station_data.get("idx")
    city = station_data.get("city", {}).get("name", "")
    geo = station_data.get("city", {}).get("geo", [])
    if not idx or not city:
        print("Skipping station due to missing idx or city name.")
        return

    query = {"idx": idx, "city.name": city}
    timestamp = datetime.now(timezone.utc)
    new_reading = {
        "time": station_data.get("time", {}),
        "aqi": station_data.get("aqi"),
        "iaqi": station_data.get("iaqi"),
        "updated_at": timestamp
    }
    update_doc = {
        "$set": {"current": station_data, "updated_at": timestamp},
        "$push": {"readings": new_reading},
        "$setOnInsert": {
            "created_at": timestamp,
            "forecastdaily": {},
            "station": station_data.get("station", {}),
            "city": {"name": city, "geo": geo}
        }
    }
    result = collection.update_one(query, update_doc, upsert=True)
    if result.matched_count:
        print(f"Updated station idx {idx} ({city}).")
    else:
        print(f"Inserted new station idx {idx} ({city}).")

def compute_daily_summary(readings: list, day: str, param: str) -> dict:
    values = []
    for reading in readings:
        iso_time = reading.get("time", {}).get("iso")
        reading_day = get_date_from_iso(iso_time) if iso_time else None
        if reading_day == day:
            if param == "aqi":
                value = reading.get("aqi")
            else:
                iaqi = reading.get("iaqi", {})
                param_obj = iaqi.get(param)
                value = param_obj.get("v") if isinstance(param_obj, dict) else param_obj
            if isinstance(value, (int, float)):
                values.append(value)
    if not values:
        return None
    avg_val = sum(values) / len(values)
    return {"day": day, "avg": round(avg_val, 2), "min": min(values), "max": max(values)}

def update_daily_forecast():
    today = datetime.now(timezone.utc).date().isoformat()
    parameters = ["aqi", "o3", "pm10", "pm25", "uvi", "no2", "dew", "p", "t", "w", "wg"]
    docs = list(collection.find({}))
    for doc in docs:
        readings = doc.get("readings", [])
        forecastdaily = doc.get("forecastdaily", {})
        days = {get_date_from_iso(r.get("time", {}).get("iso")) for r in readings
                if r.get("time", {}).get("iso") and get_date_from_iso(r.get("time", {}).get("iso")) < today}
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
        asyncio.run(run_batches())
        update_daily_forecast()
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    main()
