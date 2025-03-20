"""
Flask Application for Continuous Data Fetching and MongoDB Updates

This application fetches data from an external API and updates a MongoDB database.
It runs a background task that fetches data every hour and prunes old data.
The Flask server has a single route ("/") to keep the application awake on Glitch.

Deployment Instructions:
1. Create a new Glitch project and upload this app.py file.
2. Configure environment variables (API_KEY, MONGO_URI) in the .env file or Glitch's Secrets settings.
3. Install the required dependencies listed in requirements.txt.
4. Start the project to run the Flask server and background task.
"""

import os
import logging
import asyncio
import aiohttp
import nest_asyncio
from flask import Flask
from pymongo import MongoClient
from datetime import datetime, timedelta
import threading
import time

# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client['your_database_name']  # Replace with your database name
collection = db['your_collection_name']  # Replace with your collection name

# Function to fetch data from the external API
async def fetch_data(session, uid):
    url = f"https://api.waqi.info/feed/{uid}/?token={os.getenv('API_KEY')}"
    async with session.get(url) as response:
        return await response.json()

# Function to process and update MongoDB
async def process_data():
    async with aiohttp.ClientSession() as session:
        uids = ['station_uid1', 'station_uid2']  # Replace with actual station UIDs
        for uid in uids:
            data = await fetch_data(session, uid)
            if data and 'data' in data:
                # Update or insert data into MongoDB
                collection.update_one({'uid': uid}, {'$set': data['data']}, upsert=True)
                logging.info(f"Updated data for UID: {uid}")

# Background task to fetch data every hour
def background_task():
    nest_asyncio.apply()
    while True:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_data())
        loop.close()
        
        # Prune data older than 48 hours
        cutoff_time = datetime.now() - timedelta(hours=48)
        collection.delete_many({'timestamp': {'$lt': cutoff_time}})
        
        # Sleep until the next full hour
        now = datetime.now()
        seconds_until_next_hour = (60 - now.minute) * 60 - now.second
        time.sleep(seconds_until_next_hour)

# Start the background task in a separate thread
threading.Thread(target=background_task, daemon=True).start()

# Flask route to keep the application awake
@app.route('/')
def index():
    return "OK"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
