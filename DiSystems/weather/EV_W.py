import asyncio
import aiohttp
import json
import os
from time import time

# load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

OPENW = CONFIG["openweather"]
API_KEY = OPENW["api_key"]
BASE_URL = OPENW["base_url"]
INTERVAL = OPENW.get("poll_interval_sec", 10)

CENTRAL_API = CONFIG["central_api_url"]


class EVWeatherService:
    def __init__(self):
        self.last_state = {}  

    async def start(self):
        print("[EV_W] Weather Service started (OpenWeather)")
        while True:
            try:
                cities = await self.fetch_cities()
                for city in cities:
                    await self.check_city(city)
            except Exception as e:
                print("[EV_W] Error:", e)

            await asyncio.sleep(INTERVAL)

    # get cities dynamically from central
    async def fetch_cities(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{CENTRAL_API}/locations", timeout=3) as r:
                return await r.json()

    # query OpenWeather 
    async def check_city(self, city):
        params = {
            "q": city,
            "appid": API_KEY,
            "units": "metric"  # °C !
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(BASE_URL, params=params, timeout=5) as r:
                data = await r.json()

        temp = data["main"]["temp"]
        wind = data["wind"]["speed"]

        print(f"[EV_W] {city}: {temp}°C, wind {wind}")

        # always send update 
        await self.send_weather_update(city, temp, wind)

        # alert logic 
        if temp < 0:
            if self.last_state.get(city) != "ALERT":
                await self.send_alert(city, "LOW_TEMPERATURE", temp, wind)
                self.last_state[city] = "ALERT"
        else:
            self.last_state[city] = "OK"

    # REST calls to central
    async def send_alert(self, city, alert, temp, wind):
        print(f"[EV_W] ALERT {alert} in {city}")
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{CENTRAL_API}/weather_alert",
                json={
                    "city": city,
                    "alert": alert,
                    "temp": temp,
                    "wind": wind
                }
            )

    async def send_weather_update(self, city, temp, wind):
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{CENTRAL_API}/weather_update",
                json={
                    "city": city,
                    "temp": temp,
                    "wind": wind
                }
            )


async def main():
    service = EVWeatherService()
    await service.start()


if __name__ == "__main__":
    asyncio.run(main())
