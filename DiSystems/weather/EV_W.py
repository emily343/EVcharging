import asyncio
import json
import os
import aiohttp
from aiokafka import AIOKafkaProducer
from time import time
from central.central_db import log_audit


# ---- Load config ----
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
TOPIC_ALERTS = CONFIG["topics"]["weather_alerts"]
CENTRAL_API_URL = CONFIG.get("central_api_url", "http://127.0.0.1:8000/api")


# Dummy API Key – kann jeder beliebige sein (Open-Meteo braucht keinen)
WEATHER_URL = "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&windspeed_unit=kmh&current_weather=true"

# Example city coordinates
CITY_COORDS = {
    "Berlin": (52.52, 13.40),
    "Madrid": (40.41, -3.70),
    "Paris": (48.85, 2.35),
    "Rome": (41.90, 12.49),
    "Lisbon": (38.72, -9.14),
}


class EVWeatherService:
    def __init__(self):
        self.kafka = None

    async def start(self):
        print("[EV_W] Starting Weather Service…")

        self.kafka = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await self.kafka.start()

        while True:
            await self.check_all_cities()
            await asyncio.sleep(15)  # every 15 seconds

    async def check_all_cities(self):
        for city, (lat, lon) in CITY_COORDS.items():
            try:
                await self.check_city(city, lat, lon)
            except Exception as e:
                print(f"[EV_W] Error fetching weather for {city}: {e}")

    async def check_city(self, city, lat, lon):
        url = WEATHER_URL.format(lat=lat, lon=lon)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()

        current = data.get("current_weather", {})
        temp = current.get("temperature")
        wind = current.get("windspeed")
        condition = str(current.get("weathercode"))

        # ---- Determine alerts ----
        alerts = []

        if wind and wind > 50:
            alerts.append("HIGH_WIND")

        # Weathercode 95–99 = Thunderstorm (Open-Meteo codes)
        if condition in ("95", "96", "99"):
            alerts.append("STORM_WARNING")

        if temp and temp > 35:
            alerts.append("HEAT_WARNING")

        # send alerts 
        for alert in alerts:
            msg = {
                "timestamp": time(),
                "city": city,
                "alert": alert,
                "temp": temp,
                "wind": wind,
            }



            # central REST API 
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f"{CENTRAL_API_URL}/weather_alert",
                        json={
                            "city": city,
                            "alert": alert,
                            "temp": temp,
                            "wind": wind
                        },
                        timeout=3
                    )
            except Exception as e:
                print(f"[EV_W] Central API unreachable: {e}")

            # always send weather update (dashboard data)
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f"{CENTRAL_API_URL}/weather_update",
                        json={
                            "city": city,
                            "temp": temp,
                            "wind": wind
                        },
                        timeout=3
                    )
            except Exception as e:
                print(f"[EV_W] Weather update failed: {e}")



async def main():
    service = EVWeatherService()
    await service.start()


if __name__ == "__main__":
    asyncio.run(main())
