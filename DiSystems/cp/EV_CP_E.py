import asyncio
import json, os
import random
import sys
import time
from common.protocol_utils import pack_message, unpack_message
from aiokafka import AIOKafkaProducer

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
TELEMETRY_TOPIC = CONFIG["topics"]["telemetry"]
ALERTS_TOPIC = CONFIG["topics"]["alerts"]

CENTRAL_HOST = "127.0.0.1"
CENTRAL_PORT = 9002

if len(sys.argv) < 5:
    print("Usage: python cp_engine.py <CP_ID> <LOCATION> <PRICE> <MONITOR_PORT>")
    sys.exit(1)

CP_ID = sys.argv[1]
LOCATION = sys.argv[2]
PRICE = float(sys.argv[3])
MONITOR_PORT = int(sys.argv[4])


class CPEngine:
    def __init__(self):
        self.session = None
        self.driver_id = None
        self.producer = None
        self.supplying = False
        self.central_writer = None
        self.central_reader = None

    async def start(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await self.producer.start()
        asyncio.create_task(self.monitor_server())
        await self.connect_central()

    async def monitor_server(self):
        #monitor server, to receive Heartbeat messages 
        async def handle(reader, writer):
            try:
                while True:
                    data = await reader.readuntil(b'\x03')
                    lrc = await reader.readexactly(1)
                    raw = data + lrc
                    payload, ok = unpack_message(raw)
                    writer.write(b'\x06' if ok else b'\x15')
                    await writer.drain()
            except asyncio.IncompleteReadError:
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(handle, '0.0.0.0', MONITOR_PORT)
        print(f"[{CP_ID}] Monitor server listening on {MONITOR_PORT}")
        async with server:
            await server.serve_forever()

    async def connect_central(self):
        #conects to central and sends alerts when there is a mistake 
        while True:
            try:
                reader, writer = await asyncio.open_connection(CENTRAL_HOST, CENTRAL_PORT)
                self.central_reader = reader
                self.central_writer = writer
                reg = f"REGISTER#{CP_ID}#{LOCATION}#{PRICE}"
                writer.write(pack_message(reg))
                await writer.drain()
                print(f"[{CP_ID}] REGISTER sent.")
                asyncio.create_task(self.listen_central())
                break
            except Exception:
                print(f"[{CP_ID}] Central not reachable — retry in 3s")
                await self.send_alert("CONNECT_FAIL")
                await asyncio.sleep(3)

        while True:
            await asyncio.sleep(60)

    async def send_alert(self, reason: str):
        #sends alert to kafka 
        alert = {"cp_id": CP_ID, "reason": reason, "ts": time.strftime("%Y-%m-%d %H:%M:%S")}
        try:
            await self.producer.send_and_wait(ALERTS_TOPIC, json.dumps(alert).encode())
            print(f"[{CP_ID}] ⚠️ ALERT sent: {reason}")
        except Exception as e:
            print(f"[{CP_ID}] Failed to send alert: {e}")

    async def listen_central(self):
        #receives commands from central
        try:
            while True:
                data = await self.central_reader.readuntil(b'\x03')
                lrc = await self.central_reader.readexactly(1)
                raw = data + lrc
                payload, ok = unpack_message(raw)
                if not ok or payload is None:
                    continue

                print(f"[{CP_ID}] Central -> {payload}")

                if payload.startswith("START#"):
                    _, session_id, driver_id = payload.split("#")
                    self.session = session_id
                    self.driver_id = driver_id
                    asyncio.create_task(self.start_supply())

                elif payload.startswith("STOP#"):
                    _, session_id = payload.split("#")
                    if session_id == self.session:
                        print(f"[{CP_ID}] STOP received for session {session_id}")
                        self.supplying = False

        except asyncio.IncompleteReadError:
            print(f"[{CP_ID}] Central disconnected.")
            await self.send_alert("NO_RESPONSE")
            await self.connect_central()
        except Exception as e:
            print(f"[{CP_ID}] listen_central error: {e}")
            await self.send_alert("ERROR")

    async def start_supply(self):
        #charging session simulation
        try:
            print(f"[{CP_ID}] Started charging session for driver {self.driver_id}")
            self.supplying = True
            total_kwh = 0.0
            kwh_rate = 7.2

            while self.supplying:
                kw = kwh_rate + random.uniform(-0.2, 0.2)
                total_kwh += (kw / 3600.0)
                eur = total_kwh * PRICE

                telemetry = {
                    "cp_id": CP_ID,
                    "session_id": self.session,
                    "driver_id": self.driver_id,
                    "kw": round(kw, 3),
                    "total_kwh": round(total_kwh, 6),
                    "eur": round(eur, 4),
                    "status": "CHARGING",
                    "ts": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                await self.producer.send_and_wait(TELEMETRY_TOPIC, json.dumps(telemetry).encode())
                await asyncio.sleep(1)

                # 20 sekunden ladezeit
                if total_kwh >= 0.03:
                    self.supplying = False
                    break

        except Exception as e:
            print(f"[{CP_ID}] Error during supply: {e}")
            await self.send_alert("ERROR")
        finally:
            final = {
                "cp_id": CP_ID,
                "session_id": self.session,
                "driver_id": self.driver_id,
                "status": "FINISHED",
                "ts": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            await self.producer.send_and_wait(TELEMETRY_TOPIC, json.dumps(final).encode())
            print(f"[{CP_ID}] Session finished.")
            self.supplying = False
            self.session = None
            self.driver_id = None


async def main():
    engine = CPEngine()
    await engine.start()


if __name__ == "__main__":
    asyncio.run(main())
