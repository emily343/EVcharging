import asyncio
import json
import os
import sys
import time

from aiokafka import AIOKafkaProducer
from common.protocol_utils import pack_message, unpack_message


# -----------------------------
# CONFIG
# -----------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
STATUS_TOPIC = CONFIG["topics"]["cp_status"]


# -----------------------------
# ARGUMENTS
# -----------------------------
if len(sys.argv) < 5:
    print("Usage: python -m cp.EV_CP_M <ENGINE_HOST> <ENGINE_PORT> <MONITOR_ID> <CREDENTIAL>")
    sys.exit(1)

ENGINE_HOST = sys.argv[1]
ENGINE_PORT = int(sys.argv[2])
MONITOR_ID = sys.argv[3]
CREDENTIAL = sys.argv[4]

CENTRAL_HOST = os.getenv("CENTRAL_HOST", "127.0.0.1")
CENTRAL_PORT = int(os.getenv("CENTRAL_PORT", "9002"))


class CPMonitor:
    def __init__(self):
        self.kafka = None
        self.engine_alive = False

        self.central_reader = None
        self.central_writer = None

    # -------------------------------------------------
    # 1) AUTHENTICATION WITH CENTRAL
    # -------------------------------------------------
    async def authenticate_with_central(self):
        """Authenticate monitor at Central via TCP."""
        while True:
            try:
                r, w = await asyncio.open_connection(CENTRAL_HOST, CENTRAL_PORT)
                self.central_reader = r
                self.central_writer = w

                # Format must match EV_Central.py
                auth_msg = f"AUTH_MON#{MONITOR_ID}#{CREDENTIAL}"
                w.write(pack_message(auth_msg))
                await w.drain()

                # Wait for ACK
                raw = await r.readuntil(b"\x03")
                lrc = await r.readexactly(1)
                payload, ok = unpack_message(raw + lrc)

                if ok and payload.startswith("AUTH_MON_OK"):
                    print(f"[MON-{MONITOR_ID}] Authenticated at Central ✓")
                    return
                else:
                    print(f"[MON-{MONITOR_ID}] Auth failed → {payload}")
                    await asyncio.sleep(2)

            except Exception as e:
                print(f"[MON-{MONITOR_ID}] Central not reachable: {e}")
                await asyncio.sleep(2)

    # -------------------------------------------------
    # 2) REPORT STATUS TO CENTRAL (Kafka)
    # -------------------------------------------------
    async def send_status(self, status: str):
        msg = {"cp_id": MONITOR_ID, "status": status}
        print(f"[MON-{MONITOR_ID}] STATUS {status}")
        await self.kafka.send_and_wait(STATUS_TOPIC, json.dumps(msg).encode())

    # -------------------------------------------------
    # 3) MAIN LOOP
    # -------------------------------------------------
    async def start(self):
        self.kafka = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await self.kafka.start()

        # Authenticate monitor once at startup
        await self.authenticate_with_central()

        print(f"[MON-{MONITOR_ID}] Monitor running, engine = {ENGINE_HOST}:{ENGINE_PORT}")

        while True:
            try:
                reader, writer = await asyncio.open_connection(ENGINE_HOST, ENGINE_PORT)
                print(f"[MON-{MONITOR_ID}] CONNECTED to Engine ✓")

                # Just switched from offline → online
                if not self.engine_alive:
                    await self.send_status("OK")
                    self.engine_alive = True

                # Heartbeat loop
                while True:
                    writer.write(pack_message("PING"))
                    await writer.drain()

                    try:
                        # Engine must reply with ACK byte 0x06
                        resp = await asyncio.wait_for(reader.readexactly(1), timeout=2)
                    except asyncio.TimeoutError:
                        print(f"[MON-{MONITOR_ID}] TIMEOUT (no ACK from engine)")
                        raise ConnectionError

                    if resp != b"\x06":
                        print(f"[MON-{MONITOR_ID}] BAD ENGINE RESPONSE")
                        raise ConnectionError

                    # Heartbeat OK
                    await self.send_status("OK")
                    await asyncio.sleep(2)

            except Exception as e:
                if self.engine_alive:
                    print(f"[MON-{MONITOR_ID}] Engine LOST ✗: {e}")
                    await self.send_status("DISCONNECTED")
                    self.engine_alive = False

                # Retry engine connection
                await asyncio.sleep(2)


# Entry point
async def main():
    mon = CPMonitor()
    await mon.start()


if __name__ == "__main__":
    asyncio.run(main())
