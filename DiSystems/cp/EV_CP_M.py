# cp_monitor.py
import asyncio
import json, os
import sys
from common.protocol_utils import pack_message, unpack_message
from aiokafka import AIOKafkaProducer
import time

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
TELEMETRY_TOPIC = CONFIG["topics"]["telemetry"]
ALERTS_TOPIC = CONFIG["topics"]["alerts"]

if len(sys.argv) < 4:
    print("Usage: python cp_monitor.py <ENGINE_HOST> <ENGINE_PORT> <CP_ID>")
    sys.exit(1)

ENGINE_HOST = sys.argv[1]
ENGINE_PORT = int(sys.argv[2])
CP_ID = sys.argv[3]

class Monitor:
    def __init__(self):
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await self.producer.start()
        while True:
            try:
                reader, writer = await asyncio.open_connection(ENGINE_HOST, ENGINE_PORT)
                print("Connected to engine monitor port")
                try:
                    while True:
                        # send heartbeat payload (simple string)
                        msg = pack_message("HEARTBEAT#MONITOR")
                        writer.write(msg)
                        await writer.drain()
                        # expect single-byte ACK/NACK
                        resp = await reader.readexactly(1)
                        if resp != b'\x06':  # ACK
                            await self.send_alert("KO_RESPONSE")
                            break
                        await asyncio.sleep(1)
                except asyncio.IncompleteReadError:
                    print("Engine connection closed.")
                    await self.send_alert("NO_RESPONSE")
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except:
                        pass
                except Exception as e:
                    print("Monitor inner error:", e)
                    await self.send_alert("ERROR")
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except:
                        pass
            except Exception as e:
                print("Cannot connect to engine monitor port:", e)
                await self.send_alert("CONNECT_FAIL")
                await asyncio.sleep(3)

    async def send_alert(self, reason):
        payload = {
            "cp_id": CP_ID,
            "reason": reason,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }
        print("Sending alert:", payload)
        await self.producer.send_and_wait(ALERTS_TOPIC, json.dumps(payload).encode('utf-8'))

async def main():
    m = Monitor()
    await m.start()

if __name__ == "__main__":
    asyncio.run(main())
