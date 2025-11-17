import asyncio, json, os, sys, time
from common.protocol_utils import pack_message, unpack_message
from aiokafka import AIOKafkaProducer

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
STATUS_TOPIC = CONFIG["topics"]["cp_status"]

if len(sys.argv) < 4:
    print("Usage: python -m cp.EV_CP_M <ENGINE_HOST> <ENGINE_PORT> <CP_ID>")
    sys.exit(1)

ENGINE_HOST = sys.argv[1]
ENGINE_PORT = int(sys.argv[2])
CP_ID = sys.argv[3]


class CPMonitor:
    def __init__(self):
        self.kafka = None
        self.alive = False

    async def start(self):
        self.kafka = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await self.kafka.start()

        print(f"[MON-{CP_ID}] Monitor started for {ENGINE_HOST}:{ENGINE_PORT}")

        while True:
            try:
                reader, writer = await asyncio.open_connection(ENGINE_HOST, ENGINE_PORT)
                print(f"[MON-{CP_ID}] CONNECTED")

                # Recovered
                if not self.alive:
                    await self.send_status("OK")
                    self.alive = True

                while True:
                    writer.write(pack_message("PING"))
                    await writer.drain()

                    try:
                        resp = await asyncio.wait_for(reader.readexactly(1), timeout=2)
                    except asyncio.TimeoutError:
                        print(f"[MON-{CP_ID}] TIMEOUT")
                        raise ConnectionError

                    if resp != b'\x06':
                        print(f"[MON-{CP_ID}] BAD ACK")
                        raise ConnectionError

                  
                    await self.send_status("OK")

                    await asyncio.sleep(2)

            except:
                if self.alive:
                    print(f"[MON-{CP_ID}] LOST ENGINE")
                    await self.send_status("DISCONNECTED")
                    self.alive = False
                await asyncio.sleep(2)

    async def send_status(self, status):
        msg = {"cp_id": CP_ID, "status": status}
        print(f"[MON-{CP_ID}] STATUS:", msg)
        await self.kafka.send_and_wait(STATUS_TOPIC, json.dumps(msg).encode())


async def main():
    m = CPMonitor()
    await m.start()

if __name__ == "__main__":
    asyncio.run(main())
