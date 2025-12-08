import asyncio, json, os, sys, time
from common.protocol_utils import pack_message, unpack_message
from aiokafka import AIOKafkaProducer

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
STATUS_TOPIC = CONFIG["topics"]["cp_status"]

if len(sys.argv) < 5:
    print("Usage: python -m cp.EV_CP_M <ENGINE_HOST> <ENGINE_PORT> <MONITOR_ID> <CREDENTIAL>")
    sys.exit(1)

ENGINE_HOST = sys.argv[1]
ENGINE_PORT = int(sys.argv[2])
MONITOR_ID = sys.argv[3]
CREDENTIAL = sys.argv[4]     # comes from Registry


CENTRAL_HOST = "127.0.0.1"
CENTRAL_PORT = 9002


class CPMonitor:
    def __init__(self):
        self.kafka = None
        self.alive = False
        self.central_writer = None
        self.central_reader = None

    # --- 1) AUTHENTICATE WITH CENTRAL ---
    async def auth_with_central(self):
        while True:
            try:
                r, w = await asyncio.open_connection(CENTRAL_HOST, CENTRAL_PORT)
                self.central_reader = r
                self.central_writer = w

                # send auth
                auth_msg = f"AUTH_MON#{MONITOR_ID}#{CREDENTIAL}"
                w.write(pack_message(auth_msg))
                await w.drain()

                # wait for response
                data = await r.readuntil(b"\x03")
                lrc = await r.readexactly(1)
                payload, ok = unpack_message(data + lrc)

                if ok and payload.startswith("AUTH_MON_OK"):
                    print(f"[MON-{MONITOR_ID}] Authenticated at Central")
                    return
                else:
                    print(f"[MON-{MONITOR_ID}] Central auth failed: {payload}")
                    await asyncio.sleep(2)

            except Exception as e:
                print(f"[MON-{MONITOR_ID}] Central not reachable: {e}")
                await asyncio.sleep(2)

    # --- 2) MAIN LOOP ---
    async def start(self):
        self.kafka = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await self.kafka.start()

        # AUTH with Central
        await self.auth_with_central()

        print(f"[MON-{MONITOR_ID}] Monitor running, engine = {ENGINE_HOST}:{ENGINE_PORT}")

        # HEARTBEAT LOOP
        while True:
            try:
                reader, writer = await asyncio.open_connection(ENGINE_HOST, ENGINE_PORT)
                print(f"[MON-{MONITOR_ID}] CONNECTED to Engine")

                if not self.alive:
                    await self.send_status("OK")
                    self.alive = True

                while True:
                    writer.write(pack_message("PING"))
                    await writer.drain()

                    try:
                        resp = await asyncio.wait_for(reader.readexactly(1), timeout=2)
                    except asyncio.TimeoutError:
                        print(f"[MON-{MONITOR_ID}] TIMEOUT")
                        raise ConnectionError

                    if resp != b"\x06":
                        print(f"[MON-{MONITOR_ID}] BAD ACK")
                        raise ConnectionError

                    await self.send_status("OK")

                    await asyncio.sleep(2)

            except:
                if self.alive:
                    print(f"[MON-{MONITOR_ID}] LOST ENGINE")
                    await self.send_status("DISCONNECTED")
                    self.alive = False

                await asyncio.sleep(2)

    async def send_status(self, status):
        msg = {"cp_id": MONITOR_ID, "status": status}
        print(f"[MON-{MONITOR_ID}] STATUS:", msg)
        await self.kafka.send_and_wait(STATUS_TOPIC, json.dumps(msg).encode())


async def main():
    m = CPMonitor()
    await m.start()

if __name__ == "__main__":
    asyncio.run(main())
