import asyncio
import json, os, random, sys, time
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from common.protocol_utils import pack_message, unpack_message

# load config 
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
TOPICS = CONFIG["topics"]

TELEMETRY_TOPIC = TOPICS["telemetry"]
STATUS_TOPIC = TOPICS["cp_status"]
EVENTS_TOPIC = TOPICS["cp_events"]
CMD_TOPIC = TOPICS["central_cmd"]

# parse args
if len(sys.argv) < 5:
    print("Usage: python -m cp.EV_CP_E <CP_ID> <LOCATION> <PRICE> <MONITOR_PORT>")
    sys.exit(1)

CP_ID = sys.argv[1]
LOCATION = sys.argv[2]
PRICE = float(sys.argv[3])
MONITOR_PORT = int(sys.argv[4])

CENTRAL_HOST = "127.0.0.1"
CENTRAL_PORT = 9002


class CPEngine:
    def __init__(self):
        self.session = None
        self.driver = None
        self.total_kwh = 0.0
        self.supplying = False
        self.central_writer = None
        self.central_reader = None

    async def start(self):
        print(f"[{CP_ID}] Engine starting…")

        # Kafka Producer
        self.kafka = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        await self.kafka.start()

        # Kafka Consumer for central commands
        self.cmd_consumer = AIOKafkaConsumer(
            CMD_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=f"cp_{CP_ID}",
            auto_offset_reset="latest",
        )
        await self.cmd_consumer.start()
        asyncio.create_task(self.listen_cmd())

        # Heartbeat monitor listener
        asyncio.create_task(self.monitor_server())

        # Register to Central (TCP)
        await self.connect_central()

        # to keep engine alive
        while True:
            await asyncio.sleep(3600)

    # TCP: connect to Central 
    async def connect_central(self):
        while True:
            try:
                r, w = await asyncio.open_connection(CENTRAL_HOST, CENTRAL_PORT)
                self.central_reader, self.central_writer = r, w

                reg = f"REGISTER#{CP_ID}#{LOCATION}#{PRICE}"
                w.write(pack_message(reg))
                await w.drain()

                print(f"[{CP_ID}] ✅ Registered at Central")

                asyncio.create_task(self.listen_central())
                return
            except:
                print(f"[{CP_ID}] ❌ Central offline, retrying…")
                await self.send_status("DISCONNECTED")
                await asyncio.sleep(2)

    # TCP messages from Central
    async def listen_central(self):
        try:
            while True:
                data = await self.central_reader.readuntil(b'\x03')
                lrc = await self.central_reader.readexactly(1)
                payload, ok = unpack_message(data + lrc)
                if not ok:
                    continue

                if payload.startswith("START#"):
                    _, session, driver = payload.split("#")
                    self.session = session
                    self.driver = driver
                    asyncio.create_task(self.start_supply())

                elif payload.startswith("STOP#"):
                    self.supplying = False

        except:
            print(f"[{CP_ID}] ⚠️ Lost connection to Central")
            await self.send_status("DISCONNECTED")
            asyncio.create_task(self.connect_central())

    # Charging Simulation 
    async def start_supply(self):
        print(f"[{CP_ID}] 🚗 Charging session {self.session} for driver {self.driver}")
        self.supplying = True
        self.total_kwh = 0.0

        await self.send_status("OK")  # CP is fully operational

        while self.supplying:
            kw = 7.2 + random.uniform(-0.3, 0.3)
            self.total_kwh += kw / 3600.0
            eur = self.total_kwh * PRICE

            telemetry = {
                "cp_id": CP_ID,
                "session_id": self.session,
                "driver": self.driver,
                "kw": round(kw, 3),
                "eur": round(eur, 4),
                "status": "CHARGING"
            }
            await self.kafka.send_and_wait(TELEMETRY_TOPIC, json.dumps(telemetry).encode())

            if self.total_kwh >= 0.03:  # ~20 seconds
                break

            await asyncio.sleep(1)

        final = {
            "cp_id": CP_ID,
            "session_id": self.session,
            "driver": self.driver,
            "status": "FINISHED",
            "eur": round(self.total_kwh * PRICE, 4)
        }
        await self.kafka.send_and_wait(TELEMETRY_TOPIC, json.dumps(final).encode())

        print(f"[{CP_ID}] 🔌 Charging complete")

        self.supplying = False
        self.session = None
        self.driver = None

    # Kafka broadcast listener
    async def listen_cmd(self):
        async for msg in self.cmd_consumer:
            data = json.loads(msg.value.decode())
            cmd = data.get("cmd")
            cp = data.get("cp_id")

            if cmd == "STOP_ALL" or (cmd == "STOP" and cp == CP_ID):
                print(f"[{CP_ID}] ⛔ Forced stop received")
                self.supplying = False

            elif cmd == "RESUME_ALL" or (cmd == "RESUME" and cp == CP_ID):
                print(f"[{CP_ID}] OK — waiting for next START")

    # TCP Heartbeat listener (Monitor connects here) 
    async def monitor_server(self):
        async def handle(reader, writer):
            try:
                while True:
                    data = await reader.readuntil(b'\x03')
                    lrc = await reader.readexactly(1)
                    _, ok = unpack_message(data + lrc)
                    writer.write(b'\x06' if ok else b'\x15')
                    await writer.drain()
            except:
                pass

        server = await asyncio.start_server(handle, "0.0.0.0", MONITOR_PORT)
        print(f"[{CP_ID}] 🫀 Monitor listening on {MONITOR_PORT}")
        async with server:
            await server.serve_forever()

    # Kafka helper 
    async def send_status(self, status):
        msg = {"cp_id": CP_ID, "status": status}
        await self.kafka.send_and_wait(STATUS_TOPIC, json.dumps(msg).encode())


async def main():
    cp = CPEngine()
    await cp.start()


if __name__ == "__main__":
    asyncio.run(main())
