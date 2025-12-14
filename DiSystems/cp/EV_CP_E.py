import asyncio
import json, os, random, sys, time
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from common.protocol_utils import pack_message, unpack_message
from common.crypto_utils import encrypt_kafka_payload

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
if len(sys.argv) < 6:
    print("Usage: python -m cp.EV_CP_E <CP_ID> <LOCATION> <PRICE> <MONITOR_PORT> <CREDENTIAL>")
    sys.exit(1)

CP_ID = sys.argv[1]
LOCATION = sys.argv[2]
PRICE = float(sys.argv[3])
MONITOR_PORT = int(sys.argv[4])
CREDENTIAL = sys.argv[5]


CENTRAL_HOST = os.getenv("CENTRAL_HOST", CONFIG["central_host"])
CENTRAL_PORT = int(os.getenv("CENTRAL_PORT", CONFIG["central_port"]))


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

                auth_msg = f"AUTH_CP#{CP_ID}#{CREDENTIAL}"
                w.write(pack_message(auth_msg))
                await w.drain()


                print(f"[{CP_ID}] ✅ Registered at Central")

                asyncio.create_task(self.listen_central())
                return
            except:
                print(f"[{CP_ID}] ❌ Central offline, retrying…")
                await self.send_status("DISCONNECTED")
                await asyncio.sleep(2)

    # TCP messages from Central (encrypted after AUTH_CP)
    async def listen_central(self):
        from common.crypto_utils import aes_decrypt

        try:
            while True:
                data = await self.central_reader.readuntil(b'\x03')
                lrc = await self.central_reader.readexactly(1)
                payload, ok = unpack_message(data + lrc)
                if not ok or payload is None:
                    continue

                #encrypted command
                if payload.startswith("ENC#"):
                    _, encrypted_json = payload.split("#", 1)

                    try:
                        decrypted = aes_decrypt(self.sym_key, encrypted_json)
                    except Exception as e:
                        print(f"[{CP_ID}] ❌ Decryption failed:", e)
                        continue

                    print(f"[{CP_ID}] 🔐 Decrypted:", decrypted)

                    # start
                    if decrypted.startswith("START#"):
                        _, session, driver = decrypted.split("#")
                        self.session = session
                        self.driver = driver
                        asyncio.create_task(self.start_supply())
                        continue

                    # stop
                    if decrypted.startswith("STOP#"):
                        _, session_id = decrypted.split("#")
                        if self.session == session_id:
                            self.supplying = False
                        continue

                    # unknown
                    print(f"[{CP_ID}] Unknown decrypted message:", decrypted)
                    continue


                #plaintext command 
                if payload.startswith("START#"):
                    _, session, driver = payload.split("#")
                    self.session = session
                    self.driver = driver
                    asyncio.create_task(self.start_supply())
                    continue

                if payload.startswith("STOP#"):
                    self.supplying = False
                    continue

                if payload.startswith("AUTH_OK#"):
                    _, cp_id, sym_key = payload.split("#")
                    self.sym_key = sym_key
                    print(f"[{CP_ID}]  Authenticated. AES key received.")
                    await self.send_status("OK")
                    continue

                if payload.startswith("AUTH_FAIL"):
                    print(f"[{CP_ID}]  AUTH FAIL: {payload}")
                    return


        except Exception as e:
            print(f"[{CP_ID}] ⚠️ Lost connection to Central:", e)
            await self.send_status("DISCONNECTED")
            asyncio.create_task(self.connect_central())


    # Charging Simulation 
    async def start_supply(self):
        print(f"[{CP_ID}] 🚗 Charging session {self.session} for driver {self.driver}")
        self.supplying = True
        self.total_kwh = 0.0

        await self.send_status("OK")  # CP is fully operational

        # Demo: etwa 10 Sekunden laden, ca. 1.0 kWh
        while self.supplying:
            kw = 7.2 + random.uniform(-0.3, 0.3)

            # statt kw/3600 sehr kleine Werte -> hier einfach 0.1 kWh pro Sekunde
            self.total_kwh += 0.1
            eur = self.total_kwh * PRICE

            telemetry = {
                "cp_id": CP_ID,
                "session_id": self.session,
                "driver": self.driver,
                "kw": round(kw, 3),
                "eur": round(eur, 4),
                "status": "CHARGING"
            }
            encrypted_final = encrypt_kafka_payload(self.sym_key, final)

            await self.kafka.send_and_wait(
                TELEMETRY_TOPIC,
                encrypted_final.encode()
            )
            



            # nach 10 Schritten (~1.0 kWh) stoppen
            if self.total_kwh >= 1.0:
                break

            await asyncio.sleep(1)

        # finale Nachricht mit kWh UND €
        final = {
            "cp_id": CP_ID,
            "session_id": self.session,
            "driver": self.driver,
            "status": "FINISHED",
            "kw": round(self.total_kwh, 3),
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
            target = data.get("cp_id")

            # stops only charging session
            if cmd == "STOP_ALL" or (cmd == "STOP" and target == CP_ID):
                print(f"\n[{CP_ID}] STOP received — stopping charging session")

                self.supplying = False   
                self.session = None
                self.driver = None

                # CP bleibt verfügbar
                await self.send_status("OK")

           
            # CP waits for new start
            elif cmd == "RESUME_ALL" or (cmd == "RESUME" and target == CP_ID):
                print(f"\n[{CP_ID}] RESUME received")

                # RESUME doesn't start a new session
                self.supplying = False
                self.session = None
                self.driver = None

                await self.send_status("OK")


            # OUT = CP außer Betrieb nehmen
            elif cmd == "OUT" and target == CP_ID:
                print(f"\n[{CP_ID}] OUT OF SERVICE received")

                self.supplying = False
                self.session = None
                self.driver = None
                await self.send_status("OUT_OF_SERVICE")
                

            # ACTIVATE = CP wieder aktivieren
            elif cmd == "ACTIVATE" and target == CP_ID:
                print(f"\n[{CP_ID}] ACTIVATE received")

                self.supplying = False
                self.session = None
                self.driver = None

                await self.send_status("OK")
                



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
        print(f"[{CP_ID}] 🫀 Waiting for monitor on port {MONITOR_PORT}…")
        async with server:
            await server.serve_forever()

    # Kafka helper 
    async def send_status(self, status):
        msg = {"cp_id": CP_ID, "status": status, "source": "ENGINE"}
        await self.kafka.send_and_wait(STATUS_TOPIC, json.dumps(msg).encode())


async def main():
    cp = CPEngine()
    await cp.start()


if __name__ == "__main__":
    asyncio.run(main())
