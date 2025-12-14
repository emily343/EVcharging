import asyncio
import sys
import os
from common.protocol_utils import pack_message, unpack_message

SERVICES_FILE = os.path.join(os.path.dirname(__file__), "..", "services.txt")

# parse CLI args 
central_ip = sys.argv[sys.argv.index("--central-ip") + 1]
central_port = int(sys.argv[sys.argv.index("--central-port") + 1])
driver_id = sys.argv[sys.argv.index("--driver-id") + 1]
auto_mode = "--auto" in sys.argv


class Driver:
    def __init__(self, central_ip, central_port, driver_id, auto=False):
        self.central_ip = central_ip
        self.central_port = central_port
        self.driver_id = driver_id
        self.reader = None
        self.writer = None
        self.current_session = None
        self.auto = auto

    # TCP Connect
    async def connect(self):
        while True:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.central_ip, self.central_port)
                print(f"✅ Connected to Central at {self.central_ip}:{self.central_port}")
                break
            except:
                print("❌ Central not reachable — retry...")
                await asyncio.sleep(2)

    # Start Driver 
    async def start(self):
        await self.connect()

        # send auth
        self.writer.write(pack_message(f"AUTH_REQ#{self.driver_id}"))
        await self.writer.drain()

        while True:
            try:
                data = await self.reader.readuntil(b'\x03')
                lrc = await self.reader.readexactly(1)
                payload, ok = unpack_message(data + lrc)

                if not ok:
                    continue

                # Handshake complete
                if payload.startswith("AUTH_RESP"):
                    print(f"👋 Driver {self.driver_id} authenticated.")
                    if self.auto:
                        asyncio.create_task(self.run_auto_cycle())
                    else:
                        asyncio.create_task(self.user_input())
                
                # no cp available 
                if payload == "NO_CP_AVAILABLE":
                    print(" No charging point available at the moment.")
                    continue

                elif payload.startswith("DISCONNECTED"):
                    print("⚠️ CP disconnected. Waiting for new charging point...")
                    self.current_session = None
                    continue

                # Start session
                elif payload.startswith("START#"):
                    _, session_id, cp_id = payload.split("#")
                    self.current_session = session_id
                    print(f"⚡ Charging started at {cp_id} | Session {session_id}")

                # Ticket from central
                elif payload.startswith("TICKET#"):
                    _, session, kw, eur = payload.split("#")
                    print("\n===== ✅ SESSION SUMMARY =====")
                    print(f"Driver: {self.driver_id}")
                    print(f"Session: {session}")
                    print(f"Energy: {kw} kWh")
                    print(f"Cost:   {eur} €")
                    print("=============================\n")
                    self.current_session = None

                elif payload.startswith("STOP_ERROR"):
                    print("⚠️ Central reports: No active session to stop.")
                    continue

                # Central forced stop
                elif payload.startswith("STOP#"):
                    print("🛑 Charging stopped by Central.")
                    print("🔄 Ready for new start.")
                    self.current_session = None
                    


            except asyncio.IncompleteReadError:
                print("⚠️ Central disconnected — reconnecting…")
                await self.connect()
                self.writer.write(pack_message(f"AUTH_REQ#{self.driver_id}"))
                await self.writer.drain()

    # ---------------- Manual Mode ----------------
    async def user_input(self):
        loop = asyncio.get_event_loop()
        while True:
            cmd = await loop.run_in_executor(None, input, "> ")

            if cmd == "start":
                self.writer.write(
                    pack_message(f"START_REQ#{self.driver_id}")
                )
                await self.writer.drain()
        
            elif cmd == "stop":
                self.writer.write(
                    pack_message(f"STOP_REQ#{self.current_session}")
                )
                await self.writer.drain()



            elif cmd in ["quit", "exit"]:
                print("👋 Bye!")
                self.writer.close()
                await self.writer.wait_closed()
                break

            else:
                print("Commands: start | stop | exit")

    # Auto Mode 
    async def run_auto_cycle(self):
        with open(SERVICES_FILE) as f:
            cps = [line.strip() for line in f]

        for cp in cps:
            print(f"🚘 Auto: Requesting charge at {cp}")
            self.writer.write(pack_message(f"START_REQ#{self.driver_id}"))
            await self.writer.drain()
            await asyncio.sleep(8)

            if self.current_session:self.writer.write(
                pack_message(f"STOP_REQ#{self.current_session}")
            )
            await self.writer.drain()
               
            
            await asyncio.sleep(3)


async def main():
    d = Driver(central_ip, central_port, driver_id, auto_mode)
    await d.start()

if __name__ == "__main__":
    asyncio.run(main())
