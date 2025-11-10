import asyncio #python library for asynchronous network communication
import sys #reads command line parameters
import os #used to build files
from common.protocol_utils import pack_message, unpack_message 

SERVICES_FILE = os.path.join(os.path.dirname(__file__), "..", "services.txt") #file path to services.txt, contains list of cps

#read CLI arguments
#sys.argv: python list that contains everything written after the command 
central_ip = sys.argv[sys.argv.index("--central-ip") + 1] #get central server IP (find --central-ip, take next value in the list)
central_port = int(sys.argv[sys.argv.index("--central-port") + 1]) #get central server port, convert to int 
driver_id = sys.argv[sys.argv.index("--driver-id") + 1] #get driver id
auto_mode = "--auto" in sys.argv #checks if auto mode is enabled

#driver class
class Driver:
    #constructor,defines driver object 
    def __init__(self, central_ip, central_port, driver_id, auto=False):
        self.central_ip = central_ip #where to connect 
        self.central_port = central_port #port of central
        self.driver_id = driver_id 
        self.reader = None 
        self.writer = None  
        self.auto = auto #auto mode flag
        self.current_session = None #charging session id

    #driver startup, connects to central server over tcp 
    async def start(self): 
        #opens a connection to central
        self.reader, self.writer = await asyncio.open_connection(self.central_ip, self.central_port)
        print(f"Connected to Central at {self.central_ip}:{self.central_port}")

        self.writer.write(pack_message(f"AUTH_REQ#{self.driver_id}")) #sends authentication request to central
        await self.writer.drain() #makes sure message is sent 

        #continozs loop, listens for messages from central 
        while True:
            data = await self.reader.readuntil(b'\x03') #reads incoming bytes until end of message marker 
            lrc = await self.reader.readexactly(1) #LRC checksum, reads one more byte to validate message integrity
            payload, ok = unpack_message(data + lrc) #decodes message and checks LRC
            if not ok: 
                continue #message gets ignored

            #if authentication successfull
            if payload.startswith("AUTH_RESP"):
                if self.auto: asyncio.create_task(self.run_service_sequence()) #auto mode: start automated charging cycle
                else: asyncio.create_task(self.user_input_loop()) #manual mode: start user input loop

            #start charging session
            elif payload.startswith("START#"):
                _, session, _ = payload.split("#") #parse message to extract session ID 
                self.current_session = session #stores active session ID 
                print(f"SESSION STARTED: {session}")

            elif payload.startswith("STOP#"):
                print("SESSION STOPPED")
                self.current_session = None

    async def user_input_loop(self):
        while True:
            cmd = await asyncio.get_event_loop().run_in_executor(None, input, "> ")

            if cmd.lower() == "start":
                self.writer.write(pack_message(f"START_REQ#{self.driver_id}"))
                await self.writer.drain()

            elif cmd.lower() == "stop":
                if self.current_session:
                    self.writer.write(pack_message(f"STOP_REQ#{self.current_session}"))
                    await self.writer.drain()
                    print("Stop request sent.")
                else:
                    print("No active charging session.")

            elif cmd.lower() in ["quit", "exit"]:
                self.writer.close()
                await self.writer.wait_closed()
                break

    async def run_service_sequence(self):
        with open(SERVICES_FILE) as f:
            cp_list = [c.strip() for c in f]

        for cp in cp_list:
            self.writer.write(pack_message(f"START_REQ#{self.driver_id}"))
            await self.writer.drain()
            await asyncio.sleep(8)

            if self.current_session:
                self.writer.write(pack_message(f"STOP_REQ#{self.current_session}"))
                await self.writer.drain()

            await asyncio.sleep(3)


async def main():
    d = Driver(central_ip, central_port, driver_id, auto=auto_mode)
    await d.start()


if __name__ == "__main__":
    asyncio.run(main())
