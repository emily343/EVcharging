import asyncio #pythons asynchronious framework
import json
import sys
import os
import time
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from colorama import Fore, Style, init as colorama_init #to color terminal text 
from common.protocol_utils import pack_message, unpack_message #messaging funcitons 
from .central_db import init_db, save_cp, update_cp_status, get_free_cp, get_all_cps #imports functions from central_db

colorama_init() #initialize coloroma 

#load configurations 
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json") #pfad to common/config.json 
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f) #reads json and safes it as python-dictionary

#data from config.json
KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"] #adress of kafka broker
TELEMETRY_TOPIC = CONFIG["topics"]["telemetry"] #topics
ALERTS_TOPIC = CONFIG["topics"]["alerts"] #alerts for warnings etc. 

CP_REGISTRY = {} #safes data locally for tcp-connections

#function that deletes screen (live dashboard)
def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


class CentralServer: #object that represents central server 
    #konstruktor, gets called when central starts
    def __init__(self, host="0.0.0.0", port=9002):
        self.host = host
        self.port = port #TCP Port for Cps and drivers 
        #erstellt kafka producer (producer sends messages)
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        #erstellt kafka consumer (consumer receives messages)
        self.kafka_consumer = AIOKafkaConsumer(
            TELEMETRY_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="central_group", auto_offset_reset="latest" #centralgroup = to get messages consistently,"Latest" =  start with newest messages, not oldest
        )
        #receives warnings/errors from cp engines and monitors 
        self.alert_consumer = AIOKafkaConsumer(
            ALERTS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="central_alert_group", auto_offset_reset="latest"
        )

    #launches central system  
    async def start(self): #asynchronous function, because central runs many task in parallel
        init_db() #initialite SQLite db 
        await self.kafka_producer.start() #starts kafka_producer
        await self.kafka_consumer.start() #starts consumer (for telemetry status updates)
        await self.alert_consumer.start() #starts consumer (for warnings and errors)

        #creates server so cps and drviers can connect 
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"{Fore.CYAN}EV_Central listening on {self.host}:{self.port}{Style.RESET_ALL}") #prints message to see that central is running 

        #start two background async tasks
        asyncio.create_task(self.consume_telemetry()) #continuously receives CP status 
        asyncio.create_task(self.consume_alerts()) #continuously receives CP error messages 

        #runs TCP server forever -> as long as the program is running, turns on EV Central 
        async with server:
            await server.serve_forever()

    #handle_client method, handles incoming connections to the central 
    async def handle_client(self, reader, writer): 
        addr = writer.get_extra_info('peername') #gets IP address and port of connected client 
        print(f"Connection from {addr}")

        #to make sure system doesn't crash 
        try:
            #infinite loop -> connection stays open 
            while True:
                data = await reader.readuntil(b'\x03') #reads data until end-of-message marker (every message ends with this byte)
                lrc = await reader.readexactly(1) #reads one more byte, checksum LRC, checks message integrity 
                payload, ok = unpack_message(data + lrc) #calls protocol parser, payload= decoded message string, ok=True/false

                #if message is corrupted or can't be decoded
                if not ok or payload is None:
                    writer.write(pack_message("NACK")) #send negative acknowledgement = invalid message send
                    await writer.drain() #esnures nack is pushed out 
                    continue #wait for next message, prevents briken messages from crashing the system 

                msg = payload.strip() #remove whitespace or newlines
                print(f"MSG: {msg}")

                #handling register message 
                if msg.startswith("REGISTER#"): #if charging point is registering 
                    _, cp_id, location, price = msg.split("#") #extract parameters
                    CP_REGISTRY[cp_id] = {"writer": writer, "location": location, "price": float(price), "status": "ACTIVADO"} #save CP in memory dictionary
                    save_cp(cp_id, location, price, "ACTIVADO") #save CP in db 
                    writer.write(pack_message(f"ACK#REGISTER#{cp_id}#OK")) #sends confirmation
                    await writer.drain() #force push the send 
                    self.print_dashboard() #refresh the dashboard
                    continue
                
                #handling authentication (driver login)
                if msg.startswith("AUTH_REQ#"): #driver wants to start charging 
                    _, driver_id = msg.split("#")
                    writer.write(pack_message(f"AUTH_RESP#{driver_id}#ALLOW")) #central respons allow to driver request
                    await writer.drain()
                    continue

                #handling start request
                if msg.startswith("START_REQ#"):
                    _, driver_id = msg.split("#")
                    cp_id = get_free_cp() #find available cp from db 
                    if not cp_id: #if non are free, driver needs to wait 
                        writer.write(pack_message("NO_CP_AVAILABLE"))
                        await writer.drain()
                        continue

                    
                    info = CP_REGISTRY[cp_id] #store cp-info
                    session_id = f"S-{driver_id}-{cp_id}" #create session-id

                    #send start message to cp 
                    info["writer"].write(pack_message(f"START#{session_id}#{driver_id}"))
                    await info["writer"].drain()

                    # notify driver about session id
                    writer.write(pack_message(f"START#{session_id}#{cp_id}"))
                    await writer.drain()

                    #update db 
                    update_cp_status(cp_id, "SUMINISTRANDO")
                    info["status"] = "SUMINISTRANDO"
                    self.print_dashboard()
                    continue

                #handling stop
                if msg.startswith("STOP_REQ#"): 
                    _, session_id = msg.split("#")
                    _, driver_id, cp_id = session_id.split("-") #ectract session info from message 

                    #send stop to exact charger 
                    info = CP_REGISTRY.get(cp_id)
                    if info:
                        info["writer"].write(pack_message(f"STOP#{session_id}"))
                        await info["writer"].drain()

                        #update dashboard and db
                        update_cp_status(cp_id, "ACTIVADO")
                        info["status"] = "ACTIVADO"
                        self.print_dashboard()
                    continue
        
        except:
            print(f"Client disconnected: {addr}")

    #listens to telemetry data coming from all cps via kafka 
    async def consume_telemetry(self):
        async for msg in self.kafka_consumer: #reads every message from kafka telemetry topic
            data = json.loads(msg.value.decode()) #decode kafka messages and convert them to python dict. 
            cp_id = data["cp_id"] #identifies which cp sent update
            status = data.get("status", "") #gets status

            #updates status to db
            if status == "CHARGING":
                update_cp_status(cp_id, "SUMINISTRANDO")
            elif status == "FINISHED":
                update_cp_status(cp_id, "ACTIVADO")

            #refreshes dashboard after each update 
            self.print_dashboard()

    #listens for alerts and errors from cp
    async def consume_alerts(self):
        async for msg in self.alert_consumer: #listens to kafka alert topics
            data = json.loads(msg.value.decode()) #decodes messages
            cp_id = data["cp_id"] #which charger reported issue
            reason = data["reason"] #what type of problem

            if reason in ["NO_RESPONSE", "CONNECT_FAIL"]:
                update_cp_status(cp_id, "DESCONECTADO")
            elif reason in ["KO_RESPONSE", "ERROR"]: 
                update_cp_status(cp_id, "AVERIADO")

            self.print_dashboard()

    #updates  dashboard console
    def print_dashboard(self):
        clear() #clears termina screen
        print(f"{Fore.CYAN}=== EV CHARGING NETWORK DASHBOARD ==={Style.RESET_ALL}")
        rows = get_all_cps() #fetches all cps from db

        #decides color to display each cp 
        for cp_id, location, price, status in rows:
            color = {
                "ACTIVADO": Fore.GREEN,
                "SUMINISTRANDO": Fore.YELLOW,
                "AVERIADO": Fore.RED,
                "DESCONECTADO": Fore.LIGHTBLACK_EX
            }.get(status, Fore.WHITE)

            print(color + f"{cp_id:8} | {location:10} | {price:.2f} €/kWh | {status:12}" + Style.RESET_ALL)

        print("\nCtrl+C to exit")

#entry point of central, starts the whole system
async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9002 #checks if user passed a port as a command line argument, if not default 9002
    server = CentralServer(port=port) #creates centralserver instance
    await server.start()

#file only runs when executed directly
if __name__ == "__main__":
    asyncio.run(main())
