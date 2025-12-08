import asyncio
import json
import sys
import os
import time
from typing import Dict
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from colorama import Fore, Style, init as colorama_init

from common.protocol_utils import pack_message, unpack_message
from common.crypto_utils import aes_encrypt

from .central_db import (
    init_db,
    registry_get_cp,
    save_cp,
    update_cp_status,
    get_free_cp,
    get_all_cps,
    reset_all_cp_status,
    log_audit,
    store_cp_key,
    get_cp_key
)

colorama_init()

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

# Kafka topics
KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
TOPICS = CONFIG["topics"]
TELEMETRY_TOPIC = TOPICS["telemetry"]
STATUS_TOPIC = TOPICS["cp_status"]
EVENTS_TOPIC = TOPICS["cp_events"]
DRIVER_EVENTS_TOPIC = TOPICS["driver_events"]
CENTRAL_CMD_TOPIC = TOPICS["central_cmd"]

HEARTBEAT_TIMEOUT_SEC = 10
HEARTBEAT_CHECK_PERIOD = 5


def clear():
    os.system("cls" if os.name == "nt" else "clear")


class CentralServer:
    def __init__(self, host="0.0.0.0", port=9002):
        self.host = host
        self.port = port

        # NEW: session keys for encryption
        self.session_keys: Dict[str, str] = {}

        # TCP sockets
        self.cp_sockets: Dict[str, asyncio.StreamWriter] = {}
        self.driver_sockets: Dict[str, asyncio.StreamWriter] = {}
        self.sessions: Dict[str, Dict] = {}

        # CP telemetry/meta
        self.cp_meta: Dict[str, Dict] = {}

        # heartbeat timestamps
        self.last_seen: Dict[str, float] = {}

        # Kafka
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        self.kafka_consumers = [
            AIOKafkaConsumer(
                TELEMETRY_TOPIC,
                STATUS_TOPIC,
                EVENTS_TOPIC,
                DRIVER_EVENTS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="central_group",
                auto_offset_reset="latest",
            )
        ]

    async def start(self):
        init_db()
        reset_all_cp_status("DESCONECTADO")

        await self.kafka_producer.start()
        for c in self.kafka_consumers:
            await c.start()

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"{Fore.CYAN}EV_Central listening on {self.host}:{self.port}{Style.RESET_ALL}")

        asyncio.create_task(self.kafka_loop())
        asyncio.create_task(self.heartbeat_watcher())
        asyncio.create_task(self.central_cli())

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info("peername")
        print(f"TCP connection from {addr}")

        try:
            while True:
                data = await reader.readuntil(b"\x03")
                lrc = await reader.readexactly(1)
                payload, ok = unpack_message(data + lrc)

                if not ok or payload is None:
                    writer.write(pack_message("NACK"))
                    await writer.drain()
                    continue

                msg = payload.strip()

               
                # authentication cp
                if msg.startswith("AUTH_CP#"):
                    _, cp_id, credential_plain = msg.split("#")
                    import hashlib, secrets
                    from .central_db import get_db_connection

                    row = registry_get_cp(cp_id)
                    if not row:
                        writer.write(pack_message("AUTH_FAIL#UNKNOWN_CP"))
                        await writer.drain()
                        log_audit("EV_Central", "AUTH_FAIL", f"{cp_id}: not in registry")
                        continue

                    credential_hash = hashlib.sha256(credential_plain.encode()).hexdigest()

                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT credential_hash, location FROM registry_cp WHERE cp_id=?", (cp_id,))
                    row2 = cur.fetchone()
                    conn.close()

                    if not row2 or row2[0] != credential_hash:
                        writer.write(pack_message("AUTH_FAIL#INVALID_CREDENTIAL"))
                        await writer.drain()
                        log_audit("EV_Central", "AUTH_FAIL", f"{cp_id}: invalid credential")
                        continue

                    # generate symmetric AES key
                    sym_key = secrets.token_hex(32)
                    store_cp_key(cp_id, sym_key)
                    self.session_keys[cp_id] = sym_key

                    # register TCP socket
                    self.cp_sockets[cp_id] = writer

                    # initialize meta
                    self.cp_meta.setdefault(cp_id, {})
                    self.cp_meta[cp_id].update({
                        "location": row2[1],
                        "price": 0.0,
                        "status": "ACTIVADO",
                        "session": "-",
                        "driver": "-",
                        "kw": 0.0,
                        "eur": 0.0,
                    })

                    self.last_seen[cp_id] = time.time()
                    update_cp_status(cp_id, "ACTIVADO")

                    log_audit("EV_Central", "AUTH_OK", f"{cp_id} authenticated")

                    writer.write(pack_message(f"AUTH_OK#{cp_id}#{sym_key}"))
                    await writer.drain()
                    continue

     
                # authentication monitor 
                if msg.startswith("AUTH_MON#"):
                    _, mon_id, credential_plain = msg.split("#")

                    import hashlib
                    from .central_db import registry_get_cp

                    row = registry_get_cp(mon_id)
                    if not row:
                        writer.write(pack_message("AUTH_MON_FAIL#UNKNOWN"))
                        await writer.drain()
                        log_audit("EV_Central", "AUTH_MON_FAIL", f"{mon_id}: not found")
                        continue

                    credential_hash = hashlib.sha256(credential_plain.encode()).hexdigest()

                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT credential_hash FROM registry_cp WHERE cp_id=?", (mon_id,))
                    db_hash = cur.fetchone()
                    conn.close()

                    if not db_hash or db_hash[0] != credential_hash:
                        writer.write(pack_message("AUTH_MON_FAIL#INVALID"))
                        await writer.drain()
                        log_audit("EV_Central", "AUTH_MON_FAIL", f"{mon_id}: wrong credential")
                        continue

                    writer.write(pack_message(f"AUTH_MON_OK#{mon_id}"))
                    await writer.drain()
                    log_audit("EV_Central", "AUTH_MON_OK", mon_id)
                    continue

               
                # register (after successful AUTH only)
                if msg.startswith("REGISTER#"):
                    _, cp_id, location, price = msg.split("#")
                    price = float(price)

                    self.cp_sockets[cp_id] = writer

                    self.cp_meta.setdefault(cp_id, {})
                    self.cp_meta[cp_id].update({
                        "location": location,
                        "price": price,
                        "status": "ACTIVADO",
                        "kw": 0.0,
                        "eur": 0.0,
                        "driver": "-",
                        "session": "-",
                    })

                    save_cp(cp_id, location, price, "ACTIVADO")
                    self.last_seen[cp_id] = time.time()

                    writer.write(pack_message(f"ACK#REGISTER#{cp_id}#OK"))
                    await writer.drain()
                    await self.publish_event("CP_REGISTERED", cp_id=cp_id)
                    continue

                #driver auth
                if msg.startswith("AUTH_REQ#"):
                    _, driver_id = msg.split("#")
                    self.driver_sockets[driver_id] = writer
                    writer.write(pack_message(f"AUTH_RESP#{driver_id}#ALLOW"))
                    await writer.drain()
                    await self.publish_driver_event("DRIVER_AUTH_OK", driver_id=driver_id)
                    continue

                
                # start request (Driver → Central)
                if msg.startswith("START_REQ#"):
                    _, driver_id = msg.split("#")
                    cp_id = get_free_cp()

                    if not cp_id:
                        writer.write(pack_message("NO_CP_AVAILABLE"))
                        await writer.drain()
                        continue

                    info = self.cp_meta.get(cp_id, {})
                    session_id = f"S-{driver_id}-{cp_id}"
                    self.sessions[session_id] = {"driver_id": driver_id, "cp_id": cp_id}

                    # persist session
                    from .central_db import get_db_connection
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT OR REPLACE INTO sessions(session_id, cp_id, driver_id, start_time) VALUES (?, ?, ?, ?)",
                        (session_id, cp_id, driver_id, time.time()),
                    )
                    conn.commit()
                    conn.close()

                    # encrypted START → CP
                    cpw = self.cp_sockets.get(cp_id)
                    if cpw:
                        sym_key = self.session_keys.get(cp_id)
                        encrypted = aes_encrypt(sym_key, f"START#{session_id}#{driver_id}")
                        cpw.write(pack_message(f"ENC#{encrypted}"))
                        await cpw.drain()

                    # notify driver
                    writer.write(pack_message(f"START#{session_id}#{cp_id}"))
                    await writer.drain()

                    # update states
                    update_cp_status(cp_id, "SUMINISTRANDO")
                    info.update({"status": "SUMINISTRANDO", "driver": driver_id, "session": session_id})
                    self.last_seen[cp_id] = time.time()

                    await self.publish_event("SESSION_STARTED", cp_id=cp_id, session_id=session_id)
                    continue

                #stop request
                if msg.startswith("STOP_REQ#"):
                    _, session_id = msg.split("#")
                    session = self.sessions.get(session_id)

                    if not session:
                        continue

                    cp_id = session["cp_id"]
                    cpw = self.cp_sockets.get(cp_id)

                    if cpw:
                        sym_key = self.session_keys.get(cp_id)
                        encrypted = aes_encrypt(sym_key, f"STOP#{session_id}")
                        cpw.write(pack_message(f"ENC#{encrypted}"))
                        await cpw.drain()

                    update_cp_status(cp_id, "ACTIVADO")
                    self.cp_meta[cp_id].update({
                        "status": "ACTIVADO",
                        "session": "-",
                        "driver": "-",
                    })

                    await self.publish_event("SESSION_STOP_REQUESTED", cp_id=cp_id, session_id=session_id)
                    continue

        except Exception as e:
            print(f"Client disconnected {addr}: {e}")

    #kafka loops 
    async def kafka_loop(self):
        consumer = self.kafka_consumers[0]
        last_update = 0

        async for rec in consumer:
            topic = rec.topic
            data = json.loads(rec.value.decode())

            if topic == TELEMETRY_TOPIC:
                self.on_telemetry(data)
            elif topic == STATUS_TOPIC:
                self.on_status(data)
            elif topic == EVENTS_TOPIC:
                self.on_cp_event(data)
            elif topic == DRIVER_EVENTS_TOPIC:
                self.on_driver_event(data)

            if time.time() - last_update > 0.5:
                last_update = time.time()

    def on_telemetry(self, data):
        cp_id = data.get("cp_id")
        if not cp_id:
            return

        meta = self.cp_meta.setdefault(cp_id, {})
        meta["kw"] = float(data.get("kw", 0.0))
        meta["eur"] = float(data.get("eur", 0.0))

        if "driver" in data:
            meta["driver"] = data["driver"]
        if "session_id" in data:
            meta["session"] = data["session_id"]

        status = data.get("status")

        if status == "CHARGING":
            update_cp_status(cp_id, "SUMINISTRANDO")
            meta["status"] = "SUMINISTRANDO"

        elif status == "FINISHED":
            session_id = data.get("session_id", "-")
            kwh = float(data.get("kw", 0.0))
            eur = float(data.get("eur", 0.0))

            from .central_db import get_db_connection
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE sessions SET end_time=?, kwh=?, eur=? WHERE session_id=?",
                (time.time(), kwh, eur, session_id),
            )
            conn.commit()
            conn.close()

            sess = self.sessions.get(session_id)
            if sess:
                d_id = sess["driver_id"]
                dw = self.driver_sockets.get(d_id)
                if dw:
                    dw.write(pack_message(f"TICKET#{session_id}#{kwh}#{eur}"))
                    asyncio.create_task(dw.drain())

            update_cp_status(cp_id, "ACTIVADO")
            meta.update({
                "status": "ACTIVADO",
                "session": "-",
                "driver": "-",
                "kw": 0.0,
                "eur": 0.0,
            })

        elif status == "IDLE":
            update_cp_status(cp_id, "ACTIVADO")
            meta["status"] = "ACTIVADO"

        self.last_seen[cp_id] = time.time()

    def on_status(self, data):
        cp_id = data.get("cp_id")
        if not cp_id:
            return

        st = data.get("status", "").upper()
        meta = self.cp_meta.setdefault(cp_id, {})

        mapping = {
            "OK": "ACTIVADO",
            "ACTIVATED": "ACTIVADO",
            "RECOVER": "ACTIVADO",
            "FAULT": "AVERIADO",
            "OUT_OF_SERVICE": "PARADO",
            "DISCONNECTED": "DESCONECTADO",
        }

        if st in mapping:
            update_cp_status(cp_id, mapping[st])
            meta["status"] = mapping[st]

        self.last_seen[cp_id] = time.time()

    def on_cp_event(self, data):
        cp_id = data.get("cp_id")
        if cp_id:
            self.last_seen[cp_id] = time.time()

    def on_driver_event(self, data):
        pass

    #heartbeat watcher
    async def heartbeat_watcher(self):
        while True:
            await asyncio.sleep(HEARTBEAT_CHECK_PERIOD)
            now = time.time()

            for cp_id, ts in list(self.last_seen.items()):
                meta = self.cp_meta.get(cp_id, {})
                if meta.get("status") == "PARADO":
                    continue

                if now - ts > HEARTBEAT_TIMEOUT_SEC:
                    update_cp_status(cp_id, "DESCONECTADO")
                    meta["status"] = "DESCONECTADO"

    #central cli
    async def central_cli(self):
        loop = asyncio.get_event_loop()
        while True:
            cmdline = await loop.run_in_executor(None, input, "central> ")
            parts = cmdline.strip().split()
            if not parts:
                continue

            cmd = parts[0].lower()
            target = parts[1].upper() if len(parts) > 1 else None

            if cmd == "stop_all":
                await self.publish_central_cmd({"cmd": "STOP_ALL"})
                print("STOP_ALL sent.")

            elif cmd == "resume_all":
                await self.publish_central_cmd({"cmd": "RESUME_ALL"})
                print("RESUME_ALL sent.")

            elif cmd == "stop" and target:
                await self.publish_central_cmd({"cmd": "STOP", "cp_id": target})
                print(f"{target} STOP sent.")

            elif cmd == "resume" and target:
                await self.publish_central_cmd({"cmd": "RESUME", "cp_id": target})
                print(f"{target} RESUME sent.")

            elif cmd == "out" and target:
                update_cp_status(target, "PARADO")
                print(f"{target} OUT OF SERVICE.")

            elif cmd == "activate" and target:
                update_cp_status(target, "ACTIVADO")
                print(f"{target} ACTIVATED.")

            else:
                print("Commands: stop <CP>, resume <CP>, out <CP>, activate <CP>, stop_all, resume_all")

   #kafka produce helpers
    async def publish_event(self, evt_type, **fields):
        await self.kafka_producer.send_and_wait(
            EVENTS_TOPIC,
            json.dumps({"type": evt_type, **fields}).encode()
        )

    async def publish_driver_event(self, evt_type, driver_id, **fields):
        await self.kafka_producer.send_and_wait(
            DRIVER_EVENTS_TOPIC,
            json.dumps({"type": evt_type, "driver_id": driver_id, **fields}).encode()
        )

    async def publish_central_cmd(self, cmd_obj):
        await self.kafka_producer.send_and_wait(
            CENTRAL_CMD_TOPIC,
            json.dumps(cmd_obj).encode()
        )

    def print_dashboard(self):
        clear()
        print(f"{Fore.CYAN}=== EV CHARGING NETWORK DASHBOARD ==={Style.RESET_ALL}")
        rows = get_all_cps()
        meta = self.cp_meta

        colors = {
            "ACTIVADO": Fore.GREEN,
            "SUMINISTRANDO": Fore.YELLOW,
            "AVERIADO": Fore.RED,
            "PARADO": Fore.MAGENTA,
            "DESCONECTADO": Fore.LIGHTBLACK_EX
        }

        header = f"{'CP':8} | {'Location':10} | {'€/kWh':>6} | {'State':13} | {'Sess':12} | {'Driver':7} | {'kW':>6} | {'€':>6}"
        print(header)
        print("-" * len(header))

        for (cp_id, location, price, status) in rows:
            m = meta.get(cp_id, {})
            col = colors.get(status, Fore.WHITE)

            line = f"{cp_id:8} | {location:10} | {price:6.2f} | {status:13} | {m.get('session','-'):12} | {m.get('driver','-'):7} | {m.get('kw',0.0):6.2f} | {m.get('eur',0.0):6.2f}"
            print(col + line + Style.RESET_ALL)


async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9002
    server = CentralServer(port=port)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
