import asyncio
import json
import sys
import os
import time
from typing import Dict, Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from colorama import Fore, Style, init as colorama_init


from common.protocol_utils import pack_message, unpack_message
from .central_db import (
    init_db,
    save_cp,
    update_cp_status,
    get_free_cp,
    get_all_cps,
    reset_all_cp_status,
    log_audit,
    store_cp_key
)

colorama_init()

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

# Kafka bootstrap an topics
KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
TOPICS = CONFIG["topics"]
TELEMETRY_TOPIC = TOPICS["telemetry"]  # cp -> central (kw, eur, session, driver, status)
STATUS_TOPIC = TOPICS["cp_status"]  # monitor -> central (OK/FAULT/RECOVER/OUT_OF_SERVICE/ACTIVATED)
EVENTS_TOPIC = TOPICS["cp_events"]  # cp + central events (started/stopped etc.)
DRIVER_EVENTS_TOPIC = TOPICS["driver_events"]  # optional: driver side events
CENTRAL_CMD_TOPIC = TOPICS["central_cmd"]  # central -> cp broadcast (STOP/RESUME/OUT_OF_SERVICE/ACTIVATE)

# Heartbeat / disconnect
HEARTBEAT_TIMEOUT_SEC = 10.0
HEARTBEAT_CHECK_PERIOD = 5.0


def clear():
    os.system("cls" if os.name == "nt" else "clear")


class CentralServer:
    def __init__(self, host="0.0.0.0", port=9002):
        self.host = host
        self.port = port

        # TCP registries
        self.cp_sockets: Dict[str, asyncio.StreamWriter] = {}  # cp_id -> writer
        self.driver_sockets: Dict[str, asyncio.StreamWriter] = {}  # driver_id -> writer
        self.sessions: Dict[str, Dict] = {}  # session_id -> {driver_id, cp_id}

        # In-memory CP state extras (telemetry)
        self.cp_meta: Dict[str, Dict] = {}  # cp_id -> {location, price, status, kw, eur, driver, session}

        # Last-seen timestamps for heartbeat/disconnect
        self.last_seen: Dict[str, float] = {}  # cp_id -> unix_ts

        # Kafka
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        # We consume from multiple topics
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

    # startup
    async def start(self):
        init_db()

        #  Mark all CPs as disconnected until they reconnect
        reset_all_cp_status("DESCONECTADO")

        await self.kafka_producer.start()
        for c in self.kafka_consumers:
            await c.start()

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"{Fore.CYAN}EV_Central listening on {self.host}:{self.port}{Style.RESET_ALL}")

        # Background tasks
        asyncio.create_task(self.kafka_loop())
        asyncio.create_task(self.heartbeat_watcher())
        asyncio.create_task(self.central_cli())

        async with server:
            await server.serve_forever()

    # tcp handler
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
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
                # print(f"TCP MSG: {msg}")

                ''' deactivate for release 2
                # CP registration: REGISTER#<cp_id>#<location>#<price>
                if msg.startswith("REGISTER#"):
                    _, cp_id, location, price = msg.split("#")
                    price = float(price)
                    self.cp_sockets[cp_id] = writer
                    self.cp_meta.setdefault(cp_id, {})
                    self.cp_meta[cp_id].update(
                        {
                            "location": location,
                            "price": price,
                            "status": "ACTIVADO",
                            "kw": 0.0,
                            "eur": 0.0,
                            "driver": "-",
                            "session": "-",
                        }
                    )
                    self.last_seen[cp_id] = time.time()
                    save_cp(cp_id, location, price, "ACTIVADO")
                    writer.write(pack_message(f"ACK#REGISTER#{cp_id}#OK"))
                    await writer.drain()
                    await self.publish_event("CP_REGISTERED", cp_id=cp_id, note=f"{location}/{price}")

                    continue '''

                # Driver auth: AUTH_REQ#<driver_id>
                if msg.startswith("AUTH_REQ#"):
                    _, driver_id = msg.split("#")
                    self.driver_sockets[driver_id] = writer
                    writer.write(pack_message(f"AUTH_RESP#{driver_id}#ALLOW"))
                    await writer.drain()
                    await self.publish_driver_event("DRIVER_AUTH_OK", driver_id=driver_id)
                    continue

                # Start request: START_REQ#<driver_id>
                if msg.startswith("START_REQ#"):
                    _, driver_id = msg.split("#")
                    cp_id = get_free_cp()  # DB decision (should pick ACTIVADO CP)
                    if not cp_id:
                        writer.write(pack_message("NO_CP_AVAILABLE"))
                        await writer.drain()
                        continue

                    info = self.cp_meta.get(cp_id, {})
                    session_id = f"S-{driver_id}-{cp_id}"
                    self.sessions[session_id] = {"driver_id": driver_id, "cp_id": cp_id}

                    # Persist session START into DB (use central_db helper)
                    from .central_db import get_db_connection
                    from time import time as _time

                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT OR REPLACE INTO sessions(session_id, cp_id, driver_id, start_time) VALUES (?, ?, ?, ?)",
                        (session_id, cp_id, driver_id, _time()),
                    )
                    conn.commit()
                    conn.close()

                    # forward to CP via TCP
                    cpw = self.cp_sockets.get(cp_id)
                    if cpw:
                        cpw.write(pack_message(f"START#{session_id}#{driver_id}"))
                        await cpw.drain()

                    # notify driver
                    writer.write(pack_message(f"START#{session_id}#{cp_id}"))
                    await writer.drain()

                    # update state
                    update_cp_status(cp_id, "SUMINISTRANDO")
                    info.update({"status": "SUMINISTRANDO", "driver": driver_id, "session": session_id})
                    self.last_seen[cp_id] = time.time()
                    await self.publish_event("SESSION_STARTED", cp_id=cp_id, session_id=session_id, driver_id=driver_id)

                    continue

                # Stop request: STOP_REQ#<session_id>
                if msg.startswith("STOP_REQ#"):
                    _, session_id = msg.split("#")
                    session = self.sessions.get(session_id)
                    if not session:
                        writer.write(pack_message("STOP_ERROR#NO_ACTIVE_SESSION"))
                        await writer.drain()
                        continue

                    cp_id = session["cp_id"]
                    cpw = self.cp_sockets.get(cp_id)
                    if cpw:
                        cpw.write(pack_message(f"STOP#{session_id}"))
                        await cpw.drain()

                    # we wait for telemetry FINISHED to finalize ticket, but we can also proactively mark AVAILABLE:
                    update_cp_status(cp_id, "ACTIVADO")
                    meta = self.cp_meta.get(cp_id, {})
                    meta.update({"status": "ACTIVADO", "session": "-", "driver": "-"})
                    await self.publish_event("SESSION_STOP_REQUESTED", cp_id=cp_id, session_id=session_id)
                    continue

                # (Release 2) CP authentication: AUTH_CP#<cp_id>#<credential>
                if msg.startswith("AUTH_CP#"):
                    _, cp_id, credential_plain = msg.split("#")

                    import hashlib, secrets
                    from central.central_db import get_db_connection

                    # check if CP exists in registry
                    row = registry_get_cp(cp_id)
                    if not row:
                        writer.write(pack_message("AUTH_FAIL#UNKNOWN_CP"))
                        await writer.drain()
                        log_audit("EV_Central", "AUTH_FAIL", f"{cp_id}: not registered")
                        continue

                    # check credential hash
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

                    # generate symmetric key
                    sym_key = secrets.token_hex(32)
                    store_cp_key(cp_id, sym_key)

                    # save CP as connected
                    self.cp_sockets[cp_id] = writer
                    self.cp_meta.setdefault(cp_id, {})
                    self.cp_meta[cp_id].update({
                        "location": row2[1],
                        "price": 0.0,           # optional, if Registry stores price
                        "status": "ACTIVADO",
                        "session": "-",
                        "driver": "-",
                        "kw": 0.0,
                        "eur": 0.0,
                    })

                    update_cp_status(cp_id, "ACTIVADO")
                    self.last_seen[cp_id] = time.time()

                    # save in memory for encryption
                    self.session_keys[cp_id] = sym_key

                    # audit
                    log_audit("EV_Central", "AUTH_OK", f"{cp_id} authenticated")

                    # return key to CP
                    writer.write(pack_message(f"AUTH_OK#{cp_id}#{sym_key}"))
                    await writer.drain()

                    continue

                # (Release 2) Monitor authentication: AUTH_M#<cp_id>#<credential>
                if msg.startswith("AUTH_M#"):
                    _, cp_id, credential_plain = msg.split("#")

                    import hashlib, secrets

                    # Monitors use the same credentials stored in registry_cp
                    row = registry_get_cp(cp_id)
                    if not row:
                        writer.write(pack_message("AUTH_FAIL_M#UNKNOWN"))
                        await writer.drain()
                        log_audit("EV_Central", "AUTH_FAIL_M", f"{cp_id}: monitor unknown CP")
                        continue

                    credential_hash = hashlib.sha256(credential_plain.encode()).hexdigest()

                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT credential_hash FROM registry_cp WHERE cp_id=?", (cp_id,))
                    row2 = cur.fetchone()
                    conn.close()

                    if not row2 or row2[0] != credential_hash:
                        writer.write(pack_message("AUTH_FAIL_M#BAD_CREDENTIAL"))
                        await writer.drain()
                        log_audit("EV_Central", "AUTH_FAIL_M", f"{cp_id}: invalid monitor credential")
                        continue

                    # generate sym key
                    sym_key = secrets.token_hex(32)
                    store_cp_key(cp_id + "_monitor", sym_key)

                    log_audit("EV_Central", "AUTH_OK_M", f"{cp_id} monitor auth ok")

                    writer.write(pack_message(f"AUTH_OK_M#{cp_id}#{sym_key}"))
                    await writer.drain()

                    continue


        except Exception as e:
            print(f"Client disconnected {addr}: {e}")

    # kafka loops
    async def kafka_loop(self):
        consumer = self.kafka_consumers[0]
        last_update = 0

        async for rec in consumer:
            topic = rec.topic
            try:
                data = json.loads(rec.value.decode())
            except:
                data = {"raw": rec.value.decode(errors="ignore")}

            if topic == TELEMETRY_TOPIC:
                self.on_telemetry(data)
            elif topic == STATUS_TOPIC:
                self.on_status(data)
            elif topic == EVENTS_TOPIC:
                self.on_cp_event(data)
            elif topic == DRIVER_EVENTS_TOPIC:
                self.on_driver_event(data)

            # update dashboard max 2x pro Sekunde
            now = time.time()
            if now - last_update > 0.5:
                # self.print_dashboard()
                last_update = now

    def on_telemetry(self, data: Dict):
        """
        Expected (best effort):
        {
            "cp_id": "CP001",
            "status": "CHARGING" | "FINISHED" | "IDLE",
            "kw": 7.2,
            "eur": 0.53,
            "driver": "D01",
            "session_id": "S-D01-CP001"
        }
        """
        cp_id = data.get("cp_id")
        if not cp_id:
            return

        meta = self.cp_meta.setdefault(cp_id, {})
        meta["kw"] = float(data.get("kw", meta.get("kw", 0.0)))
        meta["eur"] = float(data.get("eur", meta.get("eur", 0.0)))

        if "driver" in data:
            meta["driver"] = data["driver"]
        if "session_id" in data:
            meta["session"] = data["session_id"]

        status = data.get("status")

        # status = charging
        if status == "CHARGING":
            update_cp_status(cp_id, "SUMINISTRANDO")
            meta["status"] = "SUMINISTRANDO"

        # status = finished
        elif status == "FINISHED":
            session_id = data.get("session_id", meta.get("session", "-"))
            kwh = float(data.get("kw", 0.0))
            eur = float(data.get("eur", 0.0))

            # save session in sql
            from .central_db import get_db_connection

            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute(
                """
                UPDATE sessions
                SET end_time=?, kwh=?, eur=?
                WHERE session_id=?
                """,
                (time.time(), kwh, eur, session_id),
            )

            conn.commit()
            conn.close()

            # send ticket to driver
            sess = self.sessions.get(session_id)
            if sess:
                driver_id = sess["driver_id"]
                w = self.driver_sockets.get(driver_id)
                if w:
                    w.write(pack_message(f"TICKET#{session_id}#{kwh}#{eur}"))
                    asyncio.create_task(w.drain())

            # activate cp again
            update_cp_status(cp_id, "ACTIVADO")
            meta.update(
                {
                    "status": "ACTIVADO",
                    "session": "-",
                    "driver": "-",
                    "kw": 0.0,
                    "eur": 0.0,
                }
            )

        # status idle
        elif status == "IDLE":
            update_cp_status(cp_id, "ACTIVADO")
            meta["status"] = "ACTIVADO"

        # Update heartbeat timestamp
        self.last_seen[cp_id] = time.time()

    def on_status(self, data: Dict):
        cp_id = data.get("cp_id")
        if not cp_id:
            return

        st = data.get("status", "").upper()
        meta = self.cp_meta.setdefault(cp_id, {})

        # 1) Wenn CP out-of-service (PARADO) ist → keine OK Meldung darf ihn aktivieren
        if meta.get("status") == "PARADO" and st in ("OK", "ACTIVATED", "RECOVER"):
            return

        # 2) Wenn CP gerade CHARGING → keine OK-Meldungen akzeptieren (bereits eingebaut)
        if meta.get("status") == "SUMINISTRANDO" and st in ("OK", "ACTIVATED", "RECOVER"):
            return

        mapping = {
            "OK": "ACTIVADO",
            "ACTIVATED": "ACTIVADO",
            "RECOVER": "ACTIVADO",
            "FAULT": "AVERIADO",
            "OUT_OF_SERVICE": "PARADO",
            "DISCONNECTED": "DESCONECTADO",
        }

        db_state = mapping.get(st)
        if db_state:
            update_cp_status(cp_id, db_state)
            meta["status"] = db_state

        self.last_seen[cp_id] = time.time()

    def on_cp_event(self, data: Dict):

        cp_id = data.get("cp_id")
        if cp_id:
            self.last_seen[cp_id] = time.time()

    def on_driver_event(self, data: Dict):

        pass

    # HEARTBEAT / DISCONNECT
    async def heartbeat_watcher(self):
        while True:
            await asyncio.sleep(HEARTBEAT_CHECK_PERIOD)
            now = time.time()

            for cp_id, ts in list(self.last_seen.items()):
                meta = self.cp_meta.get(cp_id, {})

                if meta.get("status") == "PARADO":
                    continue

                # Normal heartbeat check
                if now - ts > HEARTBEAT_TIMEOUT_SEC:
                    update_cp_status(cp_id, "DESCONECTADO")
                    meta["status"] = "DESCONECTADO"

    # CENTRAL CLI (Stop/Resume/Out/Activate)
    async def central_cli(self):
        loop = asyncio.get_event_loop()
        while True:
            cmdline = await loop.run_in_executor(None, input, "central> ")
            parts = cmdline.strip().split()
            if not parts:
                continue

            cmd = parts[0].lower()
            target = parts[1].upper() if len(parts) > 1 else None

            # stop all
            if cmd == "stop_all":
                await self.publish_central_cmd({"cmd": "STOP_ALL"})
                print("Broadcast STOP_ALL sent.")

            # resume all
            elif cmd == "resume_all":
                await self.publish_central_cmd({"cmd": "RESUME_ALL"})
                print("Broadcast RESUME_ALL sent.")

            # stop cp
            elif cmd == "stop" and target:
                await self.publish_central_cmd({"cmd": "STOP", "cp_id": target})

                w = self.cp_sockets.get(target)
                if w:
                    w.write(pack_message("STOP#FORCE"))
                    await w.drain()

                print(f"{target} STOP sent (session ended, CP stays ACTIVADO).")

            # resume cp
            elif cmd == "resume" and target:
                await self.publish_central_cmd({"cmd": "RESUME", "cp_id": target})

                # No DB status change (cp stays activated)
                print(f"{target} RESUME sent (ready for new start).")

            # out of service
            elif cmd == "out" and target:
                update_cp_status(target, "PARADO")
                meta = self.cp_meta.setdefault(target, {})
                meta["status"] = "PARADO"

                print(f"{target} is OUT_OF_SERVICE (PARADO).")

            # activate
            elif cmd == "activate" and target:
                update_cp_status(target, "ACTIVADO")
                meta = self.cp_meta.setdefault(target, {})
                meta["status"] = "ACTIVADO"

                print(f"{target} ACTIVATED (back to service).")

            else:
                print("Commands: stop <CP>, resume <CP>, out <CP>, activate <CP>, stop_all, resume_all")

    # kafka produce helpers
    async def publish_event(self, evt_type: str, **fields):
        payload = {"type": evt_type, **fields}
        await self.kafka_producer.send_and_wait(EVENTS_TOPIC, json.dumps(payload).encode())

    async def publish_driver_event(self, evt_type: str, driver_id: str, **fields):
        payload = {"type": evt_type, "driver_id": driver_id, **fields}
        await self.kafka_producer.send_and_wait(DRIVER_EVENTS_TOPIC, json.dumps(payload).encode())

    async def publish_central_cmd(self, cmd_obj: Dict):
        await self.kafka_producer.send_and_wait(CENTRAL_CMD_TOPIC, json.dumps(cmd_obj).encode())

    async def publish_status(self, status: str, cp_id: str):
        payload = {"cp_id": cp_id, "status": status}
        await self.kafka_producer.send_and_wait(STATUS_TOPIC, json.dumps(payload).encode())

    # dashboard
    def print_dashboard(self):
        clear()
        print(f"{Fore.CYAN}=== EV CHARGING NETWORK DASHBOARD ==={Style.RESET_ALL}")
        rows = get_all_cps()  # (cp_id, location, price, status)

        # merge with meta (kw/eur/session)
        meta = self.cp_meta

        state_color = {
            "ACTIVADO": Fore.GREEN,  # available
            "SUMINISTRANDO": Fore.YELLOW,  # charging (could also be BLUE)
            "AVERIADO": Fore.RED,  # fault
            "PARADO": Fore.MAGENTA,  # out of service
            "DESCONECTADO": Fore.LIGHTBLACK_EX,
        }

        header = f"{'CP':8} | {'Location':10} | {'€/kWh':>6} | {'State':13} | {'Sess':12} | {'Driver':7} | {'kW':>6} | {'€':>6}"
        print(header)
        print("-" * len(header))

        for (cp_id, location, price, status) in rows:
            m = meta.get(cp_id, {})
            col = state_color.get(status, Fore.WHITE)
            sess = m.get("session", "-")
            drv = m.get("driver", "-")
            kw = m.get("kw", 0.0)
            eur = m.get("eur", 0.0)
            line = f"{cp_id:8} | {location:10} | {price:6.2f} | {status:13} | {sess:12} | {drv:7} | {kw:6.2f} | {eur:6.2f}"
            print(col + line + Style.RESET_ALL)


# enrtry point
async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9002
    server = CentralServer(port=port)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
