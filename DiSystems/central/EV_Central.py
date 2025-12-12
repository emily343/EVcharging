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
    get_cp_key,
    get_db_connection
)

colorama_init()

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
TOPICS = CONFIG["topics"]

TELEMETRY_TOPIC = TOPICS["telemetry"]
STATUS_TOPIC = TOPICS["cp_status"]
EVENTS_TOPIC = TOPICS["cp_events"]
DRIVER_EVENTS_TOPIC = TOPICS["driver_events"]
CENTRAL_CMD_TOPIC = TOPICS["central_cmd"]
WEATHER_TOPIC = TOPICS["weather_alerts"]

HEARTBEAT_TIMEOUT_SEC = 10
HEARTBEAT_CHECK_PERIOD = 5


def clear():
    os.system("cls" if os.name == "nt" else "clear")


class CentralServer:
    def __init__(self, host="0.0.0.0", port=9002):
        self.host = host
        self.port = port

        # symmetric AES keys per CP
        self.session_keys: Dict[str, str] = {}

        # tcp connections
        self.cp_sockets: Dict[str, asyncio.StreamWriter] = {}
        self.driver_sockets: Dict[str, asyncio.StreamWriter] = {}
        self.sessions: Dict[str, Dict] = {}

        # state + telemetry
        self.cp_meta: Dict[str, Dict] = {}

        # heartbeat
        self.last_seen: Dict[str, float] = {}

        # Kafka
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        self.kafka_consumers = [
            AIOKafkaConsumer(
                TELEMETRY_TOPIC,
                STATUS_TOPIC,
                EVENTS_TOPIC,
                DRIVER_EVENTS_TOPIC,
                WEATHER_TOPIC,
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

    # ---------------------------
    # TCP CLIENT HANDLING
    # ---------------------------
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

                # ---------------------------
                # AUTH CP
                # ---------------------------
                if msg.startswith("AUTH_CP#"):
                    _, cp_id, credential_plain = msg.split("#")

                    import hashlib, secrets

                    db_row = registry_get_cp(cp_id)
                    if not db_row:
                        writer.write(pack_message("AUTH_FAIL#UNKNOWN_CP"))
                        await writer.drain()
                        continue

                    hashed = hashlib.sha256(credential_plain.encode()).hexdigest()

                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT credential_hash, location FROM registry_cp WHERE cp_id=?", (cp_id,))
                    row = cur.fetchone()
                    conn.close()

                    if not row or row[0] != hashed:
                        writer.write(pack_message("AUTH_FAIL#BAD_CREDENTIAL"))
                        await writer.drain()
                        continue

                    sym_key = secrets.token_hex(32)
                    store_cp_key(cp_id, sym_key)
                    self.session_keys[cp_id] = sym_key

                    self.cp_sockets[cp_id] = writer
                    self.cp_meta.setdefault(cp_id, {})
                    self.cp_meta[cp_id].update({
                        "location": row[1],
                        "price": 0.0,
                        "status": "ACTIVADO",
                        "driver": "-",
                        "session": "-",
                        "kw": 0.0,
                        "eur": 0.0,
                    })

                    update_cp_status(cp_id, "ACTIVADO")
                    self.last_seen[cp_id] = time.time()

                    writer.write(pack_message(f"AUTH_OK#{cp_id}#{sym_key}"))
                    await writer.drain()
                    continue

                # ---------------------------
                # AUTH MONITOR
                # ---------------------------
                if msg.startswith("AUTH_MON#"):
                    _, mon_id, credential_plain = msg.split("#")

                    import hashlib

                    db_row = registry_get_cp(mon_id)
                    if not db_row:
                        writer.write(pack_message("AUTH_MON_FAIL#UNKNOWN"))
                        await writer.drain()
                        continue

                    hashed = hashlib.sha256(credential_plain.encode()).hexdigest()

                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("SELECT credential_hash FROM registry_cp WHERE cp_id=?", (mon_id,))
                    row = cur.fetchone()
                    conn.close()

                    if not row or row[0] != hashed:
                        writer.write(pack_message("AUTH_MON_FAIL#BAD_CRED"))
                        await writer.drain()
                        continue

                    writer.write(pack_message(f"AUTH_MON_OK#{mon_id}"))
                    await writer.drain()
                    continue

                # ---------------------------
                # REGISTER CP
                # ---------------------------
                if msg.startswith("REGISTER#"):
                    _, cp_id, location, price = msg.split("#")
                    price = float(price)

                    self.cp_sockets[cp_id] = writer
                    save_cp(cp_id, location, price, "ACTIVADO")

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

                    writer.write(pack_message(f"ACK#REGISTER#{cp_id}#OK"))
                    await writer.drain()
                    continue

                # ---------------------------
                # DRIVER AUTH
                # ---------------------------
                if msg.startswith("AUTH_REQ#"):
                    _, driver_id = msg.split("#")
                    self.driver_sockets[driver_id] = writer
                    writer.write(pack_message(f"AUTH_RESP#{driver_id}#ALLOW"))
                    await writer.drain()
                    continue

                # ---------------------------
                # START REQUEST
                # ---------------------------
                if msg.startswith("START_REQ#"):
                    _, driver_id = msg.split("#")

                    cp_id = get_free_cp()
                    if not cp_id:
                        writer.write(pack_message("NO_CP_AVAILABLE"))
                        await writer.drain()
                        continue

                    session_id = f"S-{driver_id}-{cp_id}"
                    self.sessions[session_id] = {"driver_id": driver_id, "cp_id": cp_id}

                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT OR REPLACE INTO sessions(session_id, cp_id, driver_id, start_time) VALUES (?, ?, ?, ?)",
                        (session_id, cp_id, driver_id, time.time()),
                    )
                    conn.commit()
                    conn.close()

                    cpw = self.cp_sockets.get(cp_id)
                    if cpw:
                        sym = self.session_keys.get(cp_id)
                        encrypted = aes_encrypt(sym, f"START#{session_id}#{driver_id}")
                        cpw.write(pack_message(f"ENC#{encrypted}"))
                        await cpw.drain()

                    writer.write(pack_message(f"START#{session_id}#{cp_id}"))
                    await writer.drain()

                    update_cp_status(cp_id, "SUMINISTRANDO")
                    self.cp_meta[cp_id]["status"] = "SUMINISTRANDO"
                    self.cp_meta[cp_id]["driver"] = driver_id
                    self.cp_meta[cp_id]["session"] = session_id
                    continue

                # ---------------------------
                # STOP REQUEST
                # ---------------------------
                if msg.startswith("STOP_REQ#"):
                    _, session_id = msg.split("#")
                    session = self.sessions.get(session_id)
                    if not session:
                        continue

                    cp_id = session["cp_id"]
                    cpw = self.cp_sockets.get(cp_id)

                    if cpw:
                        sym = self.session_keys.get(cp_id)
                        enc = aes_encrypt(sym, f"STOP#{session_id}")
                        cpw.write(pack_message(f"ENC#{enc}"))
                        await cpw.drain()

                    update_cp_status(cp_id, "ACTIVADO")
                    meta = self.cp_meta[cp_id]
                    meta.update({"status": "ACTIVADO", "driver": "-", "session": "-"})

                    continue

        except Exception as e:
            print(f"Client disconnected {addr}: {e}")

    # ---------------------------
    # KAFKA LOOP
    # ---------------------------
    async def kafka_loop(self):
        consumer = self.kafka_consumers[0]
        async for rec in consumer:
            topic = rec.topic
            try:
                data = json.loads(rec.value.decode())
            except:
                continue

            if topic == TELEMETRY_TOPIC:
                self.on_telemetry(data)
            elif topic == STATUS_TOPIC:
                self.on_status(data)
            elif topic == WEATHER_TOPIC:
                self.on_weather_alert(data)

    # ---------------------------
    # TELEMETRY
    # ---------------------------
    def on_telemetry(self, data):
        cp = data.get("cp_id")
        if not cp:
            return

        meta = self.cp_meta.setdefault(cp, {})
        meta["kw"] = float(data.get("kw", 0.0))
        meta["eur"] = float(data.get("eur", 0.0))
        if "driver" in data:
            meta["driver"] = data["driver"]
        if "session_id" in data:
            meta["session"] = data["session_id"]

        status = data.get("status")
        if status == "CHARGING":
            update_cp_status(cp, "SUMINISTRANDO")
            meta["status"] = "SUMINISTRANDO"

        elif status == "FINISHED":
            session_id = meta.get("session", "-")
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE sessions SET end_time=?, kwh=?, eur=? WHERE session_id=?",
                (time.time(), meta["kw"], meta["eur"], session_id),
            )
            conn.commit()
            conn.close()

            update_cp_status(cp, "ACTIVADO")
            meta.update({"status": "ACTIVADO", "session": "-", "driver": "-", "kw": 0.0, "eur": 0.0})

        self.last_seen[cp] = time.time()

    # ---------------------------
    # STATUS
    # ---------------------------
    def on_status(self, data):
        cp = data.get("cp_id")
        if not cp:
            return

        st = data.get("status", "").upper()

        mapping = {
            "OK": "ACTIVADO",
            "FAULT": "AVERIADO",
            "OUT_OF_SERVICE": "PARADO",
            "DISCONNECTED": "DESCONECTADO",
            "RECOVER": "ACTIVADO",
        }

        if st in mapping:
            update_cp_status(cp, mapping[st])
            self.cp_meta.setdefault(cp, {})["status"] = mapping[st]

        self.last_seen[cp] = time.time()

    # ---------------------------
    # WEATHER ALERT
    # ---------------------------
    def on_weather_alert(self, data):
        city = data.get("city")
        alert = data.get("alert")

        print(f"[Central] Weather Alert: {alert} in {city}")
        log_audit("EV_Central", "WEATHER_ALERT", f"{alert} in {city}")

        # disable charging points in that city
        for cp_id, meta in self.cp_meta.items():
            if meta.get("location") == city:
                update_cp_status(cp_id, "PARADO")
                meta["status"] = "PARADO"
                print(f"[Central] → {cp_id} set to PARADO due to {alert}")

    # ---------------------------
    # HEARTBEAT WATCHER
    # ---------------------------
    async def heartbeat_watcher(self):
        while True:
            await asyncio.sleep(HEARTBEAT_CHECK_PERIOD)
            now = time.time()

            for cp, ts in list(self.last_seen.items()):
                if now - ts > HEARTBEAT_TIMEOUT_SEC:
                    update_cp_status(cp, "DESCONECTADO")
                    self.cp_meta[cp]["status"] = "DESCONECTADO"

    # ---------------------------
    # CLI
    # ---------------------------
    async def central_cli(self):
        loop = asyncio.get_event_loop()

        while True:
            cmdline = await loop.run_in_executor(None, input, "central> ")
            parts = cmdline.strip().split()
            if not parts:
                continue

            cmd = parts[0].lower()
            cp = parts[1].upper() if len(parts) > 1 else None

            if cmd == "stop_all":
                await self.kafka_producer.send_and_wait(CENTRAL_CMD_TOPIC, b'{"cmd":"STOP_ALL"}')
                print("STOP_ALL sent.")

            elif cmd == "resume_all":
                await self.kafka_producer.send_and_wait(CENTRAL_CMD_TOPIC, b'{"cmd":"RESUME_ALL"}')
                print("RESUME_ALL sent.")

            elif cmd == "stop" and cp:
                await self.kafka_producer.send_and_wait(
                    CENTRAL_CMD_TOPIC,
                    json.dumps({"cmd": "STOP", "cp_id": cp}).encode()
                )
                print(f"{cp} STOP sent.")

            elif cmd == "resume" and cp:
                await self.kafka_producer.send_and_wait(
                    CENTRAL_CMD_TOPIC,
                    json.dumps({"cmd": "RESUME", "cp_id": cp}).encode()
                )
                print(f"{cp} RESUME sent.")

            elif cmd == "out" and cp:
                update_cp_status(cp, "PARADO")
                print(f"{cp} OUT OF SERVICE")

            elif cmd == "activate" and cp:
                update_cp_status(cp, "ACTIVADO")
                print(f"{cp} ACTIVATED")

            else:
                print("Commands: stop <CP>, resume <CP>, out <CP>, activate <CP>, stop_all, resume_all")


async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9002
    server = CentralServer(port=port)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
