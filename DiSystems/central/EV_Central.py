import asyncio
import json
import sys
import os
import time
import requests
from typing import Dict

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from colorama import Fore, Style, init as colorama_init

from common.protocol_utils import pack_message, unpack_message
from common.crypto_utils import (
    aes_encrypt,
    aes_decrypt,
    decrypt_kafka_payload,
)

from .central_db import (
    init_db,
    registry_get_cp,
    save_cp,
    update_cp_status,
    get_free_cp,
    reset_all_cp_status,
    log_audit,
    store_cp_key,
    get_db_connection
)

colorama_init()


# CONFIG
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

REGISTRY_URL = CONFIG["registry_url"]
KAFKA_BOOTSTRAP = CONFIG["kafka_bootstrap"]
TOPICS = CONFIG["topics"]

TELEMETRY_TOPIC = TOPICS["telemetry"]
STATUS_TOPIC = TOPICS["cp_status"]
CENTRAL_CMD_TOPIC = TOPICS["central_cmd"]
WEATHER_TOPIC = TOPICS["weather_alerts"]

HEARTBEAT_TIMEOUT_SEC = 10
HEARTBEAT_CHECK_PERIOD = 5



# CENTRAL SERVER
class CentralServer:
    def __init__(self, host="0.0.0.0", port=9002):
        self.host = host
        self.port = port

        self.session_keys: Dict[str, str] = {}
        self.cp_sockets: Dict[str, asyncio.StreamWriter] = {}
        self.driver_sockets: Dict[str, asyncio.StreamWriter] = {}
        self.sessions: Dict[str, Dict] = {}
        self.cp_meta: Dict[str, Dict] = {}
        self.last_seen: Dict[str, float] = {}

        self.kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP
        )

        self.kafka_consumer = AIOKafkaConsumer(
            TELEMETRY_TOPIC,
            STATUS_TOPIC,
            WEATHER_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="central_group",
            auto_offset_reset="latest",
        )

    
    async def start(self):
        init_db()
        reset_all_cp_status("DESCONECTADO")

        await self.kafka_producer.start()
        await self.kafka_consumer.start()

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"{Fore.CYAN}EV_Central listening on {self.host}:{self.port}{Style.RESET_ALL}")

        asyncio.create_task(self.kafka_loop())
        asyncio.create_task(self.heartbeat_watcher())

        async with server:
            await server.serve_forever()


    # TCP HANDLING
    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info("peername")
        print(f"TCP connection from {addr}")

        try:
            while True:
                data = await reader.readuntil(b"\x03")
                lrc = await reader.readexactly(1)
                payload, ok = unpack_message(data + lrc)

                if not ok or not payload:
                    continue

                msg = payload.strip()

                # ---------- ENCRYPTED CP → CENTRAL ----------
                if msg.startswith("ENC#"):
                    cp_id = getattr(writer, "cp_id", None)
                    if not cp_id:
                        continue

                    sym = self.session_keys.get(cp_id)
                    if not sym:
                        continue

                    encrypted_part = msg.split("#", 1)[1]
                    msg = aes_decrypt(sym, encrypted_part)

                # ---------- AUTH CP ----------
                if msg.startswith("AUTH_CP#"):
                    _, cp_id, credential = msg.split("#")

                    import hashlib, secrets
                    hashed = hashlib.sha256(credential.encode()).hexdigest()

                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT credential_hash, location FROM registry_cp WHERE cp_id=?",
                        (cp_id,)
                    )
                    row = cur.fetchone()
                    conn.close()

                    if not row or row[0] != hashed:
                        writer.write(pack_message("AUTH_FAIL"))
                        await writer.drain()
                        continue

                    sym_key = secrets.token_hex(32)
                    store_cp_key(cp_id, sym_key)
                    self.session_keys[cp_id] = sym_key

                    self.cp_sockets[cp_id] = writer
                    writer.cp_id = cp_id

                    self.cp_meta[cp_id] = {
                        "location": row[1],
                        "status": "ACTIVADO",
                        "driver": "-",
                        "session": "-",
                        "kw": 0.0,
                        "eur": 0.0,

                    }
                    self.last_seen[cp_id] = time.time()
                    update_cp_status(cp_id, "ACTIVADO")
                    save_cp(cp_id, row[1], 0.25, "ACTIVADO")
                    self.last_seen[cp_id] = time.time()

                    writer.write(pack_message(f"AUTH_OK#{cp_id}#{sym_key}"))
                    await writer.drain()
                    continue

                # ---------- AUTH MONITOR ----------
                if msg.startswith("AUTH_MON#"):
                    _, mon_id, credential = msg.split("#")

                    try:
                        r = requests.post(
                            f"{REGISTRY_URL}/auth/monitor",
                            json={"monitor_id": mon_id, "credential": credential},
                            timeout=3,
                            verify=False
                        )
                    except Exception:
                        writer.write(pack_message("AUTH_MON_FAIL"))
                        await writer.drain()
                        continue

                    if r.status_code == 200 and r.json().get("ok"):
                        writer.write(pack_message(f"AUTH_MON_OK#{mon_id}"))
                        self.last_seen[mon_id] = time.time()
                    else:
                        writer.write(pack_message("AUTH_MON_FAIL"))

                    await writer.drain()
                    continue

                # ---------- DRIVER AUTH ----------
                if msg.startswith("AUTH_REQ#"):
                    _, driver_id = msg.split("#")
                    self.driver_sockets[driver_id] = writer
                    writer.write(pack_message(f"AUTH_RESP#{driver_id}#ALLOW"))
                    await writer.drain()
                    continue

                # ---------- START ----------
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
                        "INSERT INTO sessions(session_id, cp_id, driver_id, start_time) VALUES (?, ?, ?, ?)",
                        (session_id, cp_id, driver_id, time.time())
                    )
                    log_audit(
                        "EV_Central",
                        "SESSION_START",
                        f"session_id={session_id} | cp_id={cp_id} | driver_id={driver_id}"
                    )
                    conn.commit()
                    conn.close()

                    cpw = self.cp_sockets.get(cp_id)
                    enc = aes_encrypt(self.session_keys[cp_id], f"START#{session_id}#{driver_id}")
                    cpw.write(pack_message(f"ENC#{enc}"))
                    await cpw.drain()

                    writer.write(pack_message(f"START#{session_id}#{cp_id}"))
                    await writer.drain()

                    self.cp_meta[cp_id].update({
                        "status": "SUMINISTRANDO",
                        "driver": driver_id,
                        "session": session_id
                    })
                    update_cp_status(cp_id, "SUMINISTRANDO")
                    continue

                # stop
                if msg.startswith("STOP_REQ#"):
                    _, session_id = msg.split("#")
                    session = self.sessions.get(session_id)
                    if not session:
                        continue

                    cp_id = session["cp_id"]

                    log_audit(
                        "EV_Central",
                        "SESSION_STOP",
                        f"session_id={session_id} | cp_id={cp_id}"
                    )

                    cpw = self.cp_sockets.get(cp_id)
                    enc = aes_encrypt(self.session_keys[cp_id], f"STOP#{session_id}")
                    cpw.write(pack_message(f"ENC#{enc}"))
                    await cpw.drain()

                    self.cp_meta[cp_id].update({
                        "status": "ACTIVADO",
                        "driver": "-",
                        "session": "-"
                    })
                    update_cp_status(cp_id, "ACTIVADO")
                    continue

        except Exception as e:
            print(f"Client disconnected {addr}: {e}")

    
    # KAFKA LOOP
    async def kafka_loop(self):
        async for rec in self.kafka_consumer:
            try:
                ciphertext = rec.value.decode()

                
                if rec.topic in (STATUS_TOPIC, WEATHER_TOPIC):
                    try:
                        data = json.loads(ciphertext)
                    except Exception:
                        continue

            
                else:
                    data = None
                    for sym in self.session_keys.values():
                        try:
                            data = decrypt_kafka_payload(sym, ciphertext)
                            break
                        except Exception:
                            pass

                    if not data:
                        continue
                

                if rec.topic == TELEMETRY_TOPIC:
                    self.on_telemetry(data)
                elif rec.topic == STATUS_TOPIC:
                    self.on_status(data)
                elif rec.topic == WEATHER_TOPIC:
                    self.on_weather_alert(data)

            except Exception as e:
                print("Kafka error:", e)

    
    # TELEMETRY
    def on_telemetry(self, data):
        cp = data["cp_id"]
        meta = self.cp_meta[cp]

        meta["kw"] = data.get("kw", 0.0)
        meta["eur"] = data.get("eur", 0.0)

        if data["status"] == "FINISHED":
            session_id = meta["session"]

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE sessions SET end_time=?, kwh=?, eur=? WHERE session_id=?",
                (time.time(), meta["kw"], meta["eur"], session_id)
            )
            log_audit(
                "EV_Central",
                "SESSION_FINISHED",
                f"session_id={session_id} | cp_id={cp}"
            )
            conn.commit()
            conn.close()

            #send ticket to driver
            driver_id = meta["driver"]
            drv = self.driver_sockets.get(driver_id)

            if drv:
                drv.write(
                    pack_message(
                        f"TICKET#{session_id}#{meta['kw']}#{meta['eur']}"
                    )
                )
                asyncio.create_task(drv.drain())


            meta.update({"status": "ACTIVADO", "session": "-", "driver": "-", "kw": 0.0, "eur": 0.0})
            update_cp_status(cp, "ACTIVADO")

        self.last_seen[cp] = time.time()

    

    # STATUS / WEATHER
    def on_status(self, data):
        cp = data["cp_id"]
        st = data["status"].upper()
        source = data.get("source")

        # Ensure CP meta exists and is complete 
        if cp not in self.cp_meta:
            self.cp_meta[cp] = {}

        meta = self.cp_meta[cp]

        
        meta.setdefault("engine_ok", False)
        meta.setdefault("monitor_ok", False)
        meta.setdefault("status", "DESCONECTADO")
        meta.setdefault("location", "-")
        meta.setdefault("driver", "-")
        meta.setdefault("session", "-")
        meta.setdefault("kw", 0.0)
        meta.setdefault("eur", 0.0)

        # Update flags
        if source == "ENGINE":
            meta["engine_ok"] = (st == "OK")
        elif source == "MONITOR":
            meta["monitor_ok"] = (st == "OK")

        # Central decides CP status
        if meta["engine_ok"] and meta["monitor_ok"]:
            final = "ACTIVADO"
        else:
            final = "DESCONECTADO"

        # Write to DB if changed
        if meta["status"] != final:
            meta["status"] = final
            update_cp_status(cp, final)

        # Heartbeat
        self.last_seen[cp] = time.time()


    def on_weather_alert(self, data):
        city = data["city"]
        alert = data["alert"]

        log_audit("EV_Central", "WEATHER_ALERT", f"{alert} in {city}")

        for cp_id, meta in self.cp_meta.items():
            if meta["location"] == city:
                self.abort_session_due_to_cp_failure(cp_id, f"weather_{alert.lower()}")
                update_cp_status(cp_id, "PARADO")

   

    # HEARTBEAT
    async def heartbeat_watcher(self):
        while True:
            await asyncio.sleep(HEARTBEAT_CHECK_PERIOD)
            now = time.time()

            for cp, ts in list(self.last_seen.items()):
                if now - ts > HEARTBEAT_TIMEOUT_SEC:
                    self.abort_session_due_to_cp_failure(cp, "heartbeat_timeout")
                    update_cp_status(cp, "DESCONECTADO")

    

    # SESSION ABORT
    def abort_session_due_to_cp_failure(self, cp_id: str, reason: str):
        meta = self.cp_meta.get(cp_id)
        if not meta or meta["session"] == "-":
            return

        session_id = meta["session"]
        driver_id = meta["driver"]

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE sessions SET end_time=?, kwh=?, eur=? WHERE session_id=?",
            (time.time(), meta["kw"], meta["eur"], session_id)
        )
        conn.commit()
        conn.close()

        log_audit(
            "EV_Central",
            "SESSION_ABORTED",
            f"session_id={session_id} | cp_id={cp_id} | driver_id={driver_id} | reason={reason}"
        )

        drv = self.driver_sockets.get(driver_id)
        if drv:
            drv.write(pack_message(f"STOP#{session_id}"))
            asyncio.create_task(drv.drain())

        meta.update({
            "status": "DESCONECTADO",
            "session": "-",
            "driver": "-",
            "kw": 0.0,
            "eur": 0.0,
        })
        self.sessions.pop(session_id, None)



async def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9002
    server = CentralServer(port=port)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
