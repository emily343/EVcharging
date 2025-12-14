from flask import Flask, jsonify, request
import os
import json
import asyncio

from aiokafka import AIOKafkaProducer

from central.central_db import (
    get_recent_alerts,
    get_db_connection,
    log_audit,
)

#load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    CONFIG = json.load(f)

CENTRAL_CMD_TOPIC = CONFIG["topics"]["central_cmd"]

app = Flask(__name__)

#kafka producers
producer = None

async def send_kafka_cmd(payload: dict):
    global producer

    if producer is None:
        producer = AIOKafkaProducer(
            bootstrap_servers=CONFIG["kafka_bootstrap"]
        )
        await producer.start()

    await producer.send_and_wait(
        CENTRAL_CMD_TOPIC,
        json.dumps(payload).encode()
    )

#api routes
@app.route("/api/health")
def health():
    return jsonify({
        "service": "EV_Central_API",
        "status": "OK"
    })


#list fo all cps
@app.route("/api/cps", methods=["GET"])
def list_cps():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            cp.id,
            cp.location,
            cp.price,
            cp.status,
            cp.temperature,
            cp.wind,
            s.session_id,
            s.driver_id,
            s.kwh,
            s.eur
        FROM charging_points cp
        LEFT JOIN sessions s
            ON cp.id = s.cp_id
        AND s.end_time IS NULL
    """)

    rows = cur.fetchall()
    conn.close()

    cps = []
    for (
        cp_id,
        location,
        price,
        status,
        temperature,
        wind,
        session_id,
        driver_id,
        kwh,
        eur
    ) in rows:
        cps.append({
            "cp_id": cp_id,
            "location": location,
            "price": price,
            "status": status,
            "temperature": temperature,
            "wind": wind,
            "session": session_id or "-",
            "driver": driver_id or "-",
            "kw": kwh if kwh is not None else "-",
            "eur": eur if eur is not None else "-"
        })


    return jsonify(cps)


#cp details
@app.route("/api/cps/<cp_id>", methods=["GET"])
def cp_details(cp_id):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, location, price, status, temperature, wind
        FROM charging_points
        WHERE id = ?
    """, (cp_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return jsonify({"error": True, "msg": "not found"}), 404

    cp_id, location, price, status, temperature, wind = row

    return jsonify({
        "cp_id": cp_id,
        "location": location,
        "price": price,
        "status": status,
        "temperature": temperature,
        "wind": wind
    })


#alerts
@app.route("/api/alerts", methods=["GET"])
def list_alerts():
    rows = get_recent_alerts(limit=30)
    alerts = []

    for t, source, action, details in rows:
        alerts.append({
            "timestamp": t,
            "source": source,
            "action": action,
            "details": details
        })

    return jsonify(alerts)


#session history
@app.route("/api/sessions", methods=["GET"])
def session_history():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT session_id, cp_id, driver_id, start_time, end_time, kwh, eur
        FROM sessions
        ORDER BY start_time DESC
        LIMIT 100
    """)

    rows = cur.fetchall()
    conn.close()

    result = []
    for sid, cp, drv, start, end, kwh, eur in rows:
        result.append({
            "session_id": sid,
            "cp_id": cp,
            "driver_id": drv,
            "start_time": start,
            "end_time": end,
            "kwh": kwh,
            "eur": eur
        })

    return jsonify(result)


#weather alerts
@app.route("/api/weather_alert", methods=["POST"])
def weather_alert():
    data = request.get_json()
    if not data:
        return jsonify({"ok": False}), 400

    city = data.get("city")
    alert = data.get("alert")
    temp = data.get("temp")
    wind = data.get("wind")

    if not city or not alert:
        return jsonify({"ok": False}), 400

    log_audit(
        "EV_Weather",
        "WEATHER_ALERT",
        f"{alert} in {city} (T={temp}, W={wind})"
    )

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE charging_points
        SET status = 'PARADO',
            temperature = ?,
            wind = ?
        WHERE location = ?
    """, (temp, wind, city))
    conn.commit()
    conn.close()

    return jsonify({"ok": True}), 200

#weather updates
@app.route("/api/weather_update", methods=["POST"])
def weather_update():
    data = request.get_json()
    if not data:
        return jsonify({"ok": False}), 400

    city = data.get("city")
    temp = data.get("temp")
    wind = data.get("wind")

    if city is None:
        return jsonify({"ok": False}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE charging_points
        SET temperature=?, wind=?
        WHERE location=?
    """, (temp, wind, city))
    conn.commit()
    conn.close()

    return jsonify({"ok": True})



#central commands (buttons)
@app.route("/api/central_cmd", methods=["POST"])
def central_cmd():
    data = request.get_json()
    if not data or "cmd" not in data:
        return jsonify({"ok": False, "msg": "cmd required"}), 400

    cmd = data["cmd"].upper()
    cp_id = data.get("cp_id")

    payload = {"cmd": cmd}
    if cp_id:
        payload["cp_id"] = cp_id.upper()

    log_audit(
        "CENTRAL_API",
        "CENTRAL_CMD",
        f"cmd={cmd}" + (f" | cp_id={cp_id}" if cp_id else "")
    )

    asyncio.run(send_kafka_cmd(payload))

    return jsonify({"ok": True, "sent": payload}), 200

@app.route("/api/locations", methods=["GET"])
def get_locations():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT location FROM charging_points")
    rows = cur.fetchall()
    conn.close()

    return jsonify([r[0] for r in rows])


#main
if __name__ == "__main__":
    print("Central API running at http://127.0.0.1:8000")
    app.run(host="0.0.0.0", port=8000)

