from flask import Flask, jsonify
import os, json
from central.central_db import (
    get_all_cps,
    get_recent_alerts,
)
from central.central_db import get_db_connection

app = Flask(__name__)


@app.route("/api/health")
def health():
    return jsonify({"service": "EV_Central_API", "status": "OK"})


@app.route("/api/cps", methods=["GET"])
def list_cps():
    rows = get_all_cps()  # (cp_id, location, price, status)
    cps = []
    for cp_id, location, price, status in rows:
        cps.append({
            "cp_id": cp_id,
            "location": location,
            "price": price,
            "status": status
        })
    return jsonify(cps)


@app.route("/api/cps/<cp_id>", methods=["GET"])
def cp_details(cp_id):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT cp_id, location, price, status
        FROM charging_points
        WHERE cp_id=?
    """, (cp_id,))
    row = cur.fetchone()

    conn.close()

    if not row:
        return jsonify({"error": True, "msg": "not found"}), 404

    cp_id, location, price, status = row
    return jsonify({
        "cp_id": cp_id,
        "location": location,
        "price": price,
        "status": status
    })


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


if __name__ == "__main__":
    print("Central API running at http://127.0.0.1:8000")
    app.run(host="0.0.0.0", port=8000)
