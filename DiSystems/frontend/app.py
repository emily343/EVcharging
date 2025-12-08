from flask import Flask, render_template, request, jsonify
import requests

API_BASE = "http://127.0.0.1:8000/api"   # REST-API läuft via central_api.py

app = Flask(__name__)


@app.route("/")
def dashboard():
    cps = requests.get(f"{API_BASE}/cps").json()
    alerts = requests.get(f"{API_BASE}/alerts").json()
    return render_template("dashboard.html", cps=cps, alerts=alerts)


@app.route("/sessions")
def sessions():
    history = requests.get(f"{API_BASE}/sessions").json()
    return render_template("sessions.html", sessions=history)


# -------- CENTRAL COMMANDS (Buttons) ----------
@app.route("/cmd/<cmd>/<cp_id>", methods=["POST"])
def send_cmd(cmd, cp_id):
    url = f"{API_BASE}/central_cmd"
    payload = {"cmd": cmd.upper(), "cp_id": cp_id.upper()}
    r = requests.post(url, json=payload)
    return jsonify(r.json())


if __name__ == "__main__":
    print("Frontend running at http://127.0.0.1:8500")
    app.run(host="0.0.0.0", port=8500, debug=True)
