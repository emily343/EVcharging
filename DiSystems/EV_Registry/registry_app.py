from flask import Flask, request, jsonify
import secrets
import hashlib
import os

from central.central_db import (
    init_db,
    registry_save_cp,
    registry_deactivate_cp,
    registry_get_cp,
    log_audit
)

BASE_DIR = os.path.dirname(__file__)
CERT_DIR = os.path.join(BASE_DIR, "certs")
app = Flask(__name__)



# helper
def hash_credential(secret_plain: str) -> str:
    return hashlib.sha256(secret_plain.encode()).hexdigest()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "service": "EV_Registry",
        "status": "OK"
    })


@app.route("/register/cp", methods=["POST"])
def register_cp():
    data = request.get_json()

    if not data or not all(k in data for k in ("cp_id", "location", "credential")):
        return jsonify({
            "error": True,
            "msg": "cp_id, location and credential required"
        }), 400

    cp_id = data["cp_id"]
    location = data["location"]
    credential_plain = data["credential"]

    
    secret_hash = hash_credential(credential_plain)

    try:
        # Save CP in registry
        registry_save_cp(cp_id, location, secret_hash)

    
        log_audit(
            source="EV_Registry",
            action="REGISTER_CP",
            details=f"cp_id={cp_id} | location={location}"
        )

        return jsonify({
            "error": False,
            "cp_id": cp_id,
            "location": location
        }), 201

    except Exception as e:
        log_audit(
            source="EV_Registry",
            action="REGISTER_CP_FAILED",
            details=f"cp_id={cp_id} | error={str(e)}"
        )
        return jsonify({"error": True, "msg": str(e)}), 500



@app.route("/unregister/<cp_id>", methods=["DELETE"])
def unregister(cp_id):
    ok = registry_deactivate_cp(cp_id)
    log_audit("EV_Registry", "UNREGISTER_CP", cp_id)

    if ok:
        return jsonify({"error": False, "msg": "deactivated"}), 200
    return jsonify({"error": True, "msg": "not found"}), 404


@app.route("/cp/<cp_id>", methods=["GET"])
def info(cp_id):
    row = registry_get_cp(cp_id)
    if not row:
        return jsonify({"error": True, "msg": "not found"}), 404
    
    cp_id, location, active = row
    return jsonify({
        "cp_id": cp_id,
        "location": location,
        "active": active
    })


@app.route("/auth/monitor", methods=["POST"])
def auth_monitor():
    data = request.get_json()
    if not data:
        return jsonify({"ok": False}), 400

    mon_id = data.get("monitor_id")
    credential = data.get("credential")

    if not mon_id or not credential:
        return jsonify({"ok": False}), 400

    row = registry_get_cp(mon_id)
    if not row:
        return jsonify({"ok": False}), 403

    cp_id, location, active = row
    if not active:
        return jsonify({"ok": False}), 403

    hashed = hash_credential(credential)

    from central.central_db import get_db_connection
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT credential_hash FROM registry_cp WHERE cp_id=?",
        (mon_id,)
    )
    db_hash = cur.fetchone()[0]
    conn.close()

    if hashed != db_hash:
        return jsonify({"ok": False}), 403

    return jsonify({"ok": True}), 200




if __name__ == "__main__":
    init_db()
    print("EV_Registry running on https://localhost:5001")
    
    
    app.run(
    host="0.0.0.0",
    port=5001,
    debug=True,
    ssl_context=(
        os.path.join(CERT_DIR, "registry.crt"),
        os.path.join(CERT_DIR, "registry.key")
    )
)

    
