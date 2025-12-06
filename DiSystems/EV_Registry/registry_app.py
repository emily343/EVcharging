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


@app.route("/register", methods=["POST"])
def register_cp():
    """
    Erwartet:
    {
        "cp_id": "CP001",
        "location": "Berlin"
    }
    """
    data = request.get_json()

    if not data or "cp_id" not in data or "location" not in data:
        return jsonify({"error": True, "msg": "cp_id and location required"}), 400

    cp_id = data["cp_id"]
    location = data["location"]

    # credentials
    secret_plain = secrets.token_hex(16)
    secret_hash = hash_credential(secret_plain)

    try:
        # save in central db 
        registry_save_cp(cp_id, location, secret_hash)

        # audit-log
        log_audit("EV_Registry", "REGISTER_CP", f"{cp_id} at {location}")

        # klartext-credentials zurückgeben
        return jsonify({
            "error": False,
            "cp_id": cp_id,
            "location": location,
            "credential": secret_plain
        }), 201

    except Exception as e:
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


if __name__ == "__main__":
    init_db()
    print("EV_Registry running on https://localhost:5001")
    
    # activate SSL later here
    app.run(
        host="0.0.0.0",
        port=5001,
        debug=True,
        ssl_context=(
            "certs/registry.crt",
            "certs/registry.key"
        )
    )
