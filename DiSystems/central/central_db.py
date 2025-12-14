import sqlite3
import os
from datetime import datetime

DB_FILE = os.path.join(os.path.dirname(__file__), "central.db")


def get_db_connection():
    conn = sqlite3.connect(
        DB_FILE,
        timeout=5,              
        isolation_level=None,    
        check_same_thread=False
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn




def init_db():
    conn = get_db_connection()
    c = conn.cursor()

    # Charging points table 
    c.execute("""
       CREATE TABLE IF NOT EXISTS charging_points (
        id TEXT PRIMARY KEY,
        location TEXT,
        price REAL,
        status TEXT,
        temperature REAL,
        wind REAL
        )
    """)

    # Drivers 
    c.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            id TEXT PRIMARY KEY
        )
    """)

    # Sessions 
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            cp_id TEXT,
            driver_id TEXT,
            start_time REAL,
            end_time REAL,
            kwh REAL,
            eur REAL
        )
    """)

   
    c.execute("""
        CREATE TABLE IF NOT EXISTS registry_cp (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id TEXT UNIQUE NOT NULL,
            location TEXT NOT NULL,
            credential_hash TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            active INTEGER DEFAULT 1
        )
    """)


    c.execute("""
        CREATE TABLE IF NOT EXISTS cp_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id TEXT UNIQUE NOT NULL,
            sym_key TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Audit log
    c.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            action TEXT,
            details TEXT
        )
    """)

    
    c.execute("""
        CREATE TABLE IF NOT EXISTS weather_alerts (
            alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            cp_id TEXT,
            alert_type TEXT,
            message TEXT
        )
    """)

    conn.commit()
    conn.close()


#cps
def save_cp(cp_id, location, price, status="DESCONECTADO"):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
    """
    INSERT OR REPLACE INTO charging_points
    (id, location, price, status, temperature, wind)
    VALUES (?, ?, ?, ?, NULL, NULL)
    """,
    (cp_id, location, price, status)
    )
    conn.commit()
    conn.close()


def update_cp_status(cp_id, status):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "UPDATE charging_points SET status=? WHERE id=?",
        (status, cp_id)
    )
    conn.commit()
    conn.close()


def get_free_cp():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id FROM charging_points
        WHERE status = 'ACTIVADO'
        LIMIT 1
    """)
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None




def get_all_cps():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM charging_points")
    rows = c.fetchall()
    conn.close()
    return rows


def reset_all_cp_status(new_status):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "UPDATE charging_points SET status = ?",
        (new_status,)
    )
    conn.commit()
    conn.close()


#registry functions
def registry_save_cp(cp_id: str, location: str, credential_hash: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO registry_cp (cp_id, location, credential_hash, active)
        VALUES (?, ?, ?, 1)
        """,
        (cp_id, location, credential_hash)
    )
    conn.commit()
    conn.close()


def registry_deactivate_cp(cp_id: str) -> bool:
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE registry_cp SET active = 0 WHERE cp_id=?", (cp_id,))
    changed = (c.rowcount > 0)
    conn.commit()
    conn.close()
    return changed


def registry_get_cp(cp_id: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT cp_id, location, active FROM registry_cp WHERE cp_id = ?",
        (cp_id,)
    )
    row = c.fetchone()
    conn.close()
    return row


#cp key management 
def store_cp_key(cp_id: str, sym_key: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO cp_keys (cp_id, sym_key, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(cp_id) DO UPDATE SET
            sym_key = excluded.sym_key,
            created_at = excluded.created_at
        """,
        (cp_id, sym_key, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def get_cp_key(cp_id: str) -> str | None:
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT sym_key FROM cp_keys WHERE cp_id=?", (cp_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None



# audit log
def log_audit(source: str, action: str, details: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO audit_log (timestamp, source, action, details)
        VALUES (?, ?, ?, ?)
        """,
        (datetime.now().isoformat(), source, action, details)
    )
    conn.commit()
    conn.close()


# Weather Alerts 
def add_weather_alert(cp_id: str, alert_type: str, message: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO weather_alerts (cp_id, alert_type, message)
        VALUES (?, ?, ?)
        """,
        (cp_id, alert_type, message)
    )
    conn.commit()
    conn.close()


def get_recent_alerts(limit=20):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT alert_id, timestamp, cp_id, alert_type, message
        FROM weather_alerts
        ORDER BY timestamp DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows
