import sqlite3
import os

DB_FILE = os.path.join(os.path.dirname(__file__), "central.db")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS charging_points (
                    id TEXT PRIMARY KEY,
                    location TEXT,
                    price REAL,
                    status TEXT
                )""")
    c.execute("""CREATE TABLE IF NOT EXISTS drivers (
                    id TEXT PRIMARY KEY
                )""")
    c.execute("""CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    cp_id TEXT,
                    driver_id TEXT,
                    status TEXT,
                    energy REAL,
                    eur REAL
                )""")
    conn.commit()
    conn.close()

def save_cp(cp_id, location, price, status="DESCONECTADO"):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("REPLACE INTO charging_points VALUES (?, ?, ?, ?)", (cp_id, location, price, status))
    conn.commit()
    conn.close()

def update_cp_status(cp_id, status):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE charging_points SET status=? WHERE id=?", (status, cp_id))
    conn.commit()
    conn.close()

def get_free_cp():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id FROM charging_points WHERE status='ACTIVADO' LIMIT 1")
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def get_all_cps():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT * FROM charging_points")
    rows = c.fetchall()
    conn.close()
    return rows

def reset_all_cp_status(new_status):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE charging_points SET status = ?", (new_status,))
    conn.commit()
    conn.close()



