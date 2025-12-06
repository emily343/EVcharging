import sqlite3
import os
from datetime import datetime

DB_FILE = os.path.join(os.path.dirname(__file__), "central.db")


def get_db_connection():
    """Return a new DB connection (needed by Central and other modules)."""
    conn = sqlite3.connect(DB_FILE)
    return conn


def init_db():
 
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Charging points table 
    c.execute("""
        CREATE TABLE IF NOT EXISTS charging_points (
            id TEXT PRIMARY KEY,
            location TEXT,
            price REAL,
            status TEXT
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

    # NEU: Tabelle für registrierte CPs (von EV_Registry verwaltet)
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

    # NEU: Tabelle für symmetrische Schlüssel pro CP (von EV_Central verwaltet)
    c.execute("""
        CREATE TABLE IF NOT EXISTS cp_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cp_id TEXT UNIQUE NOT NULL,
            sym_key TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # audit-log table 
    c.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            action TEXT,
            details TEXT
        )
    """)

    conn.commit()
    conn.close()




def save_cp(cp_id, location, price, status="DESCONECTADO"):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "REPLACE INTO charging_points VALUES (?, ?, ?, ?)",
        (cp_id, location, price, status)
    )
    conn.commit()
    conn.close()


def update_cp_status(cp_id, status):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE charging_points SET status=? WHERE id=?",
        (status, cp_id)
    )
    conn.commit()
    conn.close()


def get_free_cp():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT id FROM charging_points WHERE status='ACTIVADO' LIMIT 1"
    )
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
    c.execute(
        "UPDATE charging_points SET status = ?",
        (new_status,)
    )
    conn.commit()
    conn.close()


# =========================
# NEUE Hilfsfunktionen für Release 2 (Registry, Keys, Audit)
# =========================

def registry_save_cp(cp_id: str, location: str, credential_hash: str):
    """
    Speichert einen registrierten CP in der Tabelle registry_cp.
    Wird vom EV_Registry-Modul benutzt.
    """
    conn = sqlite3.connect(DB_FILE)
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
    """
    Setzt active = 0 für einen CP.
    Gibt True zurück, wenn ein Datensatz betroffen war, sonst False.
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "UPDATE registry_cp SET active = 0 WHERE cp_id = ?",
        (cp_id,)
    )
    changed = (c.rowcount > 0)
    conn.commit()
    conn.close()
    return changed


def registry_get_cp(cp_id: str):
    """
    Holt einen registrierten CP (für Debug/Checks).
    Gibt ein Tupel (cp_id, location, active) oder None zurück.
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT cp_id, location, active FROM registry_cp WHERE cp_id = ?",
        (cp_id,)
    )
    row = c.fetchone()
    conn.close()
    return row  # None oder (cp_id, location, active)


def store_cp_key(cp_id: str, sym_key: str):
    """
    Speichert oder aktualisiert den symmetrischen Schlüssel zu einem CP.
    Wird von EV_Central nach erfolgreicher Authentifizierung benutzt.
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # REPLACE: überschreibt vorhandenen Eintrag für cp_id
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
    """
    Gibt den gespeicherten symmetrischen Schlüssel eines CP zurück oder None.
    """
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT sym_key FROM cp_keys WHERE cp_id = ?",
        (cp_id,)
    )
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def log_audit(source: str, action: str, details: str):
    """
    Fügt einen Eintrag ins Audit-Log ein.
    Wird von Central, Registry, EV_W etc. benutzt.
    """
    conn = sqlite3.connect(DB_FILE)
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
