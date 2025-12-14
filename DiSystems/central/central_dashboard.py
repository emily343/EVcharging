import time
import os
import sqlite3
from colorama import Fore, Style, init as colorama_init
from central.central_db import get_db_connection


# old file from release 1, we don't use it anymore
colorama_init()

DB_FILE = os.path.join(os.path.dirname(__file__), "central.db")

def clear():
    os.system("cls" if os.name == "nt" else "clear")

def load_cps():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM charging_points")
    rows = c.fetchall()
    conn.close()
    return rows

state_color = {
    "ACTIVADO": Fore.GREEN,
    "SUMINISTRANDO": Fore.YELLOW,
    "AVERIADO": Fore.RED,
    "PARADO": Fore.MAGENTA,
    "DESCONECTADO": Fore.LIGHTBLACK_EX
}

def dashboard_loop():
    while True:
        cps = load_cps()

        clear()
        print(f"{Fore.CYAN}=== EV CHARGING NETWORK DASHBOARD ==={Style.RESET_ALL}")

        header = f"{'CP':8} | {'Location':10} | {'€/kWh':>6} | {'State':13}"
        print(header)
        print("-" * len(header))

        for cp_id, location, price, status in cps:
            col = state_color.get(status, Fore.WHITE)
            line = f"{cp_id:8} | {location:10} | {price:6.2f} | {status:13}"
            print(col + line + Style.RESET_ALL)

        time.sleep(1)

if __name__ == "__main__":
    dashboard_loop()
