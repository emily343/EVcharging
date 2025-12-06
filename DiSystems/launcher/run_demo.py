import subprocess
import time
import os

# ---- PROJECT PATHS ----
PROJECT_ROOT = os.path.expanduser("~/Desktop/EVCharging/DiSystems")
VENV_ACTIVATE = "source venv/bin/activate"

def open_new_terminal_mac(command, title="EV"):
    """
    Opens a new macOS Terminal window and runs the given shell command.
    """
    apple_script = f'''
    tell application "Terminal"
        do script "{command}"
        activate
    end tell
    '''
    subprocess.Popen(["osascript", "-e", apple_script])


def run_module(module, args="", title="EV"):
    """
    Starts a python module in a new Terminal with venv active.
    """
    cmd = f'cd {PROJECT_ROOT}; {VENV_ACTIVATE}; python3 -m {module} {args}'
    open_new_terminal_mac(cmd, title)


# -----------------------------------------------
#               STARTUP SEQUENCE
# -----------------------------------------------

print("=== Starting EVCharging Demo ===")

# 1) CENTRAL
print("Starting CENTRAL...")
run_module("central.EV_Central", "", "CENTRAL")
time.sleep(3)

# 2) DASHBOARD
print("Starting Dashboard...")
run_module("central.central_dashboard", "", "DASHBOARD")
time.sleep(2)

# 3) CHARGING POINTS
charging_points = [
    ("CP001", "Berlin", 0.25, 9101),
    ("CP002", "Madrid", 0.30, 9102),
    ("CP003", "Paris", 0.27, 9103),
    ("CP004", "Rome", 0.29, 9104),
    ("CP005", "Lisbon", 0.31, 9105),
]

for cp_id, city, price, port in charging_points:
    print(f"\nStarting engine for {cp_id} ({city})...")
    run_module("cp.EV_CP_E", f"{cp_id} {city} {price} {port}", title=f"E_{cp_id}")
    time.sleep(1)

    print(f"Starting monitor for {cp_id}...")
    run_module("cp.EV_CP_M", f"127.0.0.1 {port} {cp_id}", title=f"M_{cp_id}")
    time.sleep(1)

# 4) DRIVERS
drivers = ["D01", "D02", "D03"]

for d in drivers:
    print(f"\nStarting driver {d}...")
    run_module(
        "driver.EV_Driver",
        f"--central-ip 127.0.0.1 --central-port 9002 --driver-id {d}",
        title=f"D_{d}"
    )
    time.sleep(1)

print("\n=== All components launched successfully! ===")
print("Drivers listen for commands: 'start' / 'stop'")
print("Dashboard updates automatically")
print("Central CLI supports: stop, resume, out, activate, stop_all, resume_all")
