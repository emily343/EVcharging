import subprocess
import time
import os

#to try out project
PROJECT_ROOT = os.path.expanduser("~/Desktop/EVCharging/DiSystems")
VENV_ACTIVATE = "source venv/bin/activate"

def open_new_terminal_mac(module, args="", title="EV"):
   
    cmd = f'cd {PROJECT_ROOT}; {VENV_ACTIVATE}; python3 -m {module} {args}'
    
    apple_script = f'''
    tell application "Terminal"
        do script "{cmd}"
        activate
    end tell
    '''
    subprocess.Popen(["osascript", "-e", apple_script])


# start central
print("Starting CENTRAL...")
open_new_terminal_mac("central.EV_Central", "", "CENTRAL")
time.sleep(4)

# start CP and Monitor 
charging_points = [
    ("CP001", "Berlin", 0.25, 9101),
    ("CP002", "Madrid", 0.30, 9102),
    ("CP003", "Paris", 0.27, 9103),
    ("CP004", "Rome", 0.29, 9104),
    ("CP005", "Lisbon", 0.31, 9105),
]

for cp_id, city, price, port in charging_points:
    print(f" Starting {cp_id} in {city}...")
    open_new_terminal_mac("cp.EV_CP_E", f"{cp_id} {city} {price} {port}", title=cp_id)
    time.sleep(1)

    print(f"Starting monitor for {cp_id}")
    open_new_terminal_mac("cp.EV_CP_M", f"127.0.0.1 {port} {cp_id}", title=f"MON_{cp_id}")
    time.sleep(1)

# start DRIVER 
drivers = ["D01", "D02", "D03", "D04", "D05"]

for d in drivers:
    print(f" Starting driver {d}...")
    open_new_terminal_mac("driver.EV_Driver", f"--central-ip 127.0.0.1 --central-port 9002 --driver-id {d}", title=f"DRV_{d}")
    time.sleep(1)

print("\n All components launched!")
print(" Type 'start'/'stop' in driver windows")
print(" Auto mode: add --auto to EV_Driver")
print("Dashboard in CENTRAL terminal")
