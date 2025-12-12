import os
import subprocess
import sys
import platform
import time

BASE = os.path.expanduser("~/Desktop/EVcharging/DiSystems")

PY = sys.executable  # aktuelles Python (z. B. venv)
IS_MAC = platform.system() == "Darwin"
IS_WINDOWS = platform.system() == "Windows"


def open_terminal(command: str):
    """
    Öffnet einen neuen Terminal-Tab oder ein neues Fenster
    und führt den angegebenen Befehl darin aus.
    """
    if IS_MAC:
        # neues Terminal-Fenster öffnen
        script = f'''
        tell application "Terminal"
            do script "{command}"
        end tell
        '''
        subprocess.Popen(["osascript", "-e", script])

    elif IS_WINDOWS:
        subprocess.Popen(f'start cmd /k {command}', shell=True)

    else:
        # Linux
        subprocess.Popen(["x-terminal-emulator", "-e", command])


def run_service(name, module, extra_env=None, args=""):
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    cmd = f'cd {BASE}; source venv/bin/activate; {PY} -m {module} {args}'
    print(f"→ Starte {name} …")
    open_terminal(cmd)
    time.sleep(1)


print("🚀 Starte vollständiges EV-Charging Netzwerk (LOCALHOST)…")

# 1) Registry
run_service("EV_Registry", "EV_Registry.registry_app")

# 2) Central
run_service("EV_Central", "central.EV_Central")

# 3) Weather
run_service("EV_W", "weather.EV_W")

# 4) Frontend
run_service("Frontend Dashboard", "frontend.app")

# 5) CP Engines
run_service(
    "CP001 Engine",
    "cp.EV_CP_E",
    extra_env={"CENTRAL_HOST": "127.0.0.1", "CENTRAL_PORT": "9002"},
    args="CP001 Berlin 0.25 9101"
)

run_service(
    "CP002 Engine",
    "cp.EV_CP_E",
    extra_env={"CENTRAL_HOST": "127.0.0.1", "CENTRAL_PORT": "9002"},
    args="CP002 Madrid 0.30 9102"
)

# 6) CP Monitors
run_service(
    "CP001 Monitor",
    "cp.EV_CP_M",
    args="127.0.0.1 9101 CP001 test123"
)

run_service(
    "CP002 Monitor",
    "cp.EV_CP_M",
    args="127.0.0.1 9102 CP002 test123"
)

print("\n🎉 Alles gestartet!")
print("Dashboard → http://127.0.0.1:8080")
print("Registry → https://127.0.0.1:5001 (curl -k testbar)")
print("Central CLI → siehe Terminal-Fenster")
