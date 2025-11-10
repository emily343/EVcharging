# EV Charging Distributed System (Sockets + Kafka)

This project simulates a real electric vehicle charging network with:

- Central Control Server
- Charging Point Engine
- Charging Point Monitor (health module)
- Driver App
- Kafka event streaming (telemetry and alerts)
- SQLite database


## Architecture

- CP <-> Central 
- CP Monitor -> CP Engine 
- CP & Monitor -> Kafka (telemetry + alerts)
- Driver <-> Central socket authorization & session control

## How to run: 

### Installation

cd DiSystems
python -m venv venv
source venv/bin/activate    (Windows: venv\Scripts\activate)
pip install -r common/requirements.txt


### 1. Start Kafka

docker compose up -d

### 2. Start Central

cd DiSystems
source venv/bin/activate
python -m central.EV_Central 9002

### 3. Start Charging Points & Monitors (new Terminal)

cd DiSystems
venv\Scripts\activate
python -m cp.EV_CP_E CP001 Berlin 0.25 9101 --central-ip <CENTRAL-IP>
python -m cp.EV_CP_M CP001 9101 --central-ip <CENTRAL-IP>

### 4. Start Driver (new Terminal)

python -m driver.EV_Driver --central-ip <CENTRAL-IP> --central-port 9002 --driver-id D01

#### driver commands: 
- start
- stop
