# EV Charging Distributed System 

This project implements a distributed EV charging system combining
**TCP sockets**, **Kafka**, **REST APIs** and a **central dashboard**.

The system manages charging sessions, monitors charging point health,
reacts to weather conditions and allows manual intervention via a web interface.

---

## Current Features

- Central server coordinating all charging points
- Charging Point Engine simulating charging sessions
- Charging Point Monitor with heartbeat-based failure detection
- Driver client to start and stop charging sessions
- Web dashboard to monitor system state in real time
- Weather service influencing charging availability
- Kafka-based telemetry, status updates and commands
- SQLite database with persistent sessions and audit logs

---

## Architecture Overview

- **Charging Point ↔ Central** (TCP sockets, encrypted)
- **Monitor → Central** (heartbeat & status via Kafka)
- **Driver ↔ Central** (TCP session control)
- **Weather Service → Central API** (REST)
- **Central ↔ Kafka** (telemetry, status, commands)
- **Dashboard → Central API** (REST)

---

## Security Overview

- Asymmetric authentication between **Registry and Charging Points**
- Symmetric AES encryption between **Central and Charging Points**
- Encrypted Kafka telemetry messages
- All critical actions recorded in the audit log

## Requirements
- Python 3.10+
- Docker & Docker Compose
- Kafka
- SQLite
