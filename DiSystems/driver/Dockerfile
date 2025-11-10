#EV_Driver Dockerfile 
FROM python:3.11-slim

WORKDIR /app
COPY ../common /app/common
COPY EV_Driver.py /app/
COPY ../common/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "EV_Driver.py", "--central-ip", "ev_central", "--central-port", "9002", "--driver-id", "D01", "--auto"]

