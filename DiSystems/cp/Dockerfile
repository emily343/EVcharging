#EV_CP Dockerfile 
FROM python:3.11-slim

WORKDIR /app
COPY ../common /app/common
COPY EV_CP_E.py EV_CP_M.py /app/
COPY ../common/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# container stays active
CMD ["sleep", "infinity"]
